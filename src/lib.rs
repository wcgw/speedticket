//! A sharded, finite-budget semaphore.
//!
//! A fixed number of permits is partitioned across participants so that the
//! common case — claiming and releasing against your own shard — needs no
//! cross-thread coordination. There is no notion of time and no refill.
//!
//! Create a [`Limit`] with a total budget, then call [`Limit::participant`]
//! once per thread; each registration carves out a share of the budget. A
//! participant that exhausts its share steals idle permits from its peers.
//!
//! ```
//! use speedticket::Limit;
//!
//! let limit = Limit::new(12);
//! std::thread::scope(|s| {
//!     for _ in 0..3 {
//!         let me = limit.participant().expect("capacity");
//!         s.spawn(move || {
//!             if let Some(_permit) = me.try_claim() {
//!                 // do bounded work; the permit is released on drop
//!             }
//!         });
//!     }
//! });
//! ```
//!
//! A [`Permit`] must be released on the participant that claimed it; trying
//! to move it to another thread does not compile:
//!
//! ```compile_fail
//! let limit = speedticket::Limit::new(1);
//! let permit = limit.try_claim().unwrap();
//! std::thread::scope(|s| {
//!     s.spawn(move || drop(permit));
//! });
//! ```
//!
//! Likewise a single participant cannot be shared across threads:
//!
//! ```compile_fail
//! let limit = std::sync::Arc::new(speedticket::Limit::new(1));
//! let shared = std::sync::Arc::clone(&limit);
//! std::thread::spawn(move || shared.try_claim().is_some());
//! ```
//!
//! On a multi-threaded async runtime, whose tasks migrate between threads
//! while holding a permit, use a [`Pool`] instead. With the `async` feature,
//! `Pool::claim` waits for a permit rather than failing.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod pool;
mod shard;
mod sync;
#[cfg(feature = "async")]
mod wait;

pub use pool::{OwnedPermit, Pool, PoolPermit};

use std::cell::Cell;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::PoisonError;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::{error, fmt, ptr, thread};

use shard::Shard;
use sync::{Arc, AtomicUsize, Mutex, MutexGuard};

/// Lower bound on the default participant capacity of [`Limit::new`].
const MIN_DEFAULT_CAPACITY: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// A participant's handle on a shared, finite budget of permits.
///
/// `Limit` is [`Send`] but not [`Sync`]: move each participant to the thread
/// that uses it, and call [`Limit::participant`] to register more. Sharing a
/// single participant across threads (and contending its shard) is ruled out
/// at compile time.
///
/// There is deliberately no `Clone`: registering a participant allocates a
/// slot and carves a share of the budget, so it is the explicit, fallible
/// [`Limit::participant`].
///
/// Dropping a participant redistributes its idle permits to its peers. When
/// the last participant is dropped, the pool is gone.
pub struct Limit {
    shared: Arc<Shared>,
    slot: usize,
    /// Makes `Limit` `!Sync` while keeping it `Send`.
    _not_sync: PhantomData<Cell<()>>,
}

/// State common to every participant of one pool.
struct Shared {
    total: u64,
    shards: Box<[Shard]>,
    /// One past the highest slot ever occupied; bounds the steal scan. Only
    /// grows, and is only written under the `registry` lock.
    high_water: AtomicUsize,
    /// Free slots. Only written under the `registry` lock; read without it
    /// by threads of a [`Pool`] that found it at capacity, to know when
    /// joining is worth a try again.
    vacancies: AtomicUsize,
    /// The join-lock: guards slot occupancy on the cold registration and
    /// deregistration paths. Claims and releases never take it.
    registry: Mutex<Registry>,
    first_slot: FirstSlot,
    /// Claims waiting for permits; every deposit notifies it.
    #[cfg(feature = "async")]
    waiters: wait::WaitQueue,
}

/// The slot that holds the whole budget at creation.
const FIRST_SLOT: usize = 0;

/// What the [`FIRST_SLOT`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirstSlot {
    /// The first [`Limit`]: a participant like any other.
    Participant,
    /// A [`Pool`]'s home shard, used by threads without a participant of
    /// their own. It takes no share: newcomers drain it first, and leavers
    /// refill it only when no participant remains.
    Home,
}

/// Which slots are occupied by a live participant.
struct Registry {
    occupied: Box<[bool]>,
}

/// A claimed permit, returned to its participant's shard on drop.
///
/// A `Permit` borrows the [`Limit`] it was claimed from and is not [`Send`],
/// so it is always released on the same participant.
///
/// Leaking a permit (e.g. via [`std::mem::forget`]) permanently shrinks the
/// pool's effective budget.
#[must_use = "dropping a Permit releases it immediately"]
#[derive(Debug)]
pub struct Permit<'a> {
    limit: &'a Limit,
}

/// Returned by [`Limit::participant`] when every participant slot is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AtCapacity;

impl fmt::Display for AtCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("limit is at its maximum number of participants")
    }
}

impl error::Error for AtCapacity {}

impl Limit {
    /// Creates a pool of `total` permits and returns its first participant,
    /// which holds all of them.
    ///
    /// The pool accepts up to `max(available_parallelism(), 16)`
    /// participants; use [`Limit::with_capacity`] to choose explicitly.
    ///
    /// ```
    /// let limit = speedticket::Limit::new(1);
    /// let permit = limit.try_claim().expect("one permit");
    /// assert!(limit.try_claim().is_none());
    /// drop(permit);
    /// assert!(limit.try_claim().is_some());
    /// ```
    pub fn new(total: u64) -> Self {
        let parallelism = thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        Self::with_capacity(total, parallelism.max(MIN_DEFAULT_CAPACITY))
    }

    /// Creates a pool of `total` permits accepting at most `max_participants`
    /// participants, and returns its first participant, which holds all of
    /// them.
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// let two = NonZeroUsize::new(2).expect("non-zero");
    /// let limit = speedticket::Limit::with_capacity(10, two);
    /// let _second = limit.participant().expect("room for two");
    /// assert!(limit.participant().is_err());
    /// ```
    pub fn with_capacity(total: u64, max_participants: NonZeroUsize) -> Self {
        let shared = Shared::new(total, max_participants.get(), FirstSlot::Participant);
        Self::from_parts(shared, FIRST_SLOT)
    }

    fn from_parts(shared: Arc<Shared>, slot: usize) -> Self {
        Self {
            shared,
            slot,
            _not_sync: PhantomData,
        }
    }

    /// Registers a new participant in this pool and returns its handle.
    ///
    /// The newcomer aims for an even share, `total / N` for `N` participants,
    /// by taking *idle* permits from peers that hold more than that. Only
    /// idle permits move, so if peers are busy the newcomer starts with
    /// whatever it could gather — possibly none.
    ///
    /// This is a cold path and takes the pool's join-lock.
    ///
    /// # Errors
    ///
    /// [`AtCapacity`] when the pool already has its maximum number of
    /// participants.
    ///
    /// ```
    /// let first = speedticket::Limit::new(12); // holds 12
    /// let second = first.participant()?;      // 6 and 6
    /// let third = first.participant()?;       // 4, 4 and 4
    /// # Ok::<(), speedticket::AtCapacity>(())
    /// ```
    pub fn participant(&self) -> Result<Limit, AtCapacity> {
        let slot = self.shared.join()?;
        Ok(Self::from_parts(Arc::clone(&self.shared), slot))
    }

    /// Claims one permit without blocking.
    ///
    /// Takes from this participant's own shard when it can. Otherwise it
    /// makes one bounded pass over its peers, stealing half the idle permits
    /// of the one with the most; if that pass finds nothing it returns `None`
    /// rather than wait.
    ///
    /// The number of simultaneously claimed permits never exceeds the pool's
    /// total. A claim may, rarely, return `None` while a permit is in flight
    /// between shards; retrying resolves it.
    ///
    /// ```
    /// let first = speedticket::Limit::new(2);
    /// let second = first.participant()?; // 1 each
    /// let a = first.try_claim().expect("own shard");
    /// let b = first.try_claim().expect("stolen from `second`");
    /// assert!(second.try_claim().is_none());
    /// # Ok::<(), speedticket::AtCapacity>(())
    /// ```
    #[inline]
    pub fn try_claim(&self) -> Option<Permit<'_>> {
        // Build the `Permit` only on success: dropping one releases a permit.
        self.shared.claim(self.slot).then(|| Permit { limit: self })
    }

    fn shard(&self) -> &Shard {
        &self.shared.shards[self.slot]
    }
}

impl fmt::Debug for Limit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Limit")
            .field("total", &self.shared.total)
            .field("slot", &self.slot)
            .field("idle", &self.shard().idle())
            .finish()
    }
}

impl Drop for Limit {
    /// Deregisters this participant and spreads its idle permits evenly over
    /// the remaining peers.
    fn drop(&mut self) {
        self.shared.leave(self.slot);
    }
}

impl Shared {
    /// A pool of `capacity` slots whose [`FIRST_SLOT`] is occupied and holds
    /// all `total` permits.
    fn new(total: u64, capacity: usize, first_slot: FirstSlot) -> Arc<Self> {
        let shards = (0..capacity)
            .map(|slot| Shard::new(if slot == FIRST_SLOT { total } else { 0 }))
            .collect();
        let mut occupied = vec![false; capacity].into_boxed_slice();
        occupied[FIRST_SLOT] = true;
        Arc::new(Self {
            total,
            shards,
            high_water: AtomicUsize::new(FIRST_SLOT + 1),
            vacancies: AtomicUsize::new(capacity - 1),
            registry: Mutex::new(Registry { occupied }),
            first_slot,
            #[cfg(feature = "async")]
            waiters: wait::WaitQueue::new(),
        })
    }

    /// The occupied slots that take a share of the budget: every live one,
    /// bar a home shard.
    fn participants<'r>(&self, registry: &'r Registry) -> impl Iterator<Item = usize> + 'r {
        let home = (self.first_slot == FirstSlot::Home).then_some(FIRST_SLOT);
        registry
            .live_slots()
            .filter(move |&slot| Some(slot) != home)
    }

    /// Whether a slot was free when last looked at, without taking the lock.
    fn has_vacancy(&self) -> bool {
        self.vacancies.load(Relaxed) > 0
    }

    /// Occupies a free slot and carves it an even share of the budget, out
    /// of peers' idle surplus. See [`Limit::participant`].
    fn join(&self) -> Result<usize, AtCapacity> {
        let mut registry = self.registry();
        let slot = registry.free_slot().ok_or(AtCapacity)?;
        let participants = self.participants(&registry).count() + 1;
        let target = self.total / participants as u64;

        let mut need = target;
        if self.first_slot == FirstSlot::Home {
            // A home shard keeps no share: drain it before touching peers.
            need -= self.shards[FIRST_SLOT].take(|_| need);
        }
        for peer in self.participants(&registry) {
            if need == 0 {
                break;
            }
            let taken = self.shards[peer].take(|idle| idle.saturating_sub(target).min(need));
            need -= taken;
        }
        // Raised before the deposit, and `SeqCst` like it, so a waiter that
        // sees the deposit also scans this slot (see `wait.rs`).
        self.high_water.fetch_max(slot + 1, SeqCst);
        self.give(slot, target - need);

        registry.occupied[slot] = true;
        self.vacancies.fetch_sub(1, Relaxed);
        Ok(slot)
    }

    /// Vacates `slot` and spreads its idle permits evenly over the remaining
    /// participants, or hands them to a home shard if none remain.
    fn leave(&self, slot: usize) {
        let mut registry = self.registry();
        registry.occupied[slot] = false;
        self.vacancies.fetch_add(1, Relaxed);
        // Under the join-lock nobody can occupy this slot yet, and nothing but
        // the (now gone) owner deposits into it, so it stays empty for reuse.
        let idle = self.shards[slot].drain();
        let peers = self.participants(&registry).count() as u64;
        if peers == 0 {
            if self.first_slot == FirstSlot::Home {
                self.give(FIRST_SLOT, idle);
            }
            // Otherwise this was the last participant: the pool, and its
            // permits, are gone.
            return;
        }
        let (share, remainder) = (idle / peers, idle % peers);
        for (i, peer) in (0u64..).zip(self.participants(&registry)) {
            self.give(peer, share + u64::from(i < remainder));
        }
    }

    /// Deposits `n` permits into `slot`'s shard, waking any claims waiting
    /// for them. Every deposit goes through here.
    #[inline]
    fn give(&self, slot: usize, n: u64) {
        self.shards[slot].give(n);
        #[cfg(feature = "async")]
        self.waiters.notify(n);
    }

    /// Claims one permit for `slot`: from its own shard, else by stealing.
    #[inline]
    fn claim(&self, slot: usize) -> bool {
        self.shards[slot].take(|_| 1) == 1 || self.steal(slot)
    }

    /// Steals a batch from the peer with the most idle permits, keeping one
    /// as the claimed permit and banking the rest in `slot`. If a racing claim
    /// empties that peer first, steals from the first other peer with any.
    /// Takes from each peer at most once, starting just after `slot`.
    #[cold]
    #[inline(never)]
    fn steal(&self, slot: usize) -> bool {
        let scanned = &self.shards[..self.high_water.load(Relaxed)];
        // Peers after this slot, then wrapping round to those before it.
        let peers = scanned.iter().skip(slot + 1).chain(&scanned[..slot]);
        // The first of the richest peers in that order: its batch is the
        // biggest, and its cache line was read just now.
        let (richest, _) = peers.clone().fold((None, 0), |(richest, most), peer| {
            let idle = peer.idle();
            if idle > most {
                (Some(peer), idle)
            } else {
                (richest, most)
            }
        });
        let Some(richest) = richest else {
            return false;
        };
        let half = |idle: u64| (idle / 2).max(1);
        let stolen = Some(richest.take(half))
            .filter(|&stolen| stolen > 0)
            .or_else(|| {
                peers
                    .filter(|&peer| !ptr::eq(peer, richest))
                    .map(|peer| peer.take(half))
                    .find(|&stolen| stolen > 0)
            });
        match stolen {
            Some(stolen) => {
                self.give(slot, stolen - 1);
                true
            }
            None => false,
        }
    }

    fn registry(&self) -> MutexGuard<'_, Registry> {
        // The critical sections only flip flags and move counts between
        // atomics; a panic in one cannot leave the registry inconsistent.
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Registry {
    fn free_slot(&self) -> Option<usize> {
        self.occupied.iter().position(|&occupied| !occupied)
    }

    fn live_slots(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.occupied.len()).filter(|&slot| self.occupied[slot])
    }
}

impl Drop for Permit<'_> {
    #[inline]
    fn drop(&mut self) {
        self.limit.shared.give(self.limit.slot, 1);
    }
}

// Loom builds are exercised by `tests/loom.rs`; these use real threads.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    fn cap(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test capacity is non-zero")
    }

    fn idle(limit: &Limit) -> u64 {
        limit.shard().idle()
    }

    fn pool_idle(limit: &Limit) -> u64 {
        limit.shared.shards.iter().map(Shard::idle).sum()
    }

    #[test]
    fn limit_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Limit>();
    }

    #[test]
    fn first_participant_holds_everything() {
        let limit = Limit::new(3);
        let permits: Vec<_> = (0..3).map_while(|_| limit.try_claim()).collect();
        assert_eq!(permits.len(), 3);
        assert!(limit.try_claim().is_none());
        drop(permits);
        assert_eq!(idle(&limit), 3);
    }

    #[test]
    fn empty_pool_never_admits() {
        let limit = Limit::new(0);
        let other = limit.participant().unwrap();
        assert!(limit.try_claim().is_none());
        assert!(other.try_claim().is_none());
    }

    #[test]
    fn participants_carve_even_shares() {
        let a = Limit::new(12);
        let b = a.participant().unwrap();
        assert_eq!((idle(&a), idle(&b)), (6, 6));
        let c = b.participant().unwrap();
        assert_eq!((idle(&a), idle(&b), idle(&c)), (4, 4, 4));
    }

    #[test]
    fn uneven_total_keeps_remainder_with_peers() {
        let a = Limit::new(13);
        let b = a.participant().unwrap();
        assert_eq!((idle(&a), idle(&b)), (7, 6));
        assert_eq!(pool_idle(&a), 13);
    }

    #[test]
    fn carving_takes_only_idle_permits() {
        let a = Limit::new(12);
        let b = a.participant().unwrap();
        let _busy: Vec<_> = (0..5).map_while(|_| a.try_claim()).collect();
        // a: 1 idle, b: 6 idle; target is 4, only b has surplus (2).
        let c = a.participant().unwrap();
        assert_eq!((idle(&a), idle(&b), idle(&c)), (1, 4, 2));
    }

    #[test]
    fn participant_fails_at_capacity() {
        let a = Limit::with_capacity(4, cap(2));
        let _b = a.participant().unwrap();
        assert_eq!(a.participant().unwrap_err(), AtCapacity);
    }

    #[test]
    fn exhausted_participant_steals_half() {
        let a = Limit::new(12);
        let b = a.participant().unwrap();
        let mut held: Vec<_> = (0..6).map_while(|_| a.try_claim()).collect();
        assert_eq!(idle(&a), 0);
        held.push(a.try_claim().expect("steal from b"));
        // Stole 3 from b: one claimed, two banked locally.
        assert_eq!((idle(&a), idle(&b)), (2, 3));
        drop(held);
        assert_eq!((idle(&a), idle(&b)), (9, 3));
    }

    #[test]
    fn steal_takes_last_idle_permit() {
        let a = Limit::new(2);
        let b = a.participant().unwrap();
        let _mine = a.try_claim().unwrap();
        let _stolen = a.try_claim().expect("steal b's only permit");
        assert_eq!(idle(&b), 0);
        assert!(a.try_claim().is_none());
        assert!(b.try_claim().is_none());
    }

    #[test]
    fn steal_prefers_the_richest_peer() {
        let a = Limit::with_capacity(12, cap(3));
        let b = a.participant().unwrap();
        let c = a.participant().unwrap();
        let _b_held: Vec<_> = (0..3).map_while(|_| b.try_claim()).collect();
        let mut held: Vec<_> = (0..4).map_while(|_| a.try_claim()).collect();
        assert_eq!((idle(&a), idle(&b), idle(&c)), (0, 1, 4));
        held.push(a.try_claim().expect("steal from c"));
        // a passes over b's single permit for half of c's four: one claimed,
        // one banked locally.
        assert_eq!((idle(&a), idle(&b), idle(&c)), (1, 1, 2));
    }

    #[test]
    fn steal_skips_empty_peers() {
        let a = Limit::with_capacity(4, cap(3));
        let b = a.participant().unwrap();
        let c = a.participant().unwrap();
        let _b_held: Vec<_> = (0..2).map_while(|_| b.try_claim()).collect();
        let _a_held: Vec<_> = (0..1).map_while(|_| a.try_claim()).collect();
        // a skips the empty b and steals c's last permit.
        assert_eq!((idle(&a), idle(&b), idle(&c)), (0, 0, 1));
        let _p = a.try_claim().expect("steal from c");
        assert_eq!((idle(&a), idle(&c)), (0, 0));
    }

    #[test]
    fn drop_redistributes_idle_permits() {
        let a = Limit::new(12);
        let b = a.participant().unwrap();
        let c = a.participant().unwrap();
        drop(c);
        assert_eq!((idle(&a), idle(&b)), (6, 6));
        drop(b);
        assert_eq!(idle(&a), 12);
    }

    #[test]
    fn drop_spreads_remainder() {
        let a = Limit::with_capacity(5, cap(3));
        let b = a.participant().unwrap();
        let c = a.participant().unwrap();
        // b carved 2 from a, then c carved 1 from a (first peer with surplus).
        assert_eq!((idle(&a), idle(&b), idle(&c)), (2, 2, 1));
        drop(a);
        assert_eq!((idle(&b), idle(&c)), (3, 2));
        assert_eq!(pool_idle(&b), 5);
    }

    #[test]
    fn freed_slot_is_reused_empty() {
        let a = Limit::with_capacity(8, cap(2));
        let b = a.participant().unwrap();
        let slot = b.slot;
        drop(b);
        assert_eq!(idle(&a), 8);
        let b = a.participant().unwrap();
        assert_eq!(b.slot, slot);
        assert_eq!((idle(&a), idle(&b)), (4, 4));
    }

    #[test]
    fn leaked_permit_shrinks_budget() {
        let a = Limit::new(2);
        std::mem::forget(a.try_claim().unwrap());
        let b = a.participant().unwrap();
        assert_eq!(pool_idle(&b), 1);
    }

    #[test]
    fn concurrent_claims_never_over_admit() {
        const TOTAL: u64 = 8;
        const THREADS: usize = 8;
        const ROUNDS: usize = 20_000;

        let first = Limit::new(TOTAL);
        let in_use = AtomicU64::new(0);
        let peak = AtomicU64::new(0);
        let participants: Vec<_> = (1..THREADS).map(|_| first.participant().unwrap()).collect();

        thread::scope(|s| {
            for me in participants {
                let (in_use, peak) = (&in_use, &peak);
                s.spawn(move || {
                    for _ in 0..ROUNDS {
                        let Some(permit) = me.try_claim() else {
                            thread::yield_now();
                            continue;
                        };
                        let now = in_use.fetch_add(1, Relaxed) + 1;
                        peak.fetch_max(now, Relaxed);
                        in_use.fetch_sub(1, Relaxed);
                        drop(permit);
                    }
                });
            }
        });

        assert!(peak.load(Relaxed) <= TOTAL);
        // Every participant but `first` has dropped and handed back its idle
        // permits; nothing was claimed at the time.
        assert_eq!(idle(&first), TOTAL);
    }
}
