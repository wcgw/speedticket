//! A [`Sync`] front end that binds participants to OS threads, for runtimes
//! (e.g. tokio's multi-threaded scheduler) whose tasks migrate between
//! threads while holding a permit.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::num::NonZeroUsize;
// Plain `std` even under loom: these only track handles and whether a `Pool`
// is still alive, they never guard permits.
use std::sync::{Arc as StdArc, Weak};
use std::thread;

use crate::sync::{Arc, thread_local};
use crate::{FIRST_SLOT, FirstSlot, Limit, MIN_DEFAULT_CAPACITY, Shared};

/// The pool's home shard: threads without a participant of their own claim
/// from and release into it.
const HOME: usize = FIRST_SLOT;

// A `const` initialiser spares every access a lazy-init check; loom's
// `thread_local!` has no `const` form.
#[cfg(not(loom))]
thread_local! {
    static LOCAL: Local = const { Local::new() };
}
#[cfg(loom)]
thread_local! {
    static LOCAL: Local = Local::new();
}

/// This thread's state across every [`Pool`] it has claimed from. One
/// thread-local rather than two, so the pieces are torn down in a fixed
/// order when the thread exits.
struct Local {
    /// The binding this thread used last, if it has a participant: claims
    /// and releases on that pool skip `bindings`.
    recent: Cell<Recent>,
    /// That binding's handle, kept apart so that only owned claims pay to
    /// move it in and out of its cell.
    recent_handle: Cell<Option<StdArc<Handle>>>,
    /// One binding per pool.
    bindings: RefCell<Vec<Binding>>,
}

/// A shared, finite budget of permits that any thread may claim from.
///
/// `Pool` is [`Sync`]: share it by reference, in a `static`, or behind an
/// [`Arc`](StdArc). Each thread that claims is lazily registered as a
/// participant with its own shard, so claims and releases stay on that
/// thread's cache line, as with [`Limit`]. Unlike a [`Limit`]'s permits, a
/// pool's permits are [`Send`] and may be released on any thread: a permit
/// goes back to the shard of the thread that drops it.
///
/// A thread's participant is deregistered, handing its idle permits to its
/// peers, when the thread exits or calls [`Pool::leave_current_thread`].
/// Threads that find the pool at capacity, and permits dropped on threads
/// that never claimed, use the pool's home shard instead, which they all
/// contend on; a thread on the home shard registers once a slot frees up.
///
/// Handing back on exit happens in a thread-local destructor. Joining a
/// thread waits for those, but leaving a [`thread::scope`] does not, so call
/// [`Pool::leave_current_thread`] where the share must be back by then.
///
/// ```
/// use std::thread;
///
/// let pool = speedticket::Pool::new(2);
/// let permit = pool.try_claim_owned().expect("a permit");
/// // Released on another thread.
/// thread::spawn(move || drop(permit)).join().unwrap();
/// assert!(pool.try_claim().is_some());
/// ```
pub struct Pool {
    shared: Arc<Shared>,
    /// Identifies this pool in each thread's bindings, and, through their
    /// weak references, tells them when it is gone.
    token: StdArc<()>,
}

/// A permit claimed with [`Pool::try_claim`], released on drop into the shard
/// of whichever thread drops it.
#[must_use = "dropping a PoolPermit releases it immediately"]
#[derive(Debug)]
pub struct PoolPermit<'a> {
    pool: &'a Pool,
}

/// A `'static` permit claimed with [`Pool::try_claim_owned`], released on drop
/// into the shard of whichever thread drops it. It can be held across an
/// `.await` in a spawned task.
///
/// It keeps the pool's shards alive, but not the [`Pool`] itself: dropping
/// the pool while owned permits are out is fine.
#[must_use = "dropping an OwnedPermit releases it immediately"]
pub struct OwnedPermit {
    handle: StdArc<Handle>,
}

/// One thread's reference-counted handle on a pool's shards. Owned permits
/// clone the claiming thread's handle rather than one pool-wide `Arc`, so
/// their reference counting stays on a cache line that thread owns; only a
/// permit dropped on another thread touches it from elsewhere.
struct Handle {
    shared: Arc<Shared>,
    /// The pool's token, identifying it without keeping it alive.
    pool: Weak<()>,
}

impl Local {
    const fn new() -> Self {
        Self {
            recent: Cell::new(Recent::NONE),
            recent_handle: Cell::new(None),
            bindings: RefCell::new(Vec::new()),
        }
    }

    /// This thread's slot in the pool identified by `token`, if cached.
    fn recent_slot(&self, token: *const ()) -> Option<usize> {
        let recent = self.recent.get();
        (recent.pool == token).then_some(recent.slot)
    }

    /// As [`Local::recent_slot`], with a clone of the binding's handle.
    fn recent_handle(&self, token: *const ()) -> Option<(usize, StdArc<Handle>)> {
        let slot = self.recent_slot(token)?;
        let handle = self.recent_handle.take()?;
        // Clone needed: the caller's permit holds this thread's handle.
        let clone = StdArc::clone(&handle);
        self.recent_handle.set(Some(handle));
        Some((slot, clone))
    }

    /// Caches `binding` if it has a participant, replacing what was cached.
    fn remember(&self, binding: &Binding) {
        if let Some(limit) = &binding.limit {
            self.recent.set(Recent {
                pool: binding.handle.pool.as_ptr(),
                slot: limit.slot,
            });
            // Clone needed: the cache holds this thread's handle too.
            self.recent_handle.set(Some(StdArc::clone(&binding.handle)));
        }
    }

    /// Empties the cache if it holds the binding for `token`.
    fn forget(&self, token: *const ()) {
        if self.recent.get().pool == token {
            self.recent.set(Recent::NONE);
            self.recent_handle.set(None);
        }
    }
}

/// Which slot the [`Binding`] a thread used last gives it. Only bindings
/// with a participant are cached, and a binding clears the cache as it
/// drops, so a hit never names a slot the thread has left.
#[derive(Clone, Copy)]
struct Recent {
    /// The pool's token. The binding's weak reference keeps the token's
    /// allocation, and so this address, from being reused while cached.
    pool: *const (),
    slot: usize,
}

impl Recent {
    /// Nothing cached: no pool's token is null.
    const NONE: Self = Self {
        pool: std::ptr::null(),
        slot: HOME,
    };
}

/// One thread's registration with one pool.
struct Binding {
    handle: StdArc<Handle>,
    /// `None` while the thread has no participant: the pool was at capacity
    /// when it last tried to join, so it uses [`HOME`].
    limit: Option<Limit>,
}

impl Pool {
    /// Creates a pool of `total` permits accepting up to
    /// `max(available_parallelism(), 16)` participating threads.
    pub fn new(total: u64) -> Self {
        let parallelism = thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        Self::with_capacity(total, parallelism.max(MIN_DEFAULT_CAPACITY))
    }

    /// Creates a pool of `total` permits accepting up to `max_threads`
    /// participating threads. Further threads share the pool's home shard.
    pub fn with_capacity(total: u64, max_threads: NonZeroUsize) -> Self {
        Self {
            shared: Shared::new(total, max_threads.get() + 1, FirstSlot::Home),
            token: StdArc::new(()),
        }
    }

    /// Claims one permit without blocking, registering the current thread as
    /// a participant on its first claim. Otherwise as [`Limit::try_claim`].
    pub fn try_claim(&self) -> Option<PoolPermit<'_>> {
        let token = self.token();
        let slot = LOCAL
            .try_with(|local| {
                local
                    .recent_slot(token)
                    .unwrap_or_else(|| self.bind(local, Binding::slot))
            })
            // The thread is exiting and its bindings are gone.
            .unwrap_or(HOME);
        self.shared.claim(slot).then(|| PoolPermit { pool: self })
    }

    /// As [`Pool::try_claim`], but the permit is `'static`.
    pub fn try_claim_owned(&self) -> Option<OwnedPermit> {
        // The permit holds a clone of this thread's handle. On a denied claim
        // it is dropped again, on this thread's own cache line.
        let token = self.token();
        let (slot, handle) = LOCAL
            .try_with(|local| {
                local.recent_handle(token).unwrap_or_else(|| {
                    // Clone needed: the permit holds this thread's handle.
                    self.bind(local, |binding| {
                        (binding.slot(), StdArc::clone(&binding.handle))
                    })
                })
            })
            // The thread is exiting: a handle of its own, just this once.
            .unwrap_or_else(|_| (HOME, StdArc::new(self.handle())));
        // Build the permit only on success: dropping one releases a permit.
        self.shared.claim(slot).then(|| OwnedPermit { handle })
    }

    /// Deregisters the current thread now rather than when it exits, handing
    /// its idle permits to its peers. A later claim on this thread registers
    /// it again.
    ///
    /// Useful where a runtime reports a worker stopping (e.g. tokio's
    /// `on_thread_stop`), so its share is back in circulation without waiting
    /// on thread-local destructors.
    pub fn leave_current_thread(&self) {
        let token = self.token();
        let binding = LOCAL
            .try_with(|local| {
                let mut bindings = local.bindings.borrow_mut();
                let index = bindings.iter().position(|binding| binding.is(token))?;
                Some(bindings.swap_remove(index))
            })
            .ok()
            .flatten();
        // Dropped outside the borrow: this is what deregisters the thread.
        drop(binding);
    }

    /// Applies `f` to the current thread's binding, creating it if needed and
    /// registering a participant if it has none and a slot may be free, then
    /// caches it in `local` for next time.
    fn bind<R>(&self, local: &Local, f: impl FnOnce(&Binding) -> R) -> R {
        let mut bindings = local.bindings.borrow_mut();
        let token = self.token();
        let index = match bindings.iter().position(|binding| binding.is(token)) {
            Some(index) => index,
            None => {
                // Cold path: drop bindings to pools that are gone first.
                bindings.retain(Binding::is_live);
                bindings.push(Binding {
                    handle: StdArc::new(self.handle()),
                    limit: None,
                });
                bindings.len() - 1
            }
        };
        let binding = &mut bindings[index];
        if binding.limit.is_none() && self.shared.has_vacancy() {
            binding.limit = self
                .shared
                .join()
                .ok()
                .map(|slot| Limit::from_parts(Arc::clone(&self.shared), slot));
        }
        local.remember(binding);
        f(binding)
    }

    fn handle(&self) -> Handle {
        Handle {
            shared: Arc::clone(&self.shared),
            pool: StdArc::downgrade(&self.token),
        }
    }

    fn token(&self) -> *const () {
        StdArc::as_ptr(&self.token)
    }
}

/// Returns one permit to the current thread's shard in the pool identified
/// by `token`, or to [`HOME`] if it has none. Depositing only into this
/// thread's own participant, or into the home shard, keeps
/// [`Shared::leave`] sound: a slot's owner is the only one to refill it.
fn release(shared: &Shared, token: *const ()) {
    let slot = LOCAL
        .try_with(|local| {
            local.recent_slot(token).or_else(|| {
                let bindings = local.bindings.try_borrow().ok()?;
                let binding = bindings.iter().find(|binding| binding.is(token))?;
                Some(binding.slot())
            })
        })
        .ok()
        .flatten()
        .unwrap_or(HOME);
    shared.shards[slot].give(1);
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("total", &self.shared.total)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for OwnedPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPermit").finish_non_exhaustive()
    }
}

impl Drop for PoolPermit<'_> {
    fn drop(&mut self) {
        release(&self.pool.shared, self.pool.token());
    }
}

impl Drop for OwnedPermit {
    fn drop(&mut self) {
        release(&self.handle.shared, self.handle.pool.as_ptr());
    }
}

impl Drop for Binding {
    /// Clears the binding from its thread's cache before its participant
    /// leaves.
    fn drop(&mut self) {
        let token = self.handle.pool.as_ptr();
        // Fails while the thread exits, when the cache is being dropped too.
        let _ = LOCAL.try_with(|local| local.forget(token));
    }
}

impl Binding {
    fn is(&self, token: *const ()) -> bool {
        // The weak reference keeps the token's allocation, so its address
        // cannot be reused by another pool while this binding exists.
        self.handle.pool.as_ptr() == token
    }

    fn is_live(&self) -> bool {
        self.handle.pool.strong_count() > 0
    }

    fn slot(&self) -> usize {
        self.limit.as_ref().map_or(HOME, |limit| limit.slot)
    }
}

// Loom builds are exercised by `tests/loom.rs`; these use real threads.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::mem;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    fn cap(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test capacity is non-zero")
    }

    fn idle(pool: &Pool, slot: usize) -> u64 {
        pool.shared.shards[slot].idle()
    }

    fn pool_idle(pool: &Pool) -> u64 {
        pool.shared.shards.iter().map(|shard| shard.idle()).sum()
    }

    #[test]
    fn pool_and_permits_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Pool>();
        assert_send_sync::<PoolPermit<'_>>();
        assert_send_sync::<OwnedPermit>();
    }

    #[test]
    fn first_claim_registers_the_thread() {
        let pool = Pool::with_capacity(4, cap(2));
        let permit = pool.try_claim().unwrap();
        // The only participant: drained the home shard, claimed one.
        assert_eq!((idle(&pool, HOME), idle(&pool, 1)), (0, 3));
        drop(permit);
        assert_eq!(idle(&pool, 1), 4);
        let second = thread::scope(|s| s.spawn(|| drop(pool.try_claim())).join());
        second.unwrap();
        // A second thread carved half from the first, then exited and handed
        // it back.
        assert_eq!((idle(&pool, HOME), idle(&pool, 1)), (0, 4));
    }

    #[test]
    fn permit_released_on_another_thread_lands_in_its_shard() {
        let pool = Pool::with_capacity(4, cap(2));
        let permit = pool.try_claim_owned().unwrap();
        // Joined explicitly: unlike leaving the scope, that also waits for the
        // thread's thread-local destructors, which hand its share back.
        thread::scope(|s| {
            s.spawn(|| {
                drop(pool.try_claim()); // registers this thread, in slot 2
                let before = idle(&pool, 2);
                drop(permit);
                assert_eq!(idle(&pool, 2), before + 1);
            })
            .join()
            .unwrap();
        });
        // The thread exited and handed its idle permits back.
        assert_eq!(pool_idle(&pool), 4);
    }

    #[test]
    fn permit_released_on_unregistered_thread_goes_home() {
        let pool = Pool::with_capacity(2, cap(2));
        let permit = pool.try_claim_owned().unwrap();
        let home_before = idle(&pool, HOME);
        thread::spawn(move || drop(permit)).join().unwrap();
        assert_eq!(idle(&pool, HOME), home_before + 1);
    }

    #[test]
    fn exiting_thread_hands_back_its_share() {
        let pool = Pool::with_capacity(8, cap(4));
        thread::scope(|s| {
            // See `permit_released_on_another_thread_lands_in_its_shard`.
            s.spawn(|| drop(pool.try_claim().unwrap())).join().unwrap();
        });
        let _mine = pool.try_claim().unwrap();
        assert_eq!(pool_idle(&pool), 7);
    }

    #[test]
    fn threads_past_capacity_share_the_home_shard() {
        let pool = Pool::with_capacity(4, cap(1));
        let _mine = pool.try_claim().unwrap();
        thread::scope(|s| {
            s.spawn(|| {
                let permit = pool.try_claim().expect("from home");
                drop(permit);
                assert!(LOCAL.with(|l| l.bindings.borrow().iter().all(|b| b.limit.is_none())));
            });
        });
        assert_eq!(pool_idle(&pool), 3);
    }

    #[test]
    fn leaving_hands_back_the_share_and_rejoins_on_claim() {
        let pool = Pool::with_capacity(4, cap(2));
        drop(pool.try_claim());
        assert_eq!(idle(&pool, 1), 4);
        // The last participant leaving hands everything back home.
        pool.leave_current_thread();
        assert_eq!((idle(&pool, HOME), idle(&pool, 1)), (4, 0));
        let _permit = pool.try_claim().unwrap();
        assert_eq!((idle(&pool, HOME), idle(&pool, 1)), (0, 3));
    }

    #[test]
    fn thread_on_home_shard_joins_once_a_slot_frees() {
        let pool = Pool::with_capacity(4, cap(1));
        let (joined, left) = (Barrier::new(2), Barrier::new(2));
        let is_participant = || LOCAL.with(|l| l.bindings.borrow()[0].limit.is_some());
        thread::scope(|s| {
            s.spawn(|| {
                drop(pool.try_claim()); // takes the only slot
                joined.wait();
                left.wait();
                pool.leave_current_thread();
                left.wait();
            });
            joined.wait();
            drop(pool.try_claim());
            assert!(!is_participant(), "at capacity, so on the home shard");
            left.wait();
            left.wait();
            drop(pool.try_claim());
            assert!(is_participant(), "joined the freed slot");
        });
        assert_eq!(pool_idle(&pool), 4);
    }

    #[test]
    fn owned_permits_share_their_threads_handle() {
        let pool = Pool::with_capacity(4, cap(2));
        let a = pool.try_claim_owned().unwrap();
        let b = pool.try_claim_owned().unwrap();
        assert!(StdArc::ptr_eq(&a.handle, &b.handle));
    }

    #[test]
    fn owned_permit_outlives_its_pool() {
        let pool = Pool::with_capacity(2, cap(2));
        let permit = pool.try_claim_owned().unwrap();
        drop(pool);
        drop(permit);
    }

    #[test]
    fn dropped_pool_bindings_are_pruned() {
        let first = Pool::with_capacity(2, cap(1));
        drop(first.try_claim());
        drop(first);
        let second = Pool::with_capacity(2, cap(1));
        drop(second.try_claim());
        assert_eq!(LOCAL.with(|l| l.bindings.borrow().len()), 1);
    }

    #[test]
    fn concurrent_claims_with_migrating_permits_never_over_admit() {
        const TOTAL: u64 = 8;
        const THREADS: usize = 8;
        const ROUNDS: usize = 20_000;

        let pool = Pool::with_capacity(TOTAL, cap(THREADS / 2));
        let in_use = StdArc::new(AtomicU64::new(0));
        let peak = StdArc::new(AtomicU64::new(0));
        let (tx, rx) = std::sync::mpsc::channel::<OwnedPermit>();
        let rx = StdArc::new(std::sync::Mutex::new(rx));

        thread::scope(|s| {
            let workers: Vec<_> = (0..THREADS)
                .map(|_| {
                    let (pool, in_use, peak, tx, rx) = (&pool, &in_use, &peak, tx.clone(), &rx);
                    s.spawn(move || {
                        for round in 0..ROUNDS {
                            if let Some(permit) = pool.try_claim_owned() {
                                let now = in_use.fetch_add(1, Relaxed) + 1;
                                peak.fetch_max(now, Relaxed);
                                // Hand every other permit to whichever thread
                                // picks it up next.
                                if round % 2 == 0 {
                                    tx.send(permit).unwrap();
                                } else {
                                    in_use.fetch_sub(1, Relaxed);
                                    drop(permit);
                                }
                            }
                            let received = rx.lock().unwrap().try_recv();
                            if let Ok(permit) = received {
                                in_use.fetch_sub(1, Relaxed);
                                drop(permit);
                            }
                        }
                    })
                })
                .collect();
            // See `permit_released_on_another_thread_lands_in_its_shard`.
            for worker in workers {
                worker.join().unwrap();
            }
        });
        drop(tx);
        drop(rx);

        assert!(peak.load(Relaxed) <= TOTAL);
        let claimed = (0..)
            .map_while(|_| pool.try_claim().map(mem::forget))
            .count();
        assert_eq!(claimed as u64, TOTAL);
    }
}
