//! Model-checked concurrency tests. Run with:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test --release --test loom --features async
//! ```
//!
//! Without `--features async` the waiting models are skipped.
//!
//! Budgets and participant counts are kept tiny: loom explores every
//! interleaving, and that space grows exponentially. Exploration is bounded to
//! [`DEFAULT_PREEMPTIONS`] preemptions per execution unless
//! `LOOM_MAX_PREEMPTIONS` is set (e.g. `LOOM_MAX_PREEMPTIONS=5` takes ~20s).
#![cfg(loom)]

use std::mem;
use std::num::NonZeroUsize;

use loom::model::Builder;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering::SeqCst};
use loom::thread;
use speedticket::{Limit, OwnedPermit, Pool};

/// Unbounded, `never_over_admits_and_conserves_permits` does not finish in
/// minutes; every planted bug we tried is caught well within this bound.
const DEFAULT_PREEMPTIONS: usize = 3;

/// Runs `f` under loom, honouring `LOOM_*` environment overrides.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut builder = Builder::new();
    builder.preemption_bound.get_or_insert(DEFAULT_PREEMPTIONS);
    builder.check(f);
}

/// Claims until denied, leaking every permit, and returns how many it got.
/// Single-threaded, a denial means the participant and its peers are empty.
fn claim_all(limit: &Limit) -> u64 {
    let mut claimed = 0;
    while let Some(permit) = limit.try_claim() {
        mem::forget(permit);
        claimed += 1;
    }
    claimed
}

/// As [`claim_all`], from the current thread's participant in `pool`.
fn claim_all_from(pool: &Pool) -> u64 {
    let mut claimed = 0;
    while let Some(permit) = pool.try_claim() {
        mem::forget(permit);
        claimed += 1;
    }
    claimed
}

/// Hard upper bound: under any interleaving of own-shard claims, steals,
/// releases and participant drops, no more than `TOTAL` permits are ever held
/// at once — and once the dust settles, none have been lost.
#[test]
fn never_over_admits_and_conserves_permits() {
    const TOTAL: u64 = 2;

    model(|| {
        let first = Limit::new(TOTAL);
        // first: 1, b: 1, c: 0 — so c can only ever steal.
        let peers = [first.participant().unwrap(), first.participant().unwrap()];
        let in_use = Arc::new(AtomicU64::new(0));

        let workers: Vec<_> = peers
            .into_iter()
            .map(|me| {
                let in_use = Arc::clone(&in_use);
                thread::spawn(move || {
                    let mut held = Vec::new();
                    for _ in 0..2 {
                        if let Some(permit) = me.try_claim() {
                            let now = in_use.fetch_add(1, SeqCst) + 1;
                            assert!(now <= TOTAL, "{now} permits held, total is {TOTAL}");
                            held.push(permit);
                        }
                    }
                    for permit in held {
                        in_use.fetch_sub(1, SeqCst);
                        drop(permit);
                    }
                    // `me` drops here, handing its idle permits back.
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }

        assert_eq!(claim_all(&first), TOTAL, "permits were lost or created");
    });
}

/// Racing participants that never release must, between them, claim exactly
/// the whole budget: a spurious denial may stop one of them early, but never
/// strands permits where no one can reach them.
#[test]
fn racing_to_exhaustion_claims_exactly_total() {
    const TOTAL: u64 = 3;

    model(|| {
        let first = Limit::new(TOTAL);
        let second = first.participant().unwrap(); // first: 2, second: 1

        let other = thread::spawn(move || claim_all(&second));
        let mine = claim_all(&first);
        let theirs = other.join().unwrap();

        assert_eq!(mine + theirs, TOTAL);
    });
}

/// Registering and dropping a participant (cold path, join-lock) while a peer
/// claims, steals and releases (hot path, lock-free) loses no permits.
#[test]
fn membership_changes_during_claims_conserve_permits() {
    const TOTAL: u64 = 2;

    model(|| {
        let first = Limit::new(TOTAL);
        let second = first.participant().unwrap(); // first: 1, second: 1

        let worker = thread::spawn(move || {
            let a = second.try_claim();
            let b = second.try_claim();
            drop((a, b));
        });
        let third = first.participant().unwrap();
        drop(third);
        worker.join().unwrap();

        assert_eq!(claim_all(&first), TOTAL, "permits were lost or created");
    });
}

/// `TOTAL` permits, for up to three participating threads.
fn pool() -> std::sync::Arc<Pool> {
    std::sync::Arc::new(Pool::with_capacity(TOTAL, NonZeroUsize::new(3).unwrap()))
}

const TOTAL: u64 = 2;

/// Claims from `pool` on the current thread, counting the permit in `in_use`
/// and failing the model if more than `TOTAL` are ever held at once.
fn counted_claim(pool: &std::sync::Arc<Pool>, in_use: &AtomicU64) -> Option<OwnedPermit> {
    let permit = pool.try_claim_owned()?;
    let now = in_use.fetch_add(1, SeqCst) + 1;
    assert!(now <= TOTAL, "{now} permits held, total is {TOTAL}");
    Some(permit)
}

fn counted_release(permit: OwnedPermit, in_use: &AtomicU64) {
    in_use.fetch_sub(1, SeqCst);
    drop(permit);
}

/// A permit claimed on one thread is released on another — into that
/// thread's own shard — while a third thread registers, claims, releases and
/// leaves, redistributing its share into the second. No interleaving
/// over-admits, and none loses or creates a permit.
///
/// Threads leave explicitly: loom's `join` does not wait for thread-local
/// destructors (std's does), so a destructor's writes would not be ordered
/// before the final count. See `pool_exiting_threads_never_over_admit`.
#[test]
fn pool_cross_thread_release_while_threads_leave() {
    model(|| {
        let pool = pool();
        let in_use = Arc::new(AtomicU64::new(0));
        // Main registers (carving 1 of 2) and claims it.
        let migrating = counted_claim(&pool, &in_use).expect("main's own share");

        let leaver = {
            let (pool, in_use) = (pool.clone(), Arc::clone(&in_use));
            thread::spawn(move || {
                if let Some(permit) = counted_claim(&pool, &in_use) {
                    counted_release(permit, &in_use);
                }
                pool.leave_current_thread();
            })
        };
        let carrier = {
            let (pool, in_use) = (pool.clone(), Arc::clone(&in_use));
            thread::spawn(move || {
                // Registers, so the migrating permit lands in this thread's
                // own shard, racing with `leaver` redistributing into it.
                if let Some(permit) = counted_claim(&pool, &in_use) {
                    counted_release(permit, &in_use);
                }
                counted_release(migrating, &in_use);
                pool.leave_current_thread();
            })
        };
        leaver.join().unwrap();
        carrier.join().unwrap();

        assert_eq!(claim_all_from(&pool), TOTAL, "permits were lost or created");
    });
}

/// A permit dropped on a thread that never claimed goes to the pool's home
/// shard, racing with a newcomer that has nothing to carve and so steals
/// from that same shard, then leaves.
#[test]
fn pool_release_on_unbound_thread_races_home_steal() {
    model(|| {
        let pool = pool();
        // Main registers (carving 1 of 2) and claims it; home keeps 1.
        let migrating = pool.try_claim_owned().expect("main's own share");

        let stealer = {
            let pool = pool.clone();
            thread::spawn(move || {
                drop(pool.try_claim());
                pool.leave_current_thread();
            })
        };
        let carrier = thread::spawn(move || drop(migrating));
        stealer.join().unwrap();
        carrier.join().unwrap();

        assert_eq!(claim_all_from(&pool), TOTAL, "permits were lost or created");
    });
}

/// Threads that simply exit hand their share back from a thread-local
/// destructor. Under loom that destructor races even with code after `join`,
/// so main's claims overlap the redistribution: none may over-admit.
#[test]
fn pool_exiting_threads_never_over_admit() {
    model(|| {
        let pool = pool();
        let in_use = Arc::new(AtomicU64::new(0));
        let migrating = counted_claim(&pool, &in_use).expect("main's own share");

        let exiter = {
            let (pool, in_use) = (pool.clone(), Arc::clone(&in_use));
            thread::spawn(move || {
                if let Some(permit) = counted_claim(&pool, &in_use) {
                    counted_release(permit, &in_use);
                }
                counted_release(migrating, &in_use);
                // Exits here; its participant leaves from the TLS destructor.
            })
        };
        exiter.join().unwrap();
        let held: Vec<_> = (0..TOTAL)
            .map_while(|_| counted_claim(&pool, &in_use))
            .collect();
        for permit in held {
            counted_release(permit, &in_use);
        }
    });
}

/// A thread stuck on the home shard (the pool is at capacity) joins as soon
/// as the participant holding the only slot leaves, racing that departure's
/// hand-back of its share to the home shard, and a permit released after its
/// claimer left. No interleaving over-admits, loses or creates a permit.
#[test]
fn pool_home_thread_joins_freed_slot() {
    model(|| {
        let pool = std::sync::Arc::new(Pool::with_capacity(TOTAL, NonZeroUsize::new(1).unwrap()));
        let in_use = Arc::new(AtomicU64::new(0));
        // Main takes the only slot, with the whole budget, and claims one.
        let held = counted_claim(&pool, &in_use).expect("main's own share");

        let waiting = {
            let (pool, in_use) = (pool.clone(), Arc::clone(&in_use));
            thread::spawn(move || {
                // On the home shard at first; may join once main has left.
                for _ in 0..2 {
                    if let Some(permit) = counted_claim(&pool, &in_use) {
                        counted_release(permit, &in_use);
                    }
                }
                pool.leave_current_thread();
            })
        };
        pool.leave_current_thread();
        // Main has no participant now, so this goes to the home shard.
        counted_release(held, &in_use);
        waiting.join().unwrap();

        assert_eq!(claim_all_from(&pool), TOTAL, "permits were lost or created");
    });
}

/// Loom reports a deadlock if a waiting claim is never woken, so these
/// models need no assertion beyond completing.
#[cfg(feature = "async")]
mod waiting {
    use super::*;
    use loom::future::block_on;

    /// A release wakes a claim waiting on an exhausted pool, however the
    /// release interleaves with the claim registering and retrying.
    #[test]
    fn release_wakes_a_waiting_claim() {
        model(|| {
            let pool = std::sync::Arc::new(Pool::with_capacity(1, NonZeroUsize::new(2).unwrap()));
            let held = pool.try_claim_owned().expect("the only permit");
            let waiter = {
                let pool = pool.clone();
                thread::spawn(move || drop(block_on(pool.claim())))
            };
            drop(held);
            waiter.join().unwrap();
        });
    }

    /// A claim registers and retries while another thread's steal has a
    /// permit in flight between shards: the thief's banking of the batch, or
    /// its release, must wake the claim if its retry missed the permit.
    #[test]
    fn waiting_claim_is_woken_past_a_steal_in_flight() {
        model(|| {
            let pool = std::sync::Arc::new(Pool::with_capacity(2, NonZeroUsize::new(3).unwrap()));
            // Main joins first, with both permits, and holds one.
            let held = pool.try_claim_owned().expect("main's own share");
            let thief = {
                let pool = pool.clone();
                // Joins with nothing to carve, so it must steal main's other
                // permit; holds it briefly, then releases it.
                thread::spawn(move || drop(pool.try_claim()))
            };
            let waiter = {
                let pool = pool.clone();
                thread::spawn(move || drop(block_on(pool.claim())))
            };
            drop(held);
            thief.join().unwrap();
            waiter.join().unwrap();
        });
    }

    /// A thief banks part of its haul and keeps the rest: nothing is ever
    /// released, so only the banking itself can wake a claim whose retry
    /// missed the batch in flight.
    #[test]
    fn waiting_claim_is_woken_by_a_thiefs_banked_batch() {
        model(|| {
            // One participant slot: main takes it, with all four permits.
            let pool = std::sync::Arc::new(Pool::with_capacity(4, NonZeroUsize::new(1).unwrap()));
            drop(pool.try_claim());
            let thief = {
                let pool = pool.clone();
                // At capacity, so on the home shard: steals half of main's
                // idle permits, banks all but one at home, keeps that one.
                thread::spawn(move || pool.try_claim().map(mem::forget))
            };
            let waiter = {
                let pool = pool.clone();
                thread::spawn(move || mem::forget(block_on(pool.claim())))
            };
            // Main keeps what it can of the rest; demand never exceeds four.
            for _ in 0..2 {
                if let Some(permit) = pool.try_claim() {
                    mem::forget(permit);
                }
            }
            thief.join().unwrap();
            waiter.join().unwrap();
        });
    }

    /// The oldest waiter is cancelled, concurrently with the release that
    /// notifies it: the wake-up must reach the remaining waiter instead.
    #[test]
    fn cancelled_waiter_passes_its_wake_up_on() {
        use std::future::Future;
        use std::task::{Context, Waker};

        model(|| {
            let pool = std::sync::Arc::new(Pool::with_capacity(1, NonZeroUsize::new(3).unwrap()));
            let held = pool.try_claim_owned().expect("the only permit");
            let cancelled = {
                let pool = pool.clone();
                thread::spawn(move || {
                    let mut claim = Box::pin(pool.claim());
                    // Waits once (or wins outright), then gives up.
                    let _ = claim.as_mut().poll(&mut Context::from_waker(Waker::noop()));
                })
            };
            let waiter = {
                let pool = pool.clone();
                thread::spawn(move || drop(block_on(pool.claim())))
            };
            drop(held);
            cancelled.join().unwrap();
            waiter.join().unwrap();
        });
    }
}
