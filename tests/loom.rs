//! Model-checked concurrency tests. Run with:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test --release --test loom
//! ```
//!
//! Budgets and participant counts are kept tiny: loom explores every
//! interleaving, and that space grows exponentially. Exploration is bounded to
//! [`DEFAULT_PREEMPTIONS`] preemptions per execution unless
//! `LOOM_MAX_PREEMPTIONS` is set (e.g. `LOOM_MAX_PREEMPTIONS=5` takes ~20s).
#![cfg(loom)]

use std::mem;

use loom::model::Builder;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering::SeqCst};
use loom::thread;
use speedticket::Limit;

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
