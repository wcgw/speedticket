//! Keeps the pool pinned at exhaustion so claims run on the steal path.
//!
//! Every thread claims until denied, gives back `churn` permits, and repeats,
//! with no barrier between threads. A thread's released permits land in its
//! own shard and are reclaimed locally, after which its next claim finds its
//! shard empty and must scan peers — who each hold at most a few idle permits,
//! so batch-stealing (half the victim's idle) can't amortize anything.
//!
//! Measured with temporary instrumentation (not committed), at 4–6 threads:
//!
//! | churn | claims entering the steal path | claims completing a steal |
//! |-------|--------------------------------|---------------------------|
//! | 1     | ~81–85% (mostly fruitless scans) | ~23–24%                 |
//! | 4     | ~58–69%                        | ~35–41%                   |
//!
//! For contrast, `fill_drain` steals on ~9–13% of claims.

mod common;

use std::hint::black_box;
use std::iter;
use std::time::Duration;

use common::{
    Participant, THREAD_COUNTS, TOTAL, pool, race, sharded, single_atomic, tokio_semaphore,
};
use criterion::{Criterion, criterion_group, criterion_main};

/// Permits each thread gives back per round: 1 stresses the scan (peers are
/// mostly empty), 4 stresses the steal itself (peers mostly have a little).
const CHURNS: [usize; 2] = [1, 4];

/// Runs `rounds` claim-until-denied, release-`churn` rounds on every thread.
fn exhaustion_churn<P: Participant>(participants: Vec<P>, churn: usize, rounds: u64) -> Duration {
    let (elapsed, _) = race(participants, |me| {
        let mut held = Vec::with_capacity(TOTAL as usize);
        for _ in 0..rounds {
            held.extend(iter::from_fn(|| me.try_claim()));
            held.truncate(held.len().saturating_sub(churn));
        }
    });
    black_box(elapsed)
}

fn bench(c: &mut Criterion) {
    for threads in THREAD_COUNTS {
        for churn in CHURNS {
            let mut group = c.benchmark_group(format!(
                "steal/{threads}_threads/{TOTAL}_permits/churn_{churn}"
            ));
            group.bench_function("speedticket", |b| {
                b.iter_custom(|rounds| exhaustion_churn(sharded(threads), churn, rounds));
            });
            group.bench_function("pool", |b| {
                b.iter_custom(|rounds| exhaustion_churn(pool(threads), churn, rounds));
            });
            group.bench_function("tokio", |b| {
                b.iter_custom(|rounds| exhaustion_churn(tokio_semaphore(threads), churn, rounds));
            });
            group.bench_function("single_atomic", |b| {
                b.iter_custom(|rounds| exhaustion_churn(single_atomic(threads), churn, rounds));
            });
            group.finish();
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
