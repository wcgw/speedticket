//! The common case: every thread claims a permit and drops it straight away,
//! with the budget far from exhausted.
//!
//! Isolates the per-operation cost of each semaphore as threads are added.
//! `speedticket` stays on each thread's own shard; `tokio::sync::Semaphore`
//! CASes one shared counter per claim and takes its wait-list lock per
//! release. The owned variants add an `Arc` clone and drop per permit, as a
//! spawned task holding one across an `.await` would.

mod common;

use std::hint::black_box;
use std::time::Duration;

use common::{
    Participant, TOTAL, owned_pool, owned_tokio_semaphore, pool, race, sharded, single_atomic,
    tokio_semaphore,
};
use criterion::{Criterion, criterion_group, criterion_main};

const THREAD_COUNTS: [usize; 5] = [1, 2, 4, 8, 12];

/// Runs `rounds` claim-then-release rounds on every thread.
fn claim_release<P: Participant>(participants: Vec<P>, rounds: u64) -> Duration {
    let (elapsed, claims) = race(participants, |me| {
        (0..rounds)
            .filter(|_| me.try_claim().map(black_box).is_some())
            .count() as u64
    });
    // With at most one permit held per thread, no claim should be denied.
    let threads = claims.len() as u64;
    let claimed: u64 = claims.into_iter().sum();
    assert_eq!(claimed, threads * rounds, "a claim was denied");
    black_box(elapsed)
}

fn bench(c: &mut Criterion) {
    for threads in THREAD_COUNTS {
        let mut group =
            c.benchmark_group(format!("claim_release/{threads}_threads/{TOTAL}_permits"));
        group.bench_function("speedticket", |b| {
            b.iter_custom(|rounds| claim_release(sharded(threads), rounds));
        });
        group.bench_function("pool", |b| {
            b.iter_custom(|rounds| claim_release(pool(threads), rounds));
        });
        group.bench_function("pool_owned", |b| {
            b.iter_custom(|rounds| claim_release(owned_pool(threads), rounds));
        });
        group.bench_function("tokio", |b| {
            b.iter_custom(|rounds| claim_release(tokio_semaphore(threads), rounds));
        });
        group.bench_function("tokio_owned", |b| {
            b.iter_custom(|rounds| claim_release(owned_tokio_semaphore(threads), rounds));
        });
        group.bench_function("single_atomic", |b| {
            b.iter_custom(|rounds| claim_release(single_atomic(threads), rounds));
        });
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
