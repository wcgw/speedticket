//! N threads race to claim the whole budget, then all release it again.
//!
//! Compares `speedticket::Limit` and `speedticket::Pool` against
//! `tokio::sync::Semaphore` and a baseline semaphore built on a single shared
//! `AtomicU64`. Every variant pays the same per-round barrier cost.

mod common;

use std::hint::black_box;
use std::sync::Barrier;
use std::time::Duration;

use common::{
    Participant, THREAD_COUNTS, TOTAL, pool, race, sharded, single_atomic, tokio_semaphore,
};
use criterion::{Criterion, criterion_group, criterion_main};

/// Runs `rounds` fill-then-drain rounds, one thread per participant.
fn fill_drain<P: Participant>(participants: Vec<P>, rounds: u64) -> Duration {
    let round = Barrier::new(participants.len());
    let (elapsed, claims) = race(participants, |me| {
        let mut held = Vec::with_capacity(TOTAL as usize);
        let mut claimed = 0;
        for _ in 0..rounds {
            while let Some(permit) = me.try_claim() {
                held.push(permit);
            }
            claimed += held.len() as u64;
            round.wait(); // everyone has stopped climbing
            held.clear();
            round.wait(); // everyone is back down
        }
        claimed
    });
    // Each round must reach the full budget, or we'd be timing less work.
    let claimed: u64 = claims.into_iter().sum();
    assert_eq!(claimed, TOTAL * rounds, "a round stopped short of TOTAL");
    black_box(elapsed)
}

fn bench(c: &mut Criterion) {
    for threads in THREAD_COUNTS {
        let mut group = c.benchmark_group(format!("fill_drain/{threads}_threads/{TOTAL}_permits"));
        group.bench_function("speedticket", |b| {
            b.iter_custom(|rounds| fill_drain(sharded(threads), rounds));
        });
        group.bench_function("pool", |b| {
            b.iter_custom(|rounds| fill_drain(pool(threads), rounds));
        });
        group.bench_function("tokio", |b| {
            b.iter_custom(|rounds| fill_drain(tokio_semaphore(threads), rounds));
        });
        group.bench_function("single_atomic", |b| {
            b.iter_custom(|rounds| fill_drain(single_atomic(threads), rounds));
        });
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
