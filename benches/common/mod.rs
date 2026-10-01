//! Harness shared by the benches: the pools under test and a timed race.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use speedticket::Limit;

pub const TOTAL: u64 = 1000;
pub const THREAD_COUNTS: [usize; 2] = [4, 6];

/// One thread's view of a pool: claim permits, release them on drop.
pub trait Participant: Send {
    type Permit<'a>
    where
        Self: 'a;

    fn try_claim(&self) -> Option<Self::Permit<'_>>;
}

impl Participant for Limit {
    type Permit<'a> = speedticket::Permit<'a>;

    fn try_claim(&self) -> Option<Self::Permit<'_>> {
        Limit::try_claim(self)
    }
}

/// Baseline: every participant contends on one shared counter.
pub struct AtomicLimit {
    idle: AtomicU64,
}

pub struct AtomicPermit<'a> {
    idle: &'a AtomicU64,
}

impl Drop for AtomicPermit<'_> {
    fn drop(&mut self) {
        self.idle.fetch_add(1, Relaxed);
    }
}

impl Participant for Arc<AtomicLimit> {
    type Permit<'a> = AtomicPermit<'a>;

    fn try_claim(&self) -> Option<Self::Permit<'_>> {
        self.idle
            .fetch_update(Relaxed, Relaxed, |idle| idle.checked_sub(1))
            .ok()
            .map(|_| AtomicPermit { idle: &self.idle })
    }
}

/// One `speedticket` participant per thread, sharing a pool of `TOTAL`.
pub fn sharded(threads: usize) -> Vec<Limit> {
    let first = Limit::new(TOTAL);
    let mut participants: Vec<_> = (1..threads)
        .map(|_| first.participant().expect("capacity for every thread"))
        .collect();
    participants.push(first);
    participants
}

/// One handle per thread on a single shared counter of `TOTAL`.
pub fn single_atomic(threads: usize) -> Vec<Arc<AtomicLimit>> {
    let pool = Arc::new(AtomicLimit {
        idle: AtomicU64::new(TOTAL),
    });
    // Clone needed: each thread owns a handle on the one shared counter.
    vec![pool; threads]
}

/// Runs `body` on one thread per participant, all released at once, and
/// returns the wall time from that release until every thread has finished,
/// along with each thread's result.
pub fn race<P, R>(participants: Vec<P>, body: impl Fn(P) -> R + Sync) -> (Duration, Vec<R>)
where
    P: Participant,
    R: Send,
{
    let start = Barrier::new(participants.len() + 1);
    let (start, body) = (&start, &body);
    thread::scope(|s| {
        let workers: Vec<_> = participants
            .into_iter()
            .map(|me| {
                s.spawn(move || {
                    start.wait();
                    body(me)
                })
            })
            .collect();

        start.wait();
        let began = Instant::now();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().expect("bench thread panicked"))
            .collect();
        (began.elapsed(), results)
    })
}
