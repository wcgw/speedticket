//! Capping in-flight work across a multi-threaded tokio runtime.
//!
//! Many tasks each make a (simulated) outbound call, and at most `BUDGET` of
//! them may be in flight at once. Each task holds its permit across an
//! `.await`, so the runtime may resume it on another worker thread and the
//! permit is released there, into that worker's own shard.
//!
//! Run with `cargo run --example tokio --features async`.

// tokio has no `Builder::on_thread_stop` under `--cfg loom`; there is nothing
// to model-check here, so loom builds get an empty `main`.
#![cfg_attr(loom, allow(dead_code, unused_imports))]

use std::num::NonZeroUsize;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::Duration;

use speedticket::Pool;

const WORKERS: usize = 4;
const BUDGET: u64 = 8;
const TASKS: u64 = 64;

/// The budget, shared by every task.
///
/// Kept in a `static` so `try_claim` hands out `PoolPermit<'static>`, which a
/// spawned task may hold across an `.await`. A pool that can't be a `static`
/// works just as well with `try_claim_owned`, whose permits are `'static`
/// whatever the pool's lifetime.
static POOL: LazyLock<Pool> = LazyLock::new(|| {
    // One shard per worker; any other thread shares the pool's own shard.
    let workers = NonZeroUsize::new(WORKERS).expect("at least one worker");
    Pool::with_capacity(BUDGET, workers)
});

static IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static MIGRATED: AtomicU64 = AtomicU64::new(0);

#[cfg(loom)]
fn main() {}

#[cfg(not(loom))]
fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_time()
        // A stopping worker hands its idle permits back to its peers now,
        // rather than whenever its thread-locals are torn down.
        .on_thread_stop(|| POOL.leave_current_thread())
        .build()
        .expect("a tokio runtime");

    runtime.block_on(async {
        let tasks: Vec<_> = (0..TASKS).map(|_| tokio::spawn(call())).collect();
        for task in tasks {
            task.await.expect("task panicked");
        }
    });

    println!(
        "{TASKS} calls, at most {} in flight (budget {BUDGET}); \
         {} permits released on a different thread than claimed",
        PEAK.load(Relaxed),
        MIGRATED.load(Relaxed),
    );
    assert!(PEAK.load(Relaxed) <= BUDGET);
}

/// One outbound call, made only while holding a permit.
async fn call() {
    // Waits while the budget is spent. To shed load instead, call
    // `POOL.try_claim()` once and reject the work on `None`.
    let permit = POOL.claim().await;
    let claimed_on = thread::current().id();
    let now = IN_FLIGHT.fetch_add(1, Relaxed) + 1;
    PEAK.fetch_max(now, Relaxed);

    // The call itself: the task may resume on another worker after this.
    tokio::time::sleep(Duration::from_millis(10)).await;

    IN_FLIGHT.fetch_sub(1, Relaxed);
    if thread::current().id() != claimed_on {
        MIGRATED.fetch_add(1, Relaxed);
    }
    // Released into the shard of whichever worker is running us now.
    drop(permit);
}
