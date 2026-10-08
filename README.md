# speedticket

A finite-budget semaphore that shards automatically across threads. A fixed
number of permits is partitioned per participant, so the common case — claiming
and releasing against your own shard — needs no cross-thread coordination. There
is no notion of time and no refill: permits are claimed and released, nothing
more.

A `Limit` is `Send` but **not** `Sync`. You create one with a total budget, then
call `.participant()` for each thread to get its own handle — each registration
carves out a share of the budget. Say you have a limit of 12 and add
participants one by one: the first holds 12, two participants hold 6 each, three
hold 4 each, and so on. No sharing is required across threads on the happy path.

When a participant exhausts its own share, it tries to **steal** idle permits
from another participant. Stealing is lock-free and bounded, so it can neither
deadlock nor spin; a claim that finds nothing to steal fails fast rather than
blocking. A participant that keeps running dry, churning near exhaustion,
**spills**: it releases into a shared reserve and claims from there before
stealing, so such threads meet on a single counter instead of scanning each
other's shards. It goes back to its own shard once the reserve holds a fair
share again. On the claim path, stealing and the reserve are the only points
where contention can occur. The limit is a hard upper bound (it never
over-admits) and a best-effort lower bound (it may rarely deny while a permit is
momentarily in flight).

See [DESIGN.md](DESIGN.md) for the full design.

## Usage

```rust
use std::thread;

use speedticket::Limit;

fn main() {
    // At most 64 jobs in flight across all workers.
    let limit = Limit::new(64);

    thread::scope(|s| {
        for _ in 0..4 {
            // One participant per thread; each registration carves out a share.
            let me = limit.participant().expect("room for another participant");
            s.spawn(move || {
                for _ in 0..1_000 {
                    match me.try_claim() {
                        Some(_permit) => {
                            // Do the work. The permit is released when it drops.
                        }
                        None => {
                            // Budget exhausted: shed load, back off, retry later...
                        }
                    }
                }
            });
        }
    });
}
```

`try_claim` never blocks. A `Permit` borrows the participant it came from and
is not `Send`, so it is always released on the thread that claimed it; moving
it elsewhere is a compile error. A participant can join at any time with
`participant()`, which fails with `AtCapacity` once the pool is full (by default
`max(available_parallelism, 16)` participants; use `Limit::with_capacity` to set
the limit yourself).

### With tokio (or any runtime whose tasks migrate)

On a multi-threaded runtime, tasks move between worker threads, so a permit
held across an `.await` may be released on a different thread from the one that
claimed it. Use `Pool` instead: it is `Sync`, binds each thread to its own
shard on that thread's first claim, and its permits are `Send` and released
into the shard of whichever thread drops them.

```rust
use std::sync::LazyLock;

use speedticket::Pool;

static POOL: LazyLock<Pool> = LazyLock::new(|| Pool::new(64));

async fn call() {
    let Some(_permit) = POOL.try_claim() else {
        return; // over budget: shed the work, or retry later
    };
    // `_permit` is `PoolPermit<'static>` and may be held across `.await`.
}

// With the `async` feature: wait for a permit instead of giving up.
async fn queued_call() {
    let _permit = POOL.claim().await;
}
```

A `static` hands out `PoolPermit<'static>`. Wherever the pool lives instead,
`try_claim_owned` returns an `OwnedPermit` that is `'static` too: each one
holds a reference-counted handle belonging to the thread that claimed it, so
owned claims cost a little more than `try_claim`'s but scale just as well.
Register a `Pool::leave_current_thread` hook with the runtime's
`on_thread_stop` so a stopping worker returns its share right away.

With the `async` feature, `Pool::claim` and `Pool::claim_owned` wait for a
permit when the pool is exhausted. They work with any executor (nothing
depends on tokio) and are cancel safe. Waiters are woken in the order they
started waiting, but a woken waiter still competes with every other claim, so
there is no fairness guarantee. When nobody waits, the feature costs each
release one extra load, which the benchmarks below cannot measure.

[`examples/tokio.rs`](examples/tokio.rs) puts it together. Run it with
`cargo run --example tokio --features async`.

## Benchmarks (preliminary)

The benches compare `speedticket`'s `Limit` (one participant per thread) and
`Pool` (shared by every thread) against `tokio::sync::Semaphore` and a baseline
semaphore built on a single shared `AtomicU64`, with a budget of 1000 permits.

These numbers come from a single criterion run on one desktop (AMD Ryzen 9 7900,
12 cores / 24 threads in two 6-core CCDs, each with its own L3). Boost was off,
with the `performance` governor and EPP, so clocks were capped at the 3.7 GHz
base. Runs with up to 6 threads were pinned to the six physical cores of one
CCD. The 8-thread runs were pinned to four physical cores on each CCD, so they
also pay for traffic between the CCDs. SMT siblings were left unused in both
cases:

```sh
cargo bench --no-run
taskset -c 6-11 cargo bench -- '/[1246]_threads/'
taskset -c 2-9 cargo bench -- '/8_threads/'
```

Across three runs, 40 of the 60 results varied by under 10%, but some varied by
up to 17% (`speedticket`), 27% (single atomic) and 47% (tokio). Treat them as
indicative only.

### `claim_release`: the common case

Every thread claims one permit and drops it straight away, over and over, with
the budget far from exhausted. Times are per claim-and-release, with all
threads running at once. The owned variants claim `'static` permits, as a task
holding one across an `.await` would (`Pool::try_claim_owned`,
`Semaphore::try_acquire_owned`).

| threads | `Limit` | `Pool`  | `Pool` owned | tokio   | tokio owned | single atomic |
|--------:|--------:|--------:|-------------:|--------:|------------:|--------------:|
| 1       | 4.47 ns | 4.55 ns | 8.74 ns      | 8.84 ns | 13.2 ns     | 4.55 ns       |
| 2       | 4.49 ns | 4.55 ns | 8.77 ns      | 119 ns  | 151 ns      | 37.3 ns       |
| 4       | 4.48 ns | 4.57 ns | 9.82 ns      | 352 ns  | 425 ns      | 79.9 ns       |
| 8       | 4.86 ns | 4.93 ns | 9.76 ns      | 837 ns  | 1.05 µs     | 332 ns        |

Both `speedticket` variants stay flat as threads are added, because each thread
claims and releases against its own shard. Every other variant contends on one
shared counter, and `tokio::sync::Semaphore` also takes a lock on every
release, so they slow down as soon as a second thread joins: at 8 threads,
spread over both CCDs, `Pool` is about 140–170× faster than tokio, and its owned
permits about 90–110× faster than tokio's.

### `fill_drain`: race to exhaustion, then release everything

Each round, every thread claims until denied, so all 1000 permits end up held.
The threads then wait at a barrier, release everything, and wait at a barrier
again. Times are per round and include the barrier cost, which every variant
pays.

| threads | `Limit` | `Pool`  | tokio   | single atomic |
|--------:|--------:|--------:|--------:|--------------:|
| 4       | 9.07 µs | 8.39 µs | 27.6 µs | 14.1 µs       |
| 6       | 13.0 µs | 12.6 µs | 34.8 µs | 20.1 µs       |
| 8       | 26.4 µs | 26.7 µs | 102 µs  | 50.9 µs       |

### `steal`: pinned at exhaustion

Each thread claims until denied, gives back `churn` permits, and repeats, with
no barriers between threads. This forces claims onto the steal path, where
`speedticket` has to look at its peers' shards. Times are per round.

| threads | churn | `Limit` | `Pool`  | tokio   | single atomic |
|--------:|------:|--------:|--------:|--------:|--------------:|
| 4       | 1     | 92.0 ns | 78.8 ns | 403 ns  | 69.3 ns       |
| 4       | 4     | 483 ns  | 482 ns  | 1.04 µs | 386 ns        |
| 6       | 1     | 110 ns  | 128 ns  | 613 ns  | 92.0 ns       |
| 6       | 4     | 727 ns  | 714 ns  | 1.74 µs | 579 ns        |
| 8       | 1     | 349 ns  | 383 ns  | 977 ns  | 236 ns        |
| 8       | 4     | 1.89 µs | 1.79 µs | 3.91 µs | 1.28 µs       |

Every round ends with a claim that searches every peer and finds nothing.
Threads churning like this spill: they release into the shared reserve and
claim from it before stealing, so their own shards stay quiet, and the
searches read cache lines that rarely change. The spilling threads meet on the
reserve, a single counter like the single atomic's. This is still
`speedticket`'s worst case: the single atomic is about 1.1–1.6× faster at a
churn of 1, and about 1.2–1.5× at a churn of 4. tokio is slower than both
`speedticket` variants everywhere, by about 2–5.6×.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
