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
from another participant. That is the only point where contention can occur.
Stealing is lock-free and bounded, so it can neither deadlock nor spin; a claim
that finds nothing to steal fails fast rather than blocking. The limit is a hard
upper bound (it never over-admits) and a best-effort lower bound (it may rarely
deny while a permit is momentarily in flight).

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
```

A `static` hands out `PoolPermit<'static>`. Wherever the pool lives instead,
`try_claim_owned` returns an `OwnedPermit` that is `'static` too: each one
holds a reference-counted handle belonging to the thread that claimed it, so
owned claims cost a little more than `try_claim`'s but scale just as well.
Register a `Pool::leave_current_thread` hook with the runtime's
`on_thread_stop` so a stopping worker returns its share right away. There is
no async waiting yet: `try_claim` either succeeds or returns `None`.

[`examples/tokio.rs`](examples/tokio.rs) puts it together. Run it with
`cargo run --example tokio`.

## Benchmarks (preliminary)

The benches compare `speedticket`'s `Limit` (one participant per thread) and
`Pool` (shared by every thread) against `tokio::sync::Semaphore` and a baseline
semaphore built on a single shared `AtomicU64`, with a budget of 1000 permits.

These numbers come from single criterion runs on one laptop (Intel Core Ultra 7
255U), with every thread pinned to its 8 identical efficiency cores:

```sh
cargo bench --no-run && taskset -c 4-11 cargo bench
```

Pin the threads when benchmarking on a hybrid CPU. Left unpinned, threads land
on performance, efficiency and low-power cores at random, and runs disagree by
25% or more; pinned, repeated runs on this machine agree within about ±5%.
Treat the numbers as indicative only.

### `claim_release`: the common case

Every thread claims one permit and drops it straight away, over and over, with
the budget far from exhausted. Times are per claim-and-release, with all
threads running at once. The owned variants claim `'static` permits, as a task
holding one across an `.await` would (`Pool::try_claim_owned`,
`Semaphore::try_acquire_owned`).

| threads | `Limit` | `Pool`  | `Pool` owned | tokio   | tokio owned | single atomic |
|--------:|--------:|--------:|-------------:|--------:|------------:|--------------:|
| 1       | 22.4 ns | 24.5 ns | 44.7 ns      | 47.2 ns | 65.4 ns     | 16.4 ns       |
| 2       | 35.5 ns | 37.8 ns | 68.3 ns      | 999 ns  | 1.35 µs     | 348 ns        |
| 4       | 31.5 ns | 36.6 ns | 62.4 ns      | 1.66 µs | 1.93 µs     | 1.03 µs       |
| 8       | 20.0 ns | 24.0 ns | 37.8 ns      | 3.50 µs | 4.29 µs     | 12.8 µs       |

Both `speedticket` variants stay flat as threads are added, because each thread
claims and releases against its own shard. Every other variant contends on one
shared counter, and `tokio::sync::Semaphore` also takes a lock on every
release, so they slow down as soon as a second thread joins: at 8 threads,
`Pool` is about 145× faster than tokio, and its owned permits about 115× faster
than tokio's.

### `fill_drain`: race to exhaustion, then release everything

Each round, every thread claims until denied, so all 1000 permits end up held.
The threads then wait at a barrier, release everything, and wait at a barrier
again. Times are per round and include the barrier cost, which every variant
pays.

| threads | `Limit` | `Pool`  | tokio  | single atomic |
|--------:|--------:|--------:|-------:|--------------:|
| 4       | 29.3 µs | 29.5 µs | 433 µs | 339 µs        |
| 6       | 35.8 µs | 36.4 µs | 925 µs | 920 µs        |

### `steal`: pinned at exhaustion

Each thread claims until denied, gives back `churn` permits, and repeats, with
no barriers between threads. This forces claims onto the steal path, where
`speedticket` has to look at its peers' shards. Times are per round.

| threads | churn | `Limit` | `Pool`  | tokio   | single atomic |
|--------:|------:|--------:|--------:|--------:|--------------:|
| 4       | 1     | 446 ns  | 521 ns  | 1.42 µs | 568 ns        |
| 4       | 4     | 1.94 µs | 1.88 µs | 5.09 µs | 3.98 µs       |
| 6       | 1     | 726 ns  | 810 ns  | 2.76 µs | 1.62 µs       |
| 6       | 4     | 2.90 µs | 2.84 µs | 10.2 µs | 9.24 µs       |

With a churn of 1, most claims end with a search of every peer that finds
nothing, and every search reads cache lines that other cores keep changing.
This is `speedticket`'s worst case. It still beats the single shared counter,
which every thread now fights over, but `Pool` trails `Limit` here by 12–17%:
its faster claim path fails faster, and failing faster means more of those
contended reads per round. Once peers have permits to give (churn 4), both
variants are 2–3.6× faster than tokio and the single atomic.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
