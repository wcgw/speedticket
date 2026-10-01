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

## Benchmarks (preliminary)

Both benches compare `speedticket` against a baseline semaphore built on a
single shared `AtomicU64`, with a budget of 1000 permits. Run them with
`cargo bench`.

These numbers come from single criterion runs on one laptop (Intel Core Ultra 7
255U, 14 logical CPUs, no CPU pinning or frequency control). Repeated runs on
this machine vary by about ±8%, so treat them as indicative only.

### `fill_drain`: race to exhaustion, then release everything

Each round, every thread claims until denied, so all 1000 permits end up held.
The threads then wait at a barrier, release everything, and wait at a barrier
again. Times are per round and include the barrier cost, which both variants
pay.

| threads | speedticket | single atomic | speedup |
|--------:|------------:|--------------:|--------:|
| 4       | 23.6 µs     | 224 µs        | ~9.5×   |
| 6       | 31.2 µs     | 300 µs        | ~9.6×   |

### `steal`: pinned at exhaustion

Each thread claims until denied, gives back `churn` permits, and repeats, with
no barriers between threads. This forces claims onto the steal path, where
`speedticket` has to look at its peers' shards. Times are per round.

| threads | churn | speedticket | single atomic | ratio          |
|--------:|------:|------------:|--------------:|---------------:|
| 4       | 1     | 484 ns      | 399 ns        | ~1.2× slower   |
| 4       | 4     | 1.69 µs     | 2.75 µs       | ~1.6× faster   |
| 6       | 1     | 664 ns      | 698 ns        | about even     |
| 6       | 4     | 2.58 µs     | 5.00 µs       | ~1.9× faster   |

With a churn of 1, most claims end with a search of every peer that finds
nothing. That search reads one cache line per peer, while the single atomic
needs just one read, so this is `speedticket`'s worst case. Once peers have
permits to give (churn 4), sharding wins again, and by more as the thread count
grows.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
