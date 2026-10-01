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

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
