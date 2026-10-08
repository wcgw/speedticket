# speedticket — design (v1)

A sharded, finite-budget semaphore. A fixed number of permits (`total`) is
partitioned across participants so the common case needs no cross-thread
coordination. There is no notion of time and no refill — permits are claimed
and released, nothing more.

## Core model

- **Finite budget, not a rate limit.** `total` permits exist. `claim` takes one,
  `release` returns one. Time is irrelevant.
- **Invariant:** `Σ(idle permits across all shards) + Σ(claimed permits) == total`,
  always.
- **Sharded per participant.** Each participant owns one shard (a counter of idle
  permits). The happy path (claim/release against your own shard) is uncontended.
- **Same-participant claim/release.** A permit is claimed and released on the same
  participant — enforced at compile time (see `Permit`). Shards therefore drift
  only via *steal* and *spilling*, and that drift is benign: a participant doing
  more work keeps more permits locally and steals less.
- **One reserve.** Besides the participants' shards, the pool has a *reserve*
  (slot 0): a shard no participant owns. It holds the whole budget at creation and
  keeps no share of its own. Spilling participants (see *Claiming*) release and
  bank into it, and a `Pool`'s threads without a participant of their own claim
  from it and release into it. Any thread may deposit into it.

## Types

- `Limit` — a participant handle. `Send` but `!Sync` (marker-enforced, e.g.
  `PhantomData<Cell<()>>`). You may move a participant to its thread; you may not
  share one participant across threads behind `&`/`Arc` and contend a single
  shard. This is a deliberate "one participant per thread" guardrail.
- `Permit<'a>` — RAII guard returned by a successful claim. Holds `&'a Limit` and
  returns one permit on `Drop`: to the owner's shard, or to the reserve while the
  owner spills. Because `&Limit` is `!Send`
  (as `Limit` is `!Sync`), `Permit` is automatically `!Send` — so claiming on one
  participant and releasing on another is a **compile error**, not a documented
  footgun.
- `AtCapacity` — zero-field error returned by `participant()`.

## Construction & membership

- `Limit::new(total: u64) -> Limit` — the first participant, which carves the
  whole budget out of the reserve, so it holds all `total`. Default capacity
  `max(available_parallelism(), 16)`.
- `Limit::with_capacity(total: u64, max_participants: NonZeroUsize) -> Limit` — explicit
  capacity.
- `Limit::participant(&self) -> Result<Limit, AtCapacity>` — registers a new
  participant and returns its handle. **Cold path, may take a join-lock.** It
  carves an even-ish share `≈ total / N` (N = new participant count), draining
  the reserve first, then taking *idle* permits from peers, spread across them to
  equalize (here the reserve is empty):

  ```
  total = 12
  new()            -> [12]
  participant()    -> [6, 6]        (newcomer steals 6)
  participant()    -> [4, 4, 4]     (newcomer steals 2 from each)
  ```

  Fairness is a **target, best-effort**: only *idle* permits can move, so if peers
  are busy and hold just 1 idle each, the newcomer starts with whatever it could
  gather (e.g. 2 instead of 4). Returns `Err(AtCapacity)` once the fixed array is
  full.
- **No `Clone` impl.** Registration has side effects (slot allocation + carving a
  share), so it is an explicit fallible method rather than `Clone`.
- `Drop` — deregisters the participant (frees its slot) and redistributes its
  remaining **idle** permits to peers. When the last participant drops, its idle
  permits go to the reserve; for a `Limit`, the pool is then gone with them.
  Permits that were **claimed but never released** at drop time are an accepted,
  documented leak (effective budget shrinks) — the single-counter representation
  does not track per-handle claimed counts. A `debug_assert` may warn on obvious
  misuse.

## Claiming — non-blocking only

- `try_claim(&self) -> Option<Permit<'_>>`.
- **Fast path:** a single uncontended atomic take on the owner's own shard, plus a
  non-atomic update of the participant's spill score.
- **Exhausted:** one **bounded pass** over peers, the reserve among them — rotating
  start `(self + 1) % N`, lock-free CAS, steal `max(1, victim_idle / 2)` from the
  peer with the most idle permits (the first in rotation order on a tie). If a
  racing claim empties it first, steal from the first other peer that has any. If
  the whole pass gathers nothing, return `None` (fail-fast). The pass takes from
  each peer at most once, so a claim can never spin. From the reserve, which is
  everyone's, a steal takes only a *reserve share*: `max(1, reserve_idle /
  participants)`, counting participants as for the fair share below.
- **Spilling:** a participant whose claims keep finding its own shard empty is
  churning near exhaustion: it releases into its shard, reclaims from it, then
  scans peers' shards that their owners keep writing, missing in cache on each.
  Each participant keeps a *spill score*: +2 for a claim that had to steal, +3 for
  one whose pass found nothing, −1 for one served by its own shard, so the score
  rises once more than a third of claims steal. At 16 the participant *spills*: it
  releases into the reserve, and a claim that finds its own shard empty takes
  from the reserve before stealing, banking the batch in the reserve. Spilling
  participants leave their shards quiet, so scans read them from cache, and they
  meet on the reserve, a single counter like a plain atomic semaphore's. Once the
  reserve holds a fair share (`total` divided by the participant slots up to the
  highest one ever occupied, and at least 2), the next such claim takes a reserve
  share home, and the participant stops spilling and resets its score. Taking a
  share rather than half drains the reserve evenly across everyone coming back to
  it. Spilling only changes where permits are deposited and looked for first,
  never how many exist.
- **Hard upper bound:** the number of simultaneously-claimed permits never exceeds
  `total`, under any race.
- **Best-effort lower bound:** a `claim` may *occasionally* return `None` even
  though a permit was momentarily idle-but-in-flight during the pass. This is the
  accepted price for a bounded, non-spinning steal; it self-corrects on retry.
  Over-admission is a real bug; a rare conservative deny is not.

## Concurrency properties

- **No deadlock** — a steal never holds a lock on more than one shard (it holds
  none; it is lock-free CAS). Deadlock is structurally impossible.
- **No livelock** — lock-freedom guarantees system-wide progress; the bounded
  single-pass + fail-fast bounds any individual claim.
- **Ping-pong damped** — stealing a *batch* (half the victim's idle) plus
  release-to-local means a thread does not instantly re-exhaust and steal back.
- **Spilling has hysteresis** — a participant spills only after a run of claims
  that mostly steal or miss, and returns only once the reserve holds a fair share,
  so it does not flip between modes on every claim.
- The rotating start offset spreads stealers across victims to cut CAS collisions;
  a strict global order is **not** required for correctness.

## Memory layout

- One fixed-capacity array of per-shard atomic counters: the reserve at slot 0,
  then one per participant slot.
- Each slot is **padded to a cache line** (`#[repr(align(64))]`, hand-rolled — no
  `crossbeam-utils` dependency) to eliminate false sharing between participants on
  different cores. Memory cost is trivial (tens of bytes × capacity).
- **One reserve, used adaptively** — permits live in participants' shards or in
  the reserve. Joining drains the reserve first, spilling participants release
  and bank into it, and the last participant to leave hands its idle permits to
  it. Carving, stealing, spilling and redistribution on leave all move permits
  between slots.

## Implementation notes (decided at coding time)

- `Relaxed` ordering on the counters: the limit gates admission but guards no
  cross-thread data (the `Permit` is `!Send`, so there is no cross-thread handoff
  to synchronize). Revisit if a permit ever needs to establish happens-before.
- `compare_exchange` loops for the underflow-safe take on one's own shard and for
  the steal from a peer's shard or the reserve.
- The spill state is a `Cell`: on the `Limit`, or, for a `Pool`, in the thread's
  thread-local state, for the pool it used last (switching pools resets it). Per
  thread either way, so it needs no atomics.
- The join-lock protects slot allocation / the occupancy registry on the cold
  `participant()` and `Drop` paths; the hot claim/release path takes no lock.

## Deferred (not v1)

- **Weighted / batch `claim(n)`** (all-or-nothing, with rollback on partial).
- A **blocking** waiting variant (the async one is below).

## Async waiting (`async` feature)

- `Pool::claim` / `claim_owned`: try, else **register** a waiter, **retry**,
  and only then sleep until notified; loop. Cancel safe: a cancelled waiter
  holds nothing, and passes on a notification it was sent.
- **Every deposit notifies**: releases, banking a stolen batch, a newcomer's
  carved share, and redistribution on leave. A waiter's retry can miss
  permits that are in flight between shards; whoever lands them wakes it.
- **No lost wakeups** (Dekker): a depositor adds with a `SeqCst` RMW, then
  `SeqCst`-loads the waiter count; a waiter increments the count, then
  `fence(SeqCst)`, then retries. Either the depositor sees the waiter, or the
  retry sees the deposit. `high_water` is raised (`SeqCst`) before a
  newcomer's share is deposited, so the retry also scans the new shard.
- **Hot path**: on x86 the `SeqCst` add costs what the `Relaxed` one did, and
  the count is a read-mostly line of its own; nobody waiting costs one load.
  The waiter queue (a mutex-guarded FIFO) is only touched when someone waits.
- Woken FIFO, but no hand-off: a woken waiter re-competes, so **no fairness**.
- Loom treats `SeqCst` accesses as `AcqRel`, so under `--cfg loom` the
  depositor side uses the equivalent `fence(SeqCst)`, which loom models.

