# Swim caps: from one bucket to a bucket per door

A swimming pool holds only so many people. To enforce that, every swimmer must
wear a cap, taken from a bucket holding exactly as many caps as the pool has
room for. No cap left, no swim. Leave the water, put the cap back. That bucket
is a semaphore, and the caps are its permits.

[`speedticket`](README.md) is that bucket, rebuilt for a pool with many doors.
This is how it got there, one problem at a time.

## 1. One bucket

Every swimmer goes through the same bucket twice, in and out, and only one hand
fits in it at a time. With one door nobody notices. With eight, people queue at
the bucket for far longer than it takes to grab a cap: in our benchmark, taking
and returning a cap costs 4.55 ns for a lone thread, and 332 ns each once eight
threads share the bucket.[^bench]

In code: one atomic counter, and every core fighting over its cache line.

## 2. A bucket per door

So we split the caps: one bucket at each door (a thread, or *participant*),
each with an even share.

```
12 caps, one door      [12]
a second door opens    [6] [6]
a third                [4] [4] [4]
```

Swimmers take from their own door's bucket and return to it, so no two doors
reach into the same one. The same benchmark now stays between 4.5 and 4.9 ns
from one thread to eight. There are still only 12 caps, so the pool still
cannot overfill.

Only idle caps move when a door opens, since you cannot take one off a
swimmer's head. A door that closes hands its idle caps to the others.

## 3. An empty bucket: steal

Even shares are a guess. Door A gets busy and its bucket runs dry while caps
sit idle at B and C. The pool is not full, so turning the swimmer away would be
wrong. Instead the swimmer walks one lap around the pool and takes **half** of
another bucket: one cap to wear, the rest for A's bucket.

```
before   A [    ]   B [oooo]   C [oooo]
after    A [o   ]   B [oo  ]   C [oooo]   + one cap in the water
```

Half rather than one, so the next swimmers at A need no lap, and B keeps enough
not to come straight back for it. No circling: if every bucket is empty, the
swimmer is turned away (`try_claim` returns `None`). That has a price: a cap
being carried between two buckets is in neither, so now and then a swimmer is
turned away while the pool still has room. Trying again resolves it.

## 4. Steal from the fullest bucket

At first, the lap stopped at the first bucket with anything in it:

```
before         A [    ]   B [o   ]   C [oooo]
first found    A [    ]   B [    ]   C [oooo]   + one cap in the water
fullest        A [o   ]   B [o   ]   C [oo  ]   + one cap in the water
```

Taking B's only cap helps nobody: A banks nothing and B is left empty, so both
need a lap for their next swimmer. Now the swimmer looks into every bucket
first, then takes half of the fullest. If someone else empties that one in
between, the swimmer takes from the first bucket that still has a cap,
reaching into each at most once.

## 5. A nearly full pool: the bucket in the middle

One case had become worse than where we started. When the pool is nearly full,
every bucket is empty almost all the time. Each arrival walks a full lap,
mostly for nothing, and each returned cap lands in some door's bucket, where
the others have to go and find it. Pinned at capacity, eight threads took
1.38 µs per round where the single bucket of step 1 took 238 ns.[^bench]

So we added one more bucket, in the middle of the hall, belonging to no door:
the *reserve*. A door that keeps running dry starts to **spill**: its swimmers
return their caps to the middle bucket, and its arrivals look there before
walking a lap. Busy doors meet at one bucket, exactly as in step 1, but only
when that is the best anyone can do. Their own buckets stay empty and
untouched, which makes them cheap to check on a lap.

A door decides by keeping score: +2 when a swimmer had to steal, +3 when a lap
found nothing, −1 when its own bucket had a cap. At 16 it spills, so the odd
steal changes nothing. Once the middle bucket holds a fair share again (the
caps divided by the doors), the next arrival carries half of it home and the
door is back at step 2.

Same benchmark, after: 349 ns, against 236 ns for the single bucket.

## 6. From the middle bucket, take a share, not half

The middle bucket is everyone's. At first, a door coming back took half of it,
as it would from any other bucket, and the later ones were left to split the
rest. Now a door takes what is there divided by the number of doors, so the
bucket drains evenly across everyone coming back.

## What never changed

At every step, caps only move: bucket to head, head to bucket, bucket to
bucket. None is ever made or thrown away, so the caps in buckets, in the water
and being carried between buckets always add up to the pool's capacity. The
pool got faster to get into. It never got fuller.

| At the pool          | In `speedticket`                                              |
|----------------------|---------------------------------------------------------------|
| cap                  | permit                                                        |
| bucket               | shard: an atomic count of idle permits, on its own cache line |
| door                 | participant, one per thread                                   |
| a lap                | `steal`: one bounded, lock-free pass over the other shards    |
| bucket in the middle | the reserve                                                   |

## Credit

The swim caps are inspired by Valentin Deleplace's dotGo 2019 talk,
[*Semaphores*](https://www.youtube.com/watch?v=dPkQD1dczws).

[^bench]: Numbers are from the README's benchmark tables: single runs on one
    desktop, so indicative only. Step 5's 1.38 µs and 238 ns are from the
    README at commit `59110f8`, after step 4; the others are current. Step 6
    came after the last published run and has no numbers of its own.
