//! A single participant's counter of idle permits.

use std::sync::atomic::Ordering::{self, Relaxed};

use crate::sync::AtomicU64;

// `Relaxed` throughout: the counters gate admission but guard no cross-thread
// data (a `Permit` is `!Send`, so there is no handoff to synchronize). The
// exception is `give` with the `async` feature; see `GIVE`.

/// How [`Shard::give`] deposits permits. With the `async` feature a waiter's
/// fence must order every deposit, which takes a `SeqCst` read-modify-write
/// (see `wait.rs`); on x86 the locked add costs the same either way.
#[cfg(feature = "async")]
const GIVE: Ordering = Ordering::SeqCst;
#[cfg(not(feature = "async"))]
const GIVE: Ordering = Relaxed;

/// Idle-permit counter padded to its own cache line, so participants on
/// different cores never false-share.
#[derive(Debug)]
#[repr(align(64))]
pub(crate) struct Shard {
    idle: AtomicU64,
}

impl Shard {
    pub(crate) fn new(idle: u64) -> Self {
        Self {
            idle: AtomicU64::new(idle),
        }
    }

    pub(crate) fn idle(&self) -> u64 {
        self.idle.load(Relaxed)
    }

    /// Atomically takes `amount(idle)` permits, clamped to what is idle, and
    /// returns how many were taken. `amount` is only consulted while the shard
    /// has idle permits, and is re-evaluated if a concurrent update wins the
    /// race. Never underflows.
    pub(crate) fn take(&self, amount: impl Fn(u64) -> u64) -> u64 {
        let mut idle = self.idle.load(Relaxed);
        loop {
            if idle == 0 {
                return 0;
            }
            let n = amount(idle).min(idle);
            if n == 0 {
                return 0;
            }
            match self
                .idle
                .compare_exchange_weak(idle, idle - n, Relaxed, Relaxed)
            {
                Ok(_) => return n,
                Err(actual) => idle = actual,
            }
        }
    }

    /// Returns `n` permits to this shard.
    pub(crate) fn give(&self, n: u64) {
        if n > 0 {
            self.idle.fetch_add(n, GIVE);
        }
    }

    /// Takes every idle permit, leaving the shard empty.
    pub(crate) fn drain(&self) -> u64 {
        self.idle.swap(0, Relaxed)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn is_cache_line_sized() {
        assert_eq!(std::mem::align_of::<Shard>(), 64);
        assert_eq!(std::mem::size_of::<Shard>(), 64);
    }

    #[test]
    fn take_clamps_to_idle() {
        let shard = Shard::new(3);
        assert_eq!(shard.take(|_| 10), 3);
        assert_eq!(shard.idle(), 0);
    }

    #[test]
    fn take_on_empty_does_not_consult_amount() {
        let shard = Shard::new(0);
        assert_eq!(shard.take(|_| unreachable!()), 0);
    }

    #[test]
    fn take_zero_leaves_shard_untouched() {
        let shard = Shard::new(5);
        assert_eq!(shard.take(|_| 0), 0);
        assert_eq!(shard.idle(), 5);
    }

    #[test]
    fn give_and_drain() {
        let shard = Shard::new(0);
        shard.give(4);
        shard.give(0);
        assert_eq!(shard.drain(), 4);
        assert_eq!(shard.idle(), 0);
    }
}
