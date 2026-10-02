//! Parking for claims that found the pool exhausted (`async` feature).
//!
//! A claimer that comes up empty [registers](WaitQueue::register), retries
//! its claim, and only then waits. Every deposit of permits into a shard is
//! followed by [`WaitQueue::notify`]. The two sides pair up as in Dekker's
//! algorithm, so a waiter never sleeps through permits that were deposited
//! while it registered:
//!
//! - a depositor adds to a shard with a `SeqCst` read-modify-write, then
//!   reads [`WaitQueue::pending`] with a `SeqCst` load;
//! - a waiter adds to `pending`, then issues a `SeqCst` fence, then retries,
//!   reading the shards.
//!
//! If the depositor's load misses the waiter, the deposit precedes the
//! waiter's fence in the single total order of `SeqCst` operations, so the
//! retry sees it. Otherwise the depositor sees the waiter, and wakes it.
//!
//! Waiters are woken in the order they registered, but a woken waiter must
//! still win its claim against everyone else: there is no hand-off, and so
//! no fairness guarantee.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::PoisonError;
use std::sync::atomic::Ordering::SeqCst;
use std::task::{Context, Poll, Waker};

use crate::sync::{Arc, AtomicUsize, Mutex, MutexGuard, fence};

/// Wakers collected under the lock and woken after it is released, at most
/// this many at a time.
const WAKE_BATCH: usize = 32;

/// The waiters parked on one pool.
///
/// Aligned to a cache line of its own: `pending` is read after every
/// deposit, and must not share a line with anything written more often.
#[repr(align(64))]
pub(crate) struct WaitQueue {
    /// Waiters registered and not yet notified.
    pending: AtomicUsize,
    /// Those waiters, oldest first.
    queue: Mutex<VecDeque<Arc<Waiter>>>,
}

/// One registered waiter.
#[derive(Default)]
struct Waiter {
    state: Mutex<WaiterState>,
}

#[derive(Default)]
struct WaiterState {
    notified: bool,
    waker: Option<Waker>,
}

/// A registration in a [`WaitQueue`]; awaiting it waits to be notified.
///
/// Dropping it deregisters the waiter. If it was notified but not yet
/// awaited to completion, the notification is passed on to the next waiter,
/// so that cancelling a wait cannot swallow one meant for someone else.
#[must_use = "a registration does nothing unless awaited"]
pub(crate) struct Wait<'a> {
    queue: &'a WaitQueue,
    waiter: Arc<Waiter>,
    /// Set once the wait completes: the notification has been used up.
    done: bool,
}

impl WaitQueue {
    pub(crate) fn new() -> Self {
        Self {
            pending: AtomicUsize::new(0),
            queue: Mutex::new(VecDeque::new()),
        }
    }

    /// Registers a waiter. The caller must retry its claim after this and
    /// before awaiting the returned [`Wait`]: only permits deposited after
    /// that retry are guaranteed to wake it.
    pub(crate) fn register(&self) -> Wait<'_> {
        let waiter = Arc::new(Waiter::default());
        // Clone needed: the queue and the registration both hold the waiter.
        self.lock().push_back(Arc::clone(&waiter));
        self.pending.fetch_add(1, SeqCst);
        // Pairs with the depositor's `SeqCst` add and load; see the module
        // docs.
        fence(SeqCst);
        Wait {
            queue: self,
            waiter,
            done: false,
        }
    }

    /// Wakes up to `n` waiters, after `n` permits were deposited with a
    /// `SeqCst` read-modify-write. Costs one load when nobody waits.
    #[inline]
    pub(crate) fn notify(&self, n: u64) {
        if n == 0 {
            return;
        }
        // Loom treats `SeqCst` accesses as `AcqRel`, so model the deposit
        // and the load below with the fence they are equivalent to here.
        #[cfg(loom)]
        fence(SeqCst);
        if self.pending.load(SeqCst) == 0 {
            return;
        }
        self.notify_slow(usize::try_from(n).unwrap_or(usize::MAX));
    }

    #[cold]
    #[inline(never)]
    fn notify_slow(&self, mut n: usize) {
        while n > 0 {
            let mut wakers: [Option<Waker>; WAKE_BATCH] = Default::default();
            let mut woken = 0;
            {
                let mut queue = self.lock();
                while woken < WAKE_BATCH && n > 0 {
                    let Some(waiter) = queue.pop_front() else {
                        break;
                    };
                    self.pending.fetch_sub(1, SeqCst);
                    let mut state = waiter.lock();
                    state.notified = true;
                    wakers[woken] = state.waker.take();
                    woken += 1;
                    n -= 1;
                }
            }
            // Woken outside the lock: a waker may run arbitrary code.
            wakers.into_iter().flatten().for_each(Waker::wake);
            if woken < WAKE_BATCH {
                // The queue ran dry.
                return;
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<Arc<Waiter>>> {
        // Critical sections only push, pop and remove whole entries; a panic
        // in one cannot leave the queue inconsistent.
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Waiter {
    fn lock(&self) -> MutexGuard<'_, WaiterState> {
        // As for the queue: a panic cannot leave the state inconsistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Future for Wait<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.waiter.lock();
        if state.notified {
            drop(state);
            self.done = true;
            return Poll::Ready(());
        }
        match &mut state.waker {
            Some(waker) if waker.will_wake(cx.waker()) => {}
            waker => *waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut queue = self.queue.lock();
        let queued = queue
            .iter()
            .position(|waiter| Arc::ptr_eq(waiter, &self.waiter));
        if let Some(index) = queued {
            // Never notified: just leave.
            drop(queue.remove(index));
            self.queue.pending.fetch_sub(1, SeqCst);
            return;
        }
        // Gone from the queue, so a notifier popped it, and marked it
        // notified under the queue lock: hand that notification on.
        drop(queue);
        debug_assert!(self.waiter.lock().notified);
        self.queue.notify_slow(1);
    }
}
