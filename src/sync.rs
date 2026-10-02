//! Synchronization primitives, swapped for loom's model-checked versions when
//! built with `--cfg loom`.

#[cfg(loom)]
pub(crate) use loom::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU64, AtomicUsize},
};
#[cfg(loom)]
pub(crate) use loom::thread_local;
#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU64, AtomicUsize},
};
#[cfg(not(loom))]
pub(crate) use std::thread_local;
