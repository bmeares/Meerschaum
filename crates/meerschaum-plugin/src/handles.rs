//! Plugin-side tables of open stream handles (subscriptions, log monitors).
//!
//! A handle is an opaque `u64` the host holds between calls. Each entry is locked on its own, so
//! a slow `next_batch` on one handle never blocks another handle or a regular plugin call.
use crate::{Result, error};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

pub(crate) struct Handles<T: ?Sized> {
    next: AtomicU64,
    open: Mutex<HashMap<u64, Arc<Mutex<Box<T>>>>>,
}

impl<T: ?Sized> Handles<T> {
    pub(crate) fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
            open: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn insert(&self, item: Box<T>) -> u64 {
        let handle = self.next.fetch_add(1, Ordering::Relaxed);
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(handle, Arc::new(Mutex::new(item)));
        handle
    }

    pub(crate) fn get(&self, handle: u64) -> Result<Arc<Mutex<Box<T>>>> {
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle)
            .cloned()
            .ok_or_else(|| error(format!("unknown or closed handle {handle}")))
    }

    pub(crate) fn remove(&self, handle: u64) -> Option<Arc<Mutex<Box<T>>>> {
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&handle)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.open.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// `context.handle` of a stream call.
pub(crate) fn handle_of(context: &serde_json::Value) -> Result<u64> {
    context["handle"]
        .as_u64()
        .ok_or_else(|| error("missing stream handle"))
}

/// `context.timeout_ms`, capped so a host thread is never parked for long.
pub(crate) fn timeout_of(context: &serde_json::Value) -> std::time::Duration {
    std::time::Duration::from_millis(context["timeout_ms"].as_u64().unwrap_or(250).min(60_000))
}
