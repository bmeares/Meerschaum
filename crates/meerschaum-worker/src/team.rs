//! Isolated teams' worker pools (5.1).
//!
//! Python code that runs for an isolated team runs in a pool of that team's own: its workers are
//! started with the team's configuration only (the host builds their environment; see
//! `meerschaum_core::team_scope`), every call tells the worker which connectors plugin code may
//! look up, and every callback is checked against the team ([`crate::callback::team_refusal`]).
//!
//! - **Lazy.** A team's pool is created on its first call ([`TeamPools::get`]) and holds no
//!   worker until then.
//! - **Retired when the team changes.** A pool is keyed by the team and by a fingerprint of its
//!   configuration: a connector granted or revoked, a rename, a deletion or a rotated password
//!   changes the fingerprint, and the next call replaces the pool (its idle workers stop at once,
//!   busy ones when their call returns). A revoked connector is never served by a warm worker.
//! - **Bounded.** All team pools together keep at most [`TeamPools::limit`] live workers
//!   (`system.engine.python_team_workers`). A pool that needs a new worker beyond it stops the
//!   least recently used idle worker of another team; when none is idle it waits for a place,
//!   as a full shared pool queues its calls. A nested callback never waits (it fails at once).
//! - **Short-lived.** Team workers stop after `system.engine.python_worker_team_idle_seconds`
//!   idle (60 s by default). The shared pool is not affected by any of this.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{WorkerError, WorkerResult};
use crate::pool::{PoolConfig, WorkerPool};

/// How often a pool waiting for a place under the live limit looks for an idle worker of another
/// team to stop.
const EVICTION_RETRY: Duration = Duration::from_millis(250);

/// The limit on live workers shared by the team pools.
#[derive(Clone)]
pub struct LiveLimit {
    semaphore: Arc<Semaphore>,
    limit: usize,
    pools: Weak<Inner>,
}

impl std::fmt::Debug for LiveLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveLimit")
            .field("limit", &self.limit)
            .field("available", &self.semaphore.available_permits())
            .finish()
    }
}

impl LiveLimit {
    /// The most live workers.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// A place for a new worker of `requester`: free, else freed by stopping another team's
    /// idle worker, else (`wait`) the next one freed. Without `wait`, `Err` at once.
    pub async fn acquire(
        &self,
        requester: &WorkerPool,
        wait: bool,
    ) -> WorkerResult<OwnedSemaphorePermit> {
        let mut warned = false;
        loop {
            match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => return Ok(permit),
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Err(WorkerError::ShuttingDown);
                }
                Err(tokio::sync::TryAcquireError::NoPermits) => {}
            }
            if let Some(inner) = self.pools.upgrade()
                && evict_one(&inner, requester).await
            {
                continue;
            }
            if !wait {
                return Err(WorkerError::Protocol(format!(
                    "All {} team workers are busy (`system.engine.python_team_workers`).",
                    self.limit
                )));
            }
            if !warned {
                tracing::info!(
                    "All {} team workers are busy: waiting for one to finish \
                     (`system.engine.python_team_workers`).",
                    self.limit
                );
                warned = true;
            }
            tokio::select! {
                permit = self.semaphore.clone().acquire_owned() => {
                    return permit.map_err(|_| WorkerError::ShuttingDown);
                }
                () = tokio::time::sleep(EVICTION_RETRY) => {}
            }
        }
    }
}

struct Entry {
    fingerprint: String,
    pool: WorkerPool,
}

struct Inner {
    pools: Mutex<HashMap<String, Entry>>,
    semaphore: Arc<Semaphore>,
    limit: usize,
}

/// Stop the least recently used idle worker of a pool other than `requester`.
async fn evict_one(inner: &Inner, requester: &WorkerPool) -> bool {
    let mut candidates: Vec<WorkerPool> = inner
        .pools
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .map(|entry| entry.pool.clone())
        .filter(|pool| !pool.same(requester) && pool.idle_count() > 0)
        .collect();
    candidates.sort_by_key(|pool| pool.last_used());
    for pool in candidates {
        if pool.evict_idle_one().await {
            return true;
        }
    }
    false
}

/// How [`TeamPools::get`] found the team's pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup {
    /// The pool already served this configuration.
    Warm,
    /// The team had no pool.
    Created,
    /// The team's configuration changed: its old pool was retired.
    Replaced,
}

/// The isolated teams' pools of a process, under one live limit. Cloning shares them.
#[derive(Clone)]
pub struct TeamPools {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for TeamPools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeamPools")
            .field("limit", &self.inner.limit)
            .field("pools", &self.keys())
            .finish()
    }
}

impl TeamPools {
    /// Pools that together keep at most `limit` live workers (at least one).
    pub fn new(limit: usize) -> TeamPools {
        let limit = limit.max(1);
        TeamPools {
            inner: Arc::new(Inner {
                pools: Mutex::new(HashMap::new()),
                semaphore: Arc::new(Semaphore::new(limit)),
                limit,
            }),
        }
    }

    /// The most live workers of all team pools.
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    /// The live workers of all team pools (idle and busy, and those still stopping).
    pub fn live_workers(&self) -> usize {
        self.inner.limit - self.inner.semaphore.available_permits()
    }

    /// The limit the team pools' settings carry ([`PoolConfig::live`]).
    pub fn live_limit(&self) -> LiveLimit {
        LiveLimit {
            semaphore: self.inner.semaphore.clone(),
            limit: self.inner.limit,
            pools: Arc::downgrade(&self.inner),
        }
    }

    /// The pool of the team `key` for the configuration `fingerprint`: the warm one, else a new
    /// pool with the settings `make` returns (given the live limit). A pool of an older
    /// fingerprint is retired (shut down in the background).
    pub fn get(
        &self,
        key: &str,
        fingerprint: &str,
        make: impl FnOnce(LiveLimit) -> PoolConfig,
    ) -> (WorkerPool, Lookup) {
        let mut pools = self.inner.pools.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = pools.get(key)
            && entry.fingerprint == fingerprint
        {
            return (entry.pool.clone(), Lookup::Warm);
        }
        let pool = WorkerPool::new(make(self.live_limit()));
        let old = pools.insert(
            key.to_string(),
            Entry {
                fingerprint: fingerprint.to_string(),
                pool: pool.clone(),
            },
        );
        drop(pools);
        match old {
            Some(old) => {
                shutdown_in_background(old.pool);
                (pool, Lookup::Replaced)
            }
            None => (pool, Lookup::Created),
        }
    }

    /// The fingerprint the team `key`'s pool serves.
    pub fn fingerprint(&self, key: &str) -> Option<String> {
        self.inner
            .pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(|entry| entry.fingerprint.clone())
    }

    /// The teams with a pool.
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .inner
            .pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    }

    /// The team `key`'s pool, if it has one.
    pub fn pool(&self, key: &str) -> Option<WorkerPool> {
        self.inner
            .pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(|entry| entry.pool.clone())
    }

    /// Retire the team `key`'s pool (shut down in the background). `true` when it had one.
    pub fn retire(&self, key: &str) -> bool {
        let old = self
            .inner
            .pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
        match old {
            Some(entry) => {
                shutdown_in_background(entry.pool);
                true
            }
            None => false,
        }
    }

    /// Recycle every team pool (a plugin changed: the next call starts fresh interpreters).
    pub async fn recycle_all(&self) {
        let pools: Vec<WorkerPool> = self
            .inner
            .pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|entry| entry.pool.clone())
            .collect();
        for pool in pools {
            pool.recycle().await;
        }
    }

    /// Shut every team pool down.
    pub async fn shutdown(&self) {
        let pools: Vec<WorkerPool> = self
            .inner
            .pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, entry)| entry.pool)
            .collect();
        for pool in pools {
            pool.shutdown().await;
        }
    }
}

fn shutdown_in_background(pool: WorkerPool) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(async move { pool.shutdown().await });
        }
        // No runtime: the idle workers are dropped (killed) with the pool.
        Err(_) => drop(pool),
    }
}
