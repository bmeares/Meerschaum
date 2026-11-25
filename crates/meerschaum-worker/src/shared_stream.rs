//! One worker per connector for subscriptions that register a callback and return.
//! The engine registry owns the groups and their pool, including its team scope.

use meerschaum_connector::async_trait;
use meerschaum_core::SuccessTuple;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::{CallbackHandler, CallbackRequest, Worker, WorkerError, WorkerPool, WorkerResult};

/// Connector workers in one engine registry. The last subscription releases the worker.
#[derive(Clone)]
pub struct SharedStreamWorkers {
    pool: WorkerPool,
    groups: Arc<tokio::sync::Mutex<HashMap<String, Weak<Group>>>>,
}

impl SharedStreamWorkers {
    /// Bind the registry to its engine's worker pool.
    pub fn new(pool: WorkerPool) -> Self {
        Self {
            pool,
            groups: Default::default(),
        }
    }

    pub(crate) async fn join(
        &self,
        connector: &str,
        pipe_ref: String,
        handler: Arc<dyn CallbackHandler>,
    ) -> WorkerResult<Subscription> {
        let mut groups = self.groups.lock().await;
        groups.retain(|_, group| group.strong_count() > 0);
        let group = match groups.get(connector).and_then(Weak::upgrade) {
            Some(group) if group.worker.is_alive() => group,
            _ => {
                let worker = self.pool.spawn_worker().await?;
                if !worker
                    .hello()
                    .capabilities
                    .iter()
                    .any(|c| c == "stream_callbacks")
                {
                    return Err(WorkerError::Refused(
                        "Shared subscriptions require a worker with stream_callbacks support. Upgrade the worker or disable stream.shared_worker.".into()
                    ));
                }
                let routes = Arc::new(Routes::default());
                worker.set_callback_handler(Some(self.pool.guard_handler(routes.clone())));
                let group = Arc::new(Group {
                    worker,
                    routes,
                    next: AtomicU64::new(1),
                });
                groups.insert(connector.to_string(), Arc::downgrade(&group));
                group
            }
        };
        let id = group.next.fetch_add(1, Ordering::Relaxed);
        group
            .routes
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Route { pipe_ref, handler });
        Ok(Subscription { group, id })
    }
}

#[derive(Default)]
struct Routes(Mutex<HashMap<u64, Route>>);

#[derive(Clone)]
struct Route {
    pipe_ref: String,
    handler: Arc<dyn CallbackHandler>,
}

#[async_trait]
impl CallbackHandler for Routes {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        let handler = self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&request.call_id)
            .cloned();
        match handler {
            Some(route)
                if request
                    .pipe
                    .as_ref()
                    .is_some_and(|p| p.pipe_ref() == route.pipe_ref) =>
            {
                route.handler.handle(request).await
            }
            Some(_) => {
                SuccessTuple::fail("The callback pipe differs from its stream subscription.")
            }
            None => SuccessTuple::fail("This stream subscription has ended."),
        }
    }
}

struct Group {
    worker: Worker,
    routes: Arc<Routes>,
    next: AtomicU64,
}

pub(crate) struct Subscription {
    group: Arc<Group>,
    pub id: u64,
}

impl Subscription {
    pub fn worker(&self) -> &Worker {
        &self.group.worker
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.group
            .routes
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

impl std::fmt::Debug for SharedStreamWorkers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedStreamWorkers")
            .finish_non_exhaustive()
    }
}
