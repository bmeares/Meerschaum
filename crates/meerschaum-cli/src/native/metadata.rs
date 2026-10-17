//! Bounded metadata reads for known native SQL connectors. Other connectors retain their
//! serial call path, including custom Python connectors whose worker may serialize calls.

use std::future::{Future, poll_fn};
use std::task::Poll;

use super::Instance;
use crate::value::ArgsMap;

pub(super) fn concurrency(instance: &Instance, args: &ArgsMap) -> usize {
    let Some(sql) = instance.sql().filter(|sql| sql.is_thread_safe()) else {
        return 1;
    };
    let requested = super::int(args, "workers")
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(4);
    requested.min(sql.spec().max_connections() as usize).max(1)
}

/// Poll at most `limit` reads at once, retaining input order and every read's result.
/// Reads stay in the caller's task, so borrowed queries and connector state need no copies
/// or extra worker processes. An individual `Err` is a result, not a reason to cancel peers.
pub(super) async fn ordered<T, F, Fut>(items: Vec<T>, limit: usize, read: F) -> Vec<Fut::Output>
where
    F: Fn(T) -> Fut,
    Fut: Future,
{
    if limit <= 1 {
        let mut results = Vec::with_capacity(items.len());
        for item in items {
            results.push(read(item).await);
        }
        return results;
    }
    let mut results: Vec<_> = (0..items.len()).map(|_| None).collect();
    let mut queued = items.into_iter().enumerate();
    let mut active = Vec::new();
    loop {
        while active.len() < limit {
            let Some((index, item)) = queued.next() else {
                break;
            };
            active.push((index, Box::pin(read(item))));
        }
        if active.is_empty() {
            break;
        }
        let (slot, value) = poll_fn(|cx| {
            for (slot, (_, future)) in active.iter_mut().enumerate() {
                if let Poll::Ready(value) = future.as_mut().poll(cx) {
                    return Poll::Ready((slot, value));
                }
            }
            Poll::Pending
        })
        .await;
        let (index, _) = active.swap_remove(slot);
        results[index] = Some(value);
    }
    results
        .into_iter()
        .map(|result| result.expect("every metadata read completed"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Semaphore, mpsc};

    #[tokio::test]
    async fn bounds_reads_preserves_order_and_keeps_reading_after_failure() {
        for limit in [1, 2] {
            let gates: Arc<Vec<_>> = Arc::new((0..4).map(|_| Semaphore::new(0)).collect());
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let (started, mut starts) = mpsc::unbounded_channel();
            let reads = tokio::spawn(ordered((0..4).collect(), limit, {
                let gates = gates.clone();
                let active = active.clone();
                let peak = peak.clone();
                move |index| {
                    let gates = gates.clone();
                    let active = active.clone();
                    let peak = peak.clone();
                    let started = started.clone();
                    async move {
                        let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(count, Ordering::SeqCst);
                        started.send(index).unwrap();
                        gates[index].acquire().await.unwrap().forget();
                        active.fetch_sub(1, Ordering::SeqCst);
                        if index == 1 {
                            Err("missing metadata")
                        } else {
                            Ok(index)
                        }
                    }
                }
            }));
            assert_eq!(starts.recv().await, Some(0));
            if limit == 2 {
                assert_eq!(starts.recv().await, Some(1));
                assert!(starts.try_recv().is_err());
                // Finish the second read first, including a normal connector failure.
                gates[1].add_permits(1);
                assert_eq!(starts.recv().await, Some(2));
                gates[2].add_permits(1);
                assert_eq!(starts.recv().await, Some(3));
                gates[0].add_permits(1);
                gates[3].add_permits(1);
            } else {
                assert!(starts.try_recv().is_err());
                for index in 0..4 {
                    gates[index].add_permits(1);
                    if index < 3 {
                        assert_eq!(starts.recv().await, Some(index + 1));
                        assert!(starts.try_recv().is_err());
                    }
                }
            }
            let results = tokio::time::timeout(Duration::from_secs(5), reads)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(results, vec![Ok(0), Err("missing metadata"), Ok(2), Ok(3)]);
            assert_eq!(peak.load(Ordering::SeqCst), limit);
        }
    }

    #[tokio::test]
    async fn respects_sql_capacity_explicit_serial_and_unsupported_connectors() {
        use crate::value::PyValue;
        use meerschaum_sql::uri::{ConnectionSpec, ConnectorAttributes};
        use meerschaum_sql::{SqlConnector, flavor::Flavor};

        let root = tempfile::tempdir().unwrap();
        let attrs = ConnectorAttributes {
            flavor: Some(Flavor::Sqlite),
            database: Some(
                root.path()
                    .join("metadata.db")
                    .to_string_lossy()
                    .into_owned(),
            ),
            pool_size: Some(2),
            max_overflow: Some(0),
            ..Default::default()
        };
        let sql = Arc::new(
            SqlConnector::connect("metadata", ConnectionSpec::from_attributes(&attrs).unwrap())
                .await
                .unwrap(),
        );
        let native = Instance::Sql(sql.clone());
        let mut args = ArgsMap::new();
        assert_eq!(concurrency(&native, &args), 2);
        args.insert("workers".into(), PyValue::Int(1));
        assert_eq!(concurrency(&native, &args), 1);
        args.insert("workers".into(), PyValue::Int(100));
        assert_eq!(concurrency(&native, &args), 2);
        // Even a thread-safe object in the custom/other slot is deliberately serial.
        assert_eq!(concurrency(&Instance::Other(sql), &args), 1);
        let memory_attrs = ConnectorAttributes {
            database: Some(":memory:".into()),
            ..attrs
        };
        let memory = Arc::new(
            SqlConnector::connect(
                "memory",
                ConnectionSpec::from_attributes(&memory_attrs).unwrap(),
            )
            .await
            .unwrap(),
        );
        assert_eq!(concurrency(&Instance::Sql(memory), &args), 1);
    }
}
