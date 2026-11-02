//! Password and token-secret hashing (PBKDF2) off the async threads, a bounded number at a time.
//! On an async thread a hash stalls every task polled there; unbounded, a burst of logins would
//! occupy the whole blocking pool. The synchronous functions in `meerschaum_core::users` stay
//! the primitives.

use std::sync::{Arc, LazyLock};

use meerschaum_core::users::{Token, User, hash_password, verify_password};
use tokio::sync::Semaphore;

use crate::traits::{ConnectorError, ConnectorResult};

/// Hashes and verifications running at once: one per CPU, at most 8.
fn limit() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(8)
}

static PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(limit())));

/// Run `work` on Tokio's blocking pool once a permit is free. The permit moves into the blocking
/// task, so a caller that stops waiting frees it only when `work` has finished.
async fn bounded<T: Send + 'static>(
    permits: &Arc<Semaphore>,
    work: impl FnOnce() -> T + Send + 'static,
) -> ConnectorResult<T> {
    let permit =
        permits.clone().acquire_owned().await.map_err(|e| {
            ConnectorError::Backend(format!("Password hashing is unavailable: {e}"))
        })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|e| ConnectorError::Backend(format!("Password hashing did not finish: {e}")))
}

/// `hash_password(password, None, None)`.
pub async fn hash(password: &str) -> ConnectorResult<String> {
    let password = password.to_string();
    bounded(&PERMITS, move || hash_password(&password, None, None))
        .await?
        .map_err(|e| ConnectorError::Backend(e.to_string()))
}

/// `Token::hash_secret(secret)`.
pub async fn hash_token_secret(secret: &str) -> ConnectorResult<String> {
    let secret = secret.to_string();
    bounded(&PERMITS, move || Token::hash_secret(&secret))
        .await?
        .map_err(|e| ConnectorError::Backend(e.to_string()))
}

/// `verify_password(password, hash)`.
pub async fn verify(password: &str, hash: &str) -> ConnectorResult<bool> {
    let (password, hash) = (password.to_string(), hash.to_string());
    bounded(&PERMITS, move || verify_password(&password, &hash))
        .await?
        .map_err(|e| ConnectorError::Backend(e.to_string()))
}

/// `user.get_password_hash()`: the cached hash, or a new hash of the password, cached.
pub async fn user_password_hash(user: &mut User) -> ConnectorResult<String> {
    if let Some(hash) = &user.password_hash {
        return Ok(hash.clone());
    }
    let hash = hash(&user.password).await?;
    user.password_hash = Some(hash.clone());
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    #[test]
    fn the_limit_is_one_per_cpu_up_to_eight() {
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(limit(), cpus.min(8));
        assert!((1..=8).contains(&limit()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_work_never_exceeds_its_permits() {
        let permits = Arc::new(Semaphore::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicUsize::new(0));
        let calls: Vec<_> = (0..6)
            .map(|_| {
                let (permits, active, peak, done) =
                    (permits.clone(), active.clone(), peak.clone(), done.clone());
                tokio::spawn(async move {
                    bounded(&permits, move || {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(20));
                        active.fetch_sub(1, Ordering::SeqCst);
                        done.fetch_add(1, Ordering::SeqCst);
                    })
                    .await
                })
            })
            .collect();
        for call in calls {
            call.await.unwrap().unwrap();
        }
        assert_eq!(done.load(Ordering::SeqCst), 6);
        assert!(peak.load(Ordering::SeqCst) <= 2, "{peak:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_caller_keeps_its_permit_until_the_work_ends() {
        let permits = Arc::new(Semaphore::new(1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let caller = {
            let permits = permits.clone();
            tokio::spawn(async move {
                bounded(&permits, move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv();
                })
                .await
            })
        };
        started_rx.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(permits.clone().try_acquire_owned().is_err());
        release_tx.send(()).unwrap();
        let freed = tokio::time::timeout(Duration::from_secs(30), permits.clone().acquire_owned());
        assert!(freed.await.is_ok());
    }

    #[tokio::test]
    async fn a_panic_in_the_work_is_an_error() {
        let permits = Arc::new(Semaphore::new(1));
        let result = bounded(&permits, || {
            panic!("refused");
        })
        .await;
        assert!(
            matches!(result, Err(ConnectorError::Backend(_))),
            "{result:?}"
        );
        assert!(permits.clone().try_acquire_owned().is_ok());
    }

    #[tokio::test]
    async fn a_hash_is_cached_on_the_user_and_verifies() {
        let mut user = User::new("passwords_cache", Some("test1234"));
        let first = user_password_hash(&mut user).await.unwrap();
        assert_eq!(user.password_hash.as_deref(), Some(first.as_str()));
        assert_eq!(user_password_hash(&mut user).await.unwrap(), first);
        assert!(verify("test1234", &first).await.unwrap());
        assert!(!verify("wrong", &first).await.unwrap());
    }

    /// Measure ticker gaps from before `work` starts until after it finishes.
    async fn longest_stall<F: std::future::Future>(work: F) -> (Duration, F::Output) {
        let ticks = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let ticker = tokio::spawn({
            let ticks = ticks.clone();
            async move {
                let mut ready = Some(ready_tx);
                let mut interval = tokio::time::interval(Duration::from_millis(10));
                loop {
                    interval.tick().await;
                    ticks.lock().unwrap().push(Instant::now());
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(());
                    }
                }
            }
        });
        ready_rx.await.unwrap();
        let output = work.await;
        let seen = ticks.lock().unwrap().len();
        let recorded = || ticks.lock().unwrap().len();
        tokio::time::timeout(Duration::from_secs(10), async {
            while recorded() <= seen {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the ticker stopped");
        ticker.abort();
        let ticks = ticks.lock().unwrap().clone();
        let longest = ticks.windows(2).map(|w| w[1] - w[0]).max().unwrap();
        (longest, output)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hashing_and_verifying_never_stall_the_runtime() {
        let started = Instant::now();
        let reference = hash_password("calibration", None, None).unwrap();
        let one_hash = started.elapsed();
        let (stall, hashed) = longest_stall(hash("calibration")).await;
        assert!(hashed.is_ok(), "{hashed:?}");
        assert!(
            stall < one_hash / 2,
            "a {one_hash:?} hash stalled the runtime for {stall:?}"
        );
        let (stall, verified) = longest_stall(verify("calibration", &reference)).await;
        assert!(matches!(verified, Ok(true)), "{verified:?}");
        assert!(
            stall < one_hash / 2,
            "a {one_hash:?} check stalled the runtime for {stall:?}"
        );
    }
}
