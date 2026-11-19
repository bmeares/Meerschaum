//! Private, bounded coordination between main API workers.

use std::path::PathBuf;
use std::time::{Duration, Instant};

/// No private paths or values are included in storage errors.
#[derive(Debug, thiserror::Error)]
#[error("Private execution authority storage is unavailable or busy.")]
pub struct StorageError;

/// Fixed lock stripes bound storage without unlinking a lock another worker may still hold.
/// All waiting happens off the async workers, with a deadline; cancellation drops the lock.
pub async fn lock(directory: PathBuf, identity: uuid::Uuid) -> Result<std::fs::File, StorageError> {
    tokio::task::spawn_blocking(move || {
        meerschaum_core::team_scope::create_private_dir(&directory).map_err(|_| StorageError)?;
        let stripe = u16::from_be_bytes([identity.as_bytes()[0], identity.as_bytes()[1]]) % 1024;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join(format!("{stripe:04x}.lock")))
            .map_err(|_| StorageError)?;
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(std::fs::TryLockError::WouldBlock) if Instant::now() < until => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                _ => return Err(StorageError),
            }
        }
    })
    .await
    .map_err(|_| StorageError)?
}
