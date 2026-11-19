//! Bounded database snapshots backed by one still-held SQLite write transaction.
//! Only raw same-file reads are pinned; every caller/grant/config check still executes.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use meerschaum_kernel::Frame;

use super::{SqlDriver, SqlParam, SqlValue};
use crate::{Result, SqlError};

pub(super) const MAX_ROWS: usize = 4096;
pub(super) const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOTAL: usize = 16 * 1024 * 1024;
const MAX_QUERIES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FileIdentity {
    device: u64,
    inode: u64,
}

fn unavailable() -> SqlError {
    SqlError::InvalidArgument("Could not prove the locked SQLite authorization snapshot; retry after restoring the original database.".into())
}

/// Native SQLite read-only classification accepts metadata PRAGMAs without allowing a
/// configuration write or a second statement to enter the authorization snapshot.
#[allow(unsafe_code)]
pub(super) async fn read_only_statement(
    connection: &mut sqlx::SqliteConnection,
    sql: &str,
) -> Result<()> {
    let sql = std::ffi::CString::new(sql).map_err(|_| unavailable())?;
    let mut guard = connection.lock_handle().await?;
    // SAFETY: the prepared statement belongs to this synchronously locked live handle,
    // is never stepped here, and is finalized on every path before releasing the guard.
    unsafe {
        let mut statement = std::ptr::null_mut();
        let mut tail = std::ptr::null();
        let status = libsqlite3_sys::sqlite3_prepare_v3(
            guard.as_raw_handle().as_ptr(),
            sql.as_ptr(),
            -1,
            0,
            &mut statement,
            &mut tail,
        );
        if status != libsqlite3_sys::SQLITE_OK || statement.is_null() {
            if !statement.is_null() {
                libsqlite3_sys::sqlite3_finalize(statement);
            }
            return Err(unavailable());
        }
        let readonly = libsqlite3_sys::sqlite3_stmt_readonly(statement) == 1;
        libsqlite3_sys::sqlite3_finalize(statement);
        let remaining = if tail.is_null() {
            ""
        } else {
            std::ffi::CStr::from_ptr(tail)
                .to_str()
                .map_err(|_| unavailable())?
        };
        if !readonly || !remaining.trim().is_empty() {
            return Err(unavailable());
        }
    }
    Ok(())
}

/// HAS_MOVED is a supported VFS check. txn_state proves the real write transaction,
/// without relying on SQLite's optional debug-only LOCKSTATE file control.
#[cfg(unix)]
#[allow(unsafe_code)]
pub(super) async fn identity(
    connection: &mut sqlx::SqliteConnection,
    writing: bool,
) -> Result<FileIdentity> {
    use std::ffi::{CStr, OsStr};
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};
    let mut guard = connection.lock_handle().await?;
    // SAFETY: SQLx excludes its worker while LockedSqliteHandle lives. The pointer and
    // SQLite-owned filename are used synchronously and never escape this guard.
    let result = unsafe {
        let handle = guard.as_raw_handle().as_ptr();
        if writing
            && (libsqlite3_sys::sqlite3_get_autocommit(handle) != 0
                || libsqlite3_sys::sqlite3_txn_state(handle, c"main".as_ptr())
                    != libsqlite3_sys::SQLITE_TXN_WRITE)
        {
            return Err(unavailable());
        }
        let filename = libsqlite3_sys::sqlite3_db_filename(handle, c"main".as_ptr());
        if filename.is_null() {
            return Err(unavailable());
        }
        let path = std::path::Path::new(OsStr::from_bytes(CStr::from_ptr(filename).to_bytes()));
        if !path.is_absolute() {
            return Err(unavailable());
        }
        let before = std::fs::metadata(path).map_err(|_| unavailable())?;
        let mut moved = 1i32;
        let status = libsqlite3_sys::sqlite3_file_control(
            handle,
            c"main".as_ptr(),
            libsqlite3_sys::SQLITE_FCNTL_HAS_MOVED,
            (&mut moved as *mut i32).cast(),
        );
        let after = std::fs::metadata(path).map_err(|_| unavailable())?;
        if status != libsqlite3_sys::SQLITE_OK
            || moved != 0
            || !before.is_file()
            || !after.is_file()
            || before.dev() != after.dev()
            || before.ino() != after.ino()
        {
            return Err(unavailable());
        }
        FileIdentity {
            device: after.dev(),
            inode: after.ino(),
        }
    };
    Ok(result)
}

#[cfg(not(unix))]
pub(super) async fn identity(_: &mut sqlx::SqliteConnection, _: bool) -> Result<FileIdentity> {
    Err(SqlError::InvalidArgument(
        "Scoped SQLite writes require native file-identity verification on this platform.".into(),
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Batches,
    Value,
    Docs,
}

#[derive(Clone)]
pub(super) enum Output {
    Batches(Vec<RecordBatch>),
    Value(Option<SqlValue>),
    Docs(Frame),
}

struct Entry {
    sql: String,
    params: Vec<SqlParam>,
    mode: Mode,
    output: Output,
}
struct Data {
    frozen: bool,
    bytes: usize,
    entries: Vec<Entry>,
    bindings: Vec<(Arc<()>, FileIdentity)>,
}
struct Pin {
    file: FileIdentity,
    active: AtomicBool,
    data: Mutex<Data>,
}

/// Its drop invalidates every cloned handle before the transaction can return to its pool.
pub(super) struct Owner(Handle);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.0.active.store(false, Ordering::Release);
    }
}
impl Owner {
    pub(super) fn new(file: FileIdentity) -> Self {
        Self(Handle(Arc::new(Pin {
            file,
            active: AtomicBool::new(true),
            data: Mutex::new(Data {
                frozen: false,
                bytes: 0,
                entries: Vec::new(),
                bindings: Vec::new(),
            }),
        })))
    }
    pub(super) fn handle(&self) -> Handle {
        self.0.clone()
    }
    pub(super) fn verify(&self, file: &FileIdentity) -> Result<()> {
        if !self.0.0.active.load(Ordering::Acquire) || self.0.0.file != *file {
            return Err(unavailable());
        }
        Ok(())
    }
    pub(super) fn freeze(&self) {
        self.0
            .0
            .data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .frozen = true;
    }
}

/// Process-local evidence, never serialized or selected from pipe/environment properties.
#[derive(Clone)]
pub struct Handle(Arc<Pin>);

tokio::task_local! { static CURRENT: Handle; }

/// Whether an authorization snapshot pin is active for this task's live recheck.
/// Callers with a time-based in-memory cache skip it so the pre-freeze and post-freeze
/// rechecks issue the exact same query set even across a cache TTL boundary.
pub(crate) fn active() -> bool {
    meerschaum_connector::mutation::rechecking() && CURRENT.try_with(|_| ()).is_ok()
}

impl Handle {
    pub(super) async fn verify_read(&self, connection: &mut sqlx::SqliteConnection) -> Result<()> {
        self.alive()?;
        if identity(connection, false).await? != self.0.file {
            return Err(unavailable());
        }
        Ok(())
    }
    /// Keep the same write-backed proof through trusted frame callbacks. A driver checks
    /// this context only inside an actual original-authority recheck, never for data writes.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        Box::pin(CURRENT.scope(self.clone(), future)).await
    }
    pub(super) fn frozen(&self) -> bool {
        self.0.data.lock().unwrap_or_else(|e| e.into_inner()).frozen
    }
    fn alive(&self) -> Result<()> {
        if self.0.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
    pub(super) fn get(&self, sql: &str, params: &[SqlParam], mode: Mode) -> Result<Option<Output>> {
        self.alive()?;
        let data = self.0.data.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = data
            .entries
            .iter()
            .find(|entry| entry.mode == mode && entry.sql == sql && entry.params == params)
        {
            return Ok(Some(entry.output.clone()));
        }
        if data.frozen {
            return Err(SqlError::InvalidArgument("A new database authorization query appeared after mutation began; refusing to use an unproven read.".into()));
        }
        Ok(None)
    }
    pub(super) fn store(
        &self,
        sql: &str,
        params: &[SqlParam],
        mode: Mode,
        output: Output,
        bytes: usize,
    ) -> Result<Output> {
        self.alive()?;
        let mut data = self.0.data.lock().unwrap_or_else(|e| e.into_inner());
        let size = sql
            .len()
            .saturating_add(format!("{params:?}").len())
            .saturating_add(bytes);
        if data.frozen
            || data.entries.len() >= MAX_QUERIES
            || size > MAX_BYTES
            || data.bytes.saturating_add(size) > MAX_TOTAL
        {
            return Err(SqlError::InvalidArgument(
                "The locked database authorization snapshot exceeds its bound.".into(),
            ));
        }
        data.bytes += size;
        data.entries.push(Entry {
            sql: sql.into(),
            params: params.to_vec(),
            mode,
            output: output.clone(),
        });
        Ok(output)
    }
}

/// Called before any pool helper spawns: task locals are never assumed to cross spawn.
pub(super) async fn route(driver: &SqlDriver) -> Result<Option<Handle>> {
    if !meerschaum_connector::mutation::rechecking() {
        return Ok(None);
    }
    let Some(handle) = CURRENT.try_with(Clone::clone).ok() else {
        return Ok(None);
    };
    handle.alive()?;
    if !matches!(driver.pool, super::Pool::Sqlite(_)) {
        return Ok(None);
    }
    let known = {
        let data = handle.0.data.lock().unwrap_or_else(|e| e.into_inner());
        data.bindings
            .iter()
            .find(|(id, _)| Arc::ptr_eq(id, &driver.lineage))
            .map(|(_, file)| file.clone())
    };
    let file = if let Some(file) = known {
        file
    } else {
        let super::Pool::Sqlite(pool) = &driver.pool else {
            return Ok(None);
        };
        let mut connection =
            tokio::time::timeout(std::time::Duration::from_secs(2), pool.acquire())
                .await
                .map_err(|_| unavailable())??;
        let file = identity(&mut connection, false).await?;
        let mut data = handle.0.data.lock().unwrap_or_else(|e| e.into_inner());
        if data.bindings.len() >= MAX_QUERIES {
            return Err(unavailable());
        }
        data.bindings.push((driver.lineage.clone(), file.clone()));
        file
    };
    Ok((file == handle.0.file).then_some(handle))
}

pub(super) async fn read(
    driver: &SqlDriver,
    sql: &str,
    params: &[SqlParam],
    mode: Mode,
) -> Result<Option<Output>> {
    if let Some(output) = super::mysql_authority::read(driver, sql, params, mode).await? {
        return Ok(Some(output));
    }
    let Some(handle) = route(driver).await? else {
        return Ok(None);
    };
    if let Some(output) = handle.get(sql, params, mode)? {
        return Ok(Some(output));
    }
    if sql.len() > 1024 * 1024 || params.len() > 4096 {
        return Err(unavailable());
    }
    let pool = driver
        .snapshot_sqlite_pool
        .as_ref()
        .ok_or_else(unavailable)?;
    let mut connection = pool.acquire().await?;
    if identity(&mut connection, false).await? != handle.0.file {
        return Err(unavailable());
    }
    let (output, bytes) =
        super::backend::authorization_read(&mut connection, sql, params, mode).await?;
    Ok(Some(handle.store(sql, params, mode, output, bytes)?))
}
