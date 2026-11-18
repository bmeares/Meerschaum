//! Private, bounded native source snapshots. Client cursors are map keys, never paths.
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use meerschaum_connector_api::source_fetch::{self, Cursor, PageResponse, Response};
use meerschaum_core::team_scope::{create_private_dir, write_private};
use meerschaum_server::backends::SourceFetchError;
use meerschaum_server::routes::source_fetch::Binding;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(crate) const MAX_SNAPSHOT_BYTES: usize = 512 * 1024 * 1024;
const QUOTA_UNIT: usize = 1024;
const GLOBAL_UNITS: usize = 2 * 1024 * 1024;
const MAX_PAGES: usize = 10_000;
const RECEIPTS: usize = 128;
const RECEIPT_UNITS: u32 = 128; // 128KiB reserved from the same global byte allowance.
const MARKER: &str = "mrsm-native-source-snapshot-v1";
const IDLE: Duration = Duration::from_secs(30);
const LIFETIME: Duration = Duration::from_secs(300);

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Root {
    _lock: std::fs::File,
}

struct Entry {
    directory: Directory,
    _slot: OwnedSemaphorePermit,
    _bytes: Vec<OwnedSemaphorePermit>,
    owner: String,
    fingerprint: String,
    initial_credential: String,
    last_credential: String,
    schema: String,
    configuration: String,
    query: String,
    prepared: Option<Box<dyn std::any::Any + Send + Sync>>,
    receipt: Mutex<Option<ReceiptReservation>>,
    pages: usize,
    next: u32,
    created: Instant,
    touched: Instant,
}
impl Entry {
    fn expired(&self) -> bool {
        self.created.elapsed() >= LIFETIME || self.touched.elapsed() >= IDLE
    }
}

struct ReceiptReservation {
    _bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}
struct Receipt {
    owner: String,
    credential: String,
    request: String,
    configuration: String,
    result: meerschaum_core::success::SyncResult,
    created: Instant,
    touched: Instant,
    _reservation: ReceiptReservation,
}
impl Receipt {
    fn expired(&self) -> bool {
        self.created.elapsed() >= LIFETIME || self.touched.elapsed() >= IDLE
    }
}
struct Running {
    owner: String,
    request: String,
    configuration: String,
    created: Instant,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    changed: tokio::sync::watch::Sender<bool>,
}
#[derive(Default)]
struct Operations {
    receipts: BTreeMap<Uuid, Receipt>,
    running: BTreeMap<Uuid, Running>,
}
struct RunningGuard {
    store: Weak<Store>,
    id: Uuid,
}
impl Drop for RunningGuard {
    fn drop(&mut self) {
        if let Some(store) = self.store.upgrade() {
            let operation = store
                .operations
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .running
                .remove(&self.id);
            if let Some(operation) = operation {
                let _ = operation.changed.send(true);
            }
        }
    }
}
fn configuration_digest(configuration: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(configuration.as_bytes()))
}

/// One backend owns this private root, its quota and cleanup lock.
pub(crate) struct Store {
    path: PathBuf,
    root: OnceLock<Result<Root, SourceFetchError>>,
    entries: Mutex<BTreeMap<Uuid, Entry>>,
    bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    pages: Arc<Semaphore>,
    receipt_slots: Arc<Semaphore>,
    operations: Mutex<Operations>,
}

impl Store {
    pub(crate) fn new(path: PathBuf, handle: tokio::runtime::Handle) -> Arc<Self> {
        let store = Arc::new(Self {
            path,
            root: OnceLock::new(),
            entries: Mutex::new(BTreeMap::new()),
            bytes: Arc::new(Semaphore::new(GLOBAL_UNITS)),
            slots: Arc::new(Semaphore::new(8)),
            pages: Arc::new(Semaphore::new(8)),
            receipt_slots: Arc::new(Semaphore::new(RECEIPTS)),
            operations: Mutex::new(Operations::default()),
        });
        let weak: Weak<Self> = Arc::downgrade(&store);
        handle.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let Some(store) = weak.upgrade() else {
                    break;
                };
                // File deletion never runs on an HTTP runtime worker.
                let _ = tokio::task::spawn_blocking(move || store.expire()).await;
            }
        });
        store
    }

    fn initialize(&self) -> Result<(), SourceFetchError> {
        self.root
            .get_or_init(|| {
                if std::fs::symlink_metadata(&self.path)
                    .is_ok_and(|metadata| !metadata.is_dir() || metadata.file_type().is_symlink())
                {
                    return Err(SourceFetchError::Unavailable);
                }
                create_private_dir(&self.path).map_err(|_| SourceFetchError::Unavailable)?;
                let path = self.path.join(".owner.lock");
                if std::fs::symlink_metadata(&path)
                    .is_ok_and(|metadata| !metadata.is_file() || metadata.file_type().is_symlink())
                {
                    return Err(SourceFetchError::Unavailable);
                }
                let mut options = std::fs::OpenOptions::new();
                options.create(true).append(true).read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let file = options
                    .open(path)
                    .map_err(|_| SourceFetchError::Unavailable)?;
                file.try_lock().map_err(|_| SourceFetchError::Unavailable)?;
                // Exclusive backend ownership proves no live snapshot from this root is adopted.
                for item in
                    std::fs::read_dir(&self.path).map_err(|_| SourceFetchError::Unavailable)?
                {
                    let item = item.map_err(|_| SourceFetchError::Unavailable)?;
                    let name = item.file_name();
                    let Some(id) = name
                        .to_str()
                        .and_then(|name| name.strip_prefix("snapshot-"))
                    else {
                        continue;
                    };
                    let Ok(uuid) = Uuid::parse_str(id) else {
                        continue;
                    };
                    if id != uuid.simple().to_string()
                        || !item
                            .file_type()
                            .is_ok_and(|kind| kind.is_dir() && !kind.is_symlink())
                    {
                        continue;
                    }
                    let marker = item.path().join(".marker");
                    if std::fs::symlink_metadata(&marker).is_ok_and(|metadata| {
                        metadata.is_file()
                            && !metadata.file_type().is_symlink()
                            && metadata.len() < 64
                    }) && std::fs::read_to_string(marker).ok().as_deref() == Some(MARKER)
                    {
                        std::fs::remove_dir_all(item.path())
                            .map_err(|_| SourceFetchError::Unavailable)?;
                    }
                }
                Ok(Root { _lock: file })
            })
            .as_ref()
            .map(|_| ())
            .map_err(|error| *error)
    }

    pub(crate) fn page_permit(&self) -> Result<OwnedSemaphorePermit, SourceFetchError> {
        self.pages
            .clone()
            .try_acquire_owned()
            .map_err(|_| SourceFetchError::Unavailable)
    }

    pub(crate) fn writer(self: &Arc<Self>) -> Result<Writer, SourceFetchError> {
        self.initialize()?;
        self.expire();
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| SourceFetchError::Unavailable)?;
        let id = Uuid::new_v4();
        let directory = Directory(self.path.join(format!("snapshot-{}", id.simple())));
        create_private_dir(&directory.0).map_err(|_| SourceFetchError::Unavailable)?;
        write_private(&directory.0.join(".marker"), MARKER)
            .map_err(|_| SourceFetchError::Unavailable)?;
        Ok(Writer {
            store: self.clone(),
            id,
            directory,
            slot,
            bytes: Vec::new(),
            size: 0,
            pages: 0,
            created: Instant::now(),
        })
    }

    fn expire(&self) {
        let removed = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            let expired: Vec<_> = entries
                .iter()
                .filter(|(_, entry)| entry.expired())
                .map(|(id, _)| *id)
                .collect();
            expired
                .into_iter()
                .filter_map(|id| entries.remove(&id))
                .collect::<Vec<_>>()
        };
        drop(removed);
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        operations.receipts.retain(|_, receipt| !receipt.expired());
        for operation in operations.running.values() {
            if operation.created.elapsed() >= LIFETIME {
                operation
                    .cancelled
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn close(
        &self,
        cursor: &Cursor,
        owner: Option<&str>,
        credential: &str,
    ) -> Result<(), SourceFetchError> {
        let id = Uuid::parse_str(&cursor.id).map_err(|_| SourceFetchError::Closed)?;
        let removed = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            let Some(entry) = entries.get(&id) else {
                drop(entries);
                let mut operations = self
                    .operations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(receipt) = operations.receipts.get(&id) {
                    if owner != Some(receipt.owner.as_str()) && credential != receipt.credential {
                        return Err(SourceFetchError::Closed);
                    }
                    operations.receipts.remove(&id);
                } else if let Some(operation) = operations.running.get(&id) {
                    if owner != Some(operation.owner.as_str()) {
                        return Err(SourceFetchError::Closed);
                    }
                    operation
                        .cancelled
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                return Ok(());
            };
            if owner != Some(entry.owner.as_str())
                && credential != entry.initial_credential
                && credential != entry.last_credential
            {
                return Err(SourceFetchError::Closed);
            }
            entries.remove(&id)
        };
        drop(removed);
        Ok(())
    }

    /// Detach one unread prepared replacement into exclusive main-owned execution.
    /// Page cursors cannot be upgraded into replacement leases.
    pub(crate) fn take(
        self: &Arc<Self>,
        id: &str,
        binding: &Binding,
        request: &str,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Snapshot, SourceFetchError> {
        let id = Uuid::parse_str(id).map_err(|_| SourceFetchError::Closed)?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let entry = entries.get(&id).ok_or(SourceFetchError::Closed)?;
        if entry.owner != binding.owner {
            return Err(SourceFetchError::Closed);
        }
        if entry.expired() {
            entries.remove(&id);
            return Err(SourceFetchError::Closed);
        }
        if entry.fingerprint != binding.fingerprint || entry.next != 0 || entry.prepared.is_none() {
            entries.remove(&id);
            return Err(SourceFetchError::Stale);
        }
        let mut entry = entries.remove(&id).expect("checked private snapshot");
        entry.last_credential = binding.credential.clone();
        let (changed, _) = tokio::sync::watch::channel(false);
        self.operations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .running
            .insert(
                id,
                Running {
                    owner: binding.owner.clone(),
                    request: request.into(),
                    configuration: configuration_digest(&entry.configuration),
                    created: entry.created,
                    cancelled,
                    changed,
                },
            );
        Ok(Snapshot {
            store: self.clone(),
            id,
            entry,
            _running: Some(RunningGuard {
                store: Arc::downgrade(self),
                id,
            }),
        })
    }

    pub(crate) async fn completed(
        &self,
        id: &str,
        binding: &Binding,
        request: &str,
        configuration: &str,
    ) -> Result<Option<meerschaum_core::success::SyncResult>, SourceFetchError> {
        let id = Uuid::parse_str(id).map_err(|_| SourceFetchError::Closed)?;
        let configuration = configuration_digest(configuration);
        let mut waited = false;
        loop {
            let waiting = {
                let mut operations = self
                    .operations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(receipt) = operations.receipts.get_mut(&id) {
                    if receipt.owner != binding.owner {
                        return Err(SourceFetchError::Closed);
                    }
                    if receipt.expired() {
                        operations.receipts.remove(&id);
                        return Err(SourceFetchError::Uncertain);
                    }
                    if receipt.request != request || receipt.configuration != configuration {
                        return Err(SourceFetchError::Stale);
                    }
                    receipt.credential = binding.credential.clone();
                    receipt.touched = Instant::now();
                    return Ok(Some(receipt.result.clone()));
                }
                match operations.running.get(&id) {
                    None => return Ok(None),
                    Some(operation) => {
                        if operation.owner != binding.owner {
                            return Err(SourceFetchError::Closed);
                        }
                        if operation.request != request || operation.configuration != configuration
                        {
                            return Err(SourceFetchError::Stale);
                        }
                        if operation
                            .cancelled
                            .load(std::sync::atomic::Ordering::Relaxed)
                        {
                            return Err(SourceFetchError::Closed);
                        }
                        if waited {
                            return Err(SourceFetchError::InProgress);
                        }
                        operation.changed.subscribe()
                    }
                }
            };
            let mut waiting = waiting;
            let _ = tokio::time::timeout(Duration::from_secs(20), waiting.changed()).await;
            waited = true;
        }
    }

    pub(crate) fn read(
        &self,
        cursor: &Cursor,
        binding: &Binding,
        schema: &str,
        configuration: &str,
        query: &str,
    ) -> Result<PageResponse, SourceFetchError> {
        let id = Uuid::parse_str(&cursor.id).map_err(|_| SourceFetchError::Closed)?;
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = entries.get_mut(&id) else {
            return Err(SourceFetchError::Closed);
        };
        if entry.owner != binding.owner {
            return Err(SourceFetchError::Closed);
        }
        if entry.expired() {
            entries.remove(&id);
            return Err(SourceFetchError::Closed);
        }
        if entry.fingerprint != binding.fingerprint
            || entry.schema != schema
            || entry.configuration != configuration
            || entry.query != query
            || entry.next != cursor.page
        {
            entries.remove(&id);
            return Err(SourceFetchError::Stale);
        }
        let path = entry.directory.0.join(format!("{:06}.json", cursor.page));
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| SourceFetchError::Unavailable)?
            .take(source_fetch::MAX_PAGE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SourceFetchError::Unavailable)?;
        if bytes.len() > source_fetch::MAX_PAGE_BYTES {
            entries.remove(&id);
            return Err(SourceFetchError::TooLarge);
        }
        let data: Response =
            serde_json::from_slice(&bytes).map_err(|_| SourceFetchError::Unavailable)?;
        if data.rows.len() > source_fetch::PAGE_ROWS {
            entries.remove(&id);
            return Err(SourceFetchError::TooLarge);
        }
        entry.last_credential = binding.credential.clone();
        entry.touched = Instant::now();
        entry.next += 1;
        let complete = entry.next as usize >= entry.pages;
        let next = (!complete).then(|| Cursor {
            id: cursor.id.clone(),
            page: entry.next,
        });
        if complete {
            entries.remove(&id);
        }
        Ok(PageResponse {
            version: source_fetch::PAGE_VERSION,
            data,
            cursor: next,
            complete,
        })
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Remove owned files before releasing this backend's exclusive root lock.
        self.entries
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.operations
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .receipts
            .clear();
    }
}

pub(crate) struct Writer {
    id: Uuid,
    directory: Directory,
    slot: OwnedSemaphorePermit,
    bytes: Vec<OwnedSemaphorePermit>,
    size: usize,
    pages: usize,
    created: Instant,
    // Partial directory cleanup must precede releasing the backend's root lock.
    store: Arc<Store>,
}

impl Writer {
    pub(crate) fn is_empty(&self) -> bool {
        self.pages == 0
    }

    pub(crate) fn push(&mut self, response: &Response) -> Result<(), SourceFetchError> {
        if self.pages >= MAX_PAGES {
            return Err(SourceFetchError::TooLarge);
        }
        // Leave space for the protocol wrapper/cursor in the HTTP page's same byte cap.
        let mut output = Limited {
            bytes: Vec::new(),
            limit: source_fetch::MAX_PAGE_BYTES - 1024,
        };
        serde_json::to_writer(&mut output, response).map_err(|_| SourceFetchError::TooLarge)?;
        let size = output.bytes.len();
        if self.size.saturating_add(size) > MAX_SNAPSHOT_BYTES {
            return Err(SourceFetchError::TooLarge);
        }
        let permit = self
            .store
            .bytes
            .clone()
            .try_acquire_many_owned(size.div_ceil(QUOTA_UNIT) as u32)
            .map_err(|_| SourceFetchError::Unavailable)?;
        let text = std::str::from_utf8(&output.bytes).map_err(|_| SourceFetchError::Failed)?;
        write_private(
            &self.directory.0.join(format!("{:06}.json", self.pages)),
            text,
        )
        .map_err(|_| SourceFetchError::Unavailable)?;
        self.bytes.push(permit);
        self.size += size;
        self.pages += 1;
        Ok(())
    }

    pub(crate) fn finish(
        self,
        binding: &Binding,
        schema: String,
        configuration: String,
        query: String,
    ) -> Snapshot {
        Snapshot {
            store: self.store,
            id: self.id,
            entry: Entry {
                directory: self.directory,
                _slot: self.slot,
                _bytes: self.bytes,
                owner: binding.owner.clone(),
                fingerprint: binding.fingerprint.clone(),
                initial_credential: binding.credential.clone(),
                last_credential: binding.credential.clone(),
                schema,
                configuration,
                query,
                prepared: None,
                receipt: Mutex::new(None),
                pages: self.pages,
                next: 0,
                created: self.created,
                touched: Instant::now(),
            },
            _running: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn publish(
        self,
        binding: &Binding,
        schema: String,
        configuration: String,
        query: String,
    ) -> Cursor {
        self.finish(binding, schema, configuration, query).publish()
    }
}

/// Exclusive native lease. Its directory and all quota permits are released on every
/// completion, error or cancellation path; publishing moves ownership back to the registry.
pub(crate) struct Snapshot {
    id: Uuid,
    entry: Entry,
    _running: Option<RunningGuard>,
    // Keep the exclusive backend root lock alive until files and running state are gone.
    store: Arc<Store>,
}
impl Snapshot {
    pub(crate) fn id(&self) -> String {
        self.id.simple().to_string()
    }
    pub(crate) fn pages(&self) -> usize {
        self.entry.pages
    }
    pub(crate) fn schema(&self) -> &str {
        &self.entry.schema
    }
    pub(crate) fn configuration(&self) -> &str {
        &self.entry.configuration
    }
    pub(crate) fn query(&self) -> &str {
        &self.entry.query
    }
    pub(crate) fn remaining(&self) -> Duration {
        LIFETIME.saturating_sub(self.entry.created.elapsed())
    }
    pub(crate) fn reserve_receipt(&mut self) -> Result<(), SourceFetchError> {
        self.store.expire();
        let slot = loop {
            if let Ok(slot) = self.store.receipt_slots.clone().try_acquire_owned() {
                break slot;
            }
            let removed = {
                let mut operations = self
                    .store
                    .operations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let oldest = operations
                    .receipts
                    .iter()
                    .min_by_key(|(_, receipt)| receipt.touched)
                    .map(|(id, _)| *id);
                oldest.and_then(|id| operations.receipts.remove(&id))
            };
            if removed.is_none() {
                return Err(SourceFetchError::Unavailable);
            }
            drop(removed);
        };
        let bytes = self
            .store
            .bytes
            .clone()
            .try_acquire_many_owned(RECEIPT_UNITS)
            .map_err(|_| SourceFetchError::Unavailable)?;
        *self
            .entry
            .receipt
            .get_mut()
            .unwrap_or_else(|error| error.into_inner()) = Some(ReceiptReservation {
            _bytes: bytes,
            _slot: slot,
        });
        Ok(())
    }
    pub(crate) fn complete(&self, request: &str, result: meerschaum_core::success::SyncResult) {
        let reservation = self
            .entry
            .receipt
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let Some(reservation) = reservation else {
            return;
        };
        let mut operations = self
            .store
            .operations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        operations.receipts.insert(
            self.id,
            Receipt {
                owner: self.entry.owner.clone(),
                credential: self.entry.last_credential.clone(),
                request: request.into(),
                configuration: configuration_digest(&self.entry.configuration),
                result,
                created: self.entry.created,
                touched: Instant::now(),
                _reservation: reservation,
            },
        );
        if let Some(operation) = operations.running.remove(&self.id) {
            let _ = operation.changed.send(true);
        }
    }
    pub(crate) fn set_prepared<T: Send + Sync + 'static>(&mut self, value: T) {
        self.entry.prepared = Some(Box::new(value));
    }
    pub(crate) fn prepared<T: 'static>(&self) -> Option<&T> {
        self.entry.prepared.as_ref()?.downcast_ref()
    }
    pub(crate) fn read(&self, index: usize) -> Result<Response, SourceFetchError> {
        if self.remaining().is_zero() || index >= self.entry.pages {
            return Err(SourceFetchError::Closed);
        }
        let mut bytes = Vec::new();
        std::fs::File::open(self.entry.directory.0.join(format!("{index:06}.json")))
            .map_err(|_| SourceFetchError::Unavailable)?
            .take(source_fetch::MAX_PAGE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SourceFetchError::Unavailable)?;
        if bytes.len() > source_fetch::MAX_PAGE_BYTES {
            return Err(SourceFetchError::TooLarge);
        }
        let response: Response =
            serde_json::from_slice(&bytes).map_err(|_| SourceFetchError::Unavailable)?;
        if response.rows.len() > source_fetch::PAGE_ROWS || !response.complete {
            return Err(SourceFetchError::TooLarge);
        }
        Ok(response)
    }
    pub(crate) fn publish(mut self) -> Cursor {
        self.entry.touched = Instant::now();
        let cursor = Cursor {
            id: self.id(),
            page: 0,
        };
        self.store
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(self.id, self.entry);
        cursor
    }
}

struct Limited {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            return Err(std::io::Error::other("source page bound"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(owner: &str, credential: &str, query: &str) -> Binding {
        Binding {
            owner: owner.into(),
            credential: credential.into(),
            fingerprint: query.into(),
        }
    }
    fn page() -> Response {
        Response {
            version: source_fetch::VERSION,
            rows: vec![meerschaum_core::Map::from_iter([(
                "id".into(),
                meerschaum_core::Value::Int(1),
            )])],
            dtypes: Default::default(),
            complete: true,
        }
    }
    fn store(path: PathBuf, units: usize, slots: usize) -> Arc<Store> {
        Arc::new(Store {
            path,
            root: OnceLock::new(),
            entries: Mutex::new(BTreeMap::new()),
            bytes: Arc::new(Semaphore::new(units)),
            slots: Arc::new(Semaphore::new(slots)),
            pages: Arc::new(Semaphore::new(8)),
            receipt_slots: Arc::new(Semaphore::new(RECEIPTS)),
            operations: Mutex::new(Operations::default()),
        })
    }
    fn count(store: &Store) -> usize {
        std::fs::read_dir(&store.path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("snapshot-"))
            .count()
    }

    #[test]
    fn bounded_private_spools_rotation_invalidation_and_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path().join("owned"), 4, 2);
        let original = binding("execution:grant:launch", "old-jwt", "query-v1");
        let mut writer = store.writer().unwrap();
        writer.push(&page()).unwrap();
        writer.push(&page()).unwrap();
        let mut cursor = writer.publish(
            &original,
            "schema-v1".into(),
            "source-v1".into(),
            "SELECT id FROM rows".into(),
        );
        assert_eq!(count(&store), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&store.path).unwrap().permissions().mode() & 0o777,
                0o700
            );
            let entry = store.entries.lock().unwrap();
            let directory = &entry.values().next().unwrap().directory.0;
            assert_eq!(
                std::fs::metadata(directory.join("000000.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let other = binding("execution:other:launch", "other-jwt", "query-v1");
        assert!(matches!(
            store.read(
                &cursor,
                &other,
                "schema-v1",
                "source-v1",
                "SELECT id FROM rows"
            ),
            Err(SourceFetchError::Closed)
        ));
        assert!(matches!(
            store.close(&cursor, Some(&other.owner), &other.credential),
            Err(SourceFetchError::Closed)
        ));
        assert_eq!(
            count(&store),
            1,
            "another actor cannot read or delete this snapshot"
        );
        let rotated = binding(&original.owner, "new-jwt", "query-v1");
        let first = store
            .read(
                &cursor,
                &rotated,
                "schema-v1",
                "source-v1",
                "SELECT id FROM rows",
            )
            .unwrap();
        assert!(!first.complete);
        cursor = first.cursor.unwrap();
        assert!(
            store
                .read(
                    &cursor,
                    &rotated,
                    "schema-v1",
                    "source-v1",
                    "SELECT id FROM rows"
                )
                .unwrap()
                .complete
        );
        assert_eq!(count(&store), 0);
        assert_eq!(store.bytes.available_permits(), 4);

        for (changed, schema, config) in [
            (
                binding(&original.owner, "rotated-again", "query-v2"),
                "schema-v1",
                "source-v1",
            ),
            (original.clone(), "schema-v2", "source-v1"),
            (original.clone(), "schema-v1", "source-v2"),
        ] {
            let mut writer = store.writer().unwrap();
            writer.push(&page()).unwrap();
            let cursor = writer.publish(
                &original,
                "schema-v1".into(),
                "source-v1".into(),
                "SELECT id FROM rows".into(),
            );
            assert!(matches!(
                store.read(&cursor, &changed, schema, config, "SELECT id FROM rows"),
                Err(SourceFetchError::Stale)
            ));
            assert_eq!(count(&store), 0);
            assert_eq!(store.bytes.available_permits(), 4);
        }
        let mut writer = store.writer().unwrap();
        writer.push(&page()).unwrap();
        let cursor = writer.publish(
            &original,
            "schema-v1".into(),
            "source-v1".into(),
            "SELECT id FROM rows".into(),
        );
        // An expired/revoked grant's last credential can release its own resources only.
        store.close(&cursor, None, &original.credential).unwrap();
        assert_eq!(count(&store), 0);
        assert_eq!(store.slots.available_permits(), 2);
    }

    #[test]
    fn quota_failure_cancellation_expiry_and_exclusive_root_release_resources() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("owned");
        let store = store(path.clone(), 2, 1);
        let original = binding("execution:grant:launch", "jwt", "query");
        {
            let mut writer = store.writer().unwrap();
            writer.push(&page()).unwrap();
            assert!(matches!(store.writer(), Err(SourceFetchError::Unavailable)));
            writer.push(&page()).unwrap();
            assert!(matches!(
                writer.push(&page()),
                Err(SourceFetchError::Unavailable)
            ));
            assert_eq!(count(&store), 1);
        }
        assert_eq!(
            count(&store),
            0,
            "cancelled/failed construction deletes partial spool"
        );
        assert_eq!(store.bytes.available_permits(), 2);
        let mut writer = store.writer().unwrap();
        writer.push(&page()).unwrap();
        let cursor = writer.publish(
            &original,
            "schema-v1".into(),
            "source".into(),
            "SELECT id FROM rows".into(),
        );
        store
            .entries
            .lock()
            .unwrap()
            .get_mut(&Uuid::parse_str(&cursor.id).unwrap())
            .unwrap()
            .touched = Instant::now() - IDLE;
        store.expire();
        assert_eq!(count(&store), 0);
        assert_eq!(store.slots.available_permits(), 1);
        let mut writer = store.writer().unwrap();
        writer.push(&page()).unwrap();
        let cursor = writer.publish(
            &original,
            "schema-v1".into(),
            "source".into(),
            "SELECT id FROM rows".into(),
        );
        store
            .entries
            .lock()
            .unwrap()
            .get_mut(&Uuid::parse_str(&cursor.id).unwrap())
            .unwrap()
            .created = Instant::now() - LIFETIME;
        store.expire();
        assert_eq!(count(&store), 0);
        let competing = super::tests::store(path.clone(), 2, 1);
        assert!(matches!(
            competing.writer(),
            Err(SourceFetchError::Unavailable)
        ));
        drop(competing);
        drop(store);
        let reopened = super::tests::store(path, 2, 1);
        assert!(reopened.writer().is_ok());
    }
}

#[cfg(test)]
#[path = "source_fetch_snapshot_receipts.rs"]
mod receipt_tests;
