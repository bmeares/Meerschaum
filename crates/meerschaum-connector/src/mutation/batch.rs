//! Native Main-owned batch handoff.
//! No serde / job / environment / HTTP decoder. An on-wire selector is not this object.
use std::any::Any;
use std::sync::Arc;
use std::{future::Future, pin::Pin};
type TargetRollback =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

#[derive(Clone)]
pub struct NativeBatch {
    reference: String,
    request_sha256: String,
    owner: Arc<dyn Any + Send + Sync>,
    begin: Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
    rollback: Option<TargetRollback>,
    host_guard: Option<Arc<dyn Any + Send + Sync>>,
}
impl std::fmt::Debug for NativeBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeBatch")
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}
impl NativeBatch {
    /// Only the trusted host creates this AFTER durable exact batch claim CAS.
    /// Generic HTTP/plugin data can name a selector but cannot deserialize its owner.
    pub fn from_host<T: Any + Send + Sync>(
        reference: String,
        request_sha256: String,
        owner: Arc<T>,
        begin: fn(&T) -> Result<(), String>,
    ) -> Result<Self, String> {
        if reference.len() > 2048
            || reference.split('|').count() != 4
            || reference.chars().any(char::is_control)
            || request_sha256.len() != 64
            || !request_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("The native batch lost its exact host-owned request identity.".into());
        }
        let start_owner = owner.clone();
        Ok(Self {
            reference,
            request_sha256,
            owner,
            begin: Arc::new(move || begin(&start_owner)),
            rollback: None,
            host_guard: None,
        })
    }
    /// Attach Main's exact owned native rollback recorder and never-handoff guard.
    /// No generic metadata/probe rollback is permitted to call this recorder.
    pub fn with_host_lifecycle<F, Fut>(
        mut self,
        rollback: F,
        guard: Arc<dyn Any + Send + Sync>,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        self.rollback = Some(Arc::new(move || Box::pin(rollback())));
        self.host_guard = Some(guard);
        self
    }
    /// Call ONLY after awaiting the actual target transaction's successful rollback.
    pub async fn target_rolled_back(&self) -> Result<(), String> {
        self.rollback
            .as_ref()
            .ok_or("The native rollback owner is unavailable.")?()
        .await
    }
    pub(super) fn same_claim(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owner, &other.owner)
            && self.reference == other.reference
            && self.request_sha256 == other.request_sha256
    }
    pub fn reference(&self) -> &str {
        &self.reference
    }
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }
    /// Used only by the actual target runner (never metadata/probe transactions).
    /// Its owner uses an atomic pending->native-started CAS that excludes no-handoff
    /// cancellation. Failure refuses the target before its first statement.
    pub fn begin_target(&self) -> Result<(), String> {
        (self.begin)()
    }
    /// Main's native adapter recovers ONLY its exact private typed owner. A different
    /// host owner or ordinary data callback cannot become a canonical batch claim.
    pub fn owner<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::clone(&self.owner).downcast::<T>().ok()
    }
}

/// Maximum raw request body accepted for a single native batch.
pub const MAX_NATIVE_BATCH_BODY: usize = 64 * 1024 * 1024;

/// Pure request identity, not execution authority. Both sides hash the ACTUAL
/// decoded query pairs (including repeats/order) and exact raw body bytes.
pub fn request_digest(
    reference: &str,
    operation: &str,
    query: &[(String, String)],
    body: &[u8],
) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    use std::io::Write;
    fn refused() -> String {
        "The native batch request identity exceeds its fixed bounds.".into()
    }
    if reference.len() > 2048
        || reference.split('|').count() != 4
        || reference.chars().any(char::is_control)
        || !matches!(operation, "data" | "source_replace")
        || query.len() > 512
        || body.len() > MAX_NATIVE_BATCH_BODY
    {
        return Err(refused());
    }
    struct QueryHash<'a> {
        digest: &'a mut Sha256,
        bytes: usize,
    }
    impl Write for QueryHash<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|bytes| *bytes <= 128 * 1024)
                .ok_or_else(|| std::io::Error::other("query too large"))?;
            self.digest.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut query_digest = Sha256::new();
    serde_json::to_writer(
        QueryHash {
            digest: &mut query_digest,
            bytes: 0,
        },
        query,
    )
    .map_err(|_| refused())?;
    let mut hash = Sha256::new();
    hash.update(b"mrsm-native-batch-request-v1\0");
    let encoded_query = query_digest.finalize();
    for segment in [
        reference.as_bytes(),
        operation.as_bytes(),
        &encoded_query[..],
        body,
    ] {
        hash.update((segment.len() as u64).to_be_bytes());
        hash.update(segment);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_request_bytes_and_all_dml_query_fields_are_bound_without_header_credentials() {
        let reference = "sql:main|manual:fixture|native|None";
        let query = vec![
            ("replace".into(), "False".into()),
            ("params".into(), r#"{"id":1}"#.into()),
        ];
        let body = br#"[{"id":1}]"#;
        let original = request_digest(reference, "data", &query, body).unwrap();
        let mut changed = query.clone();
        changed[0].1 = "True".into();
        assert_ne!(
            request_digest(reference, "data", &changed, body).unwrap(),
            original
        );
        changed = query.clone();
        changed.push(("replace".into(), "True".into()));
        assert_ne!(
            request_digest(reference, "data", &changed, body).unwrap(),
            original
        );
        assert_ne!(
            request_digest(reference, "data", &query, br#"[{"id":2}]"#).unwrap(),
            original
        );
        assert_ne!(
            request_digest("sql:main|manual:fixture|other|None", "data", &query, body).unwrap(),
            original
        );
        assert_ne!(
            request_digest(reference, "source_replace", &query, body).unwrap(),
            original
        );
    }
}

/// Main's process-local outcome of claiming an exact native data request.
/// No request/body/environment may deserialize this enum.
pub enum BatchClaimOutcome {
    /// Only this opaque Main owner can reach a native target writer.
    Native(NativeBatch),
    /// Actual native COMMIT and acceptance are known; no target replay is permitted.
    AlreadyCommitted(NativeBatchReceipt),
    /// Applying/prepared/committed outcome is unresolved. Refuse further target work.
    Unknown,
}

/// A known native receipt projected from Main's private durable state. Its wire
/// conversion names a receipt, not new data/EOF/child authority.
#[derive(Clone, Debug)]
pub struct NativeBatchReceipt {
    stream: uuid::Uuid,
    ordinal: u64,
    batch: uuid::Uuid,
    sha256: String,
}
impl NativeBatchReceipt {
    /// Called by the trusted host after exact canonical receipt/source validation.
    pub fn from_host(
        stream: uuid::Uuid,
        ordinal: u64,
        batch: uuid::Uuid,
        sha256: String,
    ) -> Result<Self, String> {
        if stream.is_nil()
            || batch.is_nil()
            || ordinal == 0
            || sha256.len() != 64
            || !sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("The native receipt lost its exact Main identity.".into());
        }
        Ok(Self {
            stream,
            ordinal,
            batch,
            sha256,
        })
    }
    /// The Main-issued stream ID.
    pub fn stream(&self) -> uuid::Uuid {
        self.stream
    }
    /// The Main-issued ordinal.
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
    /// The Main-issued batch ID.
    pub fn batch(&self) -> uuid::Uuid {
        self.batch
    }
    /// Actual native receipt digest, not a source-provided completion claim.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}
