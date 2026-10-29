//! The Valkey commands the connector uses, behind a trait so the connector logic can run against
//! a live server ([`crate::FredBackend`]) or the in-memory model ([`crate::MemoryBackend`]) used by
//! unit tests and offline fixture replays.

use meerschaum_connector::traits::async_trait;

/// A backend error (connection failure, wrong key type, ...).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BackendError(pub String);

/// The result type of backend commands.
pub type BackendResult<T> = Result<T, BackendError>;

/// One conditional, atomic saved-registration move; only a live source can supply its ID.
pub struct RegistrationMove {
    /// Source registration key.
    pub source: String,
    /// Source parameters key.
    pub source_parameters: String,
    /// Destination registration key.
    pub destination: String,
    /// Destination parameters key.
    pub destination_parameters: String,
    /// Internal registration sorted set.
    pub rows: String,
    /// Expected persisted ID (also the destination ID).
    pub identity: String,
    /// Exact source parameters read before this atomic operation.
    pub expected_parameters: String,
    /// New parameters, retaining the source's UUID and access.
    pub parameters: String,
    /// Source sorted-set row.
    pub old_member: String,
    /// Destination sorted-set row.
    pub new_member: String,
    /// Unchanged registration row score.
    pub score: f64,
}

/// A `ZRANGEBYSCORE` bound: a number, or `-inf` / `+inf`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    /// `-inf`.
    NegInf,
    /// `+inf`.
    PosInf,
    /// An inclusive score.
    Value(f64),
}

impl ScoreBound {
    /// The bound as Valkey syntax.
    pub fn to_arg(&self) -> String {
        match self {
            ScoreBound::NegInf => "-inf".into(),
            ScoreBound::PosInf => "+inf".into(),
            ScoreBound::Value(v) => {
                if v.fract() == 0.0 && v.abs() < 1e15 {
                    format!("{}", *v as i64)
                } else {
                    format!("{v}")
                }
            }
        }
    }

    /// Whether `score` satisfies this bound as a minimum.
    pub fn le(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => false,
            ScoreBound::Value(v) => *v <= score,
        }
    }

    /// Whether `score` satisfies this bound as a maximum.
    pub fn ge(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => false,
            ScoreBound::PosInf => true,
            ScoreBound::Value(v) => *v >= score,
        }
    }
}

/// One write of [`ValkeyBackend::claim_and_write`].
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOp {
    /// `SET key value`.
    Set {
        /// The key.
        key: String,
        /// The value.
        value: String,
    },
    /// `ZADD key score member`.
    ZAdd {
        /// The sorted set.
        key: String,
        /// The member.
        member: String,
        /// The score.
        score: f64,
    },
    /// `SADD key member`.
    SAdd {
        /// The set.
        key: String,
        /// The member.
        member: String,
    },
}

impl WriteOp {
    /// The key this operation writes.
    pub fn key(&self) -> &str {
        match self {
            WriteOp::Set { key, .. } | WriteOp::ZAdd { key, .. } | WriteOp::SAdd { key, .. } => key,
        }
    }

    /// The Valkey command name (`SET`, `ZADD`, `SADD`).
    pub fn command(&self) -> &'static str {
        match self {
            WriteOp::Set { .. } => "SET",
            WriteOp::ZAdd { .. } => "ZADD",
            WriteOp::SAdd { .. } => "SADD",
        }
    }

    /// The `TYPE` the key must have (or `none`) for the write to succeed (`None`: any, as `SET`
    /// replaces a key of any type).
    pub fn required_type(&self) -> Option<&'static str> {
        match self {
            WriteOp::Set { .. } => None,
            WriteOp::ZAdd { .. } => Some("zset"),
            WriteOp::SAdd { .. } => Some("set"),
        }
    }
}

/// The Valkey commands used by `ValkeyConnector`. String values are UTF-8 (the Python connector
/// decodes everything as UTF-8).
#[async_trait]
pub trait ValkeyBackend: Send + Sync {
    /// Compare the live source and atomically move all registration keys and its row.
    async fn move_registration(&self, operation: &RegistrationMove) -> BackendResult<bool> {
        let _ = operation;
        Err(BackendError(
            "Atomic registration moves are unsupported by this backend.".into(),
        ))
    }
    /// `PING`.
    async fn ping(&self) -> BackendResult<bool>;
    /// `GET key`.
    async fn get(&self, key: &str) -> BackendResult<Option<String>>;
    /// `SET key value`.
    async fn set(&self, key: &str, value: &str) -> BackendResult<()>;
    /// `SET key value EX seconds`.
    async fn set_ex(&self, key: &str, value: &str, seconds: i64) -> BackendResult<()>;
    /// `SET key value NX`: `true` if the key was set.
    async fn set_nx(&self, key: &str, value: &str) -> BackendResult<bool>;
    /// `SET key value NX PX ms`: `true` if the key was set.
    async fn set_nx_px(&self, key: &str, value: &str, ms: u64) -> BackendResult<bool>;
    /// Atomically extend the TTL of `key` to `ms` if its value is `value`.
    async fn compare_and_pexpire(&self, key: &str, value: &str, ms: u64) -> BackendResult<bool>;
    /// Atomically delete `key` if its value is `value`.
    async fn compare_and_delete(&self, key: &str, value: &str) -> BackendResult<bool>;
    /// Atomically claim `claim_key` and write `ops`: if `claim_key` does not exist, set it to
    /// `claim_value` (no expiry) and apply every operation, returning `true`; if it exists,
    /// write nothing and return `false`. An operation against a key of the wrong type is an
    /// error, checked before anything is written, so an error also writes nothing. Keys must
    /// be distinct from `claim_key` and from each other's types.
    async fn claim_and_write(
        &self,
        claim_key: &str,
        claim_value: &str,
        ops: &[WriteOp],
    ) -> BackendResult<bool>;
    /// `DEL key [key ...]`: the number of keys removed.
    async fn del(&self, keys: &[String]) -> BackendResult<i64>;
    /// `EXISTS key`.
    async fn exists(&self, key: &str) -> BackendResult<i64>;
    /// `INCR key`.
    async fn incr(&self, key: &str) -> BackendResult<i64>;
    /// `TYPE key` (`none`, `string`, `set`, `zset`, `list`, `hash`).
    async fn key_type(&self, key: &str) -> BackendResult<String>;
    /// `KEYS pattern` (fixtures and tests only).
    async fn keys(&self, pattern: &str) -> BackendResult<Vec<String>>;
    /// `ZADD key score member`.
    async fn zadd(&self, key: &str, member: &str, score: f64) -> BackendResult<()>;
    /// `ZREM key member`.
    async fn zrem(&self, key: &str, member: &str) -> BackendResult<i64>;
    /// `ZCARD key`.
    async fn zcard(&self, key: &str) -> BackendResult<i64>;
    /// `ZRANGE key start stop WITHSCORES` (`rev`: `ZREVRANGE`).
    async fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        rev: bool,
    ) -> BackendResult<Vec<(String, f64)>>;
    /// `ZRANGEBYSCORE key min max` (both inclusive).
    async fn zrangebyscore(
        &self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
    ) -> BackendResult<Vec<String>>;
    /// Score-bounded candidates with a native LIMIT, optionally newest first.
    /// Optional backends must implement this to support bounded performance history.
    async fn zrangebyscore_limit(
        &self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
        limit: usize,
        reverse: bool,
    ) -> BackendResult<Vec<String>> {
        let _ = (key, min, max, limit, reverse);
        Err(BackendError(
            "Bounded sorted-set reads are not implemented.".into(),
        ))
    }
    /// `SADD key member`.
    async fn sadd(&self, key: &str, member: &str) -> BackendResult<()>;
    /// `SREM key member`.
    async fn srem(&self, key: &str, member: &str) -> BackendResult<i64>;
    /// `SCARD key`.
    async fn scard(&self, key: &str) -> BackendResult<i64>;
    /// `SMEMBERS key`.
    async fn smembers(&self, key: &str) -> BackendResult<Vec<String>>;
    /// `LLEN key`.
    async fn llen(&self, key: &str) -> BackendResult<i64>;
    /// `RPUSH key value`.
    async fn rpush(&self, key: &str, value: &str) -> BackendResult<()>;
    /// `LRANGE key start stop`.
    async fn lrange(&self, key: &str, start: i64, stop: i64) -> BackendResult<Vec<String>>;
    /// `HSET key field value [field value ...]`.
    async fn hset(&self, key: &str, fields: &[(String, String)]) -> BackendResult<()>;
    /// `HGETALL key`.
    async fn hgetall(&self, key: &str) -> BackendResult<Vec<(String, String)>>;
    /// `FLUSHDB` (fixtures and tests only).
    async fn flushdb(&self) -> BackendResult<()>;
}
