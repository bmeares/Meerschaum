//! The live backend over [`fred`].

use std::time::Duration;

use fred::prelude::*;
use fred::types::Value as FValue;
use meerschaum_connector::traits::async_trait;

use crate::backend::{BackendError, BackendResult, ScoreBound, ValkeyBackend};

/// Lua: extend the TTL of `KEYS[1]` if it holds `ARGV[1]`.
const CAS_PEXPIRE: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then \
     return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";
/// Lua: delete `KEYS[1]` if it holds `ARGV[1]`.
const CAS_DELETE: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then \
     return redis.call('del', KEYS[1]) else return 0 end";

/// A Valkey connection through `fred`.
pub struct FredBackend {
    client: Client,
}

impl std::fmt::Debug for FredBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FredBackend").finish_non_exhaustive()
    }
}

fn err(e: impl std::fmt::Display) -> BackendError {
    BackendError(e.to_string())
}

fn to_string(v: FValue) -> Option<String> {
    match v {
        FValue::Null => None,
        FValue::Bytes(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        other => other.as_string(),
    }
}

fn to_strings(v: FValue) -> Vec<String> {
    v.into_array().into_iter().filter_map(to_string).collect()
}

fn to_i64(v: &FValue) -> i64 {
    v.as_i64().unwrap_or(0)
}

impl FredBackend {
    /// Connect to `url` (`redis://`, `valkey://`, `rediss://`, ...) and wait for the connection.
    /// `timeout` is the socket (command) timeout; the connection itself is bounded by
    /// `connect_timeout` (`MRSM_CONNECT_TIMEOUT`, else 5 s).
    pub async fn connect(url: &str, timeout: Option<Duration>) -> BackendResult<FredBackend> {
        Self::connect_with(
            url,
            timeout,
            meerschaum_core::connect::connect_timeout(None),
        )
        .await
    }

    /// [`connect`](FredBackend::connect) with an explicit connection timeout: a refused
    /// connection fails at once, an unresponsive host after `connect_timeout`.
    pub async fn connect_with(
        url: &str,
        timeout: Option<Duration>,
        connect_timeout: Duration,
    ) -> BackendResult<FredBackend> {
        let config = Config::from_url(url).map_err(err)?;
        let mut builder = Builder::from_config(config);
        builder.with_connection_config(|c| {
            c.connection_timeout = connect_timeout;
            if let Some(t) = timeout {
                c.internal_command_timeout = t;
            }
        });
        if let Some(t) = timeout {
            builder.with_performance_config(|p| {
                p.default_command_timeout = t;
            });
        }
        let client = builder.build().map_err(err)?;
        match tokio::time::timeout(connect_timeout, client.init()).await {
            Ok(result) => result.map_err(err)?,
            Err(_) => {
                return Err(BackendError(format!(
                    "connection timed out after {} s",
                    connect_timeout.as_secs_f64()
                )));
            }
        };
        Ok(FredBackend { client })
    }

    async fn call(&self, name: &'static str, args: Vec<String>) -> BackendResult<FValue> {
        let command =
            fred::types::CustomCommand::new_static(name, fred::types::ClusterHash::FirstKey, false);
        self.client
            .custom::<FValue, String>(command, args)
            .await
            .map_err(err)
    }

    /// Close the connection.
    pub async fn quit(&self) -> BackendResult<()> {
        self.client.quit().await.map_err(err)
    }
}

#[async_trait]
impl ValkeyBackend for FredBackend {
    async fn ping(&self) -> BackendResult<bool> {
        let v = self.call("PING", vec![]).await?;
        Ok(to_string(v).is_some_and(|s| s == "PONG"))
    }

    async fn get(&self, key: &str) -> BackendResult<Option<String>> {
        Ok(to_string(self.call("GET", vec![key.into()]).await?))
    }

    async fn set(&self, key: &str, value: &str) -> BackendResult<()> {
        self.call("SET", vec![key.into(), value.into()]).await?;
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, seconds: i64) -> BackendResult<()> {
        self.call(
            "SET",
            vec![key.into(), value.into(), "EX".into(), seconds.to_string()],
        )
        .await?;
        Ok(())
    }

    async fn set_nx_px(&self, key: &str, value: &str, ms: u64) -> BackendResult<bool> {
        let v = self
            .call(
                "SET",
                vec![
                    key.into(),
                    value.into(),
                    "NX".into(),
                    "PX".into(),
                    ms.to_string(),
                ],
            )
            .await?;
        Ok(!v.is_null())
    }

    async fn compare_and_pexpire(&self, key: &str, value: &str, ms: u64) -> BackendResult<bool> {
        let v = self
            .call(
                "EVAL",
                vec![
                    CAS_PEXPIRE.into(),
                    "1".into(),
                    key.into(),
                    value.into(),
                    ms.to_string(),
                ],
            )
            .await?;
        Ok(to_i64(&v) == 1)
    }

    async fn compare_and_delete(&self, key: &str, value: &str) -> BackendResult<bool> {
        let v = self
            .call(
                "EVAL",
                vec![CAS_DELETE.into(), "1".into(), key.into(), value.into()],
            )
            .await?;
        Ok(to_i64(&v) == 1)
    }

    async fn del(&self, keys: &[String]) -> BackendResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        Ok(to_i64(&self.call("DEL", keys.to_vec()).await?))
    }

    async fn exists(&self, key: &str) -> BackendResult<i64> {
        Ok(to_i64(&self.call("EXISTS", vec![key.into()]).await?))
    }

    async fn incr(&self, key: &str) -> BackendResult<i64> {
        Ok(to_i64(&self.call("INCR", vec![key.into()]).await?))
    }

    async fn key_type(&self, key: &str) -> BackendResult<String> {
        Ok(to_string(self.call("TYPE", vec![key.into()]).await?).unwrap_or_else(|| "none".into()))
    }

    async fn keys(&self, pattern: &str) -> BackendResult<Vec<String>> {
        let mut keys = to_strings(self.call("KEYS", vec![pattern.into()]).await?);
        keys.sort();
        Ok(keys)
    }

    async fn zadd(&self, key: &str, member: &str, score: f64) -> BackendResult<()> {
        self.call(
            "ZADD",
            vec![key.into(), ScoreBound::Value(score).to_arg(), member.into()],
        )
        .await?;
        Ok(())
    }

    async fn zrem(&self, key: &str, member: &str) -> BackendResult<i64> {
        Ok(to_i64(
            &self.call("ZREM", vec![key.into(), member.into()]).await?,
        ))
    }

    async fn zcard(&self, key: &str) -> BackendResult<i64> {
        Ok(to_i64(&self.call("ZCARD", vec![key.into()]).await?))
    }

    async fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        rev: bool,
    ) -> BackendResult<Vec<(String, f64)>> {
        let name = if rev { "ZREVRANGE" } else { "ZRANGE" };
        let v = self
            .call(
                name,
                vec![
                    key.into(),
                    start.to_string(),
                    stop.to_string(),
                    "WITHSCORES".into(),
                ],
            )
            .await?;
        let items = v.into_array();
        let mut out = Vec::new();
        // RESP3 returns `[[member, score], ...]`, RESP2 a flat `[member, score, ...]`.
        if items.first().is_some_and(|i| matches!(i, FValue::Array(_))) {
            for pair in items {
                let mut p = pair.into_array().into_iter();
                let member = p.next().and_then(to_string).unwrap_or_default();
                let score = p.next().and_then(|s| s.as_f64()).unwrap_or(0.0);
                out.push((member, score));
            }
        } else {
            let mut it = items.into_iter();
            while let (Some(m), Some(s)) = (it.next(), it.next()) {
                out.push((to_string(m).unwrap_or_default(), s.as_f64().unwrap_or(0.0)));
            }
        }
        Ok(out)
    }

    async fn zrangebyscore(
        &self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
    ) -> BackendResult<Vec<String>> {
        Ok(to_strings(
            self.call(
                "ZRANGEBYSCORE",
                vec![key.into(), min.to_arg(), max.to_arg()],
            )
            .await?,
        ))
    }

    async fn sadd(&self, key: &str, member: &str) -> BackendResult<()> {
        self.call("SADD", vec![key.into(), member.into()]).await?;
        Ok(())
    }

    async fn srem(&self, key: &str, member: &str) -> BackendResult<i64> {
        Ok(to_i64(
            &self.call("SREM", vec![key.into(), member.into()]).await?,
        ))
    }

    async fn scard(&self, key: &str) -> BackendResult<i64> {
        Ok(to_i64(&self.call("SCARD", vec![key.into()]).await?))
    }

    async fn smembers(&self, key: &str) -> BackendResult<Vec<String>> {
        Ok(to_strings(self.call("SMEMBERS", vec![key.into()]).await?))
    }

    async fn llen(&self, key: &str) -> BackendResult<i64> {
        Ok(to_i64(&self.call("LLEN", vec![key.into()]).await?))
    }

    async fn rpush(&self, key: &str, value: &str) -> BackendResult<()> {
        self.call("RPUSH", vec![key.into(), value.into()]).await?;
        Ok(())
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> BackendResult<Vec<String>> {
        Ok(to_strings(
            self.call(
                "LRANGE",
                vec![key.into(), start.to_string(), stop.to_string()],
            )
            .await?,
        ))
    }

    async fn hset(&self, key: &str, fields: &[(String, String)]) -> BackendResult<()> {
        let mut args = vec![key.to_string()];
        for (f, v) in fields {
            args.push(f.clone());
            args.push(v.clone());
        }
        self.call("HSET", args).await?;
        Ok(())
    }

    async fn hgetall(&self, key: &str) -> BackendResult<Vec<(String, String)>> {
        let v = self.call("HGETALL", vec![key.into()]).await?;
        let mut out = Vec::new();
        match v {
            FValue::Map(m) => {
                for (k, v) in m.inner() {
                    out.push((
                        k.as_str().map(|s| s.to_string()).unwrap_or_default(),
                        to_string(v).unwrap_or_default(),
                    ));
                }
            }
            other => {
                let mut it = other.into_array().into_iter();
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    out.push((
                        to_string(k).unwrap_or_default(),
                        to_string(v).unwrap_or_default(),
                    ));
                }
            }
        }
        Ok(out)
    }

    async fn flushdb(&self) -> BackendResult<()> {
        self.call("FLUSHDB", vec![]).await?;
        Ok(())
    }
}
