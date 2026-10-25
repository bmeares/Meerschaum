//! An in-memory model of the Valkey commands in [`ValkeyBackend`], with Valkey's ordering rules
//! (sorted sets by score then member bytes; small sets in insertion order, as listpack-encoded
//! sets return them). Used by unit tests and offline fixture replays; it is also a convenient
//! test double for crates that need an instance without a server.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use meerschaum_connector::traits::async_trait;

use crate::backend::{BackendError, BackendResult, ScoreBound, ValkeyBackend};

#[derive(Debug, Clone)]
enum Entry {
    Str(String),
    ZSet(Vec<(String, f64)>),
    Set(Vec<String>),
    List(Vec<String>),
    Hash(IndexMap<String, String>),
}

impl Entry {
    fn type_name(&self) -> &'static str {
        match self {
            Entry::Str(_) => "string",
            Entry::ZSet(_) => "zset",
            Entry::Set(_) => "set",
            Entry::List(_) => "list",
            Entry::Hash(_) => "hash",
        }
    }
}

/// The in-memory backend.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    data: Mutex<HashMap<String, (Entry, Option<Instant>)>>,
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn wrongtype<T>() -> BackendResult<T> {
    Err(BackendError(WRONGTYPE.into()))
}

/// Redis-style index normalization for `ZRANGE` / `LRANGE`.
fn range_indices(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if len == 0 || s > e || s >= len {
        return None;
    }
    Some((s as usize, e as usize))
}

/// Glob matching for `KEYS` (`*`, `?` and literal characters).
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

impl MemoryBackend {
    /// An empty backend.
    pub fn new() -> MemoryBackend {
        MemoryBackend::default()
    }

    fn with<R>(&self, f: impl FnOnce(&mut HashMap<String, (Entry, Option<Instant>)>) -> R) -> R {
        let mut guard = self.data.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        guard.retain(|_, (_, exp)| exp.is_none_or(|e| e > now));
        f(&mut guard)
    }

    fn zset_mut<'a>(
        map: &'a mut HashMap<String, (Entry, Option<Instant>)>,
        key: &str,
        create: bool,
    ) -> BackendResult<Option<&'a mut Vec<(String, f64)>>> {
        if create && !map.contains_key(key) {
            map.insert(key.to_string(), (Entry::ZSet(Vec::new()), None));
        }
        match map.get_mut(key) {
            None => Ok(None),
            Some((Entry::ZSet(z), _)) => Ok(Some(z)),
            Some(_) => wrongtype(),
        }
    }

    fn set_mut<'a>(
        map: &'a mut HashMap<String, (Entry, Option<Instant>)>,
        key: &str,
        create: bool,
    ) -> BackendResult<Option<&'a mut Vec<String>>> {
        if create && !map.contains_key(key) {
            map.insert(key.to_string(), (Entry::Set(Vec::new()), None));
        }
        match map.get_mut(key) {
            None => Ok(None),
            Some((Entry::Set(s), _)) => Ok(Some(s)),
            Some(_) => wrongtype(),
        }
    }

    fn list_mut<'a>(
        map: &'a mut HashMap<String, (Entry, Option<Instant>)>,
        key: &str,
        create: bool,
    ) -> BackendResult<Option<&'a mut Vec<String>>> {
        if create && !map.contains_key(key) {
            map.insert(key.to_string(), (Entry::List(Vec::new()), None));
        }
        match map.get_mut(key) {
            None => Ok(None),
            Some((Entry::List(l), _)) => Ok(Some(l)),
            Some(_) => wrongtype(),
        }
    }

    /// Remove empty containers, as Valkey deletes keys whose collection becomes empty.
    fn prune(map: &mut HashMap<String, (Entry, Option<Instant>)>, key: &str) {
        let empty = match map.get(key) {
            Some((Entry::ZSet(z), _)) => z.is_empty(),
            Some((Entry::Set(s), _)) => s.is_empty(),
            Some((Entry::List(l), _)) => l.is_empty(),
            Some((Entry::Hash(h), _)) => h.is_empty(),
            _ => false,
        };
        if empty {
            map.remove(key);
        }
    }
}

fn zcmp(a: &(String, f64), b: &(String, f64)) -> std::cmp::Ordering {
    a.1.partial_cmp(&b.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
}

#[async_trait]
impl ValkeyBackend for MemoryBackend {
    async fn ping(&self) -> BackendResult<bool> {
        Ok(true)
    }

    async fn get(&self, key: &str) -> BackendResult<Option<String>> {
        self.with(|m| match m.get(key) {
            None => Ok(None),
            Some((Entry::Str(s), _)) => Ok(Some(s.clone())),
            Some(_) => wrongtype(),
        })
    }

    async fn set(&self, key: &str, value: &str) -> BackendResult<()> {
        self.with(|m| {
            m.insert(key.to_string(), (Entry::Str(value.to_string()), None));
            Ok(())
        })
    }

    async fn set_ex(&self, key: &str, value: &str, seconds: i64) -> BackendResult<()> {
        self.with(|m| {
            let exp = Instant::now() + Duration::from_secs(seconds.max(0) as u64);
            m.insert(key.to_string(), (Entry::Str(value.to_string()), Some(exp)));
            Ok(())
        })
    }

    async fn set_nx(&self, key: &str, value: &str) -> BackendResult<bool> {
        self.with(|m| {
            if m.contains_key(key) {
                return Ok(false);
            }
            m.insert(key.to_string(), (Entry::Str(value.to_string()), None));
            Ok(true)
        })
    }

    async fn set_nx_px(&self, key: &str, value: &str, ms: u64) -> BackendResult<bool> {
        self.with(|m| {
            if m.contains_key(key) {
                return Ok(false);
            }
            let exp = Instant::now() + Duration::from_millis(ms);
            m.insert(key.to_string(), (Entry::Str(value.to_string()), Some(exp)));
            Ok(true)
        })
    }

    async fn compare_and_pexpire(&self, key: &str, value: &str, ms: u64) -> BackendResult<bool> {
        self.with(|m| match m.get_mut(key) {
            Some((Entry::Str(s), exp)) if s == value => {
                *exp = Some(Instant::now() + Duration::from_millis(ms));
                Ok(true)
            }
            _ => Ok(false),
        })
    }

    async fn compare_and_delete(&self, key: &str, value: &str) -> BackendResult<bool> {
        self.with(|m| match m.get(key) {
            Some((Entry::Str(s), _)) if s == value => {
                m.remove(key);
                Ok(true)
            }
            _ => Ok(false),
        })
    }

    async fn del(&self, keys: &[String]) -> BackendResult<i64> {
        self.with(|m| Ok(keys.iter().filter(|k| m.remove(*k).is_some()).count() as i64))
    }

    async fn exists(&self, key: &str) -> BackendResult<i64> {
        self.with(|m| Ok(m.contains_key(key) as i64))
    }

    async fn incr(&self, key: &str) -> BackendResult<i64> {
        self.with(|m| {
            let current = match m.get(key) {
                None => 0,
                Some((Entry::Str(s), _)) => s.parse::<i64>().map_err(|_| {
                    BackendError("ERR value is not an integer or out of range".into())
                })?,
                Some(_) => return wrongtype(),
            };
            let next = current + 1;
            let exp = m.get(key).and_then(|(_, e)| *e);
            m.insert(key.to_string(), (Entry::Str(next.to_string()), exp));
            Ok(next)
        })
    }

    async fn key_type(&self, key: &str) -> BackendResult<String> {
        self.with(|m| {
            Ok(m.get(key)
                .map(|(e, _)| e.type_name())
                .unwrap_or("none")
                .to_string())
        })
    }

    async fn keys(&self, pattern: &str) -> BackendResult<Vec<String>> {
        self.with(|m| {
            let mut out: Vec<String> = m
                .keys()
                .filter(|k| glob_match(pattern, k))
                .cloned()
                .collect();
            out.sort();
            Ok(out)
        })
    }

    async fn zadd(&self, key: &str, member: &str, score: f64) -> BackendResult<()> {
        self.with(|m| {
            let z = Self::zset_mut(m, key, true)?.expect("created");
            z.retain(|(mem, _)| mem != member);
            z.push((member.to_string(), score));
            z.sort_by(zcmp);
            Ok(())
        })
    }

    async fn zrem(&self, key: &str, member: &str) -> BackendResult<i64> {
        self.with(|m| {
            let removed = match Self::zset_mut(m, key, false)? {
                None => 0,
                Some(z) => {
                    let before = z.len();
                    z.retain(|(mem, _)| mem != member);
                    (before - z.len()) as i64
                }
            };
            Self::prune(m, key);
            Ok(removed)
        })
    }

    async fn zcard(&self, key: &str) -> BackendResult<i64> {
        self.with(|m| Ok(Self::zset_mut(m, key, false)?.map_or(0, |z| z.len() as i64)))
    }

    async fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        rev: bool,
    ) -> BackendResult<Vec<(String, f64)>> {
        self.with(|m| {
            let Some(z) = Self::zset_mut(m, key, false)? else {
                return Ok(Vec::new());
            };
            let mut items = z.clone();
            if rev {
                items.reverse();
            }
            Ok(match range_indices(items.len(), start, stop) {
                None => Vec::new(),
                Some((s, e)) => items[s..=e].to_vec(),
            })
        })
    }

    async fn zrangebyscore(
        &self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
    ) -> BackendResult<Vec<String>> {
        self.with(|m| {
            let Some(z) = Self::zset_mut(m, key, false)? else {
                return Ok(Vec::new());
            };
            Ok(z.iter()
                .filter(|(_, s)| min.le(*s) && max.ge(*s))
                .map(|(mem, _)| mem.clone())
                .collect())
        })
    }

    async fn sadd(&self, key: &str, member: &str) -> BackendResult<()> {
        self.with(|m| {
            let s = Self::set_mut(m, key, true)?.expect("created");
            if !s.iter().any(|x| x == member) {
                s.push(member.to_string());
            }
            Ok(())
        })
    }

    async fn srem(&self, key: &str, member: &str) -> BackendResult<i64> {
        self.with(|m| {
            let removed = match Self::set_mut(m, key, false)? {
                None => 0,
                Some(s) => {
                    let before = s.len();
                    s.retain(|x| x != member);
                    (before - s.len()) as i64
                }
            };
            Self::prune(m, key);
            Ok(removed)
        })
    }

    async fn scard(&self, key: &str) -> BackendResult<i64> {
        self.with(|m| Ok(Self::set_mut(m, key, false)?.map_or(0, |s| s.len() as i64)))
    }

    async fn smembers(&self, key: &str) -> BackendResult<Vec<String>> {
        self.with(|m| Ok(Self::set_mut(m, key, false)?.cloned().unwrap_or_default()))
    }

    async fn llen(&self, key: &str) -> BackendResult<i64> {
        self.with(|m| Ok(Self::list_mut(m, key, false)?.map_or(0, |l| l.len() as i64)))
    }

    async fn rpush(&self, key: &str, value: &str) -> BackendResult<()> {
        self.with(|m| {
            Self::list_mut(m, key, true)?
                .expect("created")
                .push(value.to_string());
            Ok(())
        })
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> BackendResult<Vec<String>> {
        self.with(|m| {
            let Some(l) = Self::list_mut(m, key, false)? else {
                return Ok(Vec::new());
            };
            Ok(match range_indices(l.len(), start, stop) {
                None => Vec::new(),
                Some((s, e)) => l[s..=e].to_vec(),
            })
        })
    }

    async fn hset(&self, key: &str, fields: &[(String, String)]) -> BackendResult<()> {
        self.with(|m| {
            if !m.contains_key(key) {
                m.insert(key.to_string(), (Entry::Hash(IndexMap::new()), None));
            }
            match m.get_mut(key) {
                Some((Entry::Hash(h), _)) => {
                    for (f, v) in fields {
                        h.insert(f.clone(), v.clone());
                    }
                    Ok(())
                }
                _ => wrongtype(),
            }
        })
    }

    async fn hgetall(&self, key: &str) -> BackendResult<Vec<(String, String)>> {
        self.with(|m| match m.get(key) {
            None => Ok(Vec::new()),
            Some((Entry::Hash(h), _)) => {
                Ok(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            Some(_) => wrongtype(),
        })
    }

    async fn flushdb(&self) -> BackendResult<()> {
        self.with(|m| {
            m.clear();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sorted_set_order_and_ranges() {
        let b = MemoryBackend::new();
        b.zadd("z", "b", 1.0).await.unwrap();
        b.zadd("z", "a", 1.0).await.unwrap();
        b.zadd("z", "c", 0.5).await.unwrap();
        let all: Vec<String> = b
            .zrange("z", 0, -1, false)
            .await
            .unwrap()
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        assert_eq!(all, ["c", "a", "b"]);
        assert_eq!(b.zrange("z", 0, 0, true).await.unwrap()[0].0, "b");
        assert_eq!(
            b.zrangebyscore("z", ScoreBound::Value(1.0), ScoreBound::PosInf)
                .await
                .unwrap(),
            ["a", "b"]
        );
        assert_eq!(b.zrem("z", "a").await.unwrap(), 1);
        assert_eq!(b.zcard("z").await.unwrap(), 2);
        assert_eq!(b.key_type("z").await.unwrap(), "zset");
        assert_eq!(b.key_type("missing").await.unwrap(), "none");
    }

    #[tokio::test]
    async fn strings_counters_and_leases() {
        let b = MemoryBackend::new();
        assert_eq!(b.incr("c").await.unwrap(), 1);
        assert_eq!(b.incr("c").await.unwrap(), 2);
        assert!(b.set_nx_px("l", "me", 10_000).await.unwrap());
        assert!(!b.set_nx_px("l", "you", 10_000).await.unwrap());
        assert!(b.compare_and_pexpire("l", "me", 10_000).await.unwrap());
        assert!(!b.compare_and_delete("l", "you").await.unwrap());
        assert!(b.compare_and_delete("l", "me").await.unwrap());
        assert!(b.set_nx_px("l", "you", 1).await.unwrap());
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(b.get("l").await.unwrap(), None);
        assert!(glob_match("mrsm_*", "mrsm_pipes"));
        assert!(!glob_match("mrsm_*", "demo"));
        assert_eq!(b.keys("*").await.unwrap(), ["c"]);
    }
}
