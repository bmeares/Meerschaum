//! Key naming and document serialization (`meerschaum/connectors/valkey/_pipes.py`,
//! `_users.py`, `_plugins.py`, `_ValkeyConnector.py`).
//!
//! Layout on the instance:
//!
//! | Key | Type | Content |
//! |---|---|---|
//! | `mrsm_pipes` | zset (score `pipe_id`) | `{"connector_keys","location_key","metric_key","pipe_id"}` |
//! | `mrsm_pipes:counter` | string | the last pipe ID (`INCR`) |
//! | `mrsm_pipe:{ck}:{mk}:{lk}` | string | the pipe ID |
//! | `mrsm_pipe:{ck}:{mk}:{lk}:parameters` | string | `json.dumps(parameters)` |
//! | `{target}` | zset (score: datetime axis, epoch seconds) or set | `{"ix": index_key[, dt_col: value]}` |
//! | `{target}:datetime_column` | string | the datetime column of the zset |
//! | `{target}:indices:{index_key}` | string | the full row as sorted JSON |
//! | `mrsm_user:user_id:{id}:{attributes,email,type,password_hash,username}` | string | user fields |
//! | `mrsm_user:username:{name}:user_id` | string | the user ID |
//! | `mrsm_plugin:{name}:{attributes,version,user_id}` | string | plugin fields |
//! | `mrsm_lease:{pipe_ref}` | string with TTL | the lease holder (new in v5, §5.8) |

use meerschaum_kernel::value::Value as KValue;

use meerschaum_api::pipe_ops::Doc;

/// `STATIC_CONFIG['valkey']['colon']`: `:` inside index values is replaced with this.
pub const COLON: &str = "-_";
/// The pipes "table".
pub const PIPES_TABLE: &str = "mrsm_pipes";
/// The pipe ID counter.
pub const PIPES_COUNTER: &str = "mrsm_pipes:counter";
/// The users "table".
pub const USERS_TABLE: &str = "mrsm_users";
/// The users key prefix.
pub const USER_PREFIX: &str = "mrsm_user";
/// The plugins "table".
pub const PLUGINS_TABLE: &str = "mrsm_plugins";
/// The plugins key prefix.
pub const PLUGIN_PREFIX: &str = "mrsm_plugin";
/// The lease key prefix (§5.8).
pub const LEASE_PREFIX: &str = "mrsm_lease";
/// `ValkeyConnector.KEY_SEPARATOR`.
pub const KEY_SEPARATOR: &str = ":";

/// `get_pipe_key(pipe)`: `mrsm_pipe:{connector_keys}:{metric_key}:{location_key}`.
pub fn get_pipe_key(connector_keys: &str, metric_key: &str, location_key: Option<&str>) -> String {
    format!(
        "mrsm_pipe:{connector_keys}:{metric_key}:{}",
        location_key.unwrap_or("None")
    )
}

/// `get_pipe_parameters_key(pipe)`.
pub fn get_pipe_parameters_key(
    connector_keys: &str,
    metric_key: &str,
    location_key: Option<&str>,
) -> String {
    get_pipe_key(connector_keys, metric_key, location_key) + ":parameters"
}

/// `shlex.quote(table)` (`ValkeyConnector.quote_table`).
pub fn quote_table(table: &str) -> String {
    if table.is_empty() {
        return "''".into();
    }
    let safe = table.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || matches!(c, '_' | '@' | '%' | '+' | '=' | ':' | ',' | '.' | '/' | '-')
    });
    if safe {
        return table.to_string();
    }
    format!("'{}'", table.replace('\'', "'\"'\"'"))
}

/// `get_counter_key(table)`.
pub fn get_counter_key(table: &str) -> String {
    format!("{}:counter", quote_table(table))
}

/// `get_datetime_column_key(table)`.
pub fn get_datetime_column_key(table: &str) -> String {
    format!("{}:datetime_column", quote_table(table))
}

/// `get_entity_key(*keys)`: join with `:`; a key containing `:` is an error.
pub fn get_entity_key(keys: &[&str]) -> Result<String, String> {
    if keys.is_empty() {
        return Err("No keys to be joined.".into());
    }
    for key in keys {
        if key.contains(KEY_SEPARATOR) {
            return Err(format!("Key cannot contain separator '{KEY_SEPARATOR}'."));
        }
    }
    Ok(keys.join(KEY_SEPARATOR))
}

/// `get_user_key(user_id_or_username, sub_key, by_username)`.
pub fn get_user_key(
    user_id_or_username: &str,
    sub_key: &str,
    by_username: bool,
) -> Result<String, String> {
    let key_type = if by_username { "username" } else { "user_id" };
    get_entity_key(&[USER_PREFIX, key_type, user_id_or_username, sub_key])
}

/// `get_plugin_key(plugin_name, sub_key)`.
pub fn get_plugin_key(plugin_name: &str, sub_key: &str) -> Result<String, String> {
    get_entity_key(&[PLUGIN_PREFIX, plugin_name, sub_key])
}

/// The lease key for a pipe (`mrsm_lease:{pipe_ref}`).
pub fn get_lease_key(pipe_ref: &str) -> String {
    format!("{LEASE_PREFIX}:{pipe_ref}")
}

/// Seconds since the epoch, truncated toward zero like Python's `int(dt.timestamp())`.
fn epoch_seconds(utc: &chrono::NaiveDateTime) -> i64 {
    utc.and_utc().timestamp_micros() / 1_000_000
}

/// The string form of one index value inside a document key: datetimes become epoch seconds
/// (`int(coerce_timezone(val).replace(tzinfo=utc).timestamp())`), everything else
/// `str(val).replace(':', COLON)`.
pub fn index_value_str(val: &KValue) -> String {
    match val {
        // `coerce_timezone` converts aware values to UTC; naive values are read as UTC.
        KValue::Datetime(dt) => epoch_seconds(&dt.utc).to_string(),
        other => other.py_str().replace(':', COLON),
    }
}

/// `get_document_key(doc, indices, table_name)`: the sorted `key-_value` pairs of the index
/// columns (all columns if `indices` is empty), joined by commas, prefixed with
/// `{table_name}:indices:` when a table is given.
pub fn get_document_key(doc: &Doc, indices: &[String], table_name: Option<&str>) -> String {
    let mut pairs: Vec<String> = doc
        .iter()
        .filter(|(k, _)| indices.is_empty() || indices.contains(k))
        .map(|(k, v)| format!("{k}{COLON}{}", index_value_str(v)))
        .collect();
    pairs.sort();
    let prefix = match table_name {
        Some(t) if !t.is_empty() => format!("{t}:indices:"),
        _ => String::new(),
    };
    prefix + &pairs.join(",")
}

/// The `ts` score `push_docs` computes for a document's datetime value.
///
/// PARITY: `dateutil_parser.parse(str(val)).replace(tzinfo=timezone.utc)` keeps the *wall time*
/// and relabels it UTC, so a non-UTC aware value is scored by its local wall time
/// (`_ValkeyConnector.py`, `push_docs`).
pub fn push_score(val: Option<&KValue>) -> Result<i64, String> {
    match val {
        None => Ok(0),
        Some(KValue::Int(i)) => Ok(*i),
        Some(KValue::Bool(b)) => Ok(*b as i64),
        Some(KValue::Datetime(dt)) => Ok(epoch_seconds(&dt.local())),
        Some(KValue::Date(d)) => Ok(d.and_hms_opt(0, 0, 0).map_or(0, |dt| epoch_seconds(&dt))),
        Some(KValue::Str(s)) => meerschaum_kernel::value::parse_datetime_value(s)
            .map(|d| epoch_seconds(&d.local()))
            .ok_or_else(|| format!("String does not contain a date: {s}")),
        Some(other) => Err(format!(
            "Cannot use a value of type '{}' as a datetime index: {}",
            other.type_name(),
            other.py_str()
        )),
    }
}

/// A datetime-axis bound (`begin` / `end`) as a score: `int(dt.replace(tzinfo=utc).timestamp())`.
///
/// PARITY: like [`push_score`], an aware bound's wall time is relabeled UTC.
pub fn bound_score(val: &KValue) -> Result<i64, String> {
    match val {
        KValue::Int(i) => Ok(*i),
        KValue::Datetime(dt) => Ok(epoch_seconds(&dt.local())),
        KValue::Str(s) => meerschaum_kernel::value::parse_datetime_value(s)
            // `coerce_timezone(dateutil_parser.parse(begin))` converts to UTC first.
            .map(|d| epoch_seconds(&d.utc))
            .ok_or_else(|| format!("String does not contain a date: {s}")),
        other => Err(format!("Invalid bound: {}", other.py_str())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_kernel::value::DateTimeValue;

    #[test]
    fn key_names() {
        assert_eq!(
            get_pipe_key("demo", "temperature", Some("home")),
            "mrsm_pipe:demo:temperature:home"
        );
        assert_eq!(
            get_pipe_parameters_key("demo", "humidity", None),
            "mrsm_pipe:demo:humidity:None:parameters"
        );
        assert_eq!(
            quote_table("demo_temperature_home"),
            "demo_temperature_home"
        );
        assert_eq!(quote_table("a b"), "'a b'");
        assert_eq!(quote_table("it's"), "'it'\"'\"'s'");
        assert_eq!(quote_table(""), "''");
        assert_eq!(get_counter_key("mrsm_pipes"), "mrsm_pipes:counter");
        assert_eq!(
            get_user_key("deadbeef", "attributes", false).unwrap(),
            "mrsm_user:user_id:deadbeef:attributes"
        );
        assert_eq!(
            get_user_key("foo", "user_id", true).unwrap(),
            "mrsm_user:username:foo:user_id"
        );
        assert!(get_user_key("a:b", "x", true).is_err());
        assert_eq!(
            get_plugin_key("noaa", "version").unwrap(),
            "mrsm_plugin:noaa:version"
        );
    }

    #[test]
    fn document_keys() {
        let dt = DateTimeValue::utc(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        let doc: Doc = [
            ("ts".to_string(), KValue::Datetime(dt)),
            ("station".to_string(), KValue::Str("a:b".into())),
            ("val".to_string(), KValue::Float(1.5)),
        ]
        .into_iter()
        .collect();
        let indices = vec!["ts".to_string(), "station".to_string()];
        assert_eq!(
            get_document_key(&doc, &indices, None),
            "station-_a-_b,ts-_1704067200"
        );
        assert_eq!(
            get_document_key(&doc, &indices, Some("demo")),
            "demo:indices:station-_a-_b,ts-_1704067200"
        );
        assert_eq!(
            get_document_key(&doc, &[], None),
            "station-_a-_b,ts-_1704067200,val-_1.5"
        );
        assert_eq!(push_score(doc.get("ts")).unwrap(), 1704067200);
        assert_eq!(push_score(None).unwrap(), 0);
        assert_eq!(push_score(Some(&KValue::Int(3))).unwrap(), 3);
    }
}
