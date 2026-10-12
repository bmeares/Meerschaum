//! Size, compression and TimescaleDB Hypercore/columnstore queries
//! (`meerschaum/connectors/sql/_compress.py`).

use crate::dtypes::{
    PrecisionScalar, are_dtypes_equal, precision_unit_alias, precision_unit_scalar, py_float,
    py_is_int,
};
use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::pipe::PipeSpec;
use crate::queries::ConnectorCtx;
use crate::value::PyValue;

/// `_ORDERBY_DIRECTION_KEYWORDS` (`_compress.py:29`).
const ORDERBY_DIRECTION_KEYWORDS: [&str; 5] = ["ASC", "DESC", "NULLS", "FIRST", "LAST"];

/// `_DEFAULT_COMPRESS_AFTER` (`_compress.py:72`).
pub const DEFAULT_COMPRESS_AFTER: &str = "7 days";

/// `_normalize_columnstore_cols(value, flavor, allow_direction)`.
pub fn normalize_columnstore_cols(
    value: &PyValue,
    flavor: Flavor,
    allow_direction: bool,
) -> Vec<String> {
    let raw: Vec<&PyValue> = match value {
        PyValue::List(items) => items.iter().collect(),
        other => vec![other],
    };
    let mut entries: Vec<String> = Vec::new();
    for item in raw {
        for part in item.py_str().split(',') {
            entries.push(crate::pytext::py_strip(part).to_string());
        }
    }
    let mut normalized = Vec::new();
    for entry in entries {
        if entry.is_empty() {
            continue;
        }
        let mut col = entry.clone();
        let mut direction = String::new();
        if allow_direction {
            let tokens: Vec<&str> = entry
                .split(crate::pytext::py_isspace)
                .filter(|t| !t.is_empty())
                .collect();
            if tokens.len() > 1
                && tokens[1..]
                    .iter()
                    .all(|t| ORDERBY_DIRECTION_KEYWORDS.contains(&t.to_uppercase().as_str()))
            {
                col = tokens[0].to_string();
                direction = format!(
                    " {}",
                    tokens[1..]
                        .iter()
                        .map(|t| t.to_uppercase())
                        .collect::<Vec<_>>()
                        .join(" ")
                );
            }
        }
        normalized.push(format!("{}{direction}", sql_item_name(&col, flavor, None)));
    }
    normalized
}

/// The resolved compression settings of `_get_compress_settings`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompressSettings {
    /// Quoted `segmentby` columns.
    pub segmentby: Vec<String>,
    /// Quoted `orderby` columns with optional directions.
    pub orderby: Vec<String>,
    /// The raw `after` value.
    pub after: Option<PyValue>,
}

/// `_get_compress_settings(pipe)`.
pub fn get_compress_settings(flavor: Flavor, pipe: &PipeSpec) -> CompressSettings {
    let settings = match pipe.param("compress") {
        Some(PyValue::Dict(d)) => Some(d.clone()),
        _ => None,
    };
    let setting = |k: &str| -> Option<PyValue> {
        settings
            .as_ref()
            .and_then(|d| d.get(k))
            .filter(|v| !v.is_none())
            .cloned()
    };
    let dt_col = pipe.column("datetime");
    let dt_col_name = dt_col.as_deref().map(|c| sql_item_name(c, flavor, None));
    let id_col = pipe.column("id");
    let id_col_name = id_col.as_deref().map(|c| sql_item_name(c, flavor, None));
    let primary_col = pipe.column("primary");
    let id_is_unique = id_col.is_some() && id_col == primary_col;

    let segmentby = match setting("segmentby") {
        None => match (&id_col_name, id_is_unique) {
            (Some(name), false) => vec![name.clone()],
            _ => Vec::new(),
        },
        Some(v) => normalize_columnstore_cols(&v, flavor, false),
    };
    let orderby = match setting("orderby") {
        None => {
            let mut out = Vec::new();
            if let Some(name) = &dt_col_name {
                out.push(format!("{name} DESC"));
            }
            if id_is_unique {
                if let Some(name) = &id_col_name {
                    if !out.contains(name) {
                        out.push(name.clone());
                    }
                }
            }
            out
        }
        Some(v) => normalize_columnstore_cols(&v, flavor, true),
    };
    CompressSettings {
        segmentby: segmentby.into_iter().filter(|c| !c.is_empty()).collect(),
        orderby: orderby.into_iter().filter(|c| !c.is_empty()).collect(),
        after: settings.as_ref().and_then(|d| d.get("after")).cloned(),
    }
}

/// `_dt_axis_is_integer(pipe)`: reads the configured dtype only.
pub fn dt_axis_is_integer(pipe: &PipeSpec) -> bool {
    let Some(dt_col) = pipe.column("datetime") else {
        return false;
    };
    // Reads `pipe.parameters['dtypes']` directly (no alias resolution, no inference).
    let dt_typ = match pipe.param("dtypes").and_then(|d| d.get(&dt_col)) {
        Some(v) => v.py_str(),
        None => "datetime".to_string(),
    };
    are_dtypes_equal(&dt_typ, "int")
}

/// `_get_precision_unit(pipe)`: the alias-resolved unit, `'second'` if unrecognized.
pub fn get_precision_unit(pipe: &PipeSpec) -> String {
    let unit = pipe.precision_unit();
    let true_unit = precision_unit_alias(&unit)
        .map(str::to_string)
        .unwrap_or(unit);
    if precision_unit_scalar(&true_unit).is_some() {
        true_unit
    } else {
        "second".to_string()
    }
}

/// `_get_precision_scalar(pipe)`.
pub fn get_precision_scalar(pipe: &PipeSpec) -> PrecisionScalar {
    precision_unit_scalar(&get_precision_unit(pipe)).unwrap_or(PrecisionScalar::Int(1))
}

/// Parse a `pandas.Timedelta` string into seconds.
///
/// Supports `'<n> <unit>'` sequences (`'7 days'`, `'1 day 2 hours'`, `'1h30min'`), `HH:MM:SS[.f]`
/// with an optional leading `'<n> days'`, and a leading sign.
/// PARITY: pandas accepts more (ISO 8601 `P1D`); those fall back to the default with a warning,
/// as unparseable values do in Python.
pub fn parse_pd_timedelta_seconds(s: &str) -> Option<f64> {
    let t = crate::pytext::py_strip(s).to_lowercase();
    if t.is_empty() {
        return None;
    }
    let (sign, body) = match t.strip_prefix('-') {
        Some(rest) => (-1.0, rest.trim_start()),
        None => (1.0, t.trim_start_matches('+').trim_start()),
    };
    let unit_seconds = |u: &str| -> Option<f64> {
        Some(match u {
            "w" | "week" | "weeks" => 604800.0,
            "d" | "day" | "days" => 86400.0,
            "h" | "hr" | "hour" | "hours" => 3600.0,
            "m" | "t" | "min" | "mins" | "minute" | "minutes" => 60.0,
            "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
            "ms" | "l" | "milli" | "millis" | "millisecond" | "milliseconds" => 1e-3,
            "us" | "u" | "micro" | "micros" | "microsecond" | "microseconds" => 1e-6,
            "ns" | "n" | "nano" | "nanos" | "nanosecond" | "nanoseconds" => 1e-9,
            _ => return None,
        })
    };
    let chars: Vec<char> = body.chars().collect();
    let mut i = 0;
    let mut total = 0.0;
    let mut matched_any = false;
    while i < chars.len() {
        while i < chars.len() && (chars[i].is_whitespace() || chars[i] == ',') {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let start = i;
        while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
            i += 1;
        }
        if start == i {
            return None;
        }
        let num_str: String = chars[start..i].iter().collect();
        if i < chars.len() && chars[i] == ':' {
            // HH:MM:SS[.f]
            let clock: String = chars[start..]
                .iter()
                .take_while(|c| c.is_ascii_digit() || **c == ':' || **c == '.')
                .collect();
            let parts: Vec<&str> = clock.split(':').collect();
            if parts.len() < 2 || parts.len() > 3 {
                return None;
            }
            let h: f64 = parts[0].parse().ok()?;
            let m: f64 = parts[1].parse().ok()?;
            let sec: f64 = if parts.len() == 3 {
                parts[2].parse().ok()?
            } else {
                0.0
            };
            total += h * 3600.0 + m * 60.0 + sec;
            i = start + clock.chars().count();
            matched_any = true;
            continue;
        }
        let num: f64 = num_str.parse().ok()?;
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        let ustart = i;
        while i < chars.len() && chars[i].is_alphabetic() {
            i += 1;
        }
        let unit: String = chars[ustart..i].iter().collect();
        if unit.is_empty() {
            // A bare number is nanoseconds in pandas.
            total += num * 1e-9;
        } else {
            total += num * unit_seconds(&unit)?;
        }
        matched_any = true;
    }
    if !matched_any {
        return None;
    }
    Some(sign * total)
}

/// `_get_integer_after(pipe, after)`: a compression `after` in integer-axis epoch units.
pub fn get_integer_after(pipe: &PipeSpec, after: Option<&PyValue>) -> i64 {
    let scalar = get_precision_scalar(pipe).as_f64();
    let default_seconds =
        parse_pd_timedelta_seconds(DEFAULT_COMPRESS_AFTER).expect("valid default");
    let after = match after {
        Some(v) if v.is_truthy() => v,
        _ => return (default_seconds * scalar) as i64,
    };
    // A bare number is already in epoch units.
    match after {
        PyValue::Float(f) => return *f as i64,
        // `float(True)` is `1.0`, so `is_int(True)` holds in Python.
        PyValue::Bool(b) => return *b as i64,
        other => {
            let s = other.py_str();
            if py_is_int(&s) {
                return py_float(&s).unwrap_or(0.0) as i64;
            }
        }
    }
    let seconds = match parse_pd_timedelta_seconds(&after.py_str()) {
        Some(s) => s,
        None => {
            tracing::warn!(
                "Could not parse `compress:after` value '{}'; falling back to {DEFAULT_COMPRESS_AFTER}.",
                after.py_str()
            );
            default_seconds
        }
    };
    (seconds * scalar) as i64
}

/// `_get_integer_now_func_name(pipe)`.
pub fn get_integer_now_func_name(pipe: &PipeSpec) -> String {
    format!("mrsm_integer_now_{}", get_precision_unit(pipe))
}

fn pipe_name(ctx: &ConnectorCtx, pipe: &PipeSpec) -> String {
    sql_item_name(
        &pipe.target,
        ctx.flavor,
        ctx.get_pipe_schema(pipe).as_deref(),
    )
}

/// `_is_hypertable`'s catalog query (TimescaleDB only).
pub fn get_is_hypertable_query(ctx: &ConnectorCtx, pipe: &PipeSpec) -> Option<String> {
    if !ctx.flavor.is_timescaledb() {
        return None;
    }
    let schema = ctx
        .get_pipe_schema(pipe)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "public".to_string());
    Some(format!(
        "SELECT 1 FROM timescaledb_information.hypertables\nWHERE hypertable_schema = '{}'\n  AND hypertable_name = '{}'",
        schema.replace('\'', "''"),
        pipe.target.replace('\'', "''")
    ))
}

/// `_get_columnstore_settings_query(pipe)`.
pub fn get_columnstore_settings_query(ctx: &ConnectorCtx, pipe: &PipeSpec) -> String {
    let settings = get_compress_settings(ctx.flavor, pipe);
    let mut set_options = vec!["timescaledb.enable_columnstore".to_string()];
    if !settings.segmentby.is_empty() {
        set_options.push(format!(
            "timescaledb.segmentby = '{}'",
            settings.segmentby.join(", ").replace('\'', "''")
        ));
    }
    if !settings.orderby.is_empty() {
        set_options.push(format!(
            "timescaledb.orderby = '{}'",
            settings.orderby.join(", ").replace('\'', "''")
        ));
    }
    format!(
        "ALTER TABLE {} SET ({})",
        pipe_name(ctx, pipe),
        set_options.join(", ")
    )
}

/// `_get_columnstore_policy_query(pipe)`.
pub fn get_columnstore_policy_query(ctx: &ConnectorCtx, pipe: &PipeSpec) -> String {
    let settings = get_compress_settings(ctx.flavor, pipe);
    let after_clause = if dt_axis_is_integer(pipe) {
        match &settings.after {
            Some(PyValue::Int(i)) => i.to_string(),
            other => get_integer_after(pipe, other.as_ref()).to_string(),
        }
    } else {
        let after = match &settings.after {
            Some(v) if v.is_truthy() => v.py_str(),
            _ => DEFAULT_COMPRESS_AFTER.to_string(),
        };
        format!("INTERVAL '{}'", after.replace('\'', "''"))
    };
    format!(
        "CALL add_columnstore_policy('{}', after => {after_clause}, if_not_exists => true)",
        pipe_name(ctx, pipe)
    )
}

/// `_get_columnstore_remove_policy_query(pipe)`.
pub fn get_columnstore_remove_policy_query(ctx: &ConnectorCtx, pipe: &PipeSpec) -> String {
    format!(
        "CALL remove_columnstore_policy('{}', if_exists => true)",
        pipe_name(ctx, pipe)
    )
}

/// `_get_columnstore_disable_query(pipe)`.
pub fn get_columnstore_disable_query(ctx: &ConnectorCtx, pipe: &PipeSpec) -> String {
    format!(
        "ALTER TABLE {} SET (timescaledb.enable_columnstore = false)",
        pipe_name(ctx, pipe)
    )
}

/// `_get_integer_now_func_queries(pipe)`.
pub fn get_integer_now_func_queries(ctx: &ConnectorCtx, pipe: &PipeSpec) -> Vec<String> {
    if !ctx.flavor.is_timescaledb() || !dt_axis_is_integer(pipe) {
        return Vec::new();
    }
    let name = pipe_name(ctx, pipe);
    let regclass_literal = format!("'{}'", name.replace('\'', "''"));
    let scalar = get_precision_scalar(pipe).py_str();
    let func_name = get_integer_now_func_name(pipe);
    vec![
        format!(
            "CREATE OR REPLACE FUNCTION {func_name}() RETURNS BIGINT\nLANGUAGE SQL STABLE AS $$\n    SELECT FLOOR(EXTRACT(EPOCH FROM now()) * {scalar})::BIGINT\n$$"
        ),
        format!(
            "SELECT set_integer_now_func({regclass_literal}, '{func_name}', replace_if_exists => true)"
        ),
    ]
}

/// The transaction groups `compress_pipe` executes (`_compress.py:573`), or an error message
/// mirroring Python's failure tuple.
pub fn get_compress_query_groups(
    ctx: &ConnectorCtx,
    pipe: &PipeSpec,
    is_hypertable: bool,
    no_policy: bool,
) -> Result<Vec<Vec<String>>, String> {
    let flavor = ctx.flavor;
    if !flavor.compressible() {
        return Err(format!(
            "Compression is not supported for flavor '{}'.",
            flavor.as_str()
        ));
    }
    let name = pipe_name(ctx, pipe);
    let mut groups = Vec::new();
    if flavor.is_timescaledb() {
        if !is_hypertable {
            return Err("not a hypertable".to_string());
        }
        let integer_now = get_integer_now_func_queries(ctx, pipe);
        if !integer_now.is_empty() {
            groups.push(integer_now);
        }
        groups.push(vec![get_columnstore_settings_query(ctx, pipe)]);
        if !no_policy {
            groups.push(vec![get_columnstore_remove_policy_query(ctx, pipe)]);
            groups.push(vec![get_columnstore_policy_query(ctx, pipe)]);
        }
        groups.push(vec![format!(
            "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('{name}') c"
        )]);
    } else if flavor.mysql_partition() {
        groups.push(vec![format!("ALTER TABLE {name} ROW_FORMAT=COMPRESSED")]);
    } else if flavor == Flavor::Mssql {
        groups.push(vec![format!(
            "ALTER TABLE {name} REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)"
        )]);
    }
    Ok(groups)
}

/// The transaction groups `decompress_pipe` executes (`_compress.py:694`).
pub fn get_decompress_query_groups(
    ctx: &ConnectorCtx,
    pipe: &PipeSpec,
    is_hypertable: bool,
    no_policy: bool,
) -> Result<Vec<Vec<String>>, String> {
    let flavor = ctx.flavor;
    if !flavor.compressible() {
        return Err(format!(
            "Decompression is not supported for flavor '{}'.",
            flavor.as_str()
        ));
    }
    let name = pipe_name(ctx, pipe);
    let mut groups = Vec::new();
    if flavor.is_timescaledb() {
        if !is_hypertable {
            return Err("not a hypertable".to_string());
        }
        if !no_policy {
            groups.push(vec![get_columnstore_remove_policy_query(ctx, pipe)]);
        }
        groups.push(vec![format!(
            "SELECT decompress_chunk(c, if_compressed => true) FROM show_chunks('{name}') c"
        )]);
        if !no_policy {
            groups.push(vec![get_columnstore_disable_query(ctx, pipe)]);
        }
    } else if flavor.mysql_partition() {
        groups.push(vec![format!("ALTER TABLE {name} ROW_FORMAT=DYNAMIC")]);
    } else if flavor == Flavor::Mssql {
        groups.push(vec![format!(
            "ALTER TABLE {name} REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = NONE)"
        )]);
    }
    Ok(groups)
}

/// The size queries `get_pipe_size` tries in order; the first non-`NULL` result wins.
///
/// `database` is the MySQL database (`schema or self.database`).
pub fn get_pipe_size_queries(
    ctx: &ConnectorCtx,
    pipe: &PipeSpec,
    database: Option<&str>,
) -> Vec<String> {
    let flavor = ctx.flavor;
    let name = pipe_name(ctx, pipe);
    let mut queries = Vec::new();
    if let Some(template) = flavor.hypertable_query_template() {
        queries.push(crate::pytext::py_format(template, &[("table_name", &name)]));
    }
    match flavor {
        Flavor::Timescaledb
        | Flavor::TimescaledbHa
        | Flavor::Postgresql
        | Flavor::Postgis
        | Flavor::Citus => {
            queries.push(format!(
                "SELECT SUM(pg_total_relation_size(relid))\nFROM pg_partition_tree('{name}')"
            ));
            queries.push(format!("SELECT pg_total_relation_size('{name}')"));
        }
        Flavor::Cockroachdb => queries.push(format!("SELECT pg_total_relation_size('{name}')")),
        Flavor::Mysql | Flavor::Mariadb => {
            let db_name = ctx
                .get_pipe_schema(pipe)
                .filter(|s| !s.is_empty())
                .or_else(|| database.map(str::to_string));
            if let Some(db) = db_name {
                queries.push(format!(
                    "SELECT data_length + index_length\nFROM information_schema.tables\nWHERE table_schema = '{}' AND table_name = '{}'",
                    db.replace('\'', "''"),
                    pipe.target.replace('\'', "''")
                ));
            }
        }
        Flavor::Mssql => queries.push(format!(
            "SELECT SUM(reserved_page_count) * 8192\nFROM sys.dm_db_partition_stats\nWHERE object_id = OBJECT_ID('{}')",
            name.replace('\'', "''")
        )),
        Flavor::Sqlite | Flavor::Geopackage => queries.push(format!(
            "SELECT SUM(pgsize) FROM dbstat WHERE name = '{}'",
            pipe.target.replace('\'', "''")
        )),
        Flavor::Duckdb | Flavor::Oracle => {}
    }
    queries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timedeltas() {
        assert_eq!(parse_pd_timedelta_seconds("7 days"), Some(604800.0));
        assert_eq!(parse_pd_timedelta_seconds("1h30min"), Some(5400.0));
        assert_eq!(parse_pd_timedelta_seconds("1 days 02:00:00"), Some(93600.0));
        assert_eq!(parse_pd_timedelta_seconds("nonsense"), None);
    }

    #[test]
    fn orderby_directions() {
        let v: PyValue = serde_json::from_str(r#"["ts desc nulls last", "a b"]"#).unwrap();
        assert_eq!(
            normalize_columnstore_cols(&v, Flavor::Timescaledb, true),
            vec!["\"ts\" DESC NULLS LAST", "\"a b\""]
        );
    }
}
