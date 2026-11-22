//! Compile typed OLD-row evidence alongside
//! the SAME existing native update statements. SQLite only; no generic SQL target parser.

use super::*;

pub(crate) struct GuardedUpdate {
    pub(super) queries: Vec<String>,
    pub(crate) assigned_rows: String,
    pub(crate) touched_old_rows: String,
}
impl GuardedUpdate {
    pub(crate) fn queries(&self) -> &[String] {
        &self.queries
    }
}
fn unknown() -> String {
    "The native update's exact matching-key/assignment contract is unavailable for this window; no unguarded update is allowed.".into()
}

// SQLite column affinity must agree. Staged columns then use the same native affinity as
// the real assignment; a generic CAST(?,type) is not a substitute for that proof.
fn affinity(dtype: &str) -> u8 {
    let dtype = dtype.to_ascii_uppercase();
    if dtype.contains("INT") {
        0
    } else if ["CHAR", "CLOB", "TEXT"]
        .iter()
        .any(|part| dtype.contains(part))
    {
        1
    } else if dtype.is_empty() || dtype.contains("BLOB") {
        2
    } else if ["REAL", "FLOA", "DOUB"]
        .iter()
        .any(|part| dtype.contains(part))
    {
        3
    } else {
        4
    }
}
fn compact(value: &str) -> String {
    let mut result = String::new();
    let mut quote = None;
    let mut chars = value.chars().peekable();
    while let Some(character) = chars.next() {
        if let Some(end) = quote {
            result.push(character);
            if character == end {
                if chars.peek() == Some(&end) {
                    result.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            }
        } else if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
            result.push(character);
        } else if !character.is_whitespace() {
            result.push(character);
        }
    }
    result
}
fn key_list(sql: &str) -> Result<Vec<String>, String> {
    if sql.len() > 32 * 1024 {
        return Err(unknown());
    }
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote = None;
    let mut chars = sql.char_indices().peekable();
    while let Some((offset, character)) = chars.next() {
        if let Some(end) = quote {
            if character == end {
                if chars.peek().is_some_and(|(_, next)| *next == end) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
        } else {
            match character {
                '"' | '\'' | '`' => quote = Some(character),
                '(' => {
                    depth = depth
                        .checked_add(1)
                        .filter(|depth| *depth <= 8)
                        .ok_or_else(unknown)?
                }
                ')' => depth = depth.checked_sub(1).ok_or_else(unknown)?,
                ',' if depth == 0 => {
                    parts.push(sql[start..offset].trim().to_string());
                    start = offset + 1;
                }
                _ => {}
            }
        }
    }
    if quote.is_some() || depth != 0 {
        return Err(unknown());
    }
    parts.push(sql[start..].trim().to_string());
    if parts.is_empty() || parts.len() > 64 || parts.iter().any(String::is_empty) {
        return Err(unknown());
    }
    Ok(parts)
}

pub(crate) fn compile_guarded_update(
    args: &UpdateQueriesArgs<'_>,
    window_axis: Option<&str>,
    whole_relation: bool,
) -> Result<GuardedUpdate, String> {
    if args.flavor!=Flavor::Sqlite || args.identity_insert || args.db_version.is_none_or(|version|{
        let mut parts=version.split('.').map(|part|part.parse::<u32>().ok());
        !matches!((parts.next(),parts.next()),(Some(Some(major)),Some(Some(minor))) if (major,minor)>=(3,33))
    }){return Err(unknown())}
    let queries = get_update_queries(args);
    let target = sql_item_name_opt(args.target, Some(Flavor::Sqlite), args.schema);
    let patch = sql_item_name_opt(
        args.patch,
        Some(Flavor::Sqlite),
        args.patch_schema.or(args.schema),
    );
    if queries.is_empty() {
        return Ok(GuardedUpdate {
            queries,
            assigned_rows: format!("SELECT p.* FROM {patch} p WHERE 0=1"),
            touched_old_rows: format!("SELECT f.* FROM {target} f WHERE 0=1"),
        });
    }
    let q = |column: &str| sql_item_name_opt(column, Some(Flavor::Sqlite), None);
    let columns = args
        .target_cols_types
        .iter()
        .filter(|(column, _)| {
            args.join_cols.contains(column) && args.patch_cols_types.contains_key(*column)
        })
        .collect::<Vec<_>>();
    if columns.is_empty() || columns.len() > 64 {
        return Err(unknown());
    }
    for (column, target_type) in &columns {
        if affinity(target_type) != affinity(&args.patch_cols_types[*column]) {
            return Err(unknown());
        }
    }
    if let Some(axis) = args.datetime_col
        && args
            .target_cols_types
            .get(axis)
            .zip(args.patch_cols_types.get(axis))
            .is_none_or(|(target, patch)| affinity(target) != affinity(patch))
    {
        return Err(unknown());
    }
    if let Some(axis) = window_axis {
        let target_type = args.target_cols_types.get(axis).ok_or_else(unknown)?;
        match args.patch_cols_types.get(axis) {
            Some(patch_type) if affinity(target_type) == affinity(patch_type) => {}
            Some(_) => return Err(unknown()),
            // UPDATE preserves an omitted axis. An INSERT branch of UPSERT may
            // receive a database-generated value; finite windows cannot assume it.
            None if args.upsert && !whole_relation => return Err(unknown()),
            None => {}
        }
    }
    let ordinary = columns
        .iter()
        .map(|(column, dtype)| {
            let name = q(column);
            if args.null_indices {
                let null = get_null_replacement(dtype, Some(Flavor::Sqlite));
                format!("COALESCE(f.{name},{null})=COALESCE(p.{name},{null})")
            } else {
                format!("f.{name}=p.{name}")
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let matches = if !args.upsert {
        let bounds = if let Some(axis) = args.datetime_col {
            let axis = q(axis);
            format!(
                "f.{axis}>=(SELECT MIN({axis}) FROM {patch}) AND f.{axis}<=(SELECT MAX({axis}) FROM {patch})"
            )
        } else {
            "1=1".into()
        };
        format!("({ordinary}) AND ({bounds})")
    } else {
        // Accept only the native selected arbiter's known plain/COALESCE expressions.
        // Unknown functions, collations, partial/exotic keys remain explicit refusal.
        let keys = key_list(
            args.conflict_target.as_deref().unwrap_or(
                &args
                    .join_cols
                    .iter()
                    .map(|column| q(column))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        )?;
        let mut seen = std::collections::BTreeSet::new();
        let mut arbiter = Vec::new();
        for expression in keys {
            let mut found = None;
            for (column, dtype) in &columns {
                let name = q(column);
                let null = get_null_replacement(dtype, Some(Flavor::Sqlite));
                if compact(&expression) == compact(&name) {
                    found = Some(((*column).clone(), format!("f.{name}=p.{name}")));
                    break;
                }
                if compact(&expression) == compact(&format!("COALESCE({name},{null})")) {
                    found = Some((
                        (*column).clone(),
                        format!("COALESCE(f.{name},{null})=COALESCE(p.{name},{null})"),
                    ));
                    break;
                }
            }
            let (column, predicate) = found.ok_or_else(unknown)?;
            if !seen.insert(column) {
                return Err(unknown());
            }
            arbiter.push(predicate);
        }
        if seen.len() != columns.len() {
            return Err(unknown());
        }
        let arbiter = arbiter.join(" AND ");
        let nullable = columns
            .iter()
            .filter(|(column, _)| Some(column.as_str()) != args.datetime_col)
            .map(|(column, _)| column.as_str())
            .collect::<Vec<_>>();
        if args.null_safe_upsert && !nullable.is_empty() {
            let all_set = nullable
                .iter()
                .map(|column| format!("p.{} IS NOT NULL", q(column)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let any_null = nullable
                .iter()
                .map(|column| format!("p.{} IS NULL", q(column)))
                .collect::<Vec<_>>()
                .join(" OR ");
            let null_match = columns
                .iter()
                .map(|(column, _)| {
                    let name = q(column);
                    if Some(column.as_str()) == args.datetime_col {
                        format!("f.{name}=p.{name}")
                    } else {
                        format!("f.{name} IS p.{name}")
                    }
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("(({all_set}) AND ({arbiter})) OR (({any_null}) AND ({null_match}))")
        } else {
            arbiter
        }
    };
    let old_rows = format!("SELECT f.* FROM {target} AS f JOIN {patch} AS p ON ({matches})");
    let assigned_rows = if !args.upsert
        && window_axis.is_some_and(|axis| !args.patch_cols_types.contains_key(axis))
    {
        old_rows.clone()
    } else {
        format!("SELECT p.* FROM {patch} AS p")
    };
    Ok(GuardedUpdate {
        queries,
        assigned_rows,
        touched_old_rows: old_rows,
    })
}
