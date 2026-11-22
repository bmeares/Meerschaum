//! SQLite in in-place syncs: the storage classes of untyped staged columns as dtypes, and
//! canonical datetime text in the staged table and in the target's sync window.

use indexmap::IndexMap;

use crate::driver::SqlTransaction;
use crate::dtypes::get_db_type_from_pd_type;
use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::queries::metadef::{SQLITE_STORED_DATETIME_GLOB, sqlite_canonical_datetime};

use super::{SqlConnector, frame_docs, kv_str};

/// Whether a SQLite column has no declared type (a computed column of `CREATE TABLE ... AS`).
pub(super) fn untyped(declared: &str) -> bool {
    declared.is_empty() || declared.eq_ignore_ascii_case("null")
}

/// SQLite: `UPDATE` statements rewriting the staged table's datetime columns (those of `cols`
/// in `dt_cols`) in canonical text ([`sqlite_canonical_datetime`]).
pub(super) fn sqlite_canonical_updates<'a>(
    table: &str,
    cols: impl Iterator<Item = &'a String>,
    dt_cols: &[String],
    flavor: Flavor,
) -> Vec<String> {
    cols.filter(|c| dt_cols.contains(c))
        .map(|c| {
            let q = sql_item_name(c, flavor, None);
            format!(
                "UPDATE {table} SET {q} = {} WHERE typeof({q}) = 'text' AND NOT ({q} GLOB '{SQLITE_STORED_DATETIME_GLOB}')",
                sqlite_canonical_datetime(&q)
            )
        })
        .collect()
}

/// SQLite: rewrite the target's non-canonical datetime text in a window, a coarse date-prefix
/// range around `[begin, end]` (SQL expressions; `None`: unbounded) that the datetime index can
/// drive, so that no row of the window escapes a text comparison.
pub(super) fn sqlite_repair_window_query(
    table: &str,
    dt: &str,
    begin: Option<String>,
    end: Option<String>,
) -> String {
    let mut wheres = vec![
        format!("typeof({dt}) = 'text'"),
        format!("NOT ({dt} GLOB '{SQLITE_STORED_DATETIME_GLOB}')"),
    ];
    if let Some(b) = begin {
        wheres.insert(0, format!("{dt} >= date({b}, '-1 day')"));
    }
    if let Some(e) = end {
        wheres.insert(1.min(wheres.len()), format!("{dt} < date({e}, '+2 day')"));
    }
    format!(
        "UPDATE OR IGNORE {table} SET {dt} = {}\nWHERE {}",
        sqlite_canonical_datetime(dt),
        wheres.join("\n    AND ")
    )
}

/// The storage class names (`typeof`) of a column's values as a pandas dtype: `int64`,
/// `float64`, `string`, `bytes`, or `None` when mixed with text or unknown.
pub(super) fn dtype_from_storage_classes(classes: &str) -> Option<&'static str> {
    let mut set: Vec<&str> = classes
        .split(',')
        .map(str::trim)
        .filter(|c| !c.is_empty() && *c != "null")
        .collect();
    set.sort_unstable();
    set.dedup();
    match set.as_slice() {
        ["integer"] => Some("int64"),
        ["real"] | ["integer", "real"] => Some("float64"),
        ["text"] => Some("string"),
        ["blob"] => Some("bytes"),
        _ => None,
    }
}

impl SqlConnector {
    /// A datetime / integer bound as a SQLite expression (`datetime('...', '0 day')` or the
    /// integer).
    pub(super) fn sqlite_bound_sql(&self, bound: &meerschaum_connector::DateBound) -> String {
        match bound {
            meerschaum_connector::DateBound::Int(i) => i.to_string(),
            meerschaum_connector::DateBound::Datetime(d) => format!(
                "datetime('{}', '0 day')",
                d.utc.format("%Y-%m-%d %H:%M:%S%.6f")
            ),
        }
    }

    /// SQLite: the database types for a table created from the staged `table`. A column the
    /// staged table declares a type for (a plain column reference) keeps it, as v4's
    /// `CREATE TABLE ... AS` does; an untyped (computed) column takes the pipe's dtype, else the
    /// storage class of its values; `''` (no type) when none applies.
    pub(super) async fn sqlite_typed_columns(
        &self,
        table: &str,
        staged: &IndexMap<String, String>,
        explicit: &IndexMap<String, String>,
    ) -> IndexMap<String, String> {
        let classes = match self
            .driver()
            .read_frame(&storage_classes_query(table, staged.keys(), self.flavor))
            .await
        {
            Ok(df) => frame_docs(&df).into_iter().next().unwrap_or_default(),
            Err(_) => Default::default(),
        };
        self.typed_from_classes(staged, explicit, &classes)
    }

    /// [`Self::sqlite_typed_columns`] inside the sync's transaction.
    pub(super) async fn sqlite_typed_columns_tx(
        &self,
        tx: &mut Option<SqlTransaction>,
        table: &str,
        staged: &IndexMap<String, String>,
        explicit: &IndexMap<String, String>,
    ) -> IndexMap<String, String> {
        let classes = match tx.as_mut() {
            Some(t) => match t
                .read_frame(&storage_classes_query(table, staged.keys(), self.flavor))
                .await
            {
                Ok(df) => frame_docs(&df).into_iter().next().unwrap_or_default(),
                Err(_) => Default::default(),
            },
            None => Default::default(),
        };
        self.typed_from_classes(staged, explicit, &classes)
    }

    fn typed_from_classes(
        &self,
        staged: &IndexMap<String, String>,
        explicit: &IndexMap<String, String>,
        classes: &IndexMap<String, meerschaum_kernel::Value>,
    ) -> IndexMap<String, String> {
        staged
            .iter()
            .enumerate()
            .map(|(i, (col, declared))| {
                let typ = if !untyped(declared) {
                    declared.clone()
                } else if let Some(dtype) = explicit.get(col) {
                    get_db_type_from_pd_type(dtype, Some(self.flavor))
                } else {
                    kv_str(classes.get(&format!("c{i}")))
                        .as_deref()
                        .and_then(dtype_from_storage_classes)
                        .map(|t| get_db_type_from_pd_type(t, Some(self.flavor)))
                        .unwrap_or_default()
                };
                (col.clone(), typ)
            })
            .collect()
    }
}

/// One row: the distinct storage classes of each column's non-null values (`c0`, `c1`, ...).
fn storage_classes_query<'a>(
    table: &str,
    cols: impl Iterator<Item = &'a String>,
    flavor: Flavor,
) -> String {
    let selects = cols
        .enumerate()
        .map(|(i, c)| {
            let q = sql_item_name(c, flavor, None);
            format!(
                "group_concat(DISTINCT CASE WHEN {q} IS NOT NULL THEN typeof({q}) END) AS \"c{i}\""
            )
        })
        .collect::<Vec<_>>();
    format!("SELECT {} FROM {table}", selects.join(", "))
}
