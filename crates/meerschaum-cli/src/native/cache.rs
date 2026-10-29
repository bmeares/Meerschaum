//! `delete cache` / `clear cache` (`meerschaum/actions/delete.py::_delete_cache`): remove the
//! local cache directory, or clean a SQL connector's metadata cache and stale temporary tables,
//! or a Valkey connector's `.cache*` keys.

use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::ArgsMap;

use super::{Instance, flag, info, print_tuple, session, warn, words, yes_no};

/// `humanize.naturalsize(bytes)` (decimal units, one decimal).
pub fn naturalsize(bytes: u64) -> String {
    const SUFFIXES: [&str; 8] = ["kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    if bytes == 1 {
        return "1 Byte".to_string();
    }
    if bytes < 1000 {
        return format!("{bytes} Bytes");
    }
    let mut exp = 0usize;
    let mut value = bytes as f64;
    while value >= 1000.0 && exp < SUFFIXES.len() {
        value /= 1000.0;
        exp += 1;
    }
    format!("{value:.1} {}", SUFFIXES[exp - 1])
}

/// `get_directory_size(path)`: the sum of `st_size` over `path.rglob('*')`.
fn directory_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if let Ok(meta) = std::fs::metadata(&p) {
            total += meta.len();
        }
        if std::fs::symlink_metadata(&p).is_ok_and(|m| m.is_dir()) {
            total += directory_size(&p);
        }
    }
    total
}

/// `delete cache [connector keys ...]`.
pub fn delete_cache(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut connector_keys = words(args, "action");
    connector_keys.extend(words(args, "connector_keys"));
    let chunksize = match super::int(args, "chunksize") {
        Some(n) if n >= 1 => n as usize,
        _ => 1000,
    };
    let (force, yes, noask) = (flag(args, "force"), flag(args, "yes"), flag(args, "noask"));
    let paths = rt.config().paths();
    if connector_keys.is_empty() {
        let cache_path = paths.path("CACHE_RESOURCES_PATH");
        let size = naturalsize(directory_size(&cache_path));
        if !force
            && !yes_no(
                rt,
                &format!(
                    "Are you sure you want to remove the cache directory ({size})?\n    {}",
                    cache_path.display()
                ),
                "y",
                yes,
                noask,
            )?
        {
            return Ok(SuccessTuple::fail("Nothing was deleted."));
        }
        let cleanup = (|| -> std::io::Result<()> {
            if cache_path.exists() {
                std::fs::remove_dir_all(&cache_path)?;
            }
            std::fs::create_dir_all(&cache_path)?;
            // The Python worker re-creates its plugin symlinks (`sync_plugins_symlinks`) the
            // next time it starts.
            let plugins = paths.path("PLUGINS_RESOURCES_PATH");
            if let Ok(entries) = std::fs::read_dir(&plugins) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if std::fs::symlink_metadata(&p).is_ok_and(|m| m.file_type().is_symlink()) {
                        let _ = std::fs::remove_file(&p);
                    }
                }
            }
            Ok(())
        })();
        if let Err(e) = cleanup {
            return Ok(SuccessTuple::fail(format!(
                "Failed to clean up cache:\n{e}"
            )));
        }
        session(rt).invalidate_manifest();
        return Ok(SuccessTuple::ok(format!("Successfully cleared {size}.")));
    }

    let mut results: Vec<(String, SuccessTuple)> = Vec::new();
    for keys in &connector_keys {
        let conn = match super::connectors::connect_instance(rt, keys) {
            Some(c) => c,
            None => {
                let message = format!("Connector '{keys}' does not exist.");
                warn(rt, &message);
                results.push((keys.clone(), SuccessTuple::fail(message)));
                continue;
            }
        };
        if !force
            && !yes_no(
                rt,
                &format!("Clean cache for '{}'?", conn.keys()),
                "y",
                yes,
                noask,
            )?
        {
            info(rt, &format!("Nothing was deleted from '{}'.", conn.keys()));
            continue;
        }
        let result = match &conn {
            Instance::Sql(sql) => clean_sql(rt, sql, &paths),
            Instance::Other(_) if conn.type_name() == "valkey" => clean_valkey(rt, keys, chunksize),
            _ => SuccessTuple::fail(format!("Cannot clean '{}'.", conn.keys())),
        };
        print_tuple(
            rt,
            &result,
            TupleArgs {
                calm: true,
                ..TupleArgs::default()
            },
        );
        results.push((keys.clone(), result));
    }
    let max_len = connector_keys
        .iter()
        .map(|k| k.chars().count())
        .max()
        .unwrap_or(0);
    let success = results.iter().all(|(_, r)| r.success);
    let header = rt.with_formatter(|f| {
        f.make_header_with(
            &format!(
                "Cleared cache for {} connector{}:",
                connector_keys.len(),
                super::plural(connector_keys.len())
            ),
            "─",
            0,
            false,
            1,
        )
    });
    let lines: Vec<String> = results
        .iter()
        .map(|(keys, r)| {
            let buffer = " ".repeat((max_len + 1).saturating_sub(keys.chars().count()));
            format!("{keys}{buffer}| {}", r.message)
        })
        .collect();
    Ok(SuccessTuple {
        success,
        message: format!("{header}\n    {}", lines.join("\n    ")),
    })
}

fn clean_sql(
    rt: &mut Runtime<'_>,
    sql: &std::sync::Arc<meerschaum_connector_sql::SqlConnector>,
    paths: &meerschaum_core::config::paths::Paths,
) -> SuccessTuple {
    let dir = paths.path("SQL_CONN_CACHE_RESOURCES_PATH");
    for name in [
        format!("{}.json", sql.label()),
        format!("{}-metadata.pkl", sql.label()),
    ] {
        let path = dir.join(name);
        if path.exists()
            && let Err(e) = std::fs::remove_file(&path)
        {
            return SuccessTuple::fail(e.to_string());
        }
    }
    let sess = session(rt);
    let (success, message) = sess.block_on(sql.drop_old_temporary_tables(true));
    SuccessTuple { success, message }
}

fn clean_valkey(rt: &mut Runtime<'_>, keys: &str, chunksize: usize) -> SuccessTuple {
    let sess = session(rt);
    let Ok(Instance::Other(_)) = sess.instance(keys, rt.config()) else {
        return SuccessTuple::fail(format!("Cannot clean '{keys}'."));
    };
    let (typ, label) = super::session::split_keys(keys);
    debug_assert_eq!(typ, "valkey");
    let attrs =
        meerschaum_connector_valkey::ValkeyConnector::attributes_from_config(rt.config(), &label);
    let conn = match sess.block_on(meerschaum_connector_valkey::ValkeyConnector::connect(
        &label, attrs,
    )) {
        Ok(c) => c,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let backend = conn.backend().clone();
    let found = match sess.block_on(backend.keys(".cache*")) {
        Ok(k) => k,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let mut deleted = 0usize;
    for chunk in found.chunks(chunksize) {
        if let Err(e) = sess.block_on(backend.del(chunk)) {
            return SuccessTuple::fail(e.to_string());
        }
        deleted += chunk.len();
        info(
            rt,
            &format!("Deleted {deleted} key{}.", super::plural(deleted)),
        );
    }
    SuccessTuple::ok(format!("Deleted {deleted} key{}.", super::plural(deleted)))
}
