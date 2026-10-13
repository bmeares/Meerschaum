//! The version update check, ported from `meerschaum/_internal/shell/updates.py`.
//!
//! `run_version_check_thread` refreshes two caches in a background thread when
//! `shell:updates:check_remote` is on and the last check is older than
//! `shell:updates:refresh_minutes`: the latest `meerschaum` version from `api:mrsm`
//! (`UPDATES_CACHE_PATH`) and the latest versions of plugins installed from a repository
//! (`PLUGIN_UPDATES_CACHE_PATH`, origins from each plugins directory's origins manifest). The
//! banner reads the caches (written by an earlier session, as in v4) and adds the update
//! messages. Nothing here blocks the shell or fails it: every network call has a 3 second
//! timeout and every error is swallowed.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use meerschaum_core::config::Config;
use meerschaum_core::value::Value;

use crate::format::string_width;

fn read_json(path: &Path) -> Option<serde_json::Value> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// `need_update`: no `last_check_ts`, or it is older than `refresh_minutes`.
fn needs_refresh(cache: Option<&serde_json::Value>, refresh_minutes: f64) -> bool {
    let Some(ts) = cache
        .and_then(|c| c.get("last_check_ts"))
        .and_then(|t| t.as_str())
    else {
        return true;
    };
    match DateTime::parse_from_rfc3339(ts) {
        Ok(t) => {
            let age = Utc::now().signed_duration_since(t.with_timezone(&Utc));
            age.num_milliseconds() as f64 >= refresh_minutes * 60_000.0
        }
        Err(_) => true,
    }
}

/// The inputs of the background check, captured from the config registry.
#[derive(Debug, Clone)]
pub struct UpdateCheck {
    /// `UPDATES_CACHE_PATH`.
    pub cache_path: PathBuf,
    /// `PLUGIN_UPDATES_CACHE_PATH`.
    pub plugins_cache_path: PathBuf,
    /// `shell:updates:refresh_minutes`.
    pub refresh_minutes: f64,
    /// `PLUGINS_DIR_PATHS`.
    pub plugins_dirs: Vec<PathBuf>,
    /// The origins manifest file name (`STATIC_CONFIG['plugins']['origins_filename']`).
    pub origins_filename: String,
    /// The API connectors by label (`api:mrsm` and the plugins' origin repositories).
    pub repositories: Vec<(String, meerschaum_api::client::APIConnector)>,
}

/// `shell:updates:check_remote`.
pub fn check_remote(config: &Config) -> bool {
    config
        .get(&["shell", "updates", "check_remote"])
        .is_none_or(|v| v.truthy())
}

/// The origins of installed plugins (`read_plugin_origins()`): the first directory wins.
pub fn read_plugin_origins(plugins_dirs: &[PathBuf], filename: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for dir in plugins_dirs {
        let Some(serde_json::Value::Object(map)) = read_json(&dir.join(filename)) else {
            continue;
        };
        for (name, keys) in map {
            if let serde_json::Value::String(keys) = keys
                && !keys.is_empty()
                && !out.iter().any(|(n, _)| *n == name)
            {
                out.push((name, keys));
            }
        }
    }
    out
}

/// `_get_local_plugin_version(name)`: the `__version__` literal in the plugin's file.
pub fn local_plugin_version(plugins_dirs: &[PathBuf], name: &str) -> Option<String> {
    let path = plugins_dirs.iter().find_map(|d| {
        let module = d.join(format!("{name}.py"));
        let package = d.join(name).join("__init__.py");
        if module.is_file() {
            Some(module)
        } else if package.is_file() {
            Some(package)
        } else {
            None
        }
    })?;
    let text = std::fs::read_to_string(path).ok()?;
    let re = regex::Regex::new(r"__version__(\s?)=").ok()?;
    for line in text.lines() {
        if !line.contains("__version__") || !re.is_match(line.trim()) {
            continue;
        }
        let value = line.split('=').nth(1)?.trim();
        let unquoted = value
            .strip_prefix('\'')
            .and_then(|v| v.strip_suffix('\''))
            .or_else(|| value.strip_prefix('"').and_then(|v| v.strip_suffix('"')))?;
        return Some(unquoted.to_string());
    }
    None
}

impl UpdateCheck {
    /// Capture the inputs (`None` when update checks are disabled).
    pub fn from_config(config: &Config) -> Option<UpdateCheck> {
        if !check_remote(config) {
            return None;
        }
        let paths = config.paths();
        let refresh_minutes = config
            .get(&["shell", "updates", "refresh_minutes"])
            .and_then(|v| v.as_f64())
            .unwrap_or(180.0);
        let origins_filename = meerschaum_core::static_config::static_config()
            .plugins
            .origins_filename
            .clone();
        let plugins_dirs = paths.plugins_dirs.clone();
        let mut labels: Vec<String> = vec!["mrsm".into()];
        for (_, keys) in read_plugin_origins(&plugins_dirs, &origins_filename) {
            let label = keys.strip_prefix("api:").unwrap_or(&keys).to_string();
            if !labels.contains(&label) {
                labels.push(label);
            }
        }
        let repositories = labels
            .into_iter()
            .filter_map(|label| {
                let mut conn =
                    meerschaum_api::client::APIConnector::from_config(config, &label).ok()?;
                conn.set_timeout(Duration::from_secs(3));
                Some((label, conn))
            })
            .collect();
        Some(UpdateCheck {
            cache_path: paths.path("UPDATES_CACHE_PATH"),
            plugins_cache_path: paths.path("PLUGIN_UPDATES_CACHE_PATH"),
            refresh_minutes,
            plugins_dirs,
            origins_filename,
            repositories,
        })
    }

    fn repository(&self, keys: &str) -> Option<&meerschaum_api::client::APIConnector> {
        let label = keys.strip_prefix("api:").unwrap_or(keys);
        self.repositories
            .iter()
            .find(|(l, _)| l == label)
            .map(|(_, c)| c)
    }

    /// `cache_remote_version()` then `cache_remote_plugin_versions()`.
    pub async fn run(&self) {
        let timeout = Duration::from_secs(3);
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string();
        if needs_refresh(read_json(&self.cache_path).as_ref(), self.refresh_minutes)
            && let Some(conn) = self.repository("api:mrsm")
            && let Ok(Some(version)) = tokio::time::timeout(timeout, conn.get_mrsm_version()).await
        {
            let cache = serde_json::json!({
                "last_check_ts": now,
                "remote_version": version,
            });
            if let Some(parent) = self.cache_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(&self.cache_path, cache.to_string());
        }

        if !needs_refresh(
            read_json(&self.plugins_cache_path).as_ref(),
            self.refresh_minutes,
        ) {
            return;
        }
        let mut plugins = serde_json::Map::new();
        for (name, keys) in read_plugin_origins(&self.plugins_dirs, &self.origins_filename) {
            let Some(local) = local_plugin_version(&self.plugins_dirs, &name) else {
                continue;
            };
            let record = meerschaum_connector::PluginRecord {
                name: name.clone(),
                version: None,
                user_id: None,
                attributes: Default::default(),
            };
            let remote = match self.repository(&keys) {
                Some(conn) => tokio::time::timeout(timeout, conn.get_plugin_version(&record))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .flatten(),
                None => None,
            };
            plugins.insert(
                name,
                serde_json::json!({"repo_keys": keys, "local": local, "remote": remote}),
            );
        }
        let cache = serde_json::json!({"last_check_ts": now, "plugins": plugins});
        if let Some(parent) = self.plugins_cache_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&self.plugins_cache_path, cache.to_string());
    }
}

/// `run_version_check_thread()`: refresh the caches in a detached thread.
pub fn run_version_check_thread(config: &Config) -> Option<std::thread::JoinHandle<()>> {
    let check = UpdateCheck::from_config(config)?;
    std::thread::Builder::new()
        .name("mrsm-update-check".into())
        .spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(check.run());
        })
        .ok()
}

/// `packaging.version.parse(remote) > packaging.version.parse(current)`.
fn newer(remote: &str, current: &str) -> bool {
    !meerschaum_core::version::version_ge(current, remote)
}

/// `get_remote_version_from_cache()`.
pub fn remote_version_from_cache(config: &Config) -> Option<String> {
    let cache = read_json(&config.paths().path("UPDATES_CACHE_PATH"))?;
    cache
        .get("remote_version")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn sgr(codes: &str, text: &str) -> String {
    format!("\x1b[{codes}m{text}\x1b[0m")
}

/// `get_update_message()` as it appears inside the banner. With ANSI on, the banner's own
/// style (`shell.ansi.intro.rich.style`) is applied around the message as v4's
/// `colored(intro, ...)` leaves it: the padding keeps the banner style, the message and the
/// instruction keep bold with their own colors.
pub fn update_message(
    config: &Config,
    charset: &str,
    ansi: bool,
    version: &str,
    intro_sgr: &str,
) -> String {
    let Some(remote) = remote_version_from_cache(config) else {
        return String::new();
    };
    if !newer(&remote, version) {
        return String::new();
    }
    let get = |keys: &[&str]| config.get(keys).map(|v| v.py_str()).unwrap_or_default();
    let intro = get(&["shell", charset, "intro"]);
    let message = get(&["shell", charset, "update_message"]);
    let intro_width = string_width(&intro);
    let msg_width = string_width(&message);
    let pad = " ".repeat(intro_width.saturating_sub(msg_width) / 2);
    let instruction_clean = format!("Run upgrade mrsm to install v{remote}.");
    let pad2 = " ".repeat(intro_width.saturating_sub(string_width(&instruction_clean)) / 2);
    let message_color = match config.get(&["shell", "ansi", "update_message", "color"]) {
        Some(Value::List(colors)) => colors.first().map(|c| c.py_str()).unwrap_or_default(),
        _ => "red".into(),
    };
    let color_code = |name: &str| -> String {
        crate::format::rich::style::Style::parse(&name.replace(' ', "_"))
            .map(|s| s.ansi_codes())
            .unwrap_or_default()
    };
    if !ansi {
        // PARITY: the instruction is colored even without ANSI (v4's `colored` ignores it).
        return format!(
            "\n\n{pad}{message}\n{pad2}{}{}{}{}.",
            sgr("37", "Run "),
            sgr("32", "upgrade mrsm"),
            sgr("37", " to install "),
            sgr("33", &format!("v{remote}")),
        );
    }
    let bold = |codes: &str, text: &str| format!("\x1b[1;{codes}m{text}");
    format!(
        "\n\n\x1b[{intro_sgr}m{pad}{}\n\x1b[{intro_sgr}m{pad2}{}{}{}{}\x1b[0;1m.\x1b[0m",
        bold(&color_code(&message_color), &message),
        bold("37", "Run "),
        bold("32", "upgrade mrsm"),
        bold("37", " to install "),
        bold("33", &format!("v{remote}")),
    )
}

/// `get_plugins_update_message()`.
pub fn plugins_update_message(config: &Config, ansi: bool) -> String {
    let Some(cache) = read_json(&config.paths().path("PLUGIN_UPDATES_CACHE_PATH")) else {
        return String::new();
    };
    let Some(serde_json::Value::Object(plugins)) = cache.get("plugins") else {
        return String::new();
    };
    let mut stale: Vec<(String, String, String)> = Vec::new();
    for (name, versions) in plugins {
        let local = versions.get("local").and_then(|v| v.as_str()).unwrap_or("");
        let remote = versions
            .get("remote")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if local.is_empty() || remote.is_empty() {
            continue;
        }
        if newer(remote, local) {
            stale.push((name.clone(), local.into(), remote.into()));
        }
    }
    if stale.is_empty() {
        return String::new();
    }
    let header = " The following plugins are out of date:";
    let mut lines = vec![if ansi {
        sgr("33", header)
    } else {
        header.to_string()
    }];
    for (name, local, remote) in &stale {
        if ansi {
            lines.push(format!(
                "   - {} ({} → {})",
                sgr("32", name),
                sgr("33", &format!("v{local}")),
                sgr("33", &format!("v{remote}"))
            ));
        } else {
            lines.push(format!("   - {name} (v{local} → v{remote})"));
        }
    }
    lines.push(if ansi {
        format!(
            "{}{}{}",
            sgr("37", " Run "),
            sgr("32", "upgrade plugins"),
            sgr("37", " to update.")
        )
    } else {
        " Run upgrade plugins to update.".to_string()
    });
    format!("\n\n{}", lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_rules() {
        assert!(needs_refresh(None, 180.0));
        let fresh = serde_json::json!({"last_check_ts": Utc::now().to_rfc3339()});
        assert!(!needs_refresh(Some(&fresh), 180.0));
        let old = serde_json::json!({"last_check_ts": "2020-01-01T00:00:00+00:00"});
        assert!(needs_refresh(Some(&old), 180.0));
        assert!(newer("99.0.0", "5.0.0"));
        assert!(!newer("5.0.0", "5.0.0"));
        assert!(newer("5.0.0", "5.0.0rc1"));
    }

    #[test]
    fn origins_and_local_versions() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(".mrsm_origins.json"),
            r#"{"alpha": "api:mrsm", "beta": ""}"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("alpha.py"), "__version__ = '1.2.3'\n").unwrap();
        let dirs = vec![dir.path().to_path_buf()];
        assert_eq!(
            read_plugin_origins(&dirs, ".mrsm_origins.json"),
            vec![("alpha".to_string(), "api:mrsm".to_string())]
        );
        assert_eq!(
            local_plugin_version(&dirs, "alpha").as_deref(),
            Some("1.2.3")
        );
    }
}
