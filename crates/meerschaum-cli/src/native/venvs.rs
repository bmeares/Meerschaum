//! Virtual environment accounting (design D20, §12.1): `show venvs`, `prune venvs`, and the
//! `upgrade` hint.
//!
//! `attempt_import` records who asked for each package in `venvs/<name>/.mrsm_requests.json`
//! (`meerschaum/utils/venv/_requests.py` documents the schema). This module reads that ledger
//! next to the installed distributions (`*.dist-info` under `site-packages`).
//!
//! # Prune policy
//!
//! A package is removed from a venv only when it has ledger entries and none of them is still
//! needed, and nothing needed depends on it:
//!
//! - a request from `meerschaum` (an extra or a core install) is always needed;
//! - a request from a plugin that is still installed is needed;
//! - any package named in an installed plugin's `required` list is needed, in every venv;
//! - packages with **no** ledger entry (installed before the ledger existed, or by tools that do
//!   not record) are kept and reported as unattributed: nothing can say who wants them;
//! - dependencies (`Requires-Dist`, without `extra` markers) of needed packages are needed;
//! - `pip`, `setuptools`, `wheel`, `uv` and `PyYAML` are never removed.
//!
//! The removal goes through `uninstall packages --venv <name>` (the same code path as v4's
//! action), then the ledger entries of the removed packages are dropped.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use meerschaum_core::SuccessTuple;
use serde_json::Value as Json;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::OptionsArgs;
use crate::value::{ArgsMap, PyValue};

use super::show::format_bytes;
use super::{flag, info, plural, warn, words, yes_no};

/// The ledger file inside a venv directory.
pub const REQUESTS_FILE_NAME: &str = ".mrsm_requests.json";

/// The requester recorded for extras and core installs.
pub const CORE_REQUESTER: &str = "meerschaum";

/// Packages that venv tooling needs; never pruned.
const PROTECTED: &[&str] = &["pip", "setuptools", "wheel", "uv", "pyyaml"];

/// PEP 503 normalization of a distribution name (version specifiers and extras are dropped).
pub fn normalize(name: &str) -> String {
    let base = name
        .trim()
        .split(['[', '=', '<', '>', ',', '!', '~', ';', ' ', '@'])
        .next()
        .unwrap_or_default();
    let mut out = String::new();
    let mut last_sep = false;
    for c in base.chars() {
        if matches!(c, '-' | '_' | '.') {
            if !last_sep {
                out.push('-');
            }
            last_sep = true;
        } else {
            out.extend(c.to_lowercase());
            last_sep = false;
        }
    }
    out
}

/// One request recorded in the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub requester: String,
    pub spec: String,
    pub first_requested: String,
    pub last_requested: String,
}

/// An installed distribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dist {
    /// The normalized name.
    pub name: String,
    /// The name as the distribution spells it.
    pub display_name: String,
    pub version: String,
    /// Normalized names of the unconditional (no `extra`) requirements.
    pub requires: Vec<String>,
    /// Who requested it, from the ledger (empty when unattributed).
    pub requests: Vec<Request>,
}

/// One virtual environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenvInfo {
    pub name: String,
    pub path: PathBuf,
    /// Bytes on disk (symlinks are not followed).
    pub size: u64,
    /// Sorted by normalized name.
    pub dists: Vec<Dist>,
}

/// The `site-packages` directories of a venv (one per Python minor version).
pub fn site_packages(venv: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let windows = venv.join("Lib").join("site-packages");
    if windows.is_dir() {
        found.push(windows);
    }
    if let Ok(entries) = std::fs::read_dir(venv.join("lib")) {
        let mut dirs: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("python"))
            })
            .map(|p| p.join("site-packages"))
            .filter(|p| p.is_dir())
            .collect();
        dirs.sort();
        found.extend(dirs);
    }
    found
}

/// Bytes under `path` (files only; symlinks are counted as their own size).
pub fn dir_size(path: &Path) -> u64 {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if meta.is_file() || meta.file_type().is_symlink() {
        return meta.len();
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries.flatten().map(|e| dir_size(&e.path())).sum()
}

/// Read `venvs/<name>/.mrsm_requests.json`: normalized package name to its requests.
/// A missing or malformed ledger is empty.
pub fn read_ledger(venv: &Path) -> BTreeMap<String, Vec<Request>> {
    let mut ledger = BTreeMap::new();
    let Ok(text) = std::fs::read_to_string(venv.join(REQUESTS_FILE_NAME)) else {
        return ledger;
    };
    let Ok(json) = serde_json::from_str::<Json>(&text) else {
        return ledger;
    };
    let Some(packages) = json.get("packages").and_then(Json::as_object) else {
        return ledger;
    };
    for (name, entry) in packages {
        let Some(requesters) = entry.get("requesters").and_then(Json::as_object) else {
            continue;
        };
        let text = |v: &Json, key: &str| {
            v.get(key)
                .and_then(Json::as_str)
                .unwrap_or_default()
                .to_string()
        };
        let mut requests: Vec<Request> = requesters
            .iter()
            .map(|(requester, v)| Request {
                requester: requester.clone(),
                spec: text(v, "spec"),
                first_requested: text(v, "first_requested"),
                last_requested: text(v, "last_requested"),
            })
            .collect();
        requests.sort_by(|a, b| a.requester.cmp(&b.requester));
        if !requests.is_empty() {
            ledger.insert(normalize(name), requests);
        }
    }
    ledger
}

/// Remove `packages` from the ledger (atomic write). A missing ledger is left alone.
pub fn forget_packages(venv: &Path, packages: &[String]) -> std::io::Result<()> {
    let path = venv.join(REQUESTS_FILE_NAME);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(());
    };
    let Ok(mut json) = serde_json::from_str::<Json>(&text) else {
        return Ok(());
    };
    let Some(map) = json.get_mut("packages").and_then(Json::as_object_mut) else {
        return Ok(());
    };
    let drop: BTreeSet<String> = packages.iter().map(|p| normalize(p)).collect();
    map.retain(|name, _| !drop.contains(&normalize(name)));
    let temp = venv.join(format!(
        ".mrsm_requests.{}.{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::write(&temp, serde_json::to_vec_pretty(&json).unwrap_or_default())?;
    std::fs::rename(&temp, &path).inspect_err(|_| {
        let _ = std::fs::remove_file(&temp);
    })
}

fn parse_metadata(text: &str) -> (Option<String>, Option<String>, Vec<String>) {
    let (mut name, mut version, mut requires) = (None, None, Vec::new());
    for line in text.lines() {
        if line.is_empty() {
            // Headers end at the first blank line; the rest is the long description.
            break;
        }
        if let Some(v) = line.strip_prefix("Name:") {
            name.get_or_insert_with(|| v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("Version:") {
            version.get_or_insert_with(|| v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("Requires-Dist:") {
            let (spec, marker) = match v.split_once(';') {
                Some((s, m)) => (s, m),
                None => (v, ""),
            };
            if marker.contains("extra") {
                continue;
            }
            let requirement = normalize(spec);
            if !requirement.is_empty() {
                requires.push(requirement);
            }
        }
    }
    (name, version, requires)
}

/// The distributions installed in `site_packages_dir`.
pub fn read_dists(site_packages_dir: &Path) -> Vec<Dist> {
    let Ok(entries) = std::fs::read_dir(site_packages_dir) else {
        return Vec::new();
    };
    let mut dists = Vec::new();
    for entry in entries.flatten() {
        let file_name = entry.file_name().to_string_lossy().into_owned();
        let Some(stem) = file_name.strip_suffix(".dist-info") else {
            continue;
        };
        let (dir_name, dir_version) = stem.split_once('-').unwrap_or((stem, ""));
        let (name, version, requires) = std::fs::read_to_string(entry.path().join("METADATA"))
            .map(|t| parse_metadata(&t))
            .unwrap_or((None, None, Vec::new()));
        let display_name = name.unwrap_or_else(|| dir_name.to_string());
        dists.push(Dist {
            name: normalize(&display_name),
            display_name,
            version: version.unwrap_or_else(|| dir_version.to_string()),
            requires,
            requests: Vec::new(),
        });
    }
    dists
}

/// Inspect one venv directory (`None` when it has no `site-packages`, like v4's `venv_exists`).
pub fn scan_venv(venv: &Path) -> Option<VenvInfo> {
    let sites = site_packages(venv);
    if sites.is_empty() {
        return None;
    }
    let ledger = read_ledger(venv);
    let mut by_name: BTreeMap<String, Dist> = BTreeMap::new();
    for site in &sites {
        for mut dist in read_dists(site) {
            dist.requests = ledger.get(&dist.name).cloned().unwrap_or_default();
            by_name.insert(dist.name.clone(), dist);
        }
    }
    Some(VenvInfo {
        name: venv.file_name()?.to_string_lossy().into_owned(),
        path: venv.to_path_buf(),
        size: dir_size(venv),
        dists: by_name.into_values().collect(),
    })
}

/// Every venv under `venvs_dir`, sorted by name.
pub fn scan_venvs(venvs_dir: &Path) -> Vec<VenvInfo> {
    let Ok(entries) = std::fs::read_dir(venvs_dir) else {
        return Vec::new();
    };
    let mut venvs: Vec<VenvInfo> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| scan_venv(&e.path()))
        .collect();
    venvs.sort_by(|a, b| a.name.cmp(&b.name));
    venvs
}

/// What the installed plugins say they need.
#[derive(Debug, Clone, Default)]
pub struct PluginNeeds {
    /// Every installed plugin (including those that failed to import) and its `required` list.
    pub plugins: BTreeMap<String, Vec<String>>,
}

impl PluginNeeds {
    fn required_names(&self) -> BTreeSet<String> {
        self.plugins
            .values()
            .flatten()
            .map(|r| normalize(r))
            .chain(crate::extras::declared_packages())
            .collect()
    }
}

/// A package that `prune venvs` would remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removal {
    pub name: String,
    pub version: String,
    /// The requesters, all of which are no longer installed.
    pub requesters: Vec<String>,
}

/// What `prune venvs` would do in one venv.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunePlan {
    pub venv: String,
    pub path: PathBuf,
    pub remove: Vec<Removal>,
    /// Packages kept because nothing recorded who wants them.
    pub unattributed: usize,
}

/// Decide what to prune from `venv` (see the module documentation for the policy).
pub fn plan_prune(venv: &VenvInfo, needs: &PluginNeeds) -> PrunePlan {
    let required = needs.required_names();
    let mut needed: BTreeSet<String> = BTreeSet::new();
    let mut unattributed = 0;
    for dist in &venv.dists {
        let wanted = PROTECTED.contains(&dist.name.as_str())
            || required.contains(&dist.name)
            || dist
                .requests
                .iter()
                .any(|r| r.requester == CORE_REQUESTER || needs.plugins.contains_key(&r.requester));
        if dist.requests.is_empty() {
            unattributed += 1;
        }
        if wanted || dist.requests.is_empty() {
            needed.insert(dist.name.clone());
        }
    }
    // Dependencies of needed packages are needed.
    let by_name: BTreeMap<&str, &Dist> = venv.dists.iter().map(|d| (d.name.as_str(), d)).collect();
    let mut queue: Vec<String> = needed.iter().cloned().collect();
    while let Some(name) = queue.pop() {
        let Some(dist) = by_name.get(name.as_str()) else {
            continue;
        };
        for requirement in &dist.requires {
            if by_name.contains_key(requirement.as_str()) && needed.insert(requirement.clone()) {
                queue.push(requirement.clone());
            }
        }
    }
    let remove = venv
        .dists
        .iter()
        .filter(|d| !needed.contains(&d.name))
        .map(|d| Removal {
            name: d.name.clone(),
            version: d.version.clone(),
            requesters: d.requests.iter().map(|r| r.requester.clone()).collect(),
        })
        .collect();
    PrunePlan {
        venv: venv.name.clone(),
        path: venv.path.clone(),
        remove,
        unattributed,
    }
}

/// Plans for every venv that has something to remove.
pub fn plan_prune_all(venvs_dir: &Path, needs: &PluginNeeds) -> Vec<PrunePlan> {
    scan_venvs(venvs_dir)
        .iter()
        .map(|v| plan_prune(v, needs))
        .filter(|p| !p.remove.is_empty())
        .collect()
}

/// The venvs directory (`VIRTENV_RESOURCES_PATH`).
fn venvs_dir(rt: &mut Runtime<'_>) -> PathBuf {
    let paths = rt.config().paths();
    paths
        .get("VIRTENV_RESOURCES_PATH")
        .map(|p| PathBuf::from(p.as_posix()))
        .unwrap_or_else(|| paths.root_dir.join("venvs"))
}

fn plugin_needs(rt: &mut Runtime<'_>) -> Result<PluginNeeds, ActionError> {
    let manifest = crate::python::manifest(rt)?;
    Ok(PluginNeeds {
        plugins: manifest
            .plugins
            .iter()
            .map(|(name, plugin)| (name.clone(), plugin.required.clone()))
            .collect(),
    })
}

fn requesters_text(dist: &Dist) -> String {
    if dist.requests.is_empty() {
        return "unattributed".to_string();
    }
    dist.requests
        .iter()
        .map(|r| r.requester.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The text `show venvs` prints below the venv list.
pub fn render_venvs(venvs: &[VenvInfo]) -> String {
    let mut out = String::new();
    for venv in venvs {
        out.push_str(&format!(
            "\n{}\n    Path: {}\n    Size: {}\n    Packages: {}\n",
            venv.name,
            venv.path.display(),
            format_bytes(Some(venv.size as i64)),
            venv.dists.len(),
        ));
        let width = venv
            .dists
            .iter()
            .map(|d| d.display_name.chars().count() + 1 + d.version.chars().count())
            .max()
            .unwrap_or(0);
        for dist in &venv.dists {
            let label = format!("{} {}", dist.display_name, dist.version);
            out.push_str(&format!(
                "      - {label:<width$}  [{}]\n",
                requesters_text(dist)
            ));
        }
    }
    out
}

/// `show venvs`: v4's list of virtual environments, then each one's path, size on disk and
/// packages with their versions and requesters.
pub fn show_venvs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let dir = venvs_dir(rt);
    let venvs = scan_venvs(&dir);
    let nopretty = flag(args, "nopretty");
    let names: Vec<String> = venvs.iter().map(|v| v.name.clone()).collect();
    let opts = OptionsArgs {
        nopretty,
        name: "Virtual Environments:".into(),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&names, &opts));
    rt.print(&text);
    if !nopretty {
        rt.print(&render_venvs(&venvs));
    }
    Ok(SuccessTuple::success())
}

/// The lines `prune venvs --dry` prints.
pub fn render_plans(plans: &[PrunePlan]) -> String {
    let mut out = String::new();
    for plan in plans {
        out.push_str(&format!(
            "\nVirtual environment '{}' ({} package{} to remove):\n",
            plan.venv,
            plan.remove.len(),
            plural(plan.remove.len()),
        ));
        for removal in &plan.remove {
            out.push_str(&format!(
                "  - {} {} (requested by: {})\n",
                removal.name,
                removal.version,
                removal.requesters.join(", ")
            ));
        }
    }
    out
}

fn unattributed_note(plans_all: &[(String, usize)]) -> Option<String> {
    let total: usize = plans_all.iter().map(|(_, n)| n).sum();
    (total > 0).then(|| {
        format!(
            "{total} package{} with no recorded requester {} kept.",
            plural(total),
            if total == 1 { "was" } else { "were" }
        )
    })
}

/// `prune venvs`: remove packages that no installed plugin and no recorded requester needs.
pub fn prune_venvs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let dir = venvs_dir(rt);
    let wanted = words(args, "action");
    let needs = match plugin_needs(rt) {
        Ok(needs) => needs,
        Err(e) => {
            return Ok(SuccessTuple::fail(format!(
                "Unable to read the installed plugins, refusing to prune: {}",
                e.message
            )));
        }
    };
    let venvs: Vec<VenvInfo> = scan_venvs(&dir)
        .into_iter()
        .filter(|v| wanted.is_empty() || wanted.contains(&v.name))
        .collect();
    let all_plans: Vec<PrunePlan> = venvs.iter().map(|v| plan_prune(v, &needs)).collect();
    let note = unattributed_note(
        &all_plans
            .iter()
            .map(|p| (p.venv.clone(), p.unattributed))
            .collect::<Vec<_>>(),
    );
    let plans: Vec<PrunePlan> = all_plans
        .into_iter()
        .filter(|p| !p.remove.is_empty())
        .collect();
    if plans.is_empty() {
        let mut message = "No packages to prune.".to_string();
        if let Some(note) = note {
            message.push(' ');
            message.push_str(&note);
        }
        return Ok(SuccessTuple::ok(message));
    }
    rt.print(&render_plans(&plans));
    if let Some(note) = &note {
        info(rt, note);
    }
    let total: usize = plans.iter().map(|p| p.remove.len()).sum();
    // `--dry` is only a parser flag for `compose`; elsewhere it arrives in `sub_args` (as in v4).
    if flag(args, "dry")
        || flag(args, "dry_run")
        || words(args, "sub_args").iter().any(|a| a == "--dry")
    {
        return Ok(SuccessTuple::ok(format!(
            "Would remove {total} package{} (dry run).",
            plural(total)
        )));
    }

    let yes = flag(args, "yes") || flag(args, "force");
    let noask = flag(args, "noask");
    if !yes && noask {
        return Ok(SuccessTuple::fail(
            "Skipping prune. Add `-y` or `-f` to agree to the prompt.",
        ));
    }
    let question = format!(
        "Remove the above {total} package{}?\n    Packages will be reinstalled on demand by `attempt_import`.\n",
        plural(total)
    );
    if !yes_no(rt, &question, "n", yes, noask)? {
        return Ok(SuccessTuple::fail("Nothing was pruned."));
    }

    let mut failures = Vec::new();
    let mut removed = 0;
    for plan in &plans {
        let names: Vec<String> = plan.remove.iter().map(|r| r.name.clone()).collect();
        let mut uninstall_args = args.clone();
        uninstall_args.insert("action".into(), PyValue::str_list(&names));
        uninstall_args.insert("venv".into(), PyValue::str(&plan.venv));
        uninstall_args.insert("sub_args".into(), PyValue::List(Vec::new()));
        // PARITY: the consent was asked above. `uninstall packages` appends `-y` to the installer's
        // arguments when `yes` or `force` is set, and `uv pip uninstall` rejects `-y` (pip's own
        // path adds the flag itself), so every consent flag is cleared for the Python call.
        uninstall_args.insert("yes".into(), PyValue::Bool(false));
        uninstall_args.insert("force".into(), PyValue::Bool(false));
        uninstall_args.insert("noask".into(), PyValue::Bool(false));
        match crate::python::core_action("uninstall packages", &uninstall_args, rt) {
            Ok(result) if result.success => {
                removed += names.len();
                if let Err(e) = forget_packages(&plan.path, &names) {
                    warn(rt, &format!("Could not update the requests ledger: {e}"));
                }
            }
            Ok(result) => failures.push(format!("{}: {}", plan.venv, result.message)),
            Err(e) => failures.push(format!("{}: {}", plan.venv, e.message)),
        }
    }
    if failures.is_empty() {
        Ok(SuccessTuple::ok(format!(
            "Removed {removed} package{}.",
            plural(removed)
        )))
    } else {
        Ok(SuccessTuple::fail(format!(
            "Removed {removed} package{}; failed:\n{}",
            plural(removed),
            failures.join("\n")
        )))
    }
}

/// `upgrade {meerschaum,packages,plugins}`: run the upgrade, then print what
/// `prune venvs --dry` would remove (v5 deletes nothing on upgrade, §12.1).
pub fn upgrade(function: &str, args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let name = function.trim_start_matches('_').replacen('_', " ", 1);
    let result = crate::python::core_action(&name, args, rt)?;
    if result.success
        && let Ok(needs) = plugin_needs(rt)
    {
        let dir = venvs_dir(rt);
        let plans = plan_prune_all(&dir, &needs);
        if !plans.is_empty() {
            let total: usize = plans.iter().map(|p| p.remove.len()).sum();
            rt.print(&render_plans(&plans));
            info(
                rt,
                &format!(
                    "{total} package{} no installed plugin needs. Run `prune venvs` to remove.",
                    plural(total)
                ),
            );
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_dist(site: &Path, name: &str, version: &str, requires: &[&str]) {
        let dir = site.join(format!("{}-{version}.dist-info", name.replace('-', "_")));
        std::fs::create_dir_all(&dir).unwrap();
        let mut metadata = format!("Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n");
        for r in requires {
            metadata.push_str(&format!("Requires-Dist: {r}\n"));
        }
        metadata.push_str("\nLong description mentioning Name: nothing\n");
        std::fs::write(dir.join("METADATA"), metadata).unwrap();
        std::fs::write(
            site.join(format!("{}.py", name.replace('-', "_"))),
            "x = 1\n",
        )
        .unwrap();
    }

    fn make_venv(venvs: &Path, name: &str) -> PathBuf {
        let venv = venvs.join(name);
        let site = venv.join("lib").join("python3.11").join("site-packages");
        std::fs::create_dir_all(&site).unwrap();
        venv
    }

    fn site(venv: &Path) -> PathBuf {
        venv.join("lib").join("python3.11").join("site-packages")
    }

    fn ledger(venv: &Path, entries: &[(&str, &[&str])]) {
        let mut packages = serde_json::Map::new();
        for (package, requesters) in entries {
            let mut map = serde_json::Map::new();
            for requester in *requesters {
                map.insert(
                    requester.to_string(),
                    serde_json::json!({
                        "spec": package,
                        "first_requested": "2026-10-13T00:00:00+00:00",
                        "last_requested": "2026-10-13T00:00:00+00:00",
                    }),
                );
            }
            packages.insert(
                package.to_string(),
                serde_json::json!({ "requesters": map }),
            );
        }
        std::fs::write(
            venv.join(REQUESTS_FILE_NAME),
            serde_json::json!({"schema_version": 1, "packages": packages}).to_string(),
        )
        .unwrap();
    }

    fn needs(plugins: &[(&str, &[&str])]) -> PluginNeeds {
        PluginNeeds {
            plugins: plugins
                .iter()
                .map(|(n, r)| (n.to_string(), r.iter().map(|s| s.to_string()).collect()))
                .collect(),
        }
    }

    #[test]
    fn normalizes_names() {
        assert_eq!(
            normalize("Dash_Bootstrap.Components>=1.0"),
            "dash-bootstrap-components"
        );
        assert_eq!(normalize("psycopg[binary]>=3"), "psycopg");
        assert_eq!(normalize("pandas ; python_version > '3'"), "pandas");
    }

    #[test]
    fn scans_a_synthetic_layout() {
        let root = tempfile::tempdir().unwrap();
        let venv = make_venv(root.path(), "mrsm");
        write_dist(
            &site(&venv),
            "pandas",
            "2.2.0",
            &["numpy>=1.2", "tqdm; extra == 'progress'"],
        );
        write_dist(&site(&venv), "numpy", "1.26.0", &[]);
        ledger(&venv, &[("pandas", &["noaa", "meerschaum"])]);
        // Not a venv: no site-packages.
        std::fs::create_dir_all(root.path().join("junk")).unwrap();

        let venvs = scan_venvs(root.path());
        assert_eq!(venvs.len(), 1);
        let info = &venvs[0];
        assert_eq!(info.name, "mrsm");
        assert!(info.size > 0);
        assert_eq!(info.dists.len(), 2);
        let pandas = info.dists.iter().find(|d| d.name == "pandas").unwrap();
        assert_eq!(pandas.version, "2.2.0");
        assert_eq!(pandas.requires, vec!["numpy"]);
        assert_eq!(
            pandas
                .requests
                .iter()
                .map(|r| r.requester.as_str())
                .collect::<Vec<_>>(),
            vec!["meerschaum", "noaa"]
        );
        let text = render_venvs(&venvs);
        assert!(text.contains("pandas 2.2.0"));
        assert!(text.contains("[meerschaum, noaa]"));
        assert!(text.contains("[unattributed]"));
        assert!(text.contains("Size:"));
    }

    #[test]
    fn prune_keeps_everything_still_needed() {
        let root = tempfile::tempdir().unwrap();
        let venv = make_venv(root.path(), "mrsm");
        let s = site(&venv);
        write_dist(&s, "pandas", "2.2.0", &["numpy>=1.2"]);
        write_dist(&s, "numpy", "1.26.0", &[]);
        write_dist(&s, "requests", "2.31.0", &["urllib3"]);
        write_dist(&s, "urllib3", "2.0.0", &[]);
        write_dist(&s, "pypdf", "4.0.0", &[]);
        write_dist(&s, "markdown", "3.5", &[]);
        write_dist(&s, "legacy", "1.0", &[]);
        write_dist(&s, "pip", "24.0", &[]);
        write_dist(&s, "gone-only", "1.0", &[]);
        write_dist(&s, "shared", "1.0", &[]);
        write_dist(&s, "gone-dep", "1.0", &[]);
        write_dist(&s, "gone-parent", "1.0", &["gone-dep"]);
        ledger(
            &venv,
            &[
                ("pandas", &["noaa"]),
                ("requests", &["meerschaum"]),
                ("pypdf", &["removed-plugin", "noaa"]),
                ("markdown", &["removed-plugin"]),
                ("gone-only", &["removed-plugin"]),
                ("shared", &["removed-plugin"]),
                ("gone-parent", &["removed-plugin"]),
                ("gone-dep", &["removed-plugin"]),
                ("pip", &["removed-plugin"]),
            ],
        );
        let info = scan_venv(&venv).unwrap();
        // `shared` is named by an installed plugin's `required` list.
        let plan = plan_prune(&info, &needs(&[("noaa", &["Shared>=1"]), ("other", &[])]));
        let removed: Vec<&str> = plan.remove.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(
            removed,
            vec!["gone-dep", "gone-only", "gone-parent", "markdown"]
        );
        assert_eq!(plan.remove[3].requesters, vec!["removed-plugin"]);
        // `legacy` has no ledger entry; numpy and urllib3 are dependencies of needed packages.
        assert_eq!(plan.unattributed, 3);
        assert!(!removed.contains(&"legacy"));
        assert!(!removed.contains(&"pip"));
        let text = render_plans(&[plan]);
        assert!(text.contains("Virtual environment 'mrsm' (4 packages to remove):"));
        assert!(text.contains("markdown 3.5 (requested by: removed-plugin)"));
    }

    #[test]
    fn forgetting_packages_rewrites_the_ledger_atomically() {
        let root = tempfile::tempdir().unwrap();
        let venv = make_venv(root.path(), "mrsm");
        ledger(&venv, &[("markdown", &["a"]), ("pandas", &["b"])]);
        forget_packages(&venv, &["Markdown".to_string()]).unwrap();
        let after = read_ledger(&venv);
        assert_eq!(after.keys().collect::<Vec<_>>(), vec!["pandas"]);
        let leftovers: Vec<_> = std::fs::read_dir(&venv)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty());
        // A missing or corrupt ledger is tolerated.
        std::fs::write(venv.join(REQUESTS_FILE_NAME), "{not json").unwrap();
        assert!(read_ledger(&venv).is_empty());
        forget_packages(&venv, &["pandas".to_string()]).unwrap();
        forget_packages(&root.path().join("nope"), &["pandas".to_string()]).unwrap();
    }

    #[test]
    fn plans_cover_only_venvs_with_something_to_remove() {
        let root = tempfile::tempdir().unwrap();
        let mrsm = make_venv(root.path(), "mrsm");
        write_dist(&site(&mrsm), "pypdf", "4.0.0", &[]);
        ledger(&mrsm, &[("pypdf", &["removed"])]);
        let plugin_venv = make_venv(root.path(), "noaa");
        write_dist(&site(&plugin_venv), "requests", "2.0", &[]);
        ledger(&plugin_venv, &[("requests", &["noaa"])]);
        let plans = plan_prune_all(root.path(), &needs(&[("noaa", &[])]));
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].venv, "mrsm");
        assert_eq!(plans[0].remove[0].name, "pypdf");
        // The plugin is listed as installed now: nothing to prune.
        assert!(plan_prune_all(root.path(), &needs(&[("noaa", &[]), ("removed", &[])])).is_empty());
    }
}
