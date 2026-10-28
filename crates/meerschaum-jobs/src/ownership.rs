//! Who the jobs of a root belong to (decision AC9; the rule is
//! [`meerschaum_core::job_access`]): a job's `owner` / `team`, the jobs of a team, and the update
//! of those jobs when the team is renamed or deleted.
//!
//! Jobs live in the root's `jobs/` directory (local jobs, and the `systemd` executor's, whose
//! properties are a local job of the same name), so a team rename or deletion updates the jobs
//! of the root where it runs: the API server's (its `/teams` routes and the console), and the
//! CLI's or Python's (`MRSM_ROOT_DIR`). Jobs on another API server's executor are that server's.
//!
//! A rename writes the new name into every job naming the old one (names compare without case);
//! a deletion writes `team: null` (Everyone). The value on disk always wins over an in-memory
//! copy ([`crate::properties::FILE_OWNED_KEYS`]), so a running job's supervisor never writes the
//! old team back.
//!
//! **Jobs started by jobs.** A job created by a process that runs inside another job (see
//! [`current_job_dir`]) belongs to that job's owner and team ([`inherit_parent_access`]):
//! `start pipeline NAME` run from the console's Execute box (an `execute-*` run of the user's),
//! or a plugin action that starts a job, starts a job of that user's, not one of Everyone's. A
//! job whose properties name an owner (even `null`, as the API server and the console record)
//! keeps it.

use std::path::{Path, PathBuf};

use meerschaum_core::access::{is_everyone, normalize_team};
use meerschaum_core::job_access::{EVERYONE, JobAccess, LEGACY_OWNER_KEY, OWNER_KEY, TEAM_KEY};
use meerschaum_core::value::{Map, Value};

use crate::config::{DaemonPaths, JobPaths, default_dot_config_dir};
use crate::daemon::{JobsContext, get_daemon_ids};
use crate::properties::{read_properties, write_existing_properties};

/// Where a root's jobs are: the root directory (`MRSM_ROOT_DIR`; its jobs are `<root>/jobs`),
/// its [`JobPaths`], or a [`JobsContext`].
pub trait JobsRoot {
    /// The root's job paths.
    fn job_paths(&self) -> JobPaths;
}

impl JobsRoot for Path {
    fn job_paths(&self) -> JobPaths {
        JobPaths::for_root(self, default_dot_config_dir())
    }
}

impl JobsRoot for PathBuf {
    fn job_paths(&self) -> JobPaths {
        self.as_path().job_paths()
    }
}

impl JobsRoot for JobPaths {
    fn job_paths(&self) -> JobPaths {
        self.clone()
    }
}

impl JobsRoot for JobsContext {
    fn job_paths(&self) -> JobPaths {
        self.paths.clone()
    }
}

/// Who the job `name` of the root belongs to (a job that does not exist: Everyone's).
pub fn job_access<R: JobsRoot + ?Sized>(root: &R, name: &str) -> JobAccess {
    access_in(&root.job_paths(), name)
}

fn access_in(paths: &JobPaths, name: &str) -> JobAccess {
    let daemon = DaemonPaths::new(paths, name);
    JobAccess::from_properties(
        name,
        &read_properties(&daemon.properties).unwrap_or_default(),
    )
}

/// The jobs of the root (local and `systemd`) whose team is `team` (names compare without case),
/// sorted. Everyone's (`team` is `"Everyone"`) are the jobs without a team.
pub fn team_jobs<R: JobsRoot + ?Sized>(root: &R, team: &str) -> Vec<String> {
    let paths = root.job_paths();
    get_daemon_ids(&paths)
        .into_iter()
        .filter(|name| access_in(&paths, name).is_team(Some(team)))
        .collect()
}

/// What [`retag_team`] changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Retagged {
    /// The jobs that now name the new team (or Everyone), sorted.
    pub jobs: Vec<String>,
    /// The jobs that could not be updated, with why.
    pub failed: Vec<(String, String)>,
}

impl Retagged {
    /// A one-line summary, e.g. `"Moved 2 jobs to Everyone."`.
    pub fn message(&self, new: Option<&str>) -> String {
        let n = self.jobs.len();
        let target = match normalize_team(new) {
            Some(team) => format!("team '{team}'"),
            None => EVERYONE.to_string(),
        };
        let mut message = format!(
            "Moved {n} job{} to {target}.",
            if n == 1 { "" } else { "s" }
        );
        if !self.failed.is_empty() {
            let failed: Vec<String> = self
                .failed
                .iter()
                .map(|(job, why)| format!("'{job}' ({why})"))
                .collect();
            message.push_str(&format!(" Could not update {}.", failed.join(", ")));
        }
        message
    }
}

/// Rename team `old` to `new` in the root's jobs that name it; `new` of `None` (or
/// `"Everyone"`) moves them to Everyone (`team: null`), as deleting the team does. Jobs of other
/// teams are untouched; Everyone itself cannot be renamed (nothing changes).
pub fn retag_team<R: JobsRoot + ?Sized>(root: &R, old: &str, new: Option<&str>) -> Retagged {
    let mut out = Retagged::default();
    if is_everyone(Some(old)) {
        return out;
    }
    let paths = root.job_paths();
    let new = normalize_team(new);
    for name in team_jobs(&paths, old) {
        let daemon = DaemonPaths::new(&paths, &name);
        // Read again right before writing: the job's supervisor may have written meanwhile.
        let Some(mut props) = read_properties(&daemon.properties) else {
            continue;
        };
        if !JobAccess::from_properties(&name, &props).is_team(Some(old)) {
            continue;
        }
        props.insert(
            TEAM_KEY.to_string(),
            new.clone().map_or(Value::Null, Value::Str),
        );
        let written = write_existing_properties(&daemon.properties, &props);
        if written.success {
            out.jobs.push(name);
        } else {
            out.failed.push((name, written.message));
        }
    }
    out
}

/// [`retag_team`] for a rename: how many jobs now name `new`.
pub fn rename_team<R: JobsRoot + ?Sized>(root: &R, old: &str, new: &str) -> usize {
    retag_team(root, old, Some(new)).jobs.len()
}

/// [`retag_team`] for a deletion: how many of `team`'s jobs moved to Everyone.
pub fn move_team_to_everyone<R: JobsRoot + ?Sized>(root: &R, team: &str) -> usize {
    retag_team(root, team, None).jobs.len()
}

/// The job this process runs in (`MRSM_DAEMON_ID`, which the supervisor and the `systemd` units
/// set for a job's worker), if any.
pub fn current_job_name() -> Option<String> {
    let var = &meerschaum_core::static_config::static_config()
        .environment
        .daemon_id;
    std::env::var(var)
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

/// The directory of the job this process runs in, if any: the directory of the heartbeat file
/// the supervisor (or a `systemd` unit's wrapper) hands its worker
/// ([`crate::health::LAPS_PATH_ENV`], inherited by everything the worker starts), whatever its
/// root; else `jobs/<MRSM_DAEMON_ID>` of `root`.
pub fn current_job_dir<R: JobsRoot + ?Sized>(root: &R) -> Option<PathBuf> {
    let from_laps = std::env::var_os(crate::health::LAPS_PATH_ENV)
        .map(PathBuf::from)
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .filter(|dir| !dir.as_os_str().is_empty());
    if from_laps.is_some() {
        return from_laps;
    }
    let name = current_job_name()?;
    if crate::names::invalid_job_name_reason(&name).is_some() {
        return None;
    }
    Some(DaemonPaths::new(&root.job_paths(), &name).dir)
}

/// Give the properties of a new job (whose directory is `job_dir`) the owner and team of the job
/// whose directory is `parent_dir` (see the module documentation; usually [`current_job_dir`]).
/// Nothing changes when the properties name an owner already (`owner`, even `null`, or the
/// pre-rc15 `mrsm_owner`), when the parent is the job itself or has neither an owner nor a team
/// (a local job), or when it cannot be read. A token's label (`token:<id>`, a run of a token
/// without an owner) is not a user and is not inherited. Returns whether anything was recorded.
pub fn inherit_parent_access(props: &mut Map, job_dir: &Path, parent_dir: Option<&Path>) -> bool {
    if props.contains_key(OWNER_KEY) || props.contains_key(LEGACY_OWNER_KEY) {
        return false;
    }
    let Some(parent_dir) = parent_dir.filter(|dir| *dir != job_dir) else {
        return false;
    };
    let Some(parent) = parent_dir
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
    else {
        return false;
    };
    let Some(parent_props) = read_properties(&parent_dir.join("properties.json")) else {
        return false;
    };
    let access = JobAccess::from_properties(&parent, &parent_props);
    let owner = access.owner.filter(|o| !o.starts_with("token:"));
    if owner.is_none() && access.team.is_none() {
        return false;
    }
    JobAccess::stamp(props, owner.as_deref(), access.team.as_deref());
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::properties::write_properties;

    fn write_job(root: &Path, name: &str, json: &str) {
        let daemon = DaemonPaths::new(&root.job_paths(), name);
        let props: Map = Value::from_json_str(json).unwrap().to_map();
        assert!(write_properties(&daemon.properties, &props).success);
    }

    fn props_of(root: &Path, name: &str) -> Map {
        read_properties(&DaemonPaths::new(&root.job_paths(), name).properties).unwrap()
    }

    #[test]
    fn renaming_and_deleting_a_team_updates_its_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_job(
            root,
            "a",
            r#"{"owner": "alice", "team": "Data", "label": "a"}"#,
        );
        write_job(root, "b", r#"{"owner": "bob", "team": "Ops"}"#);
        write_job(root, "c", r#"{"owner": "carol", "team": "data"}"#);
        write_job(root, "local", r#"{"label": "sync pipes"}"#);
        assert_eq!(team_jobs(root, "Data"), ["a", "c"]);
        assert_eq!(team_jobs(root, "Everyone"), ["local"]);
        let ctx = JobsContext::for_root(root);
        assert_eq!(team_jobs(&ctx, "DATA"), ["a", "c"]);
        assert_eq!(job_access(&ctx, "b").owner.as_deref(), Some("bob"));

        let renamed = retag_team(root, "Data", Some("Analytics"));
        assert_eq!(renamed.jobs, ["a", "c"]);
        assert!(renamed.failed.is_empty());
        assert_eq!(
            renamed.message(Some("Analytics")),
            "Moved 2 jobs to team 'Analytics'."
        );
        assert_eq!(team_jobs(root, "Data"), Vec::<String>::new());
        assert_eq!(job_access(root, "a").team.as_deref(), Some("Analytics"));
        assert_eq!(job_access(root, "a").owner.as_deref(), Some("alice"));
        assert_eq!(job_access(root, "b").team.as_deref(), Some("Ops"));
        // The rest of the properties are kept.
        assert_eq!(
            props_of(root, "a").get("label").and_then(Value::as_str),
            Some("a")
        );

        assert_eq!(move_team_to_everyone(root, "analytics"), 2);
        assert_eq!(team_jobs(root, "Everyone"), ["a", "c", "local"]);
        assert_eq!(props_of(root, "c").get(TEAM_KEY), Some(&Value::Null));
        assert_eq!(rename_team(root, "Ops", "Operations"), 1);
        // Everyone cannot be renamed; nothing names a team that has no jobs.
        assert_eq!(
            retag_team(root, "Everyone", Some("All")),
            Retagged::default()
        );
        assert_eq!(
            retag_team(root, "Nobody", None).message(None),
            "Moved 0 jobs to Everyone."
        );
    }

    #[test]
    fn a_job_started_by_a_job_belongs_to_the_parent_s_owner_and_team() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_job(
            root,
            "execute-1a2b3c4d",
            r#"{"owner": "alice", "team": "Data", "mrsm_owner": "alice", "mrsm_execution": {"kind": "temporary_execution"}}"#,
        );
        write_job(root, "local", r#"{"label": "sync pipes"}"#);
        write_job(root, "legacy-run", r#"{"mrsm_owner": "bob"}"#);
        write_job(
            root,
            "token-run",
            r#"{"owner": null, "mrsm_owner": "token:abc", "team": null}"#,
        );
        let job_dir = |name: &str| DaemonPaths::new(&root.job_paths(), name).dir;
        let child = job_dir("hourly");

        let mut props = Map::new();
        let parent = job_dir("execute-1a2b3c4d");
        assert!(inherit_parent_access(&mut props, &child, Some(&parent)));
        assert_eq!(
            Value::Map(props.clone()).json_dumps(),
            r#"{"owner": "alice", "team": "Data", "mrsm_owner": "alice"}"#
        );
        // The child is a job of the team, not a private console run.
        let access = JobAccess::from_properties("hourly", &props);
        assert!(!access.private && access.is_team(Some("data")));

        // An explicit owner (even none, as the API server records) wins.
        let mut explicit: Map = Value::from_json_str(r#"{"owner": null, "team": null}"#)
            .unwrap()
            .to_map();
        assert!(!inherit_parent_access(&mut explicit, &child, Some(&parent)));
        assert_eq!(explicit.get(OWNER_KEY), Some(&Value::Null));
        // A local parent, no parent, a missing parent, the job itself.
        for parent in [
            Some(job_dir("local")),
            None,
            Some(job_dir("gone")),
            Some(child.clone()),
        ] {
            let mut props = Map::new();
            assert!(
                !inherit_parent_access(&mut props, &child, parent.as_deref()),
                "{parent:?}"
            );
            assert!(props.is_empty(), "{parent:?}");
        }
        // A run recorded before rc15 names its owner in `mrsm_owner`.
        let mut props = Map::new();
        assert!(inherit_parent_access(
            &mut props,
            &child,
            Some(&job_dir("legacy-run"))
        ));
        assert_eq!(props.get(OWNER_KEY).and_then(Value::as_str), Some("bob"));
        assert_eq!(props.get(TEAM_KEY), Some(&Value::Null));
        // A token's label is not a user.
        let mut props = Map::new();
        assert!(!inherit_parent_access(
            &mut props,
            &child,
            Some(&job_dir("token-run"))
        ));
        assert!(props.is_empty());
        // A parent in another root (a job that starts jobs of a Compose project's root).
        let other = tempfile::tempdir().unwrap();
        let mut props = Map::new();
        let elsewhere = DaemonPaths::new(&other.path().job_paths(), "hourly").dir;
        assert!(inherit_parent_access(&mut props, &elsewhere, Some(&parent)));
        assert_eq!(props.get(OWNER_KEY).and_then(Value::as_str), Some("alice"));
    }

    #[test]
    fn a_running_job_keeps_the_renamed_team() {
        // A supervisor (any `Daemon`) holding the old properties in memory writes them back
        // after the rename: the team on disk stands.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_job(
            root,
            "busy",
            r#"{"owner": "alice", "team": "Data", "result": null}"#,
        );
        let ctx = JobsContext::for_root(root);
        let mut daemon = crate::daemon::Daemon::open("busy", &ctx).unwrap();
        let _ = daemon.properties();
        assert_eq!(rename_team(root, "Data", "Analytics"), 1);
        let mut patch = Map::new();
        patch.insert(
            "result".into(),
            Value::List(vec![Value::Bool(true), Value::Str("Success".into())]),
        );
        daemon.update_properties(&patch);
        assert!(daemon.write_properties().success);
        let props = props_of(root, "busy");
        assert_eq!(
            props.get(TEAM_KEY).and_then(Value::as_str),
            Some("Analytics")
        );
        assert!(props.get("result").is_some_and(|r| !r.is_null()));
    }
}
