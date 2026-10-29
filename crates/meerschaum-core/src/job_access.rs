//! Who a job belongs to, and who may see it (decision AC9 of 5.0.0rc15; the people asking are
//! [`access::Principal`]s).
//!
//! A job started through the API server (`POST /jobs/{name}`, `POST /execute`,
//! `POST /pipelines/{name}/sync`, the MCP `sync_pipeline` tool) or the web console records who
//! started it in its properties:
//!
//! - `owner`: the user's name;
//! - `team`: the user's primary team ([`access::primary_team`]; `null`: Everyone).
//!
//! A job started by another job (a process in that job's worker, e.g. `start pipeline NAME` run
//! from the console's Execute box) records that job's owner and team
//! (`meerschaum_jobs::ownership::inherit_parent_access`). A job created locally (the CLI,
//! Compose, systemd, Python) records neither and belongs to Everyone. The record is the
//! server's and the console's to write: a client cannot choose it ([`JobAccess::stamp`] replaces
//! whatever a request carried).
//!
//! **Visibility** ([`can_see_job`]): administrators see every job, the owner sees their own, and
//!
//! - a job of team `T` is visible to the members (and managers) of `T`; a job of Everyone (no
//!   team, as every local job) is visible to every user;
//! - a console run (an automatic execution: `POST /execute`, a Pipeline run, a plugin action from
//!   the console, the `execute-*` jobs) is private, like a pipe with `visibility: private`: its
//!   owner, the managers of its team and administrators. A run recorded before its owner was (no
//!   owner) stays with the administrators.
//!
//! Team names compare without case ([`access::same_team`]). A job naming a team that no longer
//! exists is visible to its owner and administrators only (nobody is in a deleted team):
//! renaming a team renames it in the jobs that name it, and deleting one moves them to Everyone
//! (`meerschaum_jobs::ownership`).
//!
//! **Control** (start, stop, pause, delete, input) needs the job permission of the operation on
//! a job the caller can see: the surfaces check the permission, then [`can_see_job`]. A job the
//! caller cannot see answers as one that does not exist.
//!
//! Locally (the CLI, Python with the instance's credentials) nothing is filtered: the host's
//! files are the trust boundary.

use crate::access::{self, Principal};
use crate::value::{Map, Value};

pub use crate::access::EVERYONE;

/// The property naming the user who started a job through the API server or the console.
pub const OWNER_KEY: &str = "owner";

/// The property naming the job's team (absent or `null`: Everyone).
pub const TEAM_KEY: &str = "team";

/// The property in which console runs recorded their creator before 5.0.0rc15 (still written,
/// so an older console or server sharing the root keeps its rule).
pub const LEGACY_OWNER_KEY: &str = "mrsm_owner";

/// The property describing an execution (`{"kind": ..., "origin": ..., ...}`).
pub const EXECUTION_KEY: &str = "mrsm_execution";

/// The `mrsm_execution.kind` of the console's automatic runs.
pub const TEMPORARY_KIND: &str = "temporary_execution";

/// The name prefix of console runs recorded before their kind was (`execute-*`).
pub const EXECUTION_PREFIX: &str = "execute-";

/// Who a job belongs to (from its properties).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobAccess {
    /// The user who started it through the API server or the console (`None`: a local job, or a
    /// console run recorded before its owner was).
    pub owner: Option<String>,
    /// Its team (`None`: Everyone).
    pub team: Option<String>,
    /// A console run: private to its owner, the managers of its team and administrators.
    pub private: bool,
}

fn text(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

/// Whether the properties of job `name` describe a console run (the explicit kind first; the
/// `execute-` name of runs recorded before the kind was).
pub fn is_console_run(name: &str, props: &Map) -> bool {
    match props
        .get(EXECUTION_KEY)
        .and_then(|e| e.get("kind"))
        .and_then(Value::as_str)
    {
        Some(kind) => kind == TEMPORARY_KIND,
        None => name.starts_with(EXECUTION_PREFIX),
    }
}

impl JobAccess {
    /// From the properties (`daemon.properties`) of job `name`.
    pub fn from_properties(name: &str, props: &Map) -> JobAccess {
        JobAccess {
            owner: text(props.get(OWNER_KEY))
                .or_else(|| text(props.get(LEGACY_OWNER_KEY)))
                .map(str::to_string),
            team: access::normalize_team(text(props.get(TEAM_KEY))),
            private: is_console_run(name, props),
        }
    }

    /// From the metadata the jobs routes serve for job `name`
    /// (`{"daemon": {"properties": {...}}, ...}`); no metadata is a local job's.
    pub fn from_metadata(name: &str, meta: Option<&serde_json::Value>) -> JobAccess {
        let props = meta
            .and_then(|m| m.pointer("/daemon/properties"))
            .and_then(|p| serde_json::from_value::<Value>(p.clone()).ok())
            .and_then(|v| v.as_map().cloned())
            .unwrap_or_default();
        JobAccess::from_properties(name, &props)
    }

    /// Record who started a job: `owner` and `team` (Everyone is stored as `null`) replace
    /// whatever `props` carried (a client never chooses them). The pre-rc15 `mrsm_owner` is kept
    /// in step.
    pub fn stamp(props: &mut Map, owner: Option<&str>, team: Option<&str>) {
        let owner = owner.map(str::trim).filter(|o| !o.is_empty());
        props.insert(
            OWNER_KEY.to_string(),
            owner.map_or(Value::Null, |o| Value::Str(o.to_string())),
        );
        props.insert(
            TEAM_KEY.to_string(),
            access::normalize_team(team).map_or(Value::Null, Value::Str),
        );
        match owner {
            Some(o) => {
                props.insert(LEGACY_OWNER_KEY.to_string(), Value::Str(o.to_string()));
            }
            None => {
                props.shift_remove(LEGACY_OWNER_KEY);
            }
        }
    }

    /// [`JobAccess::stamp`] for `principal`, whose primary team is `primary_team` (an anonymous
    /// principal, as under `--no-auth`, records no owner).
    pub fn stamp_principal(props: &mut Map, principal: &Principal, primary_team: Option<&str>) {
        let owner = (!principal.username.is_empty()).then_some(principal.username.as_str());
        JobAccess::stamp(props, owner, primary_team);
    }

    /// The team's name for display (`"Everyone"` for none).
    pub fn team_label(&self) -> &str {
        self.team.as_deref().unwrap_or(EVERYONE)
    }

    /// Whether this job's team is `team` (names compare without case; `None` is Everyone).
    pub fn is_team(&self, team: Option<&str>) -> bool {
        access::same_team(self.team.as_deref(), team)
    }

    /// Whether `username` started the job.
    pub fn is_owner(&self, username: &str) -> bool {
        !username.is_empty() && self.owner.as_deref() == Some(username)
    }
}

/// Whether `principal` may see the job `job` describes (see the module documentation).
pub fn can_see_job(principal: &Principal, job: &JobAccess) -> bool {
    if principal.is_admin || job.is_owner(&principal.username) {
        return true;
    }
    if job.private {
        // A console run recorded before its owner was stays with the administrators.
        return job.owner.is_some() && principal.is_manager(job.team.as_deref());
    }
    principal.is_member(job.team.as_deref())
}

/// [`can_see_job`] for job `name` with properties `props`.
pub fn can_see_job_properties(principal: &Principal, name: &str, props: &Map) -> bool {
    can_see_job(principal, &JobAccess::from_properties(name, props))
}

/// [`can_see_job`] for job `name` with the metadata the jobs routes serve.
pub fn can_see_job_metadata(
    principal: &Principal,
    name: &str,
    meta: Option<&serde_json::Value>,
) -> bool {
    can_see_job(principal, &JobAccess::from_metadata(name, meta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::TeamRole;

    fn props(json: &str) -> Map {
        Value::from_json_str(json).unwrap().to_map()
    }

    fn user(name: &str, teams: &[(&str, TeamRole)]) -> Principal {
        let mut all = vec![(EVERYONE.to_string(), TeamRole::Member)];
        all.extend(teams.iter().map(|(t, r)| (t.to_string(), *r)));
        Principal::new(name, false, Default::default(), all)
    }

    #[test]
    fn local_jobs_belong_to_everyone() {
        let job = JobAccess::from_properties("sync-weather", &props(r#"{"label": "x"}"#));
        assert_eq!(job, JobAccess::default());
        assert_eq!(job.team_label(), "Everyone");
        assert!(can_see_job(&user("bob", &[]), &job));
        assert!(can_see_job(&Principal::default(), &job));
    }

    #[test]
    fn team_jobs_are_their_team_s() {
        let job =
            JobAccess::from_properties("nightly", &props(r#"{"owner": "alice", "team": "Data"}"#));
        assert_eq!(job.owner.as_deref(), Some("alice"));
        assert_eq!(job.team.as_deref(), Some("Data"));
        assert!(!job.private);
        assert!(can_see_job(&user("alice", &[]), &job), "the owner");
        assert!(can_see_job(
            &user("bob", &[("Data", TeamRole::Member)]),
            &job
        ));
        // Team names compare without case.
        assert!(can_see_job(
            &user("bob", &[("data", TeamRole::Member)]),
            &job
        ));
        assert!(can_see_job(
            &user("carol", &[("Data", TeamRole::Manager)]),
            &job
        ));
        assert!(!can_see_job(
            &user("dave", &[("Ops", TeamRole::Manager)]),
            &job
        ));
        assert!(!can_see_job(&Principal::default(), &job));
        assert!(can_see_job(&Principal::admin("root"), &job));
        // An Everyone team (spelled out by a client) is no team.
        let everyone = JobAccess::from_properties("x", &props(r#"{"team": "everyone"}"#));
        assert_eq!(everyone.team, None);
        assert!(everyone.is_team(None) && everyone.is_team(Some("Everyone")));
        assert!(job.is_team(Some("DATA")) && !job.is_team(None));
    }

    #[test]
    fn console_runs_are_private() {
        let run = JobAccess::from_properties(
            "execute-1a2b3c4d",
            &props(
                r#"{"owner": "alice", "team": "Data", "mrsm_execution": {"kind": "temporary_execution"}}"#,
            ),
        );
        assert!(run.private);
        assert!(can_see_job(&user("alice", &[]), &run));
        assert!(!can_see_job(
            &user("bob", &[("Data", TeamRole::Member)]),
            &run
        ));
        assert!(can_see_job(
            &user("carol", &[("Data", TeamRole::Manager)]),
            &run
        ));
        // Everyone's runs: only Everyone's managers (and administrators) besides the owner.
        let mine = JobAccess::from_properties(
            "execute-x",
            &props(r#"{"mrsm_owner": "alice", "mrsm_execution": {"kind": "temporary_execution"}}"#),
        );
        assert_eq!(mine.owner.as_deref(), Some("alice"));
        assert!(!can_see_job(&user("bob", &[]), &mine));
        assert!(can_see_job(
            &user("erin", &[("Everyone", TeamRole::Manager)]),
            &mine
        ));
        // A run recorded before its owner was stays with the administrators.
        let legacy = JobAccess::from_properties("execute-legacy", &Map::new());
        assert!(legacy.private && legacy.owner.is_none());
        assert!(!can_see_job(
            &user("erin", &[("Everyone", TeamRole::Manager)]),
            &legacy
        ));
        assert!(can_see_job(&Principal::admin("root"), &legacy));
        // An anonymous caller never matches a missing owner.
        assert!(!can_see_job(&Principal::default(), &legacy));
        // The explicit kind wins over the name.
        let kept = JobAccess::from_properties(
            "execute-kept",
            &props(r#"{"mrsm_execution": {"kind": "persistent_job"}}"#),
        );
        assert!(!kept.private);
        assert!(can_see_job(&user("bob", &[]), &kept));
    }

    #[test]
    fn a_client_never_chooses_the_record() {
        let mut p = props(r#"{"owner": "admin", "team": "Ops", "mrsm_owner": "admin"}"#);
        JobAccess::stamp(&mut p, Some("bob"), Some("Data"));
        assert_eq!(
            Value::Map(p.clone()).json_dumps(),
            r#"{"owner": "bob", "team": "Data", "mrsm_owner": "bob"}"#
        );
        JobAccess::stamp(&mut p, Some("bob"), Some("Everyone"));
        assert_eq!(p.get(TEAM_KEY), Some(&Value::Null));
        JobAccess::stamp_principal(&mut p, &Principal::default(), None);
        assert_eq!(
            Value::Map(p).json_dumps(),
            r#"{"owner": null, "team": null}"#
        );
    }

    #[test]
    fn metadata_and_deleted_teams() {
        let meta =
            serde_json::json!({"daemon": {"properties": {"owner": "alice", "team": "Gone"}}});
        let job = JobAccess::from_metadata("x", Some(&meta));
        assert_eq!(job.team.as_deref(), Some("Gone"));
        // Nobody is in a deleted team: its owner and the administrators still see the job.
        assert!(!can_see_job_metadata(&user("bob", &[]), "x", Some(&meta)));
        assert!(can_see_job_metadata(&user("alice", &[]), "x", Some(&meta)));
        assert_eq!(JobAccess::from_metadata("y", None), JobAccess::default());
    }
}
