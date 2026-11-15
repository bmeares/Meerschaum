//! The API server's record of its isolated teams' scopes (5.1).
//!
//! The server keeps `team_scopes/<key>/scope.json` (connector keys and plugins, never
//! credentials) for every isolated team, so the jobs of those teams are launched with the team's
//! configuration only, also by the restart monitor after a reboot (`meerschaum_jobs::config_files`).
//! [`TeamScopes::refresh`] runs when the server starts, after every team change it makes, and
//! every [`REFRESH_INTERVAL`] (the console and the Python SDK change teams directly in the
//! instance):
//!
//! - an isolated team's record follows its settings; a team that is no longer isolated gets an
//!   `open` record (its jobs get the server's configuration again), a deleted team a `deleted`
//!   one (its jobs get no connector at all);
//! - when a team loses a connector (revoked, or the team deleted), its running jobs are
//!   restarted, so no process of the team keeps a revoked connector;
//! - team worker pools whose configuration changed are retired at once (they would be at their
//!   next call anyway).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::InstanceConnector;
use meerschaum_core::config::Config;
use meerschaum_core::team_scope::{
    self as ts, SCOPES_DIR, TeamScope, read_scope_record, write_scope_record,
};
use meerschaum_jobs::daemon::JobsContext;

use crate::native::Session;

/// How often the records are refreshed from the instance.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// What one refresh changed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Refreshed {
    /// The isolated teams' keys.
    pub isolated: Vec<String>,
    /// Teams whose connectors were revoked (or which were deleted).
    pub revoked: Vec<String>,
    /// The jobs restarted for them.
    pub restarted: Vec<String>,
    /// Team pools retired.
    pub retired: Vec<String>,
}

/// The server's team scopes.
#[derive(Clone)]
pub struct TeamScopes {
    session: Session,
    instance: Arc<dyn InstanceConnector>,
    config: Arc<dyn Fn() -> Option<Config> + Send + Sync>,
    root: PathBuf,
    jobs: Option<JobsContext>,
}

impl TeamScopes {
    /// The scopes of the teams of `instance`, recorded under `root`; `config` builds the
    /// server's configuration (read fresh each time); `jobs` is the root's jobs (to restart).
    pub fn new(
        session: Session,
        instance: Arc<dyn InstanceConnector>,
        config: Arc<dyn Fn() -> Option<Config> + Send + Sync>,
        root: PathBuf,
        jobs: Option<JobsContext>,
    ) -> TeamScopes {
        TeamScopes {
            session,
            instance,
            config,
            root,
            jobs,
        }
    }

    /// The keys of the scopes recorded under the root.
    fn recorded_keys(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(self.root.join(SCOPES_DIR)) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter(|e| e.path().join(ts::SCOPE_FILE).is_file())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect()
    }

    /// Bring the records in line with the instance's teams (see the module documentation).
    pub async fn refresh(&self) -> Result<Refreshed, String> {
        let teams = self
            .instance
            .get_teams()
            .await
            .map_err(|e| format!("The teams could not be read: {e}"))?;
        let config = (self.config)().ok_or("The configuration could not be read.")?;
        config.refresh_connectors();
        let connectors = config
            .get(&["meerschaum", "connectors"])
            .map(|v| v.to_map())
            .unwrap_or_default();
        let mut out = Refreshed::default();
        let mut current: BTreeMap<String, TeamScope> = BTreeMap::new();
        for team in teams.iter().filter(|t| !t.is_everyone()) {
            let scope = if team.is_isolated() {
                TeamScope::of(team, &connectors)
            } else {
                TeamScope::opened(team)
            };
            let key = scope.key();
            let previous = read_scope_record(&self.root, &key);
            if !team.is_isolated() && previous.is_none() {
                continue;
            }
            if team.is_isolated() {
                out.isolated.push(key.clone());
            }
            if previous.as_ref() != Some(&scope) {
                write_scope_record(&self.root, &scope).map_err(|e| e.to_string())?;
            }
            if let Some(previous) = previous
                && !previous.open
                && team.is_isolated()
                && previous
                    .connectors
                    .iter()
                    .any(|k| !scope.connectors.contains(k))
            {
                out.revoked.push(key.clone());
            }
            current.insert(key, scope);
        }
        for key in self.recorded_keys() {
            if current.contains_key(&key) {
                continue;
            }
            let Some(previous) = read_scope_record(&self.root, &key) else {
                continue;
            };
            if previous.deleted {
                continue;
            }
            let deleted = TeamScope::deleted(&previous.team, previous.team_id.as_deref());
            write_scope_record(&self.root, &deleted).map_err(|e| e.to_string())?;
            if !previous.open {
                out.revoked.push(key.clone());
            }
        }
        // Team pools whose configuration is no longer the team's.
        let expected: BTreeMap<String, String> = current
            .iter()
            .filter(|(_, scope)| !scope.open)
            .map(|(key, scope)| {
                (
                    key.clone(),
                    ts::fingerprint(scope, &ts::team_config(&config, scope)),
                )
            })
            .collect();
        out.retired = self
            .session
            .retire_teams(|key, fingerprint| expected.get(key).is_some_and(|f| f == fingerprint));
        if !out.revoked.is_empty() {
            out.restarted = self.restart_jobs(&out.revoked).await;
        }
        Ok(out)
    }

    /// Restart the running jobs of the scopes `keys`.
    async fn restart_jobs(&self, keys: &[String]) -> Vec<String> {
        let Some(jobs) = self.jobs.clone() else {
            return Vec::new();
        };
        let keys: BTreeSet<String> = keys.iter().cloned().collect();
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let mut restarted = Vec::new();
            for name in meerschaum_jobs::daemon::get_daemon_ids(&jobs.paths) {
                let Ok(mut job) = meerschaum_jobs::job::Job::open(&name, &jobs) else {
                    continue;
                };
                if job.status() != meerschaum_jobs::daemon::Status::Running {
                    continue;
                }
                let props = job.daemon().properties();
                let key = props
                    .get(meerschaum_jobs::config_files::TEAM_SCOPE_PROPERTY)
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .or_else(|| {
                        meerschaum_jobs::config_files::job_team_scope(&root, &props).map(|(k, _)| k)
                    });
                if key.is_some_and(|k| keys.contains(&k)) && job.restart(None).success {
                    restarted.push(name);
                }
            }
            restarted
        })
        .await
        .unwrap_or_default()
    }
}
