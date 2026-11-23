//! A session's team binding: the team's worker pools, the session a team member's command
//! runs in, and the guard on what the team's plugin code may change.

use meerschaum_worker::{PoolConfig, PoolScope, TeamPools};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::builtin::ActionError;
use meerschaum_core::config::Config;
use meerschaum_core::value::{Value, apply_patch_maps};

use super::instances::full_keys;
use super::*;

impl Session {
    /// The team this session runs for (`None`: the server's own session).
    pub fn team(&self) -> Option<&Arc<TeamBinding>> {
        self.team.as_ref()
    }

    /// The isolated teams' pools of this process, created with the limit of `config`
    /// (`system.engine.python_team_workers`) on first use.
    pub fn team_pools(&self, config: &Config) -> TeamPools {
        self.teams
            .pools
            .get_or_init(|| TeamPools::new(meerschaum_worker::pool::team_workers_limit(config)))
            .clone()
    }

    /// The session of an isolated team: its own connectors and worker pool, built from the
    /// team's configuration only ([`meerschaum_core::team_scope::team_config`] of the server's
    /// `config`). Python code the session runs starts in workers that never receive the
    /// server's configuration (`MRSM_CONFIG`, `MRSM_PATCH`, connector variables, config files)
    /// nor any other server variable outside an allowlist
    /// ([`meerschaum_core::team_scope::team_env`]), and whose callbacks may only touch the team's instances (and, with `member`, pipes that
    /// member may change). Reused while the team's configuration is unchanged; a change (a
    /// connector granted or revoked, a rename, a deletion, a rotated password) retires the
    /// team's workers and builds a new session. Returns the session and how it was found.
    pub fn for_team(
        &self,
        scope: &meerschaum_core::team_scope::TeamScope,
        member: Option<meerschaum_core::access::Principal>,
        config: &Config,
    ) -> Result<(Session, TeamLookup), ActionError> {
        scope
            .check_host_execution(config)
            .map_err(|message| ActionError::new("RuntimeError", message))?;
        use meerschaum_core::team_scope as ts;
        let io = |e: std::io::Error| ActionError::new("OSError", e.to_string());
        // A connector another process registered (a team's database) since the last read.
        config.refresh_connectors();
        let team_map = ts::team_config(config, scope);
        let fingerprint = ts::fingerprint(scope, &team_map);
        let key = scope.key();
        {
            let sessions = self
                .teams
                .sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some((current, session)) = sessions.get(&key)
                && *current == fingerprint
            {
                return Ok((session.clone(), TeamLookup::Warm));
            }
        }
        let paths = config.paths();
        let files =
            ts::write_scope_files(&ts::scope_dir(&paths.root_dir, &key), &team_map).map_err(io)?;
        ts::write_scope_record(&paths.root_dir, scope).map_err(io)?;
        let mut base: Vec<(String, String)> = meerschaum_core::process_env::vars()
            .into_iter()
            .filter(|(k, _)| k != "MRSM_ROOT_DIR" && k != "MRSM_PLUGINS_DIR")
            .collect();
        base.push((
            "MRSM_ROOT_DIR".into(),
            paths.root_dir.to_string_lossy().into_owned(),
        ));
        if !paths.plugins_dirs.is_empty() {
            let dirs: Vec<String> = paths
                .plugins_dirs
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect();
            base.push((
                "MRSM_PLUGINS_DIR".into(),
                serde_json::to_string(&dirs).unwrap_or_default(),
            ));
        }
        // An allowlist (`meerschaum_core::team_scope::team_env`): nothing else of the server's
        // environment (cloud credentials, tokens) reaches the team's workers.
        let passthrough = ts::Passthrough::from_config(config);
        passthrough.warn_refused();
        let mut team_env = ts::team_env(&base, &[], &files, scope, &fingerprint, &passthrough);
        // The team's filesystem sandbox: each worker restricts itself to these rules before
        // it imports anything of the team's (`system.engine.python_team_sandbox`). In `require`
        // mode on a system without Landlock, nothing of the team's starts.
        let sandbox = ts::sandbox::prepare(
            config,
            scope,
            &files,
            None,
            &files.dir.join(ts::sandbox::SPEC_FILE),
        )
        .map_err(|e| ActionError::new("RuntimeError", e.to_string()))?;
        if let Some(prepared) = &sandbox {
            ts::sandbox::apply_env(&mut team_env, prepared);
        }
        let team_config = Arc::new(
            Config::new(meerschaum_core::config::ConfigOptions::with_env(
                team_env.clone(),
            ))
            .map_err(|e| ActionError::new("ConfigError", e.to_string()))?,
        );
        let configured: std::collections::BTreeSet<String> = config
            .get(&["meerschaum", "connectors"])
            .map(|v| v.to_map())
            .unwrap_or_default()
            .iter()
            .flat_map(|(typ, labels)| {
                labels
                    .to_map()
                    .into_iter()
                    .filter(|(label, attrs)| label != "default" && !attrs.is_null())
                    .map(|(label, _)| format!("{typ}:{label}"))
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut child = Session {
            rt: self.rt.clone(),
            instances: Arc::new(Mutex::new(HashMap::new())),
            registered: Arc::new(Mutex::new(HashSet::new())),
            registered_only: Arc::new(AtomicBool::new(false)),
            python_worker: self.python_worker.clone(),
            pool: Arc::new(Mutex::new(None)),
            worker_environment: Arc::new(Mutex::new(None)),
            worker_python: self.worker_python.clone(),
            hook_results: Arc::new(Mutex::new(Vec::new())),
            manifests: self.manifests.clone(),
            source_compiler: self.source_compiler.clone(),
            teams: self.teams.clone(),
            team: None,
        };
        let guard: Option<Arc<dyn meerschaum_worker::callback::CallbackGuard>> =
            member.map(|member| {
                Arc::new(TeamPipeGuard {
                    instances: child.instances.clone(),
                    config: team_config.clone(),
                    member,
                }) as Arc<dyn meerschaum_worker::callback::CallbackGuard>
            });
        let pool_scope = PoolScope {
            scope: Arc::new(scope.clone()),
            configured: Arc::new(configured),
            guard,
        };
        let worker_python = self
            .worker_python
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let (pool, lookup) = self.team_pools(config).get(&key, &fingerprint, |live| {
            let mut settings = PoolConfig::for_team(&team_config, pool_scope, live);
            // Workers start from an empty environment: the team's allowlisted one only.
            settings.worker.env_clear = true;
            settings.worker.env = team_env.clone();
            // Each worker proves it loaded this configuration before it runs the team's code.
            settings.worker.team = Some(meerschaum_worker::worker::TeamExpectation {
                team: scope.team.clone(),
                fingerprint: fingerprint.clone(),
                connectors: scope.connectors.clone(),
                sandbox: sandbox.as_ref().map(|p| p.mode),
            });
            // A sandboxed worker works in the team's temporary directory (the server's working
            // directory is outside its rules).
            if let Some(prepared) = &sandbox {
                settings.worker.current_dir = Some(prepared.tmp_dir.clone());
            }
            if !std::env::var("MRSM_PYTHON").is_ok_and(|v| !v.trim().is_empty())
                && let Some(python) = &worker_python
            {
                settings.worker.python = python.into();
            }
            settings
        });
        *child.pool.lock().unwrap_or_else(|e| e.into_inner()) = Some(pool);
        // The keys of the team's configuration, loaded once: an action run for the team reads
        // them from memory.
        for key in team_map.keys() {
            let _ = team_config.get(&[key.as_str()]);
        }
        let mut snapshot = team_config.snapshot();
        // The values are substituted already; the originals would only slow every lookup (the
        // team's configuration is never written back).
        snapshot.shift_remove("_symlinks");
        // The engine's settings (v5-only defaults, absent from a written `system` file) in the
        // snapshot: each lookup of one finds it instead of re-substituting every default.
        if let Some(Value::Map(engine)) = team_config
            .defaults()
            .get("system")
            .and_then(|s| s.as_map())
            .and_then(|s| s.get("engine"))
            .cloned()
            && let Some(Value::Map(system)) = snapshot.get_mut("system")
        {
            let merged = match system.get("engine") {
                Some(Value::Map(own)) => apply_patch_maps(&engine, own),
                _ => engine,
            };
            system.insert("engine".into(), Value::Map(merged));
        }
        child.team = Some(Arc::new(TeamBinding {
            scope: scope.clone(),
            config: team_config,
            fingerprint: fingerprint.clone(),
            files,
            env: team_env,
            snapshot,
        }));
        let lookup = match lookup {
            TeamLookup::Warm => TeamLookup::Replaced,
            other => other,
        };
        self.teams
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, (fingerprint, child.clone()));
        Ok((child, lookup))
    }

    /// Retire the sessions (and workers) of the teams `keep` refuses (a deleted team's, or
    /// one whose configuration changed). Returns the retired teams' keys.
    pub fn retire_teams(&self, keep: impl Fn(&str, &str) -> bool) -> Vec<String> {
        let mut retired = Vec::new();
        let pools = self.teams.pools.get().cloned();
        let mut sessions = self
            .teams
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        sessions.retain(|key, (fingerprint, _)| {
            let kept = keep(key, fingerprint);
            if !kept {
                retired.push(key.clone());
                if let Some(pools) = &pools {
                    pools.retire(key);
                }
            }
            kept
        });
        retired
    }
}

/// What a callback from a team's plugin code may do to a pipe: what a member of the team
/// may change through the API ([`meerschaum_core::access::can_edit`] of the pipe's stored
/// access). A pipe that is not registered yet, or on an instance the session has not built, is
/// left to the team's instance check.
struct TeamPipeGuard {
    instances: Arc<Mutex<HashMap<String, Instance>>>,
    member: meerschaum_core::access::Principal,
    config: Arc<Config>,
}

#[meerschaum_connector::async_trait]
impl meerschaum_worker::callback::CallbackGuard for TeamPipeGuard {
    async fn check(&self, request: &meerschaum_worker::CallbackRequest) -> Result<(), String> {
        let Some(pipe) = request.pipe.as_ref() else {
            return Ok(());
        };
        let instance = self
            .instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full_keys(pipe.instance_keys()))
            .cloned();
        let Some(instance) = instance else {
            return Ok(());
        };
        let conn = instance.conn();
        let stored = match conn.get_pipe_attributes(pipe).await {
            Ok(attributes) => attributes,
            Err(_) => return Ok(()),
        };
        let Some(Value::Map(parameters)) = stored.get("parameters") else {
            return Ok(());
        };
        let access = meerschaum_core::access::PipeAccess::from_parameters(parameters);
        let connectors = self
            .config
            .get(&["meerschaum", "connectors"])
            .map(|value| value.to_map())
            .unwrap_or_default();
        let reservation =
            meerschaum_core::access::connector_reservation(&connectors, &conn.connector_keys());
        if meerschaum_core::access::can_edit(
            &self.member,
            &access,
            reservation.as_deref().unwrap_or_default(),
        ) {
            Ok(())
        } else {
            Err(format!("You may not change {pipe}."))
        }
    }
}
