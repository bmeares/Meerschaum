//! Fresh original-source admission for canonical executor jobs.
//!
//! Profile analysis does not make a runtime available. In particular, an Api result still
//! requires the host's complete API-only confinement and broker before credential delivery.

#[cfg(test)]
mod registration_tests;

use uuid::Uuid;

use super::callbacks::{self, Identity, Purpose};
use super::environment::Profile;
use crate::AppState;
use crate::error::ApiError;

/// An admission decision and its freshly resolved, bounded callback identity.
pub struct Admitted {
    pub profile: Profile,
    pub identity: Identity,
}

fn refused(message: &str) -> ApiError {
    ApiError::new(403, message)
}

fn command_access(identity: &Identity) -> crate::access::Access {
    let bounded = identity.caller();
    if identity.grant.depth != 0 {
        return bounded;
    }
    let mut source = identity.source_access().clone();
    if source.full_admin() {
        return source;
    }
    // The callback's Jobs-only RPC surface is narrower than the original user command's
    // rights. A direct command retains its fresh original token/user scopes, projected onto
    // exactly the registered job team. A child keeps the parent's callback ceiling above.
    source.primary_team = bounded.primary_team;
    source.team_records = bounded.team_records;
    source.principal.teams = bounded.principal.teams;
    source.principal = source.principal.within_team(&source.primary_team);
    let mut grants = meerschaum_core::access::TeamGrants::of(
        &source.principal.teams,
        &source.team_records,
        Some(&source.primary_team),
    );
    grants
        .connectors
        .extend(identity.transport_connectors().iter().cloned());
    source.apply_grants(grants);
    source
}

/// Main-only visibility for an already admitted reader's inert metadata cache. The original
/// authenticated request variant is required: an execution callback is never a fresh direct
/// User/Token merely because its own grant has depth zero. Never deliver or mint this access.
pub fn reader_metadata_access(
    identity: &Identity,
    request: &crate::access::Access,
) -> Result<crate::access::Access, ApiError> {
    use crate::auth::Principal;
    if request.no_auth
        || !identity.grant.required.contains("pipes:read")
        || identity.grant.purpose != Purpose::Command
        || !identity
            .owner_binding()
            .as_ref()
            .is_some_and(|binding| binding.matches(request))
    {
        return Err(refused("The metadata reader lost its original request."));
    }
    request.require(&["pipes:read"], &[])?;
    let mut caller = match (&request.auth, &identity.source_access().auth) {
        (Some(Principal::User(requested)), Some(Principal::User(current)))
            if identity.grant.depth == 0
                && requested.user_id == current.user_id
                && requested.username == current.username =>
        {
            direct_reader_access(identity)
        }
        (Some(Principal::Token(requested)), Some(Principal::Token(current)))
            if identity.grant.depth == 0
                && requested.id == current.id
                && requested.creation == current.creation =>
        {
            direct_reader_access(identity)
        }
        (Some(Principal::Execution(parent)), _) => {
            if identity.grant.depth != parent.grant.depth.saturating_add(1)
                || identity.grant.source_ceiling.clone().unwrap_or_default()
                    != parent.grant.source_ceiling.clone().unwrap_or_default()
            {
                return Err(refused(
                    "The reader cannot adopt sources outside its execution parent.",
                ));
            }
            identity.caller()
        }
        _ => {
            return Err(refused(
                "The metadata reader has a different registered source.",
            ));
        }
    };
    caller.principal.permissions =
        crate::access::intersect(&caller.principal.permissions, &request.scopes());
    caller.require(&["pipes:read"], &[])?;
    Ok(caller)
}

fn direct_reader_access(identity: &Identity) -> crate::access::Access {
    let bounded = identity.caller();
    let mut source = identity.source_access().clone();
    source.no_auth = false;
    source.credential = None;
    source.primary_team = bounded.primary_team;
    source.team_records = bounded.team_records;
    source.principal.teams = bounded.principal.teams;
    source.principal = source.principal.within_team(&source.primary_team);
    let grants = meerschaum_core::access::TeamGrants::of(
        &source.principal.teams,
        &source.team_records,
        Some(&source.primary_team),
    );
    source.apply_grants(grants);
    source
}

/// Fresh Main-only registration admission, retaining the original direct source or a child's
/// immutable callback ceiling. Never deliver this projection to a worker or mint its bearer.
pub(crate) fn registration_access(
    state: &AppState,
    identity: &Identity,
    connector_keys: &str,
    instance_keys: &str,
) -> Result<crate::access::Access, ApiError> {
    if !identity.grant.required.contains("pipes:register") {
        return Err(refused("This proof does not authorize pipe registration."));
    }
    let mut caller = command_access(identity);
    // Project exactly the requested first-party source into this Main-only proof. An
    // open-side user can use a plugin without an explicit Team grant, while the object
    // boundary above must remain fixed throughout the awaited registration hook.
    let original = identity.source_access();
    if identity.grant.depth == 0 && !original.principal.is_isolated() {
        crate::access::guard::check_source_connector(state, original, connector_keys)?;
        if let Some(name) = connector_keys.strip_prefix("plugin:") {
            if !super::plugins::valid_name(name) {
                return Err(refused("Invalid registration source."));
            }
            caller.principal.grants.plugins.insert(name.to_string());
        }
        // Keep the immutable object's Team boundary. A shared native instance can be used
        // without an explicit open-side Team grant, but a sibling's private label cannot.
        let available_in_team = |keys: &str| {
            crate::access::guard::may_use_connector(state, original, keys)
                && crate::access::guard::connector_teams(state, keys).is_none_or(|teams| {
                    teams.iter().any(|team| {
                        meerschaum_core::access::same_team(Some(team), Some(&caller.primary_team))
                    })
                })
        };
        if !matches!(
            instance_keys.split(':').next(),
            Some("sql" | "api" | "valkey")
        ) || !available_in_team(instance_keys)
        {
            return Err(refused(
                "The registration instance is unavailable within this Team.",
            ));
        }
        caller
            .principal
            .grants
            .connectors
            .insert(instance_keys.to_string());
        // A SQL definition may read from a different served instance. Carry just that
        // source into the bounded proof after applying the same connector and Team checks.
        if connector_keys.starts_with("sql:")
            && available_in_team(connector_keys)
            && state.get_api_connector(Some(connector_keys)).is_ok()
        {
            caller
                .principal
                .grants
                .connectors
                .insert(connector_keys.to_string());
        }
    }
    caller.require(&["pipes:register"], &[])?;
    crate::access::guard::check_source_connector(state, &caller, connector_keys)?;
    crate::access::guard::check_connector_teams(state, &caller, instance_keys)?;
    Ok(caller)
}

/// Analyze an already freshly validated source. This selects a profile, not permission to
/// deliver credentials or run code. Team identity comes from registration, never its old name.
pub fn profile(identity: &Identity, team_id: Uuid) -> Result<Profile, ApiError> {
    let team = identity
        .team
        .as_ref()
        .ok_or_else(|| refused("The execution has no registered team."))?;
    if team_id.is_nil()
        || identity.grant.team_id != Some(team_id)
        || team
            .id
            .as_deref()
            .and_then(|value| Uuid::parse_str(value).ok())
            != Some(team_id)
    {
        return Err(refused("The execution belongs to another registered team."));
    }
    let caller = identity.caller();
    if !meerschaum_core::access::same_team(
        caller.principal.team_boundary.as_deref(),
        Some(&team.name),
    ) || !identity
        .source_access()
        .principal
        .permits_team(Some(&team.name))
    {
        return Err(refused(
            "The original source cannot cross its execution team boundary.",
        ));
    }
    // A child's origin is deliberately flattened for later revocation, but its parent removed
    // global administrator authority. Depth preserves that ceiling for admission too.
    let direct_administrator = identity.grant.depth == 0 && identity.source_access().full_admin();
    let runs_team_code = crate::team_execution::code_running_team(&caller).is_some_and(|current| {
        current
            .id
            .as_deref()
            .and_then(|value| Uuid::parse_str(value).ok())
            == Some(team_id)
    });
    // Command authority is an immutable API-only ceiling. A later permission increase
    // may authorize a new Team launch, but cannot add credentials to this existing one.
    let profile = if identity.grant.purpose == Purpose::Command {
        Profile::Api
    } else if direct_administrator || runs_team_code {
        Profile::Team
    } else {
        Profile::Api
    };
    if profile == Profile::Api && identity.grant.purpose != Purpose::Command {
        return Err(refused(
            "This job has jobs-only callback authority. Recreate it explicitly for API-only execution.",
        ));
    }
    Ok(profile)
}

/// Recheck a stored user command for every explicit restart, including an existing remote
/// definition. Suspended proof may establish admission, but cannot authenticate or renew a
/// callback. The host must reissue its lease and deliver the supported profile afterward.
pub async fn job(
    state: &AppState,
    grant_id: Uuid,
    launch_id: Uuid,
    team_id: Uuid,
    sysargs: &[String],
) -> Result<Admitted, ApiError> {
    user_sysargs(sysargs)?;
    let identity = callbacks::load_admission_identity(state, grant_id, launch_id).await?;
    let profile = profile(&identity, team_id)?;
    let mut admission = command_access(&identity);
    if profile == Profile::Api {
        admission.no_auth = false;
        admission.principal.is_admin = false;
        admission.principal.grants.runs_code = false;
    }
    check_stored_command(
        state,
        &identity,
        &admission,
        sysargs,
        profile == Profile::Api,
    )
    .await?;
    Ok(Admitted { profile, identity })
}

/// Recheck an API-only host launch, including Everyone's jobs without a registered team.
/// Typed built-ins perform their own exact command/object checks after this source proof.
pub async fn typed_api_identity(
    state: &AppState,
    grant_id: Uuid,
    launch_id: Uuid,
    team_id: Option<Uuid>,
) -> Result<Identity, ApiError> {
    let identity = callbacks::load_admission_identity(state, grant_id, launch_id).await?;
    if identity.grant.purpose != Purpose::Command || identity.grant.team_id != team_id {
        return Err(refused(
            "The typed operation lost its immutable API source ceiling.",
        ));
    }
    if let Some(id) = team_id {
        if profile(&identity, id)? != Profile::Api {
            return Err(refused(
                "The typed operation requires its private API profile.",
            ));
        }
    } else if identity.team.is_some() || !identity.source_access().principal.permits_team(None) {
        return Err(refused(
            "The typed operation belongs to another registered team.",
        ));
    }
    Ok(identity)
}

/// Recheck an API-only host launch, including Everyone's jobs without a registered team.
/// Its credential ceiling remains API-only even if the original user later gains code rights.
/// Jobs-only authority can never be adopted to create data callbacks on a host restart.
pub async fn host_job(
    state: &AppState,
    grant_id: Uuid,
    launch_id: Uuid,
    team_id: Option<Uuid>,
    sysargs: &[String],
) -> Result<Admitted, ApiError> {
    user_sysargs(sysargs)?;
    let identity = callbacks::load_admission_identity(state, grant_id, launch_id).await?;
    if identity.grant.purpose != Purpose::Command || identity.grant.team_id != team_id {
        return Err(refused(
            "This host launch has a different source or callback ceiling.",
        ));
    }
    let mut admission = if let Some(id) = team_id {
        // Reuse the registered UUID and effective-boundary checks; keep the stricter Api
        // environment even when a fresh source could now request a new Team launch.
        let _ = profile(&identity, id)?;
        command_access(&identity)
    } else {
        if identity.team.is_some() {
            return Err(refused("This host launch belongs to a registered team."));
        }
        if identity.grant.depth == 0 {
            identity.source_access().clone()
        } else {
            identity.caller()
        }
    };
    admission.no_auth = false;
    admission.principal.is_admin = false;
    admission.principal.grants.runs_code = false;
    check_stored_command(state, &identity, &admission, sysargs, true).await?;
    Ok(Admitted {
        profile: Profile::Api,
        identity,
    })
}

async fn check_stored_command(
    state: &AppState,
    identity: &Identity,
    admission: &crate::access::Access,
    sysargs: &[String],
    api_only: bool,
) -> Result<(), ApiError> {
    let bounded = identity.caller();
    // Readiness and metadata backends may consult task-local control identity. They receive
    // the bounded original caller, never an ambient administrator clicking Start.
    super::context::scope(Some(std::sync::Arc::new(bounded.clone())), async {
        if api_only {
            if crate::execute_policy::check_bound_command(state, admission, sysargs)? {
                return Ok(());
            }
            // Current team metadata can independently permit code execution. An existing
            // API-only grant must retain its restricted action policy even after that change.
            crate::execute_policy::check_non_admin_resolved(
                state.actions.as_ref(),
                admission,
                sysargs,
            )
            .await
            .map_err(|message| refused(&message))?;
        }
        crate::execute_policy::check_command(
            state,
            admission,
            sysargs,
            crate::execute_policy::Place::Job,
        )
        .await
        .map_err(|message| refused(&message))?;
        let plan = crate::execute_access::check(state, admission, sysargs).await?;
        if plan.needs_actions {
            admission.require(&["actions:execute"], &[])?;
        }
        Ok::<_, ApiError>(())
    })
    .await
}

fn reserved(word: &str) -> bool {
    let key = word.split('=').next().unwrap_or(word);
    key.starts_with("__mrsm_")
        || key.starts_with("--_mrsm-")
        || (word.contains('=')
            && matches!(
                key,
                "MRSM_NATIVE_ENTRY"
                    | "MRSM_TEAM_PROBE"
                    | "MRSM_JOB_SUPERVISOR"
                    | "MRSM_JOB_PROGRAM"
                    | "MRSM__ENTRY_CHILD"
                    | "MRSM_SYSTEMD_JOB_CHILD"
                    | "MRSM_SYSTEMD_LOG_PATH"
                    | "MRSM_SYSTEMD_RESULT_PATH"
                    | "MRSM_SYSTEMD_STDIN_PATH"
                    | "MRSM_SYSTEMD_DELETE_JOB"
            ))
}

fn internal_role(words: &[String]) -> bool {
    let head = crate::execute_access::positionals(words);
    head.first().is_some_and(|action| action == "start")
        && head.get(1).is_some_and(|role| {
            matches!(
                role.as_str(),
                "executor" | "executors" | "worker" | "workers"
            )
        })
}

/// Reject native host/supervisor/probe/relay selectors before a trusted launch adds its own
/// wrappers. Scan every argument and every bounded chain expansion, not just the first token.
/// Ordinary data such as a metric named `executor` or `systemd` remains valid.
pub fn user_sysargs(sysargs: &[String]) -> Result<(), ApiError> {
    if sysargs.is_empty() || sysargs.iter().any(|word| reserved(word)) {
        return Err(refused(
            "Internal native launch arguments are not user commands.",
        ));
    }
    let steps =
        crate::execute_policy::command_steps(sysargs).map_err(|message| refused(&message))?;
    if steps.is_empty() {
        return Err(refused("An executor job needs a user command."));
    }
    for step in steps {
        if step.words.iter().any(|word| reserved(word)) || internal_role(&step.words) {
            return Err(refused("Internal executor roles are not user commands."));
        }
        if crate::execute_access::executor_values(&step.words).is_some_and(|values| {
            values
                .iter()
                .any(|value| !matches!(value.as_str(), "" | "local" | "None"))
        }) {
            return Err(refused(
                "A canonical executor job cannot select another job executor.",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn internal_arguments_are_refused_anywhere_and_inside_both_chain_forms() {
        for marker in [
            "__mrsm_job_supervisor__",
            "__mrsm_entry_child__",
            "__mrsm_pipeline_child__",
            "__mrsm_team_sandbox_probe__",
            "--_mrsm-confined-python",
            "--_mrsm-resolve-executor-relays",
            "MRSM_NATIVE_ENTRY=1",
            "MRSM_SYSTEMD_LOG_PATH=/untrusted/log",
        ] {
            for command in [
                words(&[marker]),
                words(&["show", "version", marker]),
                words(&["show", "version", "--", marker]),
            ] {
                assert!(user_sysargs(&command).is_err(), "{command:?}");
            }
            for prefix in [words(&["chain"]), words(&["start", "pipeline"])] {
                let payload = serde_json::json!({"sub_args_line":format!("show version + show version {marker}")}).to_string();
                let mut command = prefix;
                command.extend(["-P".into(), payload]);
                assert!(user_sysargs(&command).is_err(), "{command:?}");
            }
        }
        for command in [
            words(&["start", "executor"]),
            words(&["--debug", "start", "executor"]),
            words(&["start", "--debug", "worker"]),
            words(&["show", "version", "+", "start", "executor"]),
            words(&["show", "version", "--executor-keys=systemd"]),
            words(&["show", "version", "-eapi:other"]),
        ] {
            assert!(user_sysargs(&command).is_err(), "{command:?}");
        }
    }

    #[test]
    fn ordinary_data_names_and_safe_chains_do_not_become_internal_roles() {
        for command in [
            words(&[
                "show",
                "pipes",
                "-m",
                "executor",
                "supervisor",
                "probe",
                "relay",
                "systemd",
            ]),
            words(&["show", "version", "+", "show", "arguments"]),
            words(&["start", "jobs", "executor"]),
            words(&["show", "pipes", "-m", "start", "executor"]),
            words(&["show", "version", "-elocal"]),
            words(&[
                "chain",
                "-P",
                r#"{"sub_args_line":"show version + show arguments"}"#,
            ]),
        ] {
            assert!(user_sysargs(&command).is_ok(), "{command:?}");
        }
    }
}
