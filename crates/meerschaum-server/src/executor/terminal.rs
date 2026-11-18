//! Short-lived, owner-bound executor terminal leases. Console session credentials stay on main.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use uuid::Uuid;

use super::control::Actor;

/// Renewed by main independently of browser connections, including during brief API restarts.
pub const TERMINAL_LEASE: Duration = Duration::from_secs(90);
/// Detached sessions also count toward the executor's bounded terminal registry.
pub const MAX_TERMINAL_LEASES: usize = 512;

/// The native terminal guardian and its protected tmux launcher run on Linux.
pub const API_TERMINALS_AVAILABLE: bool = cfg!(target_os = "linux");

/// Revalidate the immutable API-only terminal source before renewing or delivering its
/// private environment. This never changes a Jobs-purpose terminal into Command authority.
pub async fn api_source(
    state: &crate::AppState,
    grant_id: Uuid,
    launch_id: Uuid,
    team_id: Option<Uuid>,
) -> Result<super::callbacks::Identity, crate::error::ApiError> {
    let identity = super::callbacks::load_identity(state, grant_id, launch_id).await?;
    if identity.grant.purpose != super::callbacks::Purpose::Command
        || identity.grant.team_id != team_id
    {
        return Err(crate::error::ApiError::new(
            403,
            "This terminal has another source or callback ceiling.",
        ));
    }
    identity
        .caller()
        .require(&["webterm:use", "actions:execute", "jobs:execute"], &[])?;
    Ok(identity)
}

/// Side-effect-free command policy for a bounded API terminal. A fresh team gaining code
/// rights does not turn this existing terminal into a system shell or restore admin powers.
pub async fn api_command(
    state: &crate::AppState,
    caller: &crate::access::Access,
    sysargs: &[String],
) -> Result<(), crate::error::ApiError> {
    if !matches!(caller.auth.as_ref(), Some(crate::auth::Principal::Execution(identity))
        if identity.grant.purpose == super::callbacks::Purpose::Command)
    {
        return Err(crate::error::ApiError::new(
            403,
            "An API terminal needs its current Command credential.",
        ));
    }
    caller.require(
        &[
            "webterm:use",
            "actions:execute",
            "jobs:execute",
            "jobs:write",
        ],
        &[],
    )?;
    super::admission::user_sysargs(sysargs)?;
    crate::execute_policy::check_non_admin_resolved(state.actions.as_ref(), caller, sysargs)
        .await
        .map_err(|message| crate::error::ApiError::new(403, message))?;
    crate::execute_policy::authorize_command(
        state,
        caller,
        sysargs,
        crate::execute_policy::Place::Job,
    )
    .await?;
    Ok(())
}

/// Main-owned placement, sent through signed control. `id` is NOT a console session id.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TerminalGrant {
    /// Unpredictable terminal id persisted by the main API.
    pub id: Uuid,
    /// Current username, used for signed caller matching only, never as a filesystem path.
    pub owner: String,
    /// Registered user identity, preventing a replacement account adopting an old terminal.
    pub owner_id: String,
}

struct Lease {
    grant: TerminalGrant,
    until: Instant,
}

/// Executor-side sessions. Admission always needs both a live lease and a signed current caller.
#[derive(Default)]
pub struct TerminalSessions {
    leases: Mutex<BTreeMap<Uuid, Lease>>,
}

fn terminal_permission(actor: &Actor) -> bool {
    actor.administrator
        || ["webterm:use", "actions:execute", "jobs:execute"]
            .iter()
            .all(|permission| {
                actor.permissions.contains(*permission) || actor.permissions.contains("*")
            })
}

impl TerminalSessions {
    /// Apply a signed grant. Main's maintenance actor may renew for a freshly checked owner;
    /// an ordinary actor can renew only its own identity, with all terminal permissions.
    pub fn renew(&self, actor: &Actor, grant: TerminalGrant) -> Result<(), String> {
        let valid = |value: &str| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        };
        if grant.id.is_nil() || !valid(&grant.owner) || !valid(&grant.owner_id) {
            return Err("Invalid executor terminal grant.".into());
        }
        if !terminal_permission(actor)
            || (!actor.administrator && actor.username.as_deref() != Some(&grant.owner))
        {
            return Err("This caller cannot renew this executor terminal.".into());
        }
        let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = leases.get(&grant.id) {
            if current.grant != grant {
                return Err("This terminal belongs to another registered identity.".into());
            }
        } else if leases.len() >= MAX_TERMINAL_LEASES {
            return Err("This executor's terminal lease capacity is exhausted.".into());
        }
        leases.insert(
            grant.id,
            Lease {
                grant,
                until: Instant::now() + TERMINAL_LEASE,
            },
        );
        Ok(())
    }

    /// Resolve a signed websocket's terminal without accepting names selected by its executor.
    pub fn name(&self, id: Uuid, actor: &Actor) -> Option<String> {
        self.grant(id, actor).map(|_| terminal_name(id))
    }

    /// Resolve the immutable registered owner as well as the current signed caller.
    pub fn grant(&self, id: Uuid, actor: &Actor) -> Option<TerminalGrant> {
        if !terminal_permission(actor) {
            return None;
        }
        let leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
        let lease = leases.get(&id)?;
        (lease.until > Instant::now() && actor.username.as_deref() == Some(&lease.grant.owner))
            .then(|| lease.grant.clone())
    }

    /// Explicit main revocation. Return the server-generated tmux name for retirement.
    pub fn revoke(&self, id: Uuid, actor: &Actor) -> Result<Option<String>, String> {
        let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(lease) = leases.get(&id)
            && !actor.administrator
            && actor.username.as_deref() != Some(&lease.grant.owner)
        {
            return Err("This caller cannot revoke this executor terminal.".into());
        }
        Ok(leases.remove(&id).map(|_| terminal_name(id)))
    }

    /// Take expired leases once; the runtime must end their tmux sessions, including detached ones.
    pub fn expired(&self) -> Vec<String> {
        let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
        let expired: Vec<Uuid> = leases
            .iter()
            .filter(|(_, lease)| lease.until <= Instant::now())
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            leases.remove(id);
        }
        expired.into_iter().map(terminal_name).collect()
    }

    /// A generation/configuration revocation invalidates every terminal before process retirement.
    pub fn clear(&self) {
        self.leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

fn terminal_name(id: Uuid) -> String {
    format!("executor-terminal-{}", id.simple())
}

#[async_trait]
impl crate::webterm::SessionLookup for TerminalSessions {
    async fn username(&self, terminal_id: &str) -> Result<Option<String>, String> {
        let Some(caller) = super::gateway::current() else {
            return Ok(None);
        };
        let Ok(id) = Uuid::parse_str(terminal_id) else {
            return Ok(None);
        };
        Ok(self.name(id, &caller.claims.actor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn actor(name: &str) -> Actor {
        Actor {
            owner: name.into(),
            username: Some(name.into()),
            administrator: false,
            permissions: ["webterm:use", "actions:execute", "jobs:execute"]
                .map(str::to_owned)
                .into_iter()
                .collect(),
        }
    }
    fn grant() -> TerminalGrant {
        TerminalGrant {
            id: Uuid::new_v4(),
            owner: "alice".into(),
            owner_id: "registered-user-1".into(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn owner_identity_permissions_and_expiry_are_all_required() {
        let sessions = TerminalSessions::default();
        let grant = grant();
        let mut alice = actor("alice");
        let bob = actor("bob");
        assert!(sessions.name(grant.id, &alice).is_none());
        assert!(sessions.renew(&bob, grant.clone()).is_err());
        sessions.renew(&alice, grant.clone()).unwrap();
        assert_eq!(
            sessions.name(grant.id, &alice),
            Some(terminal_name(grant.id))
        );
        assert!(sessions.name(grant.id, &bob).is_none());
        let mut replacement = grant.clone();
        replacement.owner_id = "new-user-same-name".into();
        assert!(sessions.renew(&alice, replacement).is_err());
        alice.permissions.remove("webterm:use");
        assert!(sessions.name(grant.id, &alice).is_none());
        assert!(sessions.renew(&alice, grant.clone()).is_err());
        alice = actor("alice");
        tokio::time::advance(TERMINAL_LEASE).await;
        assert!(sessions.name(grant.id, &alice).is_none());
        assert_eq!(sessions.expired(), vec![terminal_name(grant.id)]);
        assert!(sessions.expired().is_empty());
    }

    #[tokio::test]
    async fn unsigned_lookups_and_cross_owner_revocation_are_refused() {
        use crate::webterm::SessionLookup;
        let sessions = TerminalSessions::default();
        let grant = grant();
        sessions.renew(&actor("alice"), grant.clone()).unwrap();
        assert!(
            sessions
                .username(&grant.id.to_string())
                .await
                .unwrap()
                .is_none()
        );
        assert!(sessions.revoke(grant.id, &actor("bob")).is_err());
        assert!(sessions.name(grant.id, &actor("alice")).is_some());
        sessions.revoke(grant.id, &actor("alice")).unwrap();
        assert!(sessions.name(grant.id, &actor("alice")).is_none());
    }

    #[tokio::test]
    async fn leases_are_bounded_and_generation_revocation_clears_them() {
        let sessions = TerminalSessions::default();
        for _ in 0..MAX_TERMINAL_LEASES {
            sessions.renew(&actor("alice"), grant()).unwrap();
        }
        assert!(sessions.renew(&actor("alice"), grant()).is_err());
        sessions.clear();
        sessions.renew(&actor("alice"), grant()).unwrap();
    }
}
