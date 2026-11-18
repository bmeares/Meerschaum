//! The isolated team a request acts for (5.1), for the code that runs Python on its behalf.
//!
//! The API server decides the team from the caller's identity (and, when a request names an
//! object of another of their isolated teams, that object's team; see
//! `meerschaum_core::team_scope::acting_team`) and runs the request's handler inside
//! [`scope`]. The hosts that start Python code ([`current`]) then use that team's own worker pool
//! and configuration instead of the server's. Nothing a client or plugin code sends sets it.

use std::future::Future;
use std::sync::Arc;

use meerschaum_core::access::Principal;
use meerschaum_core::team_scope::TeamScope;

/// The team a request acts for.
#[derive(Debug, Clone, PartialEq)]
pub struct ActingTeam {
    /// The team's connectors and plugins.
    pub scope: TeamScope,
    /// A member of the team, as the access rules see one (what a callback from the team's plugin
    /// code may do to a pipe is what this member may do).
    pub member: Principal,
}

impl ActingTeam {
    /// The acting team of `team` given the server's `meerschaum:connectors` section.
    pub fn of(team: &meerschaum_core::access::Team, connectors: &meerschaum_core::Map) -> Self {
        use meerschaum_core::access::{EVERYONE, TeamGrants, TeamRole};
        let scope = TeamScope::of(team, connectors);
        let member = Principal::new(
            "",
            false,
            Default::default(),
            vec![
                (EVERYONE.to_string(), TeamRole::Member),
                (team.name.clone(), TeamRole::Member),
            ],
        )
        .with_grants(TeamGrants {
            isolated: true,
            connectors: scope.connectors.iter().cloned().collect(),
            plugins: scope.plugins.iter().cloned().collect(),
            plugins_write: false,
            instance: scope.instance.clone(),
            runs_code: false,
        });
        ActingTeam { scope, member }
    }
}

tokio::task_local! {
    static ACTING: Option<Arc<ActingTeam>>;
}

/// Run `future` for `team` (`None`: the open side).
pub async fn scope<F: Future>(team: Option<Arc<ActingTeam>>, future: F) -> F::Output {
    ACTING.scope(team, future).await
}

/// Run `f` for `team` on this thread (a blocking task).
pub fn sync_scope<R>(team: Option<Arc<ActingTeam>>, f: impl FnOnce() -> R) -> R {
    ACTING.sync_scope(team, f)
}

/// The team the current request acts for (`None`: the open side, or outside a request).
pub fn current() -> Option<Arc<ActingTeam>> {
    ACTING.try_with(|team| team.clone()).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_team_is_set_for_the_scope_only() {
        let mut team = meerschaum_core::access::Team::new("wetlands");
        team.attributes
            .insert("isolated".into(), meerschaum_core::Value::Bool(true));
        team.attributes.insert(
            "connectors".into(),
            meerschaum_core::Value::List(vec!["sql:shared".into()]),
        );
        let acting = Arc::new(ActingTeam::of(&team, &meerschaum_core::Map::new()));
        assert!(acting.member.is_isolated());
        assert!(acting.member.is_member(Some("wetlands")));
        assert!(!acting.member.is_member(Some("Everyone")));
        assert_eq!(current(), None);
        let inner = scope(Some(acting.clone()), async { current() }).await;
        assert_eq!(inner.map(|t| t.scope.team.clone()), Some("wetlands".into()));
        assert_eq!(current(), None);
        let blocking = sync_scope(Some(acting), current);
        assert!(blocking.is_some());
    }
}
