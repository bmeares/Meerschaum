//! Fresh console-bound execution permissions, separate from workload/backend readiness.

use axum::http::{HeaderValue, header};
use meerschaum_core::access::{TeamGrants, same_team};
use meerschaum_core::execution::Mode;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    access::Access,
    backends::ApiExecution,
    error::{ApiError, ApiResult},
    executor::{plugin_caller::RequestSource, terminal::API_TERMINALS_AVAILABLE},
    routes::Ctx,
};

#[derive(PartialEq)]
struct SourceStamp {
    primary_team: String,
    registered: Option<(Uuid, Mode)>,
    grants: TeamGrants,
    boundary: Option<String>,
    backend: ApiExecution,
}

impl SourceStamp {
    fn capture(caller: &Access) -> Result<Self, ApiError> {
        let team = caller.team_records.iter().find(|team| {
            same_team(Some(&team.name), Some(&caller.primary_team))
                && caller.in_team(Some(&team.name))
        });
        let registered = team
            .map(|team| {
                let id = team
                    .id
                    .as_deref()
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .filter(|id| !id.is_nil())
                    .ok_or_else(ApiError::invalid_credentials)?;
                let mode = team
                    .execution()
                    .map_err(|_| ApiError::invalid_credentials())?
                    .mode;
                Ok::<_, ApiError>((id, mode))
            })
            .transpose()?;
        Ok(Self {
            primary_team: caller.primary_team.clone(),
            registered,
            grants: caller.principal.grants.clone(),
            boundary: caller.principal.team_boundary.clone(),
            backend: caller.api_execution,
        })
    }
}

#[derive(Serialize)]
struct Permissions {
    actions_execute: bool,
    jobs_execute: bool,
    jobs_write: bool,
    webterm_use: bool,
}

#[derive(Serialize)]
struct Capabilities {
    tracked_api: bool,
    api_terminals: bool,
}

#[derive(Serialize)]
struct Source {
    primary_team: String,
    team_id: Option<Uuid>,
    isolated: bool,
    runs_code: bool,
}

#[derive(Serialize)]
struct Policy {
    non_admin_actions: bool,
}

#[derive(Serialize)]
struct Projection {
    schema: u8,
    username: String,
    source: Source,
    permissions: Permissions,
    capabilities: Capabilities,
    policy: Policy,
}

pub(super) async fn get(ctx: Ctx) -> ApiResult {
    let original = RequestSource::resolve(&ctx.state, &ctx.headers, &[]).await?;
    if !original.console_session() {
        return Err(ApiError::new(401, "A current console session is required."));
    }
    let initial = SourceStamp::capture(&original.access())?;
    let caller = original.refresh(&ctx.state, &[]).await?;
    let final_source = SourceStamp::capture(&caller)?;
    if initial != final_source {
        return Err(ApiError::new(
            403,
            "The console's execution source changed.",
        ));
    }
    // No awaits after the SAME original credential/scope/source projection.
    let (tracked_api, api_terminals) = match caller.api_execution {
        ApiExecution::Unavailable => (false, false),
        ApiExecution::Tracked { terminals } => (true, terminals && API_TERMINALS_AVAILABLE),
    };
    let projection = Projection {
        schema: 1,
        username: caller.principal.username.clone(),
        source: Source {
            primary_team: final_source.primary_team,
            team_id: final_source.registered.map(|(id, _)| id),
            isolated: caller.principal.grants.isolated,
            runs_code: caller.principal.grants.runs_code,
        },
        permissions: Permissions {
            actions_execute: caller.has("actions:execute"),
            jobs_execute: caller.has("jobs:execute"),
            jobs_write: caller.has("jobs:write"),
            webterm_use: caller.has("webterm:use"),
        },
        capabilities: Capabilities {
            tracked_api,
            api_terminals,
        },
        policy: Policy {
            non_admin_actions: ctx.state.config.actions_non_admin,
        },
    };
    let value = serde_json::to_value(projection).map_err(|_| ApiError::internal())?;
    let mut response = crate::json::ok_json(&value);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::VARY,
        HeaderValue::from_static("Authorization, Cookie"),
    );
    Ok(response)
}
