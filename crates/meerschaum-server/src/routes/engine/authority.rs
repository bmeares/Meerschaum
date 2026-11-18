//! Original request and configured-service authority retained through native writer waits.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};

use axum::http::{HeaderMap, header};
use meerschaum_connector::mutation::MutationCheck;
use meerschaum_connector::pipe_ops::Doc;
use meerschaum_core::{Map, Value};
use meerschaum_kernel::value::Value as KValue;
use serde_json::Value as Json;

use super::{registry::NativeRegistry, selection::SelectedEngine};
use crate::{
    AppState,
    access::{self, Access},
    auth::Principal,
    error::ApiError,
    routes::Ctx,
};

#[derive(Clone, PartialEq)]
enum Subject {
    NoAuth,
    User(String, Value),
    Token(
        uuid::Uuid,
        Option<Value>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    ),
    Execution(uuid::Uuid, uuid::Uuid),
}
fn subject(caller: &Access) -> Result<Subject, ApiError> {
    if caller.no_auth {
        return Ok(Subject::NoAuth);
    }
    match caller.auth.as_ref() {
        Some(Principal::User(user)) => Ok(Subject::User(
            user.username.clone(),
            user.user_id
                .clone()
                .ok_or_else(ApiError::invalid_credentials)?,
        )),
        Some(Principal::Token(token)) => Ok(Subject::Token(
            token.id.ok_or_else(ApiError::invalid_credentials)?,
            token.user_id.clone(),
            token.creation,
            token.secret_hash.clone(),
        )),
        Some(Principal::Execution(identity)) => Ok(Subject::Execution(
            identity.grant.id,
            identity.grant.launch_id,
        )),
        _ => Err(ApiError::invalid_credentials()),
    }
}

pub(super) fn finish_original(
    state: &AppState,
    headers: &HeaderMap,
    caller: &Access,
) -> Result<(), ApiError> {
    if state.config.no_auth {
        return Ok(());
    }
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ApiError::invalid_credentials)?;
    if !value
        .replace("Basic ", "")
        .replace("Bearer ", "")
        .starts_with("mrsm-key:")
    {
        crate::auth::bearer_scopes(state, headers)?;
    }
    if let Some(Principal::Token(token)) = &caller.auth
        && (!token.is_valid
            || token
                .expiration
                .is_some_and(|time| time <= chrono::Utc::now()))
    {
        return Err(ApiError::invalid_credentials());
    }
    Ok(())
}

#[derive(Clone, Default)]
pub(super) struct Proof {
    refs: BTreeSet<String>,
    runs: BTreeSet<String>,
    selections: Vec<Json>,
}
impl Proof {
    pub(super) fn refs(refs: impl IntoIterator<Item = String>) -> Self {
        Self {
            refs: refs.into_iter().collect(),
            ..Default::default()
        }
    }
    pub(super) fn runs(mut self, runs: impl IntoIterator<Item = String>) -> Self {
        self.runs.extend(runs);
        self
    }
    pub(super) fn docs(rows: &[Doc]) -> Result<Self, ApiError> {
        let mut proof = Self::default();
        for row in rows {
            for key in ["pipe_ref", "child_ref"] {
                if let Some(KValue::Str(reference)) = row.get(key)
                    && !reference.is_empty()
                {
                    proof.refs.insert(reference.clone());
                }
            }
            if let Some(KValue::Str(run)) = row.get("run_id") {
                proof.runs.insert(run.clone());
            }
            for key in ["targets", "selection"] {
                let value = match row.get(key) {
                    Some(KValue::Json(value)) => value.to_json(),
                    Some(KValue::Str(value)) => serde_json::from_str(value)
                        .map_err(|_| ApiError::new(400, "Invalid engine authority metadata."))?,
                    None | Some(KValue::Null) => continue,
                    _ => return Err(ApiError::new(400, "Invalid engine authority metadata.")),
                };
                if key == "targets" {
                    let Some(refs) = value.as_array() else {
                        return Err(ApiError::new(400, "Invalid engine targets."));
                    };
                    for reference in refs {
                        proof.refs.insert(
                            reference
                                .as_str()
                                .ok_or_else(|| ApiError::new(400, "Invalid engine target."))?
                                .into(),
                        );
                    }
                } else {
                    proof.selections.push(value);
                }
            }
        }
        proof.bounded()?;
        Ok(proof)
    }
    pub(super) fn records(
        records: &[meerschaum_connector_api::models::RunRecord],
    ) -> Result<Self, ApiError> {
        let mut result = Self::default();
        for record in records {
            result.runs.insert(record.run_id.clone());
            result.refs.extend(record.pipe_ref.clone());
            result.refs.extend(access::runs::targets_of(record));
            if let Some(selection) = record.extra.get("selection") {
                result.selections.push(match selection {
                    Value::Str(text) => serde_json::from_str(text)
                        .map_err(|_| ApiError::new(400, "Invalid engine selection."))?,
                    value => serde_json::to_value(value)
                        .map_err(|_| ApiError::new(400, "Invalid engine selection."))?,
                });
            }
        }
        result.bounded()?;
        Ok(result)
    }
    fn bounded(&self) -> Result<(), ApiError> {
        if self.refs.len() > 10_000
            || self.runs.len() > 1000
            || self.selections.len() > 1000
            || self
                .refs
                .iter()
                .any(|reference| reference.len() > 2048 || reference.chars().any(char::is_control))
            || self
                .runs
                .iter()
                .any(|run| run.is_empty() || run.len() > 512 || run.chars().any(char::is_control))
        {
            return Err(ApiError::new(
                413,
                "The engine mutation exceeds its authority bound.",
            ));
        }
        Ok(())
    }
}

struct Authority {
    ctx: Ctx,
    selected: SelectedEngine,
    original: Subject,
    held: BTreeSet<String>,
    proof: Proof,
    objects: Mutex<BTreeMap<String, (Option<String>, Map)>>,
    refusal: Mutex<Option<ApiError>>,
    cleanup: Mutex<Cleanup>,
}

#[derive(Default)]
struct Cleanup {
    confirmed: bool,
    failed: bool,
}
impl Authority {
    async fn source(
        &self,
    ) -> Result<(Option<crate::executor::pipe_operation::ReadAccess>, Access), ApiError> {
        if let Some(runtime) = self.selected.runtime_graph.get() {
            let (graph, caller) = runtime
                .fresh(
                    &self.selected,
                    &self.ctx.state,
                    &self.ctx.headers,
                    super::SCOPES,
                )
                .await?;
            Ok((Some(graph), caller))
        } else {
            Ok((None, self.selected.fresh(&self.ctx, super::SCOPES).await?))
        }
    }
    async fn current(&self) -> Result<(), ApiError> {
        self.selected.require_native_write()?;
        let (graph, caller) = self.source().await?;
        if subject(&caller)? != self.original {
            return Err(ApiError::invalid_credentials());
        }
        let mut refs = self.proof.refs.clone();
        if let Some(graph) = graph {
            let snapshot = graph.operation.snapshot().ok_or_else(ApiError::not_found)?;
            if refs.is_empty() && !self.proof.runs.is_empty() {
                refs.extend(
                    snapshot
                        .members()
                        .iter()
                        .filter(|(_, member)| member.writable)
                        .map(|(reference, _)| reference.clone()),
                );
            }
            for selection in &self.proof.selections {
                if selection
                    .get("pipeline_definitions")
                    .is_some_and(|value| !value.as_array().is_some_and(Vec::is_empty))
                {
                    return Err(ApiError::not_found());
                }
                for field in [
                    &selection["resolved_targets"],
                    &selection["targets"]["refs"],
                ] {
                    let targets = field
                        .as_array()
                        .filter(|targets| !targets.is_empty())
                        .ok_or_else(ApiError::not_found)?;
                    for reference in targets {
                        refs.insert(
                            reference
                                .as_str()
                                .ok_or_else(ApiError::not_found)?
                                .to_string(),
                        );
                    }
                }
            }
        }
        let mut filter = self
            .selected
            .filter(&self.ctx.state, &caller)
            .fresh_metadata();
        for run in &self.proof.runs {
            let records = self
                .selected
                .backend()
                .get_runs(&crate::backends::RunsFilter {
                    run_id: Some(run.clone()),
                    ..Default::default()
                })
                .await
                .map_err(super::store_error)?;
            if !records.is_empty() {
                filter.prime_run_generations(&records).await;
                if !filter.run_generation_current(run) {
                    return Err(ApiError::not_found());
                }
            }
            for record in &records {
                refs.extend(record.pipe_ref.clone());
                refs.extend(access::runs::targets_of(record));
                if !access::runs::record_pipelines_visible(&mut filter, record).await {
                    return Err(ApiError::not_found());
                }
            }
        }
        for selection in &self.proof.selections {
            if !filter.pipeline_snapshots_visible(selection).await {
                return Err(ApiError::not_found());
            }
        }
        access::runs::check_refs_editable(&mut filter, refs.iter().map(String::as_str))
            .await
            .map_err(super::refused)?;
        let registry = if self.selected.is_native_alias() {
            if refs.is_empty() {
                return Err(ApiError::new(
                    403,
                    "A logical engine mutation requires an admitted registered Pipe.",
                ));
            }
            Some(
                NativeRegistry::load(
                    &self.ctx.state,
                    self.selected.logical(),
                    self.selected.connector(),
                )
                .await?,
            )
        } else {
            None
        };
        // Every metadata/run read above can await. Refresh both original and service after
        // the last one, then perform only synchronous checks against these bounded facts.
        let (graph, mut caller) = self.source().await?;
        if subject(&caller)? != self.original {
            return Err(ApiError::invalid_credentials());
        }
        caller
            .principal
            .permissions
            .retain(|scope| self.held.contains(scope));
        caller.require(super::SCOPES, &[])?;
        if let Some(registry) = registry {
            let service = self
                .selected
                .service_access()
                .ok_or_else(ApiError::invalid_credentials)?;
            if graph.is_some() {
                service.require(&["pipes:read", "pipes:write"], &[])?;
            }
            for reference in &refs {
                let object = registry.check(&self.ctx.state, &caller, &service, reference, true)?;
                if let Some(graph) = &graph {
                    super::runtime::RuntimeGraph::target(graph, reference, object)?;
                }
                let snapshot = (
                    object.physical.registration_id(),
                    object.physical.raw_parameters().clone(),
                );
                let mut accepted = self
                    .objects
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if accepted
                    .get(reference)
                    .is_some_and(|expected| expected != &snapshot)
                {
                    return Err(ApiError::new(
                        409,
                        "The logical Pipe registration or parameters changed during this engine request.",
                    ));
                }
                accepted.entry(reference.clone()).or_insert(snapshot);
            }
        }
        finish_original(&self.ctx.state, &self.ctx.headers, &caller)
    }
}
#[async_trait::async_trait]
impl MutationCheck for Authority {
    fn rollback_completed(&self, succeeded: bool) {
        if self
            .refusal
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
        {
            return;
        }
        let mut cleanup = self
            .cleanup
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        cleanup.confirmed |= succeeded;
        cleanup.failed |= !succeeded;
    }
    fn lock_snapshot_reads(&self) -> bool {
        self.selected.is_native_alias()
    }
    async fn check(&self) -> Result<(), String> {
        let prior = self
            .refusal
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(error) = prior {
            return Err(error.detail_text());
        }
        match self.current().await {
            Ok(()) => Ok(()),
            Err(error) => {
                let reason = error.detail_text();
                self.refusal
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .get_or_insert(error);
                Err(reason)
            }
        }
    }
}

pub(super) async fn run<F: Future>(
    ctx: &Ctx,
    caller: &Access,
    proof: Proof,
    future: F,
) -> Result<F::Output, ApiError> {
    proof.bounded()?;
    let authority = Arc::new(Authority {
        ctx: ctx.clone(),
        selected: ctx.selected_engine(caller)?,
        original: subject(caller)?,
        held: caller.principal.permissions.clone(),
        proof,
        objects: Default::default(),
        refusal: Default::default(),
        cleanup: Default::default(),
    });
    authority.current().await?;
    let result = meerschaum_connector::mutation::scope(authority.clone(), future).await;
    let refusal = authority
        .refusal
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    match refusal {
        Some(error) => {
            let cleanup = authority
                .cleanup
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !cleanup.confirmed || cleanup.failed {
                return Err(ApiError::new(
                    503,
                    "The native write was refused, but its transaction cleanup could not be confirmed. An administrator must verify its outcome before retrying.",
                ));
            }
            Err(error)
        }
        None => Ok(result),
    }
}
