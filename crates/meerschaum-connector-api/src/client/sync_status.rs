//! The per-pipe sync status of an `api:` instance (rc15 contract C2): the server keeps the rows
//! on its own instance, keyed with its instance keys. This client posts its sync outcomes to
//! `POST /engine/sync-status` and reads `GET /engine/sync-status`, re-keying the rows with its own
//! `api:<label>` keys ([`meerschaum_connector::sync_status::rekey`]).

use meerschaum_connector::sync_status::{SyncOutcome, SyncStatus, rekey};
use meerschaum_core::Pipe;
use serde_json::{Value as Json, json};

use crate::routes;
use crate::traits::{ConnectorError, ConnectorResult};
use crate::wire::QueryValue;

use super::{APIConnector, ApiResponse, Body, RequestOptions};

/// A server older than 5.0.0rc15 has no sync status routes.
const UNSUPPORTED: &str =
    "This API server does not keep sync statuses (it runs a Meerschaum older than 5.0.0rc15).";

/// The body of a v5 route's answer: `Err` for `success: false`, a v4 `{"detail": ...}` or a
/// missing route.
fn v5_body(r: &ApiResponse) -> ConnectorResult<Json> {
    if r.status == 404 || r.status == 405 {
        return Err(ConnectorError::NotImplemented(UNSUPPORTED.into()));
    }
    let j = r.json_or_detail()?;
    if j.get("success").and_then(Json::as_bool) == Some(false) || !r.ok() {
        let message = j
            .get("message")
            .and_then(Json::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| r.text());
        return Err(ConnectorError::Detail(message));
    }
    Ok(j)
}

fn statuses_of(j: &Json, instance_keys: &str) -> ConnectorResult<Vec<SyncStatus>> {
    let items = j
        .get("statuses")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default();
    items
        .into_iter()
        .map(|item| {
            serde_json::from_value::<SyncStatus>(item)
                .map(|s| rekey(s, instance_keys))
                .map_err(ConnectorError::from)
        })
        .collect()
}

impl APIConnector {
    /// `POST /engine/sync-status`: merge sync outcomes of `pipe` (oldest first) into its status
    /// on the server's instance; the stored status, keyed with this client's keys.
    pub async fn post_sync_outcomes(
        &self,
        pipe: &Pipe,
        outcomes: &[SyncOutcome],
    ) -> ConnectorResult<Option<SyncStatus>> {
        let body = json!({
            "connector_keys": pipe.connector_keys(),
            "metric_key": pipe.metric_key(),
            "location_key": pipe.location_key(),
            "outcomes": outcomes,
        });
        let r = self
            .post(
                routes::ENGINE_SYNC_STATUS,
                RequestOptions {
                    query: vec![(
                        "instance_keys".into(),
                        QueryValue::opt(self.get_pipe_instance_keys(pipe)),
                    )],
                    body: Body::Json(body.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        let j = v5_body(&r)?;
        match j.get("status") {
            Some(s) if !s.is_null() => Ok(Some(rekey(
                serde_json::from_value(s.clone())?,
                pipe.instance_keys(),
            ))),
            _ => Ok(None),
        }
    }

    /// `GET /engine/sync-status?pipe_ref=`: the status of `pipe` on the server's instance, after
    /// posting what this process still holds for it
    /// ([`meerschaum_connector::sync_status::settle_pipe`]).
    pub async fn get_remote_sync_status(&self, pipe: &Pipe) -> ConnectorResult<Option<SyncStatus>> {
        meerschaum_connector::sync_status::settle_pipe(&pipe.pipe_ref()).await;
        let opts = RequestOptions::query(vec![
            ("pipe_ref", pipe.pipe_ref().into()),
            (
                "instance_keys",
                QueryValue::opt(self.get_pipe_instance_keys(pipe)),
            ),
        ]);
        let r = self.get(routes::ENGINE_SYNC_STATUS, opts).await?;
        Ok(statuses_of(&v5_body(&r)?, pipe.instance_keys())?
            .into_iter()
            .next())
    }

    /// `GET /engine/sync-status`: every status on the server's instance, keyed under
    /// `instance_keys` (this connector's keys), after posting what this process still holds
    /// for them ([`meerschaum_connector::sync_status::settle_instance`]).
    pub async fn get_remote_sync_statuses(
        &self,
        instance_keys: &str,
    ) -> ConnectorResult<Vec<SyncStatus>> {
        meerschaum_connector::sync_status::settle_instance(instance_keys).await;
        let opts = RequestOptions::query(vec![(
            "instance_keys",
            QueryValue::opt(self.instance_keys()),
        )]);
        let r = self.get(routes::ENGINE_SYNC_STATUS, opts).await?;
        statuses_of(&v5_body(&r)?, instance_keys)
    }
}
