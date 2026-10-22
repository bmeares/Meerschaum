//! The `/engine/*` route family (§5.8, new in v5): engine state for API instances. Every route
//! requires `pipes:write`. Write routes answer with a `SuccessTuple`; the lease routes with a
//! [`LeaseResponse`].

use meerschaum_core::SuccessTuple;
use meerschaum_kernel::Frame;
use serde::Serialize;

use crate::engine_pipes::{self, EnginePipeQuery};
use crate::models::{
    EdgeCursor, EngineEvent, EngineState, LeaseRequest, LeaseResponse, LedgerEntry, RunRecord,
};
use crate::routes;
use crate::traits::{ConnectorError, ConnectorResult};
use crate::wire::QueryValue;

use super::{APIConnector, Body, Method, RequestOptions};

impl APIConnector {
    async fn engine_write<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: &T,
    ) -> SuccessTuple {
        let body = match serde_json::to_string(body) {
            Ok(b) => b,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match self
            .make_request(
                method,
                path,
                RequestOptions {
                    body: Body::Json(body),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => crate::models::parse_success_tuple(r.ok(), &r.text()),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `POST /engine/ledger`: append dirty-range ledger entries.
    pub async fn post_engine_ledger(&self, entries: &[LedgerEntry]) -> SuccessTuple {
        self.engine_write(Method::Post, routes::ENGINE_LEDGER, entries)
            .await
    }

    /// `GET /engine/ledger?child_ref=&after_seq=`: the parents' entries for a child after a
    /// sequence number.
    pub async fn get_engine_ledger(
        &self,
        child_ref: &str,
        after_seq: Option<i64>,
    ) -> ConnectorResult<Vec<LedgerEntry>> {
        let opts = RequestOptions::query(vec![
            ("child_ref", child_ref.into()),
            (
                "after_seq",
                after_seq.map(QueryValue::from).unwrap_or(QueryValue::None),
            ),
        ]);
        let j = self
            .get(routes::ENGINE_LEDGER, opts)
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `PUT /engine/cursors`: upsert edge cursors.
    pub async fn put_engine_cursors(&self, cursors: &[EdgeCursor]) -> SuccessTuple {
        self.engine_write(Method::Put, routes::ENGINE_CURSORS, cursors)
            .await
    }

    /// `GET /engine/state?pipe_ref=`: a pipe's persisted engine state.
    pub async fn get_engine_state(&self, pipe_ref: &str) -> ConnectorResult<Option<EngineState>> {
        let r = self
            .get(
                routes::ENGINE_STATE,
                RequestOptions::query(vec![("pipe_ref", pipe_ref.into())]),
            )
            .await?;
        if r.status == 404 {
            return Ok(None);
        }
        let j = r.json_or_detail()?;
        if j.is_null() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_value(j)?))
    }

    /// `PUT /engine/state`: upsert pipes' engine state.
    pub async fn put_engine_state(&self, states: &[EngineState]) -> SuccessTuple {
        self.engine_write(Method::Put, routes::ENGINE_STATE, states)
            .await
    }

    /// `POST /engine/runs`: record runs and per-node results.
    pub async fn post_engine_runs(&self, records: &[RunRecord]) -> SuccessTuple {
        self.engine_write(Method::Post, routes::ENGINE_RUNS, records)
            .await
    }

    /// `GET /engine/runs?run_id=&limit=`: run history.
    pub async fn get_engine_runs(
        &self,
        run_id: Option<&str>,
        limit: Option<i64>,
    ) -> ConnectorResult<Vec<RunRecord>> {
        let opts = RequestOptions::query(vec![
            ("run_id", QueryValue::opt(run_id.map(String::from))),
            (
                "limit",
                limit.map(QueryValue::from).unwrap_or(QueryValue::None),
            ),
        ]);
        let j = self
            .get(routes::ENGINE_RUNS, opts)
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `POST /engine/events`: append a batch of run events.
    pub async fn post_engine_events(&self, events: &[EngineEvent]) -> SuccessTuple {
        self.engine_write(Method::Post, routes::ENGINE_EVENTS, events)
            .await
    }

    async fn lease_request(&self, method: Method, req: &LeaseRequest) -> ConnectorResult<bool> {
        let parts: Vec<_> = req.pipe_ref.splitn(4, '|').collect();
        let pipeline_metadata = parts.len() == 4
            && parts[1] == "mrsm"
            && ["pipeline", "pipeline_registry"].contains(&parts[2]);
        let route = if pipeline_metadata {
            "/pipelines/leases"
        } else {
            routes::ENGINE_LEASES
        };
        let r = self
            .make_request(
                method,
                route,
                RequestOptions {
                    body: Body::Json(serde_json::to_string(req)?),
                    query: if pipeline_metadata {
                        vec![(
                            "instance_keys".into(),
                            QueryValue::opt(self.instance_keys()),
                        )]
                    } else {
                        vec![]
                    },
                    ..Default::default()
                },
            )
            .await?;
        if !r.ok() {
            return Err(ConnectorError::Http {
                status: r.status,
                body: r.text(),
            });
        }
        let resp: LeaseResponse = serde_json::from_value(r.json()?)?;
        Ok(resp.success)
    }

    /// `POST /engine/leases`: acquire (or extend) a pipe's lease.
    pub async fn acquire_engine_lease(&self, req: &LeaseRequest) -> ConnectorResult<bool> {
        self.lease_request(Method::Post, req).await
    }

    /// `DELETE /engine/leases`: release a pipe's lease.
    pub async fn release_engine_lease(&self, req: &LeaseRequest) -> ConnectorResult<bool> {
        self.lease_request(Method::Delete, req).await
    }

    async fn engine_pipe_request(
        &self,
        method: Method,
        metric_key: &str,
        op: &str,
        body: Option<String>,
    ) -> ConnectorResult<serde_json::Value> {
        let r = self
            .make_request(
                method,
                &routes::engine_pipe_path(metric_key, op),
                RequestOptions {
                    body: body.map(Body::Json).unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await?;
        if r.status == 501 {
            return Err(ConnectorError::NotImplemented(r.text()));
        }
        if !r.ok() {
            return Err(ConnectorError::Http {
                status: r.status,
                body: r.text(),
            });
        }
        r.json()
    }

    /// `POST /engine/pipes/{metric}/read`: rows of an engine state pipe, kept on the server's
    /// instance, as a frame with the pipe's static dtypes (`None` when there are none).
    pub async fn read_engine_pipe(
        &self,
        metric_key: &str,
        query: &EnginePipeQuery,
    ) -> ConnectorResult<Option<Frame>> {
        let j = self
            .engine_pipe_request(
                Method::Post,
                metric_key,
                "read",
                Some(serde_json::to_string(query)?),
            )
            .await?;
        let Some(items) = j.as_array() else {
            return Err(ConnectorError::Invalid(format!(
                "Unexpected engine state response: {j}"
            )));
        };
        if items.is_empty() {
            return Ok(None);
        }
        engine_pipes::engine_frame(metric_key, engine_pipes::docs_from_records(items))
            .map(Some)
            .map_err(ConnectorError::Invalid)
    }

    /// `POST /engine/pipes/{metric}/write`: append (or, for the upsert pipes, upsert) rows.
    pub async fn write_engine_pipe(&self, metric_key: &str, df: &Frame) -> SuccessTuple {
        let body = engine_pipes::records_json(df);
        match self
            .engine_pipe_request(Method::Post, metric_key, "write", Some(body))
            .await
        {
            Ok(j) => crate::models::parse_success_tuple(true, &j.to_string()),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `POST /engine/pipes/{metric}/clear`: delete the rows matching `params` (and `end`).
    pub async fn clear_engine_pipe(
        &self,
        metric_key: &str,
        query: &EnginePipeQuery,
    ) -> SuccessTuple {
        let body = match serde_json::to_string(query) {
            Ok(b) => b,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match self
            .engine_pipe_request(Method::Post, metric_key, "clear", Some(body))
            .await
        {
            Ok(j) => crate::models::parse_success_tuple(true, &j.to_string()),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `GET /engine/pipes/{metric}/exists`: whether the state pipe's table exists.
    pub async fn engine_pipe_exists(&self, metric_key: &str) -> ConnectorResult<bool> {
        let j = self
            .engine_pipe_request(Method::Get, metric_key, "exists", None)
            .await?;
        Ok(j.as_bool().unwrap_or(false))
    }
}
