//! Numeric sync telemetry of an API instance; visibility is always enforced by its server.

use super::{APIConnector, Body, RequestOptions};
use crate::traits::{ConnectorError, ConnectorResult};
use crate::{routes, wire::QueryValue};
use meerschaum_connector::Connector;
use meerschaum_connector::sync_metrics::{MetricsQuery, MetricsResponse, SyncMetric};

impl APIConnector {
    /// Persist a bounded telemetry batch on the server's selected instance.
    pub async fn post_sync_metrics(&self, metrics: &[SyncMetric]) -> ConnectorResult<()> {
        if metrics.is_empty() {
            return Ok(());
        }
        let response = self
            .post(
                routes::ENGINE_METRICS,
                RequestOptions {
                    query: vec![(
                        "instance_keys".into(),
                        QueryValue::opt(self.instance_keys()),
                    )],
                    body: Body::Json(serde_json::json!({"metrics": metrics}).to_string()),
                    ..Default::default()
                },
            )
            .await?;
        let body = response.json_or_detail()?;
        if !response.ok() || body.get("success").and_then(serde_json::Value::as_bool) != Some(true)
        {
            return Err(ConnectorError::Detail(
                "The server refused sync telemetry.".into(),
            ));
        }
        Ok(())
    }

    /// Read bounded, caller-visible metrics. Local visibility ceilings may only narrow the result.
    pub async fn get_remote_engine_metrics(
        &self,
        query: &MetricsQuery,
    ) -> ConnectorResult<MetricsResponse> {
        let mut parameters = vec![(
            "instance_keys".into(),
            QueryValue::opt(self.instance_keys()),
        )];
        for (name, value) in [
            ("begin", query.begin.map(|value| value.to_rfc3339())),
            ("end", query.end.map(|value| value.to_rfc3339())),
            ("pipe_ref", query.pipe_ref.clone()),
            ("pipeline", query.pipeline.clone()),
            ("limit", query.limit.map(|value| value.to_string())),
            ("buckets", query.buckets.map(|value| value.to_string())),
        ] {
            if let Some(value) = value {
                parameters.push((name.into(), value.into()));
            }
        }
        for (name, values) in [
            ("visible_refs", &query.visible_refs),
            ("visible_pipelines", &query.visible_pipelines),
        ] {
            if let Some(values) = values {
                parameters.push((name.into(), serde_json::to_string(values)?.into()));
            }
        }
        let response = self
            .get(
                routes::ENGINE_METRICS,
                RequestOptions {
                    query: parameters,
                    ..Default::default()
                },
            )
            .await?;
        if !response.ok() {
            return Err(ConnectorError::Detail(
                "Cannot read engine metrics from this API instance.".into(),
            ));
        }
        let body = response.json_or_detail()?;
        let mut result: MetricsResponse =
            serde_json::from_value(body.get("metrics").cloned().ok_or_else(|| {
                ConnectorError::Invalid("The server returned no engine metrics.".into())
            })?)?;
        for metric in &mut result.history {
            metric.instance_keys = self.connector_keys();
            metric.pipe_ref = format!(
                "{}|{}|{}|{}",
                metric.instance_keys,
                metric.connector_keys,
                metric.metric_key,
                metric.location_key.as_deref().unwrap_or("None")
            );
        }
        for pipe in &mut result.pipes {
            if let Some((connector, metric, location)) =
                meerschaum_connector::sync_metrics::reference_keys(&pipe.pipe_ref)
            {
                pipe.pipe_ref = format!(
                    "{}|{connector}|{metric}|{}",
                    self.connector_keys(),
                    location.unwrap_or("None")
                );
            }
        }
        Ok(result)
    }

    /// Initialize on the served instance; only a full administrator may request this remotely.
    pub async fn initialize_remote_engine_metrics(&self) -> ConnectorResult<()> {
        let response = self
            .post(
                &format!("{}/initialize", routes::ENGINE_METRICS),
                RequestOptions {
                    query: vec![(
                        "instance_keys".into(),
                        QueryValue::opt(self.instance_keys()),
                    )],
                    ..Default::default()
                },
            )
            .await?;
        if response.ok() {
            Ok(())
        } else {
            Err(ConnectorError::Detail(
                "Cannot initialize engine metrics.".into(),
            ))
        }
    }
}
