use super::*;
use crate::{
    Connector, ConnectorResult, DataQuery, DateBound, PipeId, PipeKeysEntry, PipesKeys,
    PipesKeysQuery, SyncOptions,
};
use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_kernel::Frame;

struct MetricsOnly {
    saved_pipeline: bool,
    linked_run: bool,
}

#[crate::async_trait]
impl Connector for MetricsOnly {
    fn connector_type(&self) -> &str {
        "synthetic"
    }
    fn label(&self) -> &str {
        "metrics"
    }
    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }
}

#[crate::async_trait]
impl InstanceConnector for MetricsOnly {
    async fn registered_pipes(
        &self,
        query: &PipesKeysQuery,
    ) -> ConnectorResult<Vec<(Option<PipeId>, PipeKeysEntry)>> {
        if !query.connector_keys.is_empty() {
            return Ok(if self.saved_pipeline {
                vec![(
                    Some(PipeId::Int(2)),
                    PipeKeysEntry {
                        connector_keys: "mrsm".into(),
                        metric_key: "pipeline".into(),
                        location_key: Some("daily".into()),
                        extra: Some(
                            Value::from_json_str(r#"{"pipeline":{"id":"saved-daily"}}"#).unwrap(),
                        ),
                    },
                )]
            } else {
                Vec::new()
            });
        }
        Ok(vec![(
            Some(PipeId::Int(1)),
            PipeKeysEntry {
                connector_keys: "synthetic".into(),
                metric_key: "readings".into(),
                location_key: None,
                extra: Some(Value::Map(Map::new())),
            },
        )])
    }
    async fn read_engine_metric_rows(
        &self,
        metric: &str,
        _query: &DataQuery,
    ) -> ConnectorResult<(Vec<Doc>, bool)> {
        if metric != METRIC {
            return Err(ConnectorError::Invalid("Pipeline history was read".into()));
        }
        let pipe = Pipe::builder("synthetic", "readings")
            .instance(&self.connector_keys())
            .build()
            .unwrap();
        let outcome = SyncOutcome::new(true, "", crate::sync_status::SyncSource::Python, None)
            .with_counts(2, 0, 0);
        let mut metric = SyncMetric::from_outcome(&pipe, &outcome);
        metric.ts -= TimeDelta::seconds(1);
        metric.started = metric.ts;
        metric.registration_id = registration_stamp("1", &Map::new());
        if self.linked_run {
            metric.run_id = Some("standalone-engine-run".into());
        }
        Ok((vec![metric.to_doc()], false))
    }
    async fn register_pipe(&self, _pipe: &Pipe) -> SuccessTuple {
        unreachable!()
    }
    async fn get_pipe_attributes(&self, _pipe: &Pipe) -> ConnectorResult<Map> {
        unreachable!()
    }
    async fn get_pipe_id(&self, _pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        unreachable!()
    }
    async fn fetch_pipes_keys(&self, _query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        unreachable!()
    }
    async fn pipe_exists(&self, _pipe: &Pipe) -> ConnectorResult<bool> {
        unreachable!()
    }
    async fn drop_pipe(&self, _pipe: &Pipe) -> SuccessTuple {
        unreachable!()
    }
    async fn sync_pipe(&self, _pipe: &Pipe, _frame: Frame, _options: &SyncOptions) -> SuccessTuple {
        unreachable!()
    }
    async fn get_pipe_data(
        &self,
        _pipe: &Pipe,
        _query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        unreachable!()
    }
    async fn get_sync_time(
        &self,
        _pipe: &Pipe,
        _params: Option<&Map>,
        _newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        unreachable!()
    }
    async fn get_pipe_rowcount(
        &self,
        _pipe: &Pipe,
        _query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        unreachable!()
    }
    async fn get_pipe_columns_types(
        &self,
        _pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        unreachable!()
    }
}

#[tokio::test]
async fn standalone_metrics_skip_unneeded_pipeline_history() {
    let result = read(
        &MetricsOnly {
            saved_pipeline: false,
            linked_run: false,
        },
        &MetricsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(result.summary.syncs, 1);
    assert_eq!(result.summary.rows, 2);
    assert_eq!(result.history.len(), 1);
    assert!(!result.series.is_empty());
    assert!(result.pipelines.is_empty());
}

#[tokio::test]
async fn saved_pipelines_still_require_their_run_history() {
    let error = read(
        &MetricsOnly {
            saved_pipeline: true,
            linked_run: false,
        },
        &MetricsQuery::default(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("Pipeline history was read"));
}

#[tokio::test]
async fn unsaved_engine_runs_still_require_their_run_history() {
    let error = read(
        &MetricsOnly {
            saved_pipeline: false,
            linked_run: true,
        },
        &MetricsQuery::default(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("Pipeline history was read"));
}

#[test]
fn extreme_typed_bounds_return_validation_errors_instead_of_panicking() {
    assert!(
        MetricsQuery {
            end: Some(DateTime::<Utc>::MIN_UTC),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        MetricsQuery {
            end: Some(DateTime::<Utc>::MAX_UTC),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    let end = DateTime::<Utc>::MIN_UTC + TimeDelta::hours(1);
    assert!(
        MetricsQuery {
            begin: Some(end - TimeDelta::minutes(1)),
            end: Some(end),
            ..Default::default()
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn captured_start_preserves_submillisecond_duration_and_cannot_underflow() {
    let pipe = Pipe::builder("synthetic", "timing").build().unwrap();
    let mut outcome = SyncOutcome::new(true, "", crate::sync_status::SyncSource::Python, None)
        .with_duration(Some(Duration::from_micros(250)));
    let metric = SyncMetric::from_outcome(&pipe, &outcome);
    assert_eq!(metric.ts - metric.started, TimeDelta::microseconds(250));
    assert_eq!(
        SyncMetric::from_doc(&metric.to_doc())
            .unwrap()
            .started
            .timestamp_micros(),
        metric.started.timestamp_micros()
    );
    outcome.finished = DateTime::<Utc>::MIN_UTC;
    assert_eq!(
        SyncMetric::from_outcome(&pipe, &outcome).started,
        outcome.finished
    );
}
