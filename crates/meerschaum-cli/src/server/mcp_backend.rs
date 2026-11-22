//! The native backend's MCP tools: connector registration, pipe and graph syncs, verification and
//! bounded SQL reads.

use async_trait::async_trait;
use meerschaum_core::{Pipe, SuccessTuple};
use meerschaum_server::mcp::{ConnectorRegister, McpBackend, SqlReadError};

use super::NativeBackend;
use crate::native;

#[async_trait]
impl McpBackend for NativeBackend {
    /// `Pipe.register()`'s connector half through the session's Python worker (the same ask as
    /// `register pipes`), so the MCP `register_pipe` tool and `mrsm register pipes` agree.
    async fn connector_register(&self, pipe: &Pipe) -> ConnectorRegister {
        let caller = meerschaum_server::executor::context::current();
        match self.api_worker(caller.as_ref()).await {
            Err(error) => return ConnectorRegister::Unavailable(error),
            Ok(true) if !native::pipes::is_python_connector(pipe.connector_keys()) => {
                return ConnectorRegister::Undefined;
            }
            Ok(true) => {
                let Some(controller) = &self.executors else {
                    return ConnectorRegister::Unavailable(
                        "The confined registration runtime is unavailable.".into(),
                    );
                };
                return meerschaum_server::executor::context::scope(
                    caller,
                    controller.connector_registration(pipe),
                )
                .await
                .unwrap_or_else(ConnectorRegister::Unavailable);
            }
            Ok(false) => {}
        }
        let backend = self.clone();
        let pipe = pipe.clone();
        let acting = meerschaum_connector::acting_team::current();
        tokio::task::spawn_blocking(move || {
            let Ok((session, config)) = backend.scoped(acting.as_deref()) else {
                return ConnectorRegister::Undefined;
            };
            match native::manage::ask_connector_register(&session, &config, &pipe) {
                None => ConnectorRegister::Undefined,
                Some(Ok(outcome)) if outcome.valid => {
                    ConnectorRegister::Parameters(outcome.parameters)
                }
                Some(Ok(outcome)) => ConnectorRegister::Invalid(outcome.repr),
                Some(Err(error)) => {
                    tracing::warn!("`register()` failed for {pipe}: {error}");
                    ConnectorRegister::Undefined
                }
            }
        })
        .await
        .unwrap_or(ConnectorRegister::Undefined)
    }

    async fn sync_pipe(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
    ) -> SuccessTuple {
        self.sync(pipe, begin, end, false).await
    }

    /// `sync pipes -c CK -m MK -l LK -i IK --parents [N] --children [N]`: the pipe with its
    /// upstream or downstream pipes, in dependency order (the Syncing Engine).
    async fn sync_pipe_graph(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
        parents: Option<meerschaum_core::Value>,
        children: Option<meerschaum_core::Value>,
    ) -> SuccessTuple {
        if parents.is_none() && children.is_none() {
            return self.sync(pipe, begin, end, false).await;
        }
        let caller = meerschaum_server::executor::context::current();
        let api_worker = match self.api_worker(caller.as_ref()).await {
            Ok(value) => value,
            Err(error) => return SuccessTuple::fail(error),
        };
        if api_worker {
            let Some(executors) = &self.executors else {
                return SuccessTuple::fail("The canonical API execution runtime is unavailable.");
            };
            let operation = meerschaum_server::executor::pipe_operation::PipeOperation {
                row_params: None,
                pipe_ref: format!(
                    "{}|{}|{}|{}",
                    pipe.instance_keys(),
                    pipe.connector_keys(),
                    pipe.metric_key(),
                    pipe.location_key().unwrap_or("None")
                ),
                registration: meerschaum_core::Value::Null,
                verify: false,
                begin,
                end,
                parents,
                children,
                metadata: None,
            };
            return meerschaum_server::executor::context::scope(
                caller,
                executors.run_pipe_operation(operation, &self.session),
            )
            .await
            .map(|(result, _)| result)
            .unwrap_or_else(SuccessTuple::fail);
        }
        let mut argv: Vec<String> = vec![
            "sync".into(),
            "pipes".into(),
            "-c".into(),
            pipe.connector_keys().into(),
            "-m".into(),
            pipe.metric_key().into(),
            "-l".into(),
            pipe.location_key().unwrap_or("None").into(),
            "-i".into(),
            pipe.instance_keys().into(),
        ];
        for (flag, depth) in [("--parents", parents), ("--children", children)] {
            match depth {
                Some(meerschaum_core::Value::Int(n)) => argv.push(format!("{flag}={n}")),
                Some(_) => argv.push(flag.into()),
                None => {}
            }
        }
        for (flag, bound) in [("--begin", begin), ("--end", end)] {
            if let Some(bound) = bound {
                argv.extend([flag.to_string(), bound]);
            }
        }
        match self.run_captured(argv).await {
            Ok((result, _)) => result,
            Err(error) => SuccessTuple::fail(error),
        }
    }

    async fn verify_pipe(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
    ) -> SuccessTuple {
        self.sync(pipe, begin, end, true).await
    }

    async fn read_sql(
        &self,
        connector_keys: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<meerschaum_core::Value>, SqlReadError> {
        let backend = self.clone();
        let keys = connector_keys.to_string();
        let query = query.to_string();
        tokio::task::spawn_blocking(move || {
            let config = backend.config().map_err(|_| SqlReadError::Unsupported)?;
            let session = backend.session.clone();
            let instance = session
                .instance(&keys, &config)
                .map_err(|_| SqlReadError::Unsupported)?;
            let sql = instance.sql().ok_or(SqlReadError::Unsupported)?;
            let frame = session
                // A protected connector runs only a read statement, read-only.
                .block_on(sql.read_raw(&query))
                .map_err(|_| SqlReadError::Failed)?;
            let json = meerschaum_server::serialize::data_to_json(
                &frame,
                &meerschaum_server::serialize::DataJsonOptions {
                    epoch: false,
                    date_unit: meerschaum_server::serialize::DateUnit::Ms,
                    double_precision: 10,
                    geometry_format: meerschaum_kernel::serialize::GeometryFormat::WkbHex,
                },
            );
            let mut rows: Vec<meerschaum_core::Value> =
                serde_json::from_str(&json).map_err(|_| SqlReadError::Failed)?;
            rows.truncate(limit);
            Ok(rows)
        })
        .await
        .map_err(|_| SqlReadError::Failed)?
    }
}
