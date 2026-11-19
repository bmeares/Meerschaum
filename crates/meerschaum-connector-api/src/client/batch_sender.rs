//! Worker-side delivery for one Main-owned native batch stream.

use std::borrow::Cow;
use std::io::Read;
use std::path::Path;

use meerschaum_connector::{
    Connector,
    batch_delivery::{DeliveryLease, NodeDelivery},
    mutation::native_request_digest,
};
use meerschaum_core::team_scope::{sandbox, source_namespace};
use meerschaum_core::{Map, Pipe, Value};
use serde::Deserialize;
use serde_json::Value as Json;

#[cfg(test)]
use super::GraphBinding;
use super::{
    APIConnector, ApiResponse, BATCH_HEADER, BatchOutcome, BatchReceipt, BatchReply, BatchRequest,
    BatchSelector, Body, GraphPlanRequest, MAX_BATCH_REPLY, RequestOptions,
};
use crate::{ConnectorError, ConnectorResult, source_replace, wire::parse_query};

const REFUSED: &str = "Main could not prove delivery of the exact native batch. The operation may have committed. Inspect the original operation before making another write.";

fn refused() -> ConnectorError {
    ConnectorError::Invalid(REFUSED.into())
}

#[derive(Clone, Copy)]
pub(super) enum Operation {
    Data,
    SourceReplace,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::SourceReplace => "source_replace",
        }
    }

    fn validate_result(self, result: &Json) -> ConnectorResult<()> {
        let valid = match self {
            Self::Data => {
                matches!(
                    result.as_array().map(Vec::as_slice),
                    Some([Json::Bool(true), Json::String(_)])
                )
            }
            Self::SourceReplace => matches!(
                serde_json::from_value::<source_replace::Response>(result.clone()),
                Ok(source_replace::Response::Committed { result }) if result.success
            ),
        };
        valid.then_some(()).ok_or_else(refused)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Written {
    result: Json,
    receipt: BatchReceipt,
}

fn confirm(lease: &DeliveryLease, reply: BatchReply) -> ConnectorResult<BatchOutcome> {
    let owner = lease.owner();
    if &reply.consumer != owner.consumer()
        || &reply.issuer != owner.issuer()
        || reply.reference != owner.reference()
    {
        return Err(refused());
    }
    Ok(reply.outcome)
}

/// Holds exclusive ownership across every HTTP chunk of this counted call.
pub(super) struct BatchSender<'a> {
    conn: Cow<'a, APIConnector>,
    graph: Option<DeliveryLease>,
}

impl<'a> BatchSender<'a> {
    pub(super) fn ordinary(conn: &'a APIConnector) -> Self {
        Self {
            conn: Cow::Borrowed(conn),
            graph: None,
        }
    }

    pub(super) async fn new(conn: &'a APIConnector, pipe: &Pipe) -> ConnectorResult<Self> {
        let scoped = NodeDelivery::current_write();
        let Some(mut callback) = trusted_callback(conn)? else {
            return if scoped.is_some() {
                Err(refused())
            } else {
                Ok(Self::ordinary(conn))
            };
        };
        // Keep the strict callback's authority for both protocol and write requests. The
        // mutation remains single-shot even at the HTTP client's transport layer.
        let timeout = meerschaum_core::connect::connect_timeout(
            callback
                .attributes
                .get("connect_timeout")
                .map(super::attr_str)
                .as_deref(),
        );
        callback.http = reqwest::Client::builder()
            .connect_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| refused())?;
        let reference = pipe.pipe_ref();
        let owner = Self::delivery_owner(&callback, scoped, &reference).await?;
        let graph = if let Some(owner) = owner {
            if conn.get_pipe_instance_keys(pipe).as_deref() != Some(pipe.instance_keys()) {
                return Err(refused());
            }
            Some(owner.acquire(&reference)?)
        } else {
            None
        };
        Ok(Self {
            conn: Cow::Owned(callback),
            graph,
        })
    }

    async fn delivery_owner(
        callback: &APIConnector,
        scoped: Option<NodeDelivery>,
        reference: &str,
    ) -> ConnectorResult<Option<NodeDelivery>> {
        if let Some(owner) = scoped {
            // Verify owns the actors from the accepted Verify reply. Ready supplies
            // the actors for scheduled and bare nodes. Each batch reply checks both.
            return Ok(Some(owner));
        }
        Ok(callback
            .graph_plan(&GraphPlanRequest::Initial)
            .await?
            .map(|reply| NodeDelivery::new(reply.consumer, reply.issuer, reference.into())))
    }

    pub(super) fn is_batched(&self) -> bool {
        self.graph.is_some()
    }

    pub(super) async fn post(
        &mut self,
        path: &str,
        mut options: RequestOptions,
        operation: Operation,
    ) -> ConnectorResult<ApiResponse> {
        let Some(graph) = &mut self.graph else {
            return self.conn.post(path, options).await;
        };
        if graph.unresolved()
            || !options.use_token
            || options
                .headers
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case(BATCH_HEADER))
        {
            return Err(refused());
        }
        let digest = request_digest(
            &self.conn,
            path,
            &options,
            operation,
            graph.owner().reference(),
        )?;
        // Any dropped/failed reply leaves this sender unusable. Only exact Delivered lets the
        // next HTTP chunk proceed.
        let predecessor = graph.issue()?;
        let issue = BatchRequest::Issue {
            reference: graph.owner().reference().into(),
            predecessor,
            request_sha256: digest,
        };
        let reply = self.conn.graph_batch(&issue).await?.ok_or_else(refused)?;
        let BatchOutcome::Reserved { selector } = confirm(graph, reply)? else {
            return Err(refused());
        };
        graph.confirm_next(&selector)?;
        options
            .headers
            .push((BATCH_HEADER.into(), serde_json::to_string(&selector)?));

        let mut bounded = self.conn.as_ref().clone();
        bounded.response_limit = Some(
            bounded
                .response_limit
                .map_or(MAX_BATCH_REPLY, |limit| limit.min(MAX_BATCH_REPLY)),
        );
        let response = bounded.post(path, options).await?;
        let (bytes, receipt) = decode_written(&response, operation, &selector)?;
        let delivered = BatchRequest::Delivered {
            reference: graph.owner().reference().into(),
            receipt: receipt.clone(),
        };
        let reply = self
            .conn
            .graph_batch(&delivered)
            .await?
            .ok_or_else(refused)?;
        let BatchOutcome::Delivered { receipt: confirmed } = confirm(graph, reply)? else {
            return Err(refused());
        };
        if confirmed != receipt {
            return Err(refused());
        }
        graph.delivered(selector)?;
        Ok(ApiResponse { bytes, ..response })
    }
}

fn request_digest(
    conn: &APIConnector,
    path: &str,
    options: &RequestOptions,
    operation: Operation,
    reference: &str,
) -> ConnectorResult<String> {
    let body = match &options.body {
        Body::Text(text) | Body::Json(text) => text.as_bytes(),
        _ => return Err(refused()),
    };
    let url =
        reqwest::Url::parse(&conn.request_url(path, &options.query)).map_err(|_| refused())?;
    native_request_digest(
        reference,
        operation.name(),
        &parse_query(url.query().unwrap_or_default()),
        body,
    )
    .map_err(ConnectorError::Invalid)
}

fn decode_written(
    response: &ApiResponse,
    operation: Operation,
    selector: &BatchSelector,
) -> ConnectorResult<(Vec<u8>, BatchReceipt)> {
    if response.status != 200 || response.bytes.len() > MAX_BATCH_REPLY {
        return Err(ConnectorError::Invalid(format!(
            "Native batch write returned HTTP {} without a usable receipt. {REFUSED}",
            response.status
        )));
    }
    let written: Written = serde_json::from_slice(&response.bytes).map_err(|_| {
        ConnectorError::Invalid(format!(
            "Native batch write returned no structured commit receipt. {REFUSED}"
        ))
    })?;
    written.receipt.validate()?;
    if &written.receipt.selector != selector {
        return Err(refused());
    }
    operation.validate_result(&written.result)?;
    Ok((serde_json::to_vec(&written.result)?, written.receipt))
}

fn credential_path(conn: &APIConnector) -> Option<&Path> {
    conn.attributes
        .get("credential_file")
        .and_then(Value::as_str)
        .map(Path::new)
}

pub(super) fn require_node_delivery(conn: &APIConnector, pipe: &Pipe) -> ConnectorResult<()> {
    if crate::engine_pipes::is_engine_pipe(pipe)
        || conn.get_pipe_instance_keys(pipe).as_deref() != Some(pipe.instance_keys())
        || trusted_callback(conn)?.is_none()
    {
        return Err(refused());
    }
    Ok(())
}

fn trusted_callback(conn: &APIConnector) -> ConnectorResult<Option<APIConnector>> {
    if conn.request_authentication.is_none() {
        return Ok(None);
    }
    let callback = if conn.logical_identity.is_some() {
        sandbox::with_api_transport(|primary| {
            // All admitted logical aliases use the same first-enforced callback origin/file.
            let mut alias = primary.clone();
            alias.logical_keys = conn.connector_keys();
            APIConnector::from_execution_alias(&alias).map_err(|error| error.to_string())
        })
        .map_err(ConnectorError::Invalid)?
    } else if conn.label == "main_callback" {
        team_callback(conn)?
    } else {
        None
    };
    if let Some(callback) = &callback
        && (conn.url != callback.url || credential_path(conn) != credential_path(callback))
    {
        return Err(ConnectorError::Invalid(
            "This graph write did not use its launch's exact Main callback.".into(),
        ));
    }
    Ok(callback)
}

/// Team's native birth capture fixes the configuration directory. Ordinary Team service
/// connectors may have their own credentials and never use the launch callback protocol.
fn team_callback(conn: &APIConnector) -> ConnectorResult<Option<APIConnector>> {
    const MAX_CONFIG: usize = 8 * 1024 * 1024;
    let Some(launch) = source_namespace::launch().map_err(ConnectorError::Invalid)? else {
        return Ok(None);
    };
    let directory = launch.namespace.view.parent().ok_or_else(refused)?;
    let credential = directory.join("credential.json");
    let config = directory.join("config.json");
    if credential_path(conn) != Some(credential.as_path())
        || !launch
            .sandbox
            .rules
            .iter()
            .any(|rule| rule.path == directory && rule.access == sandbox::Access::Read)
        || launch.sandbox.rules.iter().any(|rule| {
            matches!(
                rule.access,
                sandbox::Access::ReadWrite
                    | sandbox::Access::DataFile
                    | sandbox::Access::Lock
                    | sandbox::Access::SharedMemory
                    | sandbox::Access::Device
            ) && (config.starts_with(&rule.path) || credential.starts_with(&rule.path))
        })
    {
        return Err(refused());
    }
    let metadata = std::fs::symlink_metadata(&config).map_err(|_| refused())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(refused());
        }
    }
    if !metadata.is_file() || metadata.len() > MAX_CONFIG as u64 {
        return Err(refused());
    }
    let file = std::fs::File::open(config).map_err(|_| refused())?;
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| refused())?;
    if bytes.len() > MAX_CONFIG {
        return Err(refused());
    }
    let config: Value = serde_json::from_slice(&bytes).map_err(|_| refused())?;
    let attributes = config
        .get_path(&["meerschaum", "connectors", "api", "main_callback"])
        .and_then(Value::as_map)
        .filter(|attrs| attrs.len() == 2)
        .ok_or_else(refused)?;
    let uri = attributes
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(refused)?;
    let sealed_credential = attributes
        .get("credential_file")
        .and_then(Value::as_str)
        .map(Path::new);
    if sealed_credential != Some(credential.as_path()) {
        return Err(refused());
    }
    let origin = reqwest::Url::parse(uri).map_err(|_| refused())?;
    if !matches!(origin.scheme(), "http" | "https")
        || origin.host().is_none()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.query().is_some()
        || origin.fragment().is_some()
        || origin.path() != "/"
    {
        return Err(refused());
    }
    let attributes: Map = attributes.clone();
    APIConnector::new(Some("main_callback"), attributes).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::GRAPH_BATCH_PATH;
    use crate::client::authentication::{Request, RequestAuthentication};
    use crate::wire::QueryValue;
    use serde_json::json;
    use std::{collections::BTreeMap, sync::Arc, time::Duration};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use uuid::Uuid;

    const DATA_PATH: &str = "/pipes/manual:test/native/None/data";
    const REFERENCE: &str = "sql:main|manual:test|native|None";

    struct Authentication;

    impl RequestAuthentication for Authentication {
        fn authorization(&self, _: Request<'_>) -> ConnectorResult<String> {
            Ok("Bearer synthetic-batch-test".into())
        }
    }

    #[derive(Debug)]
    struct Sent {
        path: String,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
    }

    async fn fixture(
        answers: Vec<Option<Json>>,
    ) -> (APIConnector, tokio::task::JoinHandle<Vec<Sent>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let conn = APIConnector::from_uri(&origin, Some("fixture"))
            .unwrap()
            .with_request_authentication(Arc::new(Authentication), MAX_BATCH_REPLY)
            .unwrap();
        let peer = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut requests = Vec::new();
                for answer in answers {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut socket = BufReader::new(socket);
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    let mut parts = line.split_whitespace();
                    assert_eq!(parts.next(), Some("POST"));
                    let path = parts.next().unwrap().to_string();
                    let mut headers: BTreeMap<String, String> = BTreeMap::new();
                    loop {
                        line.clear();
                        assert!(socket.read_line(&mut line).await.unwrap() > 0);
                        if line == "\r\n" {
                            break;
                        }
                        let (name, value) = line.split_once(':').unwrap();
                        headers.insert(name.to_ascii_lowercase(), value.trim().into());
                    }
                    assert_eq!(headers["authorization"], "Bearer synthetic-batch-test");
                    let mut body = vec![0; headers["content-length"].parse().unwrap()];
                    socket.read_exact(&mut body).await.unwrap();
                    requests.push(Sent { path, headers, body });
                    if let Some(answer) = answer {
                        let payload = serde_json::to_vec(&answer).unwrap();
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        let socket = socket.get_mut();
                        socket.write_all(head.as_bytes()).await.unwrap();
                        socket.write_all(&payload).await.unwrap();
                    }
                }
                requests
            })
            .await
            .expect("expected native batch requests did not arrive")
        });
        (conn, peer)
    }

    fn binding() -> GraphBinding {
        GraphBinding {
            grant: Uuid::new_v4(),
            launch: Uuid::new_v4(),
            revision: 2,
        }
    }

    fn sender<'a>(
        conn: &'a APIConnector,
        consumer: &GraphBinding,
        issuer: &GraphBinding,
    ) -> BatchSender<'a> {
        BatchSender {
            conn: Cow::Borrowed(conn),
            graph: Some(
                NodeDelivery::new(consumer.clone(), issuer.clone(), REFERENCE.into())
                    .acquire(REFERENCE)
                    .unwrap(),
            ),
        }
    }

    fn reply(
        consumer: &GraphBinding,
        issuer: &GraphBinding,
        outcome: BatchOutcome,
    ) -> Option<Json> {
        Some(json!({"success": true, "message": "", "batch": BatchReply {
            version: 1, consumer: consumer.clone(), issuer: issuer.clone(),
            reference: REFERENCE.into(), outcome,
        }}))
    }

    fn receipt(stream: Uuid, ordinal: u64) -> BatchReceipt {
        BatchReceipt {
            selector: BatchSelector {
                stream,
                ordinal,
                batch: Uuid::new_v4(),
            },
            sha256: "a".repeat(64),
        }
    }

    fn options(body: &str) -> RequestOptions {
        RequestOptions {
            query: vec![
                (
                    "repeat".into(),
                    QueryValue::Many(vec!["one two".into(), "+é".into()]),
                ),
                ("absent".into(), QueryValue::None),
                ("empty".into(), "".into()),
            ],
            body: Body::Text(body.into()),
            timeout: Some(Duration::from_secs(4)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn acknowledges_original_results_and_carries_only_delivered_predecessors() {
        let consumer = binding();
        let issuer = binding();
        let stream = Uuid::new_v4();
        let receipts = [
            receipt(stream, 1),
            receipt(stream, 2),
            receipt(stream, 3),
            receipt(stream, 4),
        ];
        let result = json!([true, "Inserted 1, updated 0 rows."]);
        let mut script = Vec::new();
        for receipt in &receipts {
            script.extend([
                reply(
                    &consumer,
                    &issuer,
                    BatchOutcome::Reserved {
                        selector: receipt.selector.clone(),
                    },
                ),
                Some(json!({"result": result, "receipt": receipt})),
                reply(
                    &consumer,
                    &issuer,
                    BatchOutcome::Delivered {
                        receipt: receipt.clone(),
                    },
                ),
            ]);
        }
        let (conn, peer) = fixture(script).await;
        let owner = NodeDelivery::new(consumer.clone(), issuer.clone(), REFERENCE.into());
        let bodies = [" { \"id\": 1 }\n", "{\"id\":2}", "{\"id\":3}", "{\"id\":4}"];
        for frame in bodies.as_chunks::<2>().0 {
            meerschaum_connector::batch_delivery::destination_write(Some(&owner), async {
                let admitted =
                    BatchSender::delivery_owner(&conn, NodeDelivery::current_write(), REFERENCE)
                        .await
                        .unwrap()
                        .unwrap();
                let mut sender = BatchSender {
                    conn: Cow::Borrowed(&conn),
                    graph: Some(admitted.acquire(REFERENCE).unwrap()),
                };
                for body in frame {
                    let original = sender
                        .post(DATA_PATH, options(body), Operation::Data)
                        .await
                        .unwrap();
                    assert_eq!(original.json().unwrap(), result);
                }
            })
            .await;
        }
        let calls = peer.await.unwrap();
        let query = [
            ("repeat".into(), "one two".into()),
            ("repeat".into(), "+é".into()),
            ("empty".into(), String::new()),
        ];
        let (batches, remainder) = calls.as_chunks::<3>();
        assert!(remainder.is_empty());
        assert_eq!(batches.len(), bodies.len());
        for (index, [issue, write, delivered]) in batches.iter().enumerate() {
            assert_eq!(issue.path, GRAPH_BATCH_PATH);
            assert_eq!(delivered.path, GRAPH_BATCH_PATH);
            let predecessor = index.checked_sub(1).map(|i| &receipts[i].selector);
            let issue: Json = serde_json::from_slice(&issue.body).unwrap();
            let digest =
                native_request_digest(REFERENCE, "data", &query, bodies[index].as_bytes()).unwrap();
            assert_eq!(
                issue,
                json!({"phase":"issue","reference":REFERENCE,"predecessor":predecessor,"request_sha256":digest})
            );
            assert_eq!(
                write.path,
                format!("{DATA_PATH}?repeat=one+two&repeat=%2B%C3%A9&empty=")
            );
            assert_eq!(write.body, bodies[index].as_bytes());
            let selector: BatchSelector =
                serde_json::from_str(&write.headers[BATCH_HEADER]).unwrap();
            assert_eq!(selector, receipts[index].selector);
            let delivered: Json = serde_json::from_slice(&delivered.body).unwrap();
            assert_eq!(
                delivered,
                json!({"phase":"delivered","reference":REFERENCE,"receipt":receipts[index]})
            );
        }
    }

    #[tokio::test]
    async fn unknown_changed_or_lost_delivery_cannot_send_another_write() {
        let consumer = binding();
        let issuer = binding();
        let receipt = receipt(Uuid::new_v4(), 1);
        let reserved = reply(
            &consumer,
            &issuer,
            BatchOutcome::Reserved {
                selector: receipt.selector.clone(),
            },
        );
        let written = json!({"result": [true, "Inserted 1, updated 0 rows."], "receipt": receipt});
        let mut changed = receipt.clone();
        changed.sha256 = "b".repeat(64);
        for script in [
            vec![reply(&consumer, &issuer, BatchOutcome::Unknown)],
            vec![reply(
                &consumer,
                &issuer,
                BatchOutcome::AlreadyCommitted {
                    receipt: receipt.clone(),
                },
            )],
            vec![reply(
                &consumer,
                &binding(),
                BatchOutcome::Reserved {
                    selector: receipt.selector.clone(),
                },
            )],
            vec![reserved.clone(), None],
            vec![reserved.clone(), Some(json!([true, "unwrapped"]))],
            vec![
                reserved.clone(),
                Some(written.clone()),
                reply(
                    &consumer,
                    &issuer,
                    BatchOutcome::Delivered { receipt: changed },
                ),
            ],
        ] {
            let expected = script.len();
            let (conn, peer) = fixture(script).await;
            let mut sender = sender(&conn, &consumer, &issuer);
            assert!(
                sender
                    .post(DATA_PATH, options("{}"), Operation::Data)
                    .await
                    .is_err()
            );
            assert!(sender.graph.as_ref().unwrap().unresolved());
            assert_eq!(
                sender
                    .post(DATA_PATH, options("{}"), Operation::Data)
                    .await
                    .unwrap_err()
                    .to_string(),
                refused().to_string()
            );
            let owner = sender.graph.as_ref().unwrap().owner().clone();
            drop(sender);
            assert!(owner.acquire(REFERENCE).is_err());
            assert_eq!(peer.await.unwrap().len(), expected);
        }
    }

    #[test]
    fn replacement_requires_committed_typed_result_for_the_exact_selector() {
        let receipt = receipt(Uuid::new_v4(), 1);
        let result = source_replace::Response::Committed {
            result: meerschaum_core::success::SyncResult::new(true, "Replaced"),
        };
        let committed = json!({"result": result, "receipt": receipt});
        let response = |body: &Json| ApiResponse {
            status: 200,
            content_type: "application/json".into(),
            bytes: serde_json::to_vec(body).unwrap(),
        };
        let (body, proven) = decode_written(
            &response(&committed),
            Operation::SourceReplace,
            &receipt.selector,
        )
        .unwrap();
        assert_eq!(proven, receipt);
        assert_eq!(
            serde_json::from_slice::<Json>(&body).unwrap(),
            committed["result"]
        );

        let mut extra = committed.clone();
        extra["unknown"] = Json::Null;
        let mut failed = committed;
        failed["result"]["result"]["success"] = false.into();
        for bad in [extra, failed] {
            assert!(
                decode_written(&response(&bad), Operation::SourceReplace, &receipt.selector)
                    .is_err()
            );
        }
        let closed = json!({"result": source_replace::Response::Closed, "receipt": receipt});
        assert!(
            decode_written(
                &response(&closed),
                Operation::SourceReplace,
                &receipt.selector
            )
            .is_err()
        );
        let other = BatchSelector {
            batch: Uuid::new_v4(),
            ..receipt.selector.clone()
        };
        assert!(
            decode_written(
                &response(&json!({"result": result, "receipt": receipt})),
                Operation::SourceReplace,
                &other
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn unrelated_authenticated_api_retains_the_ordinary_wire() {
        let (conn, peer) = fixture(vec![Some(json!([true, "legacy"]))]).await;
        let pipe = Pipe::builder("manual:test", "native")
            .instance("sql:main")
            .build()
            .unwrap();
        let mut sender = BatchSender::new(&conn, &pipe).await.unwrap();
        let response = sender
            .post(DATA_PATH, options("[]"), Operation::Data)
            .await
            .unwrap();
        assert_eq!(response.json().unwrap(), json!([true, "legacy"]));
        let calls = peer.await.unwrap();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].headers.contains_key(BATCH_HEADER));
    }
}
