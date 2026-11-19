//! HTTP client for the graph batch protocol.
use super::{APIConnector, Body, RequestOptions};
#[cfg(test)]
use crate::graph_batch::{BatchOutcome, BatchReceipt, BatchSelector, GraphBinding};
use crate::graph_batch::{
    BatchReply, BatchRequest, GRAPH_BATCH_PATH, MAX_BATCH_REPLY, MAX_BATCH_REQUEST, invalid,
};
use crate::{ConnectorError, ConnectorResult};
use std::time::Duration;
#[cfg(test)]
use uuid::Uuid;

impl APIConnector {
    pub async fn graph_batch(&self, request: &BatchRequest) -> ConnectorResult<Option<BatchReply>> {
        request.validate()?;
        let body = serde_json::to_string(request)?;
        if body.len() > MAX_BATCH_REQUEST {
            return Err(invalid());
        }
        let mut bounded = self.clone();
        bounded.response_limit = Some(
            self.response_limit
                .map_or(MAX_BATCH_REPLY, |limit| limit.min(MAX_BATCH_REPLY)),
        );
        let response = bounded
            .post(
                GRAPH_BATCH_PATH,
                RequestOptions {
                    body: Body::Json(body),
                    timeout: Some(Duration::from_secs(20)),
                    ..Default::default()
                },
            )
            .await?;
        decode_response(&response, request)
    }
}

fn decode_response(
    response: &super::ApiResponse,
    request: &BatchRequest,
) -> ConnectorResult<Option<BatchReply>> {
    if response.status != 200 || response.bytes.len() > MAX_BATCH_REPLY {
        return Err(ConnectorError::Backend("Main refused canonical native batch state; no data replay or local fallback is available.".into()));
    }
    let envelope = response.envelope()?;
    if envelope.get("success").and_then(serde_json::Value::as_bool) != Some(true)
        || !envelope
            .get("message")
            .is_some_and(serde_json::Value::is_string)
    {
        return Err(invalid());
    }
    let batch = envelope.get("batch").ok_or_else(invalid)?;
    // Explicit Main-verified nongraph. Missing/malformed is never nongraph.
    if batch.is_null() {
        return Ok(None);
    }
    let reply: BatchReply = serde_json::from_value(batch.clone())?;
    reply.validate(request)?;
    Ok(Some(reply))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn issue() -> BatchRequest {
        BatchRequest::Issue {
            reference: "sql:main|sql:source|sample|None".into(),
            predecessor: None,
            request_sha256: "ab".repeat(32),
        }
    }

    fn response(status: u16, body: Value) -> super::super::ApiResponse {
        super::super::ApiResponse {
            status,
            content_type: "application/json".into(),
            bytes: serde_json::to_vec(&body).unwrap(),
        }
    }

    #[test]
    fn only_explicit_successful_null_means_no_graph() {
        let request = issue();
        let valid = json!({"success":true,"message":"ok","batch":null});
        assert!(
            decode_response(&response(200, valid.clone()), &request)
                .unwrap()
                .is_none()
        );
        for (status, body) in [
            (503, valid),
            (200, json!({"success":true,"message":"missing"})),
            (
                200,
                json!({"success":false,"message":"refused","batch":null}),
            ),
            (200, json!({"success":true,"batch":null})),
            (
                200,
                json!({"success":true,"message":"malformed","batch":{}}),
            ),
        ] {
            assert!(decode_response(&response(status, body), &request).is_err());
        }
    }

    #[test]
    fn delivery_is_bound_to_the_exact_receipt_reference_and_phase() {
        let selector = BatchSelector {
            stream: Uuid::new_v4(),
            ordinal: 1,
            batch: Uuid::new_v4(),
        };
        let receipt = BatchReceipt {
            selector,
            sha256: "12".repeat(32),
        };
        let reference = issue().reference().to_owned();
        let delivery = BatchRequest::Delivered {
            reference: reference.clone(),
            receipt: receipt.clone(),
        };
        let actor = GraphBinding {
            grant: Uuid::new_v4(),
            launch: Uuid::new_v4(),
            revision: 1,
        };
        let mut reply = BatchReply {
            version: 1,
            consumer: actor.clone(),
            issuer: actor,
            reference,
            outcome: BatchOutcome::Delivered {
                receipt: receipt.clone(),
            },
        };
        assert!(reply.validate(&delivery).is_ok());
        assert!(reply.validate(&issue()).is_err());
        let mut other = receipt;
        other.selector.batch = Uuid::new_v4();
        reply.outcome = BatchOutcome::Delivered { receipt: other };
        assert!(reply.validate(&delivery).is_err());
        reply.outcome = BatchOutcome::Unknown;
        reply.reference.push('x');
        assert!(reply.validate(&issue()).is_err());
    }

    #[test]
    fn issue_rejects_an_invalid_digest_or_extra_commit_claim() {
        let mut value = serde_json::to_value(issue()).unwrap();
        value["request_sha256"] = json!("invalid");
        assert!(
            serde_json::from_value::<BatchRequest>(value.clone())
                .unwrap()
                .validate()
                .is_err()
        );
        value["committed"] = json!(true);
        assert!(serde_json::from_value::<BatchRequest>(value).is_err());
    }
}
