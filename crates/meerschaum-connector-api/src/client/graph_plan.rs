//! HTTP client for the graph plan protocol.
use super::{APIConnector, Body, RequestOptions};
#[cfg(test)]
use crate::graph_plan::GraphBinding;
use crate::graph_plan::{
    GRAPH_PLAN_PATH, GraphInstruction, GraphPlanReply, GraphPlanRequest, MAX_GRAPH_PLAN_BYTES,
    WINDOW_PLANNING_TIMEOUT,
};
use crate::{ConnectorError, ConnectorResult};
use std::time::Duration;
#[cfg(test)]
use uuid::Uuid;

fn decode_response(
    response: &super::ApiResponse,
    request: &GraphPlanRequest,
) -> ConnectorResult<Option<GraphPlanReply>> {
    if response.status != 200 {
        return Err(ConnectorError::Http {
            status: response.status,
            body: "Main refused its canonical graph instructions; local fallback is unavailable."
                .into(),
        });
    }
    // envelope_field intentionally tolerates older APIs/missing fields. This
    // security boundary requires an explicit current success AND explicit None.
    let envelope = response.envelope()?;
    if envelope.get("success").and_then(serde_json::Value::as_bool) != Some(true)
        || !envelope
            .get("message")
            .is_some_and(serde_json::Value::is_string)
    {
        return Err(ConnectorError::Backend(
            "Main returned an invalid graph instruction envelope.".into(),
        ));
    }
    let instruction = envelope.get("instruction").ok_or_else(|| {
        ConnectorError::Backend(
            "Main omitted graph provenance; local fallback is unavailable.".into(),
        )
    })?;
    if instruction.is_null() {
        return Ok(None);
    }
    let reply: GraphPlanReply = serde_json::from_value(instruction.clone())?;
    reply.validate()?;
    match (request, &reply.instruction) {
        (GraphPlanRequest::Dispatch, GraphInstruction::Dispatch { argv_sha256, .. })
            if argv_sha256.len() == 64 && argv_sha256.bytes().all(|c| c.is_ascii_hexdigit()) =>
        {
            Ok(Some(reply))
        }
        (GraphPlanRequest::Pipeline { .. }, GraphInstruction::Pipeline { definition, roots })
            if definition.is_object() && !roots.is_empty() && roots.len() <= 256 =>
        {
            Ok(Some(reply))
        }
        (GraphPlanRequest::Initial, GraphInstruction::Initial(_))
        | (GraphPlanRequest::Ready { .. }, GraphInstruction::Ready(_)) => Ok(Some(reply)),
        (GraphPlanRequest::Verify { .. }, GraphInstruction::Verify(_)) => Ok(Some(reply)),
        _ => Err(ConnectorError::Backend(
            "Main returned a different graph instruction phase.".into(),
        )),
    }
}
impl APIConnector {
    /// Uses the existing strict rotating execution credential/TLS client. Source
    /// errors, malformed responses and absent routes never become no-graph.
    pub async fn graph_plan(
        &self,
        request: &GraphPlanRequest,
    ) -> ConnectorResult<Option<GraphPlanReply>> {
        request.validate()?;
        let mut bounded = self.clone();
        bounded.response_limit = Some(self.response_limit.map_or(MAX_GRAPH_PLAN_BYTES, |limit| {
            limit.min(MAX_GRAPH_PLAN_BYTES)
        }));
        let transport_budget = Duration::from_secs(10);
        let timeout = match request {
            GraphPlanRequest::Ready { .. } => WINDOW_PLANNING_TIMEOUT + transport_budget,
            _ => transport_budget,
        };
        let response = bounded
            .post(
                GRAPH_PLAN_PATH,
                RequestOptions {
                    body: Body::Json(serde_json::to_string(request)?),
                    timeout: Some(timeout),
                    ..Default::default()
                },
            )
            .await?;
        decode_response(&response, request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn response(status: u16, body: serde_json::Value) -> super::super::ApiResponse {
        super::super::ApiResponse {
            status,
            content_type: "application/json".into(),
            bytes: serde_json::to_vec(&body).unwrap(),
        }
    }
    #[test]
    fn missing_malformed_or_failed_instructions_never_select_local_execution() {
        let request = GraphPlanRequest::Initial;
        for (status, body) in [
            (200, serde_json::json!({"success":true,"message":"missing"})),
            (200, serde_json::json!({"instruction":null})),
            (
                200,
                serde_json::json!({"success":false,"message":"failed","instruction":null}),
            ),
            (
                503,
                serde_json::json!({"success":true,"message":"unavailable","instruction":null}),
            ),
            (
                204,
                serde_json::json!({"success":true,"message":"wrong status","instruction":null}),
            ),
        ] {
            assert!(decode_response(&response(status, body), &request).is_err());
        }
        assert!(decode_response(&response(200,serde_json::json!({"success":true,"message":"Main verified no graph","instruction":null})),&request).unwrap().is_none());
        let reply = GraphPlanReply {
            version: 1,
            consumer: GraphBinding {
                grant: Uuid::new_v4(),
                launch: Uuid::new_v4(),
                revision: 1,
            },
            issuer: GraphBinding {
                grant: Uuid::new_v4(),
                launch: Uuid::new_v4(),
                revision: 1,
            },
            instruction: GraphInstruction::Ready(serde_json::json!({})),
        };
        assert!(
            decode_response(
                &response(
                    200,
                    serde_json::json!({"success":true,"message":"wrong phase","instruction":reply})
                ),
                &request
            )
            .is_err()
        );
    }
}
