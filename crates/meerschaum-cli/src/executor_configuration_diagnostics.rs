//! Bounded control refusal diagnostics. Never reflect an executor's arbitrary response.

use meerschaum_connector_api::client::ApiResponse;
use std::time::Duration;

fn detail(response: &ApiResponse) -> &'static str {
    if response.bytes.len() > 4096 {
        return "Executor response exceeds the diagnostic bound; body omitted.";
    }
    let Ok(body) = response.json() else {
        return "Executor response is not typed Gateway JSON; body omitted.";
    };
    // Only exact messages produced by our Gateway may enter logs. A different server
    // could put secrets, credentials or configuration into any JSON/text field.
    match body.get("detail").and_then(serde_json::Value::as_str) {
        Some("This executor generation has been revoked.") => {
            "This executor generation has been revoked."
        }
        Some("Executor configuration timed out; restart this executor generation.") => {
            "Executor configuration timed out; restart this executor generation."
        }
        Some("The executor configuration could not be applied.") => {
            "The executor configuration could not be applied."
        }
        Some("The configuration revision does not match.") => {
            "The configuration revision does not match."
        }
        Some("The configuration names another executor.") => {
            "The configuration names another executor."
        }
        Some("Invalid executor configuration.") => "Invalid executor configuration.",
        Some("Invalid plugin catalog digest.") => "Invalid plugin catalog digest.",
        Some("Invalid executor team name.") => "Invalid executor team name.",
        Some("Invalid executor plugin grants.") => "Invalid executor plugin grants.",
        Some("The executor's request capacity is exhausted; retry shortly.") => {
            "The executor's request capacity is exhausted; retry shortly."
        }
        Some("Executor request body timed out.") => "Executor request body timed out.",
        Some("Executor request body exceeds its size limit.") => {
            "Executor request body exceeds its size limit."
        }
        Some("Invalid credentials") => "Invalid credentials",
        _ => "Executor response has an unrecognized Gateway detail; body omitted.",
    }
}

pub(super) fn refused(response: &ApiResponse, elapsed: Duration) {
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    tracing::warn!(
        target: "mrsm::executor_configuration",
        status = response.status,
        elapsed_ms,
        response_bytes = response.bytes.len(),
        gateway_detail = detail(response),
        "The selected executor refused its configuration."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    fn response(value: serde_json::Value) -> ApiResponse {
        ApiResponse {
            status: 503,
            content_type: "application/json".into(),
            bytes: serde_json::to_vec(&value).unwrap(),
        }
    }
    #[test]
    fn revoked_generation_and_construction_timeout_remain_distinguishable() {
        for message in [
            "This executor generation has been revoked.",
            "Executor configuration timed out; restart this executor generation.",
            "The executor configuration could not be applied.",
        ] {
            assert_eq!(
                detail(&response(serde_json::json!({"detail":message}))),
                message
            );
        }
    }
    #[test]
    fn arbitrary_nested_secret_html_or_oversized_error_bodies_never_enter_diagnostics() {
        for value in [
            serde_json::json!({"detail":"Bearer synthetic-secret"}),
            serde_json::json!({"detail":{"password":"synthetic-secret"}}),
            serde_json::json!({"detail":"<html>synthetic-secret</html>"}),
            serde_json::json!({"detail":"This executor generation has been revoked.","extra":"x".repeat(8192)}),
        ] {
            let output = detail(&response(value));
            assert!(output.contains("omitted"));
            assert!(!output.contains("synthetic-secret"));
        }
    }
}
