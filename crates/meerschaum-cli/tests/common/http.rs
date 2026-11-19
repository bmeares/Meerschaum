//! Shared async HTTP client helpers (`reqwest`) for CLI integration tests.
#![allow(dead_code)]

use std::time::{Duration, Instant};

use serde_json::{Value as Json, json};

/// Execute a `reqwest::RequestBuilder` and return `(status_code, parsed_json_or_raw_string)`.
pub async fn response(request: reqwest::RequestBuilder) -> (u16, Json) {
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let body = response.text().await.unwrap();
    let value = serde_json::from_str(&body).unwrap_or_else(|_| json!(body));
    (status, value)
}

/// Log in via `/login` form credentials and return the access token string.
pub async fn login(
    client: &reqwest::Client,
    origin: &str,
    username: &str,
    password: &str,
) -> String {
    let (status, body) = response(
        client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("username={username}&password={password}")),
    )
    .await;
    assert_eq!(status, 200, "login {username}: {body}");
    body["access_token"].as_str().unwrap().to_string()
}

/// Wait until `GET {origin}/healthcheck` returns 200 or `timeout` elapses.
pub async fn wait_healthy(client: &reqwest::Client, origin: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(resp) = client.get(format!("{origin}/healthcheck")).send().await
            && resp.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
