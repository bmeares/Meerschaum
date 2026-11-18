//! Native source payload for the real private Metadata / Api extension acceptance.
//! The owning test supplies a system/owned toolchain and private dependency cache; this file
//! must be compiled by the confined executor, never imported or loaded by Main.

fn native_main_is_masked(value: &Value) -> bool {
    value.is_null()
        || value.as_object().is_some_and(|attributes| {
            attributes.len() == 1 && attributes.contains_key("mrsm_api_alias")
        })
}

#[api_route(POST, "/native-extension-probe/{item}", scopes = ["pipes:read", "users:read"])]
fn native_item(request: &RouteRequest) -> Result<Value> {
    let denied = request
        .query_param("main_sentinel")
        .map(|path| {
            std::fs::read(path)
                .is_err_and(|error| matches!(error.raw_os_error(), Some(1) | Some(13)))
        })
        .unwrap_or(false);
    let config = host::config(&["meerschaum", "connectors"])?;
    Ok(json!({
        "item": request.path_param("item"),
        "api": std::env::var("MRSM_API_EXECUTION").as_deref() == Ok("1"),
        "main_denied": denied,
        "main_sql_absent": config.get("sql").and_then(|sql| sql.get("main")).is_none_or(native_main_is_masked),
        "parent_env_absent": std::env::var_os("AWS_SECRET_ACCESS_KEY").is_none()
            && std::env::var_os("DB_PASSWORD").is_none(),
        "credential_headers_absent": request.header("authorization").is_none()
            && request.header("cookie").is_none(),
        "principal": request.principal,
        "scopes": request.scopes,
    }))
}

#[mcp_tool(description = "Confined native tool.", scopes = ["pipes:read"], read_only = true,
    input_schema = r#"{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}"#)]
fn native_extension_tool(call: &ToolCall) -> Value {
    json!({"text": call.arguments["text"], "principal": call.principal,
        "api": std::env::var("MRSM_API_EXECUTION").as_deref() == Ok("1")})
}

#[mcp_resource(uri = "mrsm://native-extension/resource", description = "Confined native text.",
    mime_type = "text/plain", scopes = ["pipes:read"])]
fn native_extension_resource() -> ResourceContents {
    ResourceContents::Text {
        text: "native confined resource".into(),
        mime_type: Some("text/plain".into()),
    }
}

#[mcp_prompt(description = "Confined native prompt.", arguments = ["topic"], scopes = ["pipes:read"])]
fn native_extension_prompt(prompt: &PromptGet) -> String {
    format!(
        "Native topic: {}",
        prompt.argument("topic").unwrap_or_default()
    )
}

#[web_page("native-extension-page", host_scopes = ["pipes:read"])]
fn native_extension_page() -> Result<String> {
    let main = host::config(&["meerschaum", "connectors", "sql", "main"])?;
    Ok(format!(
        "<p id=\"native-extension-page\">Private native page; Main SQL absent: {}</p>",
        native_main_is_masked(&main)
    ))
}
