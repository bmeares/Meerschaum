//! The generated adapters that call the user's functions: argument conversion and result
//! wrapping for plugin functions, instance methods, streaming acknowledgements and executors.

use super::*;

/// Every adapter is compiled in the same module as the user's private function.
pub(super) fn adapter(file: &Path, function: &ItemFn, kind: Kind, id: &str) -> Result<String> {
    let sig = &function.sig;
    let name = sig.ident.unraw().to_string();
    if kind == Kind::Setup && matches!(sig.output, syn::ReturnType::Default) {
        // PARITY: Python's `Plugin.setup` fails a setup() that returns None.
        return Err(diagnostic(
            file,
            sig.span(),
            "setup must return bool, (bool, String), SuccessTuple, or a Result of those",
        ));
    }
    if kind == Kind::Subscribe && matches!(sig.output, syn::ReturnType::Default) {
        return Err(diagnostic(
            file,
            sig.span(),
            "subscribe must return a closure FnMut(Duration) -> Batch (or a Subscription), optionally in a Result",
        ));
    }
    check_signature(file, function)?;
    let mut args = Vec::new();
    let mut setup = Vec::new();
    let mut needs_pipe = false;
    let mut needs_connector = false;
    for (param, arg) in parameters(file, function)? {
        let param = param.as_str();
        let (expected, valid, value) = match param {
            "pipe"
                if kind.data() || kind.hook() || kind.instance() || kind == Kind::CustomMethod =>
            {
                needs_pipe = true;
                (
                    "&Pipe",
                    reference_type(&arg.ty, "Pipe"),
                    "&__mrsm_pipe".to_owned(),
                )
            }
            "connector" if kind.data() || kind.instance() || kind == Kind::CustomMethod => {
                needs_connector = true;
                (
                    "&Connector",
                    reference_type(&arg.ty, "Connector"),
                    "&__mrsm_connector".to_owned(),
                )
            }
            "begin" | "end" if kind == Kind::Fetch || kind == Kind::Sync => (
                "Option<Bound>",
                generic_type(&arg.ty, "Option", "Bound"),
                format!("__mrsm_request.bound({param:?})?"),
            ),
            "data" if kind == Kind::Sync => (
                "Option<Frame>",
                generic_type(&arg.ty, "Option", "Frame"),
                "__mrsm_data".to_owned(),
            ),
            "action" if kind == Kind::Action => (
                "Vec<String>",
                generic_type(&arg.ty, "Vec", "String"),
                "__mrsm_request.action()?".to_owned(),
            ),
            "success_tuple" if kind == Kind::PostHook => (
                "SuccessTuple",
                plain_type(&arg.ty, "SuccessTuple"),
                "__mrsm_request.success_tuple()?".to_owned(),
            ),
            "sync_duration" if kind == Kind::PostHook => (
                "f64",
                plain_type(&arg.ty, "f64"),
                "__mrsm_request.sync_duration()".to_owned(),
            ),
            "kwargs" if !kind.server() => (
                "&Value",
                reference_type(&arg.ty, "Value"),
                "__mrsm_request.kwargs()".to_owned(),
            ),
            "request" if kind == Kind::Route => {
                setup.push("let __mrsm_route = meerschaum_plugin::api::RouteRequest::from_context(&__mrsm_request.context)?;");
                (
                    "&RouteRequest",
                    reference_type(&arg.ty, "RouteRequest"),
                    "&__mrsm_route".to_owned(),
                )
            }
            "call" if kind == Kind::Tool => {
                setup.push("let __mrsm_call = meerschaum_plugin::mcp::ToolCall::from_context(&__mrsm_request.context);");
                (
                    "&ToolCall",
                    reference_type(&arg.ty, "ToolCall"),
                    "&__mrsm_call".to_owned(),
                )
            }
            "resource" if kind == Kind::Resource => {
                setup.push("let __mrsm_read = meerschaum_plugin::mcp::ResourceRead::from_context(&__mrsm_request.context);");
                (
                    "&ResourceRead",
                    reference_type(&arg.ty, "ResourceRead"),
                    "&__mrsm_read".to_owned(),
                )
            }
            "prompt" if kind == Kind::Prompt => {
                setup.push("let __mrsm_prompt = meerschaum_plugin::mcp::PromptGet::from_context(&__mrsm_request.context);");
                (
                    "&PromptGet",
                    reference_type(&arg.ty, "PromptGet"),
                    "&__mrsm_prompt".to_owned(),
                )
            }
            "page" if kind == Kind::Page => {
                setup.push("let __mrsm_page = meerschaum_plugin::page::PageRequest::from_context(&__mrsm_request.context);");
                (
                    "&PageRequest",
                    reference_type(&arg.ty, "PageRequest"),
                    "&__mrsm_page".to_owned(),
                )
            }
            "data" if kind.instance() => {
                if generic_type(&arg.ty, "Vec", "Value") {
                    (
                        "Vec<Value>",
                        true,
                        "__mrsm_data.map(meerschaum_plugin::Frame::into_rows).unwrap_or_default()"
                            .to_owned(),
                    )
                } else {
                    (
                        "Option<Frame> or Vec<Value>",
                        generic_type(&arg.ty, "Option", "Frame"),
                        "__mrsm_data".to_owned(),
                    )
                }
            }
            "context" if kind.instance() || kind == Kind::CustomMethod => (
                "&Value",
                reference_type(&arg.ty, "Value"),
                "&__mrsm_request.context".to_owned(),
            ),
            field if kind.instance() && INSTANCE_FIELDS.contains(&field) => {
                let value = if generic_type(&arg.ty, "Option", "Bound") {
                    format!("__mrsm_request.bound({field:?})?")
                } else if reference_type(&arg.ty, "Value") {
                    format!("__mrsm_request.field({field:?})")
                } else if reference_type(&arg.ty, "str") {
                    format!("__mrsm_request.field({field:?}).as_str().unwrap_or_default()")
                } else {
                    format!(
                        "meerschaum_plugin::serde_json::from_value(__mrsm_request.field({field:?}).clone()).map_err(|e| meerschaum_plugin::Error::Message(format!(\"{field}: {{e}}\")))?"
                    )
                };
                (
                    "&Value, &str or an owned type such as bool, f64, Option<i64> or Option<Bound>",
                    !matches!(arg.ty.as_ref(), Type::Reference(_))
                        || reference_type(&arg.ty, "Value")
                        || reference_type(&arg.ty, "str"),
                    value,
                )
            }
            "arguments" if kind == Kind::Tool || kind == Kind::Prompt => {
                setup.push("let __mrsm_arguments = match &__mrsm_request.context[\"arguments\"] { meerschaum_plugin::serde_json::Value::Null => meerschaum_plugin::serde_json::json!({}), other => other.clone() };");
                (
                    "&Value",
                    reference_type(&arg.ty, "Value"),
                    "&__mrsm_arguments".to_owned(),
                )
            }
            _ => {
                return Err(diagnostic(
                    file,
                    arg.span(),
                    format!(
                        "unsupported {name} parameter '{param}'; {}",
                        kind.parameters()
                    ),
                ));
            }
        };
        if !valid {
            return Err(diagnostic(
                file,
                arg.ty.span(),
                format!("{name} parameter '{param}' must have type {expected}"),
            ));
        }
        args.push(value);
    }
    if needs_pipe {
        setup.insert(0, "let __mrsm_pipe = __mrsm_request.pipe()?;");
    }
    if needs_connector {
        setup.push("let __mrsm_connector = __mrsm_request.connector();");
    }
    let setup = setup.join(" ");
    let call = format!("{}({})", sig.ident, args.join(", "));
    let output = match kind {
        Kind::Fetch => format!(
            "meerschaum_plugin::sdk::FetchOutput::into_frame({call}).map(meerschaum_plugin::Reply::data)"
        ),
        Kind::Sync => format!(
            "meerschaum_plugin::sdk::SyncOutput::into_success({call}).map(meerschaum_plugin::Reply::json)"
        ),
        // PARITY: a hook's non-tuple return is `(True, "Success")`, as for actions.
        Kind::Action | Kind::PreHook | Kind::PostHook => format!(
            "meerschaum_plugin::sdk::ActionOutput::into_action({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Setup => format!(
            "meerschaum_plugin::sdk::SetupOutput::into_setup({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Register => format!(
            "meerschaum_plugin::sdk::RegisterOutput::into_parameters({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Subscribe => {
            let q = if returns_result(sig) { "?" } else { "" };
            format!(
                "let __mrsm_subscription: Box<dyn meerschaum_plugin::subscribe::Subscription> = Box::new({call}{q}); Ok(__mrsm_subscription)"
            )
        }
        Kind::Route => format!(
            "meerschaum_plugin::api::IntoRouteResponse::into_route_response({call}).map(|r| meerschaum_plugin::Reply::json(r.to_json()))"
        ),
        Kind::Tool => format!(
            "meerschaum_plugin::mcp::IntoToolResult::into_tool_result({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Resource => format!(
            "meerschaum_plugin::mcp::IntoResourceContents::into_resource_contents({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Prompt => format!(
            "meerschaum_plugin::mcp::IntoPromptReply::into_prompt_reply({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Page => format!(
            "meerschaum_plugin::page::IntoPage::into_page({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::InstanceSuccess => format!(
            "meerschaum_plugin::sdk::SyncOutput::into_success({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::InstanceData => format!(
            "meerschaum_plugin::sdk::FetchOutput::into_frame({call}).map(meerschaum_plugin::Reply::data)"
        ),
        Kind::InstanceValue | Kind::CustomMethod => {
            let q = if returns_result(sig) { "?" } else { "" };
            format!(
                "let __mrsm_value = {call}{q}; meerschaum_plugin::serde_json::to_value(__mrsm_value).map(meerschaum_plugin::Reply::json).map_err(|e| meerschaum_plugin::Error::Message(e.to_string()))"
            )
        }
    };
    let returns = if kind == Kind::Subscribe {
        "Box<dyn meerschaum_plugin::subscribe::Subscription>"
    } else {
        "meerschaum_plugin::Reply"
    };
    Ok(format!(
        "pub(super) fn {id}(__mrsm_request: &meerschaum_plugin::Request, __mrsm_data: Option<meerschaum_plugin::Frame>) -> meerschaum_plugin::Result<{returns}> {{ {setup} {output} }}"
    ))
}

/// A streaming connector's `fn ack(tokens: &[u64])` (optionally `connector: &Connector`).
pub(super) fn ack_adapter(file: &Path, function: &ItemFn) -> Result<String> {
    check_signature(file, function)?;
    let mut args = Vec::new();
    let mut connector = "";
    for (param, arg) in parameters(file, function)? {
        let (expected, valid, value) = match param.as_str() {
            "tokens" => (
                "&[u64]",
                matches!(arg.ty.as_ref(), Type::Reference(r) if r.mutability.is_none()
                    && matches!(r.elem.as_ref(), Type::Slice(s) if plain_type(&s.elem, "u64"))),
                "__mrsm_tokens",
            ),
            "connector" => {
                connector = "let __mrsm_connector = __mrsm_request.connector();";
                (
                    "&Connector",
                    reference_type(&arg.ty, "Connector"),
                    "&__mrsm_connector",
                )
            }
            other => {
                return Err(diagnostic(
                    file,
                    arg.span(),
                    format!(
                        "unsupported ack parameter '{other}'; ack accepts tokens: &[u64] and connector: &Connector"
                    ),
                ));
            }
        };
        if !valid {
            return Err(diagnostic(
                file,
                arg.ty.span(),
                format!("ack parameter '{param}' must have type {expected}"),
            ));
        }
        args.push(value);
    }
    Ok(format!(
        "pub(super) fn __mrsm_ack(__mrsm_request: &meerschaum_plugin::Request, __mrsm_tokens: &[u64]) -> meerschaum_plugin::Result<()> {{ {connector} meerschaum_plugin::subscribe::AckOutput::into_ack({}({})) }}",
        function.sig.ident,
        args.join(", ")
    ))
}

/// What an executor method returns.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Returns {
    Names,
    Bool,
    Json,
    Text,
    OptionalText,
    Success,
    Unit,
    Logs,
}

/// The executor methods (`meerschaum/jobs/_Executor.py`), the parameters each accepts besides
/// `executor: &ExecutorInfo`, and what it returns.
pub(super) const EXECUTOR_METHODS: &[(&str, &[&str], Returns)] = &[
    ("get_job_names", &[], Returns::Names),
    ("get_job_exists", &["name"], Returns::Bool),
    ("get_jobs", &[], Returns::Json),
    ("get_job_metadata", &["name"], Returns::Json),
    ("get_job_properties", &["name"], Returns::Json),
    ("get_job_status", &["name"], Returns::Text),
    ("get_job_began", &["name"], Returns::OptionalText),
    ("get_job_ended", &["name"], Returns::OptionalText),
    ("get_job_paused", &["name"], Returns::OptionalText),
    (
        "create_job",
        &["name", "sysargs", "properties"],
        Returns::Success,
    ),
    ("start_job", &["name"], Returns::Success),
    ("stop_job", &["name"], Returns::Success),
    ("pause_job", &["name"], Returns::Success),
    ("delete_job", &["name"], Returns::Success),
    ("get_logs", &["name"], Returns::OptionalText),
    ("get_job_stop_time", &["name"], Returns::OptionalText),
    ("get_job_is_blocking_on_stdin", &["name"], Returns::Bool),
    ("get_job_prompt_kwargs", &["name"], Returns::Json),
    ("write_stdin", &["name", "data"], Returns::Unit),
    (
        "monitor_logs",
        &["name", "strip_timestamps", "stop_on_exit"],
        Returns::Logs,
    ),
];

/// One executor method: `fn <method>(executor: &ExecutorInfo, name: &str, ...) -> ...`.
pub(super) fn executor_adapter(file: &Path, function: &ItemFn, id: &str) -> Result<String> {
    check_signature(file, function)?;
    let sig = &function.sig;
    let method = sig.ident.unraw().to_string();
    let (_, accepted, returns) = EXECUTOR_METHODS
        .iter()
        .find(|(m, _, _)| *m == method)
        .copied()
        .ok_or_else(|| error(format!("unknown executor method {method}")))?;
    let mut args = Vec::new();
    let mut setup = Vec::new();
    for (param, arg) in parameters(file, function)? {
        let ty = arg.ty.as_ref();
        let (expected, valid, value) = match param.as_str() {
            "executor" => (
                "&ExecutorInfo",
                reference_type(ty, "ExecutorInfo"),
                "__mrsm_executor".to_owned(),
            ),
            "name" if accepted.contains(&"name") => (
                "&str",
                reference_type(ty, "str"),
                "meerschaum_plugin::executor::name_of(__mrsm_context)?".to_owned(),
            ),
            "sysargs" if accepted.contains(&"sysargs") => {
                setup.push("let __mrsm_sysargs: Vec<String> = meerschaum_plugin::serde_json::from_value(__mrsm_context[\"sysargs\"].clone()).map_err(|e| meerschaum_plugin::Error::Message(e.to_string()))?;");
                let slice = matches!(ty, Type::Reference(r) if r.mutability.is_none()
                    && (matches!(r.elem.as_ref(), Type::Slice(s) if plain_type(&s.elem, "String"))
                        || generic_type(&r.elem, "Vec", "String")));
                (
                    "Vec<String> or &[String]",
                    slice || generic_type(ty, "Vec", "String"),
                    if slice { "&__mrsm_sysargs" } else { "__mrsm_sysargs" }.to_owned(),
                )
            }
            "properties" if accepted.contains(&"properties") => {
                setup.push("let __mrsm_properties = match &__mrsm_context[\"properties\"] { meerschaum_plugin::serde_json::Value::Null => meerschaum_plugin::serde_json::json!({}), other => other.clone() };");
                let by_ref = reference_type(ty, "Value");
                (
                    "Value or &Value",
                    by_ref || plain_type(ty, "Value"),
                    if by_ref {
                        "&__mrsm_properties"
                    } else {
                        "__mrsm_properties"
                    }
                    .to_owned(),
                )
            }
            "data" if accepted.contains(&"data") => (
                "&str",
                reference_type(ty, "str"),
                "__mrsm_context[\"data\"].as_str().ok_or_else(|| meerschaum_plugin::Error::Message(\"missing stdin data\".into()))?".to_owned(),
            ),
            flag @ ("strip_timestamps" | "stop_on_exit") if accepted.contains(&flag) => (
                "bool",
                plain_type(ty, "bool"),
                format!("__mrsm_context[{flag:?}].as_bool().unwrap_or(false)"),
            ),
            other => {
                let mut names = vec!["executor: &ExecutorInfo"];
                for name in accepted {
                    names.push(match *name {
                        "name" => "name: &str",
                        "sysargs" => "sysargs: Vec<String>",
                        "properties" => "properties: &Value",
                        "data" => "data: &str",
                        "strip_timestamps" => "strip_timestamps: bool",
                        _ => "stop_on_exit: bool",
                    });
                }
                return Err(diagnostic(
                    file,
                    arg.span(),
                    format!(
                        "unsupported {method} parameter '{other}'; {method} accepts {}",
                        names.join(", ")
                    ),
                ));
            }
        };
        if !valid {
            return Err(diagnostic(
                file,
                arg.ty.span(),
                format!("{method} parameter '{param}' must have type {expected}"),
            ));
        }
        args.push(value);
    }
    let call = format!("{}({})", sig.ident, args.join(", "));
    let q = if returns_result(sig) { "?" } else { "" };
    let reply = "meerschaum_plugin::executor::ExecutorReply";
    let json = "meerschaum_plugin::serde_json::json!(__mrsm_value)";
    let body = match returns {
        Returns::Names => {
            format!("let __mrsm_value: Vec<String> = {call}{q}; Ok({reply}::Value({json}))")
        }
        Returns::Bool => format!("let __mrsm_value: bool = {call}{q}; Ok({reply}::Value({json}))"),
        Returns::Json => format!(
            "let __mrsm_value: meerschaum_plugin::serde_json::Value = {call}{q}; Ok({reply}::Value(__mrsm_value))"
        ),
        Returns::Text => format!(
            "let __mrsm_value: String = String::from({call}{q}); Ok({reply}::Value({json}))"
        ),
        Returns::OptionalText => {
            format!("let __mrsm_value: Option<String> = {call}{q}; Ok({reply}::Value({json}))")
        }
        Returns::Success => {
            format!("meerschaum_plugin::sdk::SyncOutput::into_success({call}).map({reply}::Value)")
        }
        Returns::Unit => format!(
            "let () = {call}{q}; Ok({reply}::Value(meerschaum_plugin::serde_json::Value::Null))"
        ),
        Returns::Logs => format!(
            "let __mrsm_logs: Box<dyn meerschaum_plugin::executor::LogStream> = Box::new({call}{q}); Ok({reply}::Logs(__mrsm_logs))"
        ),
    };
    let setup = setup.join(" ");
    Ok(format!(
        "#[allow(unused_variables)] fn {id}(__mrsm_executor: &meerschaum_plugin::executor::ExecutorInfo, __mrsm_context: &meerschaum_plugin::serde_json::Value) -> meerschaum_plugin::Result<{reply}> {{ {setup} {body} }}"
    ))
}
