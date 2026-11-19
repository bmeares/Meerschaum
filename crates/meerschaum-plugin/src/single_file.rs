//! Generate a lightweight crate from beginner-facing Rust plugin shorthand.
//! Source edits preserve the user's line numbers; manifests and dispatch are generated.
use crate::{Result, error};
use proc_macro2::Span;
use quote::ToTokens;
use serde_json::{Value, json};
use std::{collections::HashSet, ops::Range, path::Path};
use syn::{
    Attribute, Expr, Item, ItemFn, ItemMod, Lit, Type, ext::IdentExt, parse::ParseStream,
    spanned::Spanned,
};

#[path = "single_file_markers.rs"]
mod markers;
use markers::{
    bool_option, host_scopes_option, plugin_argument, prompt_entry, reject_misplaced_marker,
    resource_entry, route_entry, server_items, server_markers, string_literal, text_option,
    tool_entry, unknown_option,
};
struct Edit(Range<usize>, String);

fn diagnostic(file: &Path, span: Span, message: impl std::fmt::Display) -> crate::Error {
    let start = span.start();
    error(format!(
        "{}:{}:{}: {message}",
        file.display(),
        start.line,
        start.column + 1
    ))
}

fn marker<'a>(attrs: &'a [Attribute], name: &str) -> Option<&'a Attribute> {
    attrs.iter().find(|attr| attr.path().is_ident(name))
}

fn docs(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .filter_map(|attr| {
            if let syn::Meta::NameValue(meta) = &attr.meta
                && let syn::Expr::Lit(expr) = &meta.value
                && let syn::Lit::Str(text) = &expr.lit
            {
                return Some(text.value().trim().to_owned());
            }
            None
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The module and action markers that take `(name = "...")` options.
#[derive(Clone, Copy, PartialEq)]
enum Marker {
    Action,
    Connector,
    Executor,
}

/// The only options belong to the shorthand, not the generated Cargo project. The flag is an
/// action's `daemon` (default true) or a connector's `instance` (default false).
fn options(file: &Path, attr: &Attribute, marker: Marker, default: &str) -> Result<(String, bool)> {
    let action = marker == Marker::Action;
    let mut name = default.to_owned();
    let mut daemon = action;
    let mut seen = HashSet::new();
    if matches!(attr.meta, syn::Meta::List(_)) {
        attr.parse_nested_meta(|meta| {
            let key = meta.path.to_token_stream().to_string();
            if !seen.insert(key.clone()) {
                return Err(meta.error(format!("duplicate option '{key}'")));
            }
            if meta.path.is_ident("name") {
                name = meta.value()?.parse::<syn::LitStr>()?.value();
                Ok(())
            } else if action && meta.path.is_ident("daemon") {
                daemon = meta.value()?.parse::<syn::LitBool>()?.value;
                Ok(())
            } else if marker == Marker::Connector && meta.path.is_ident("instance") {
                // `instance` alone or `instance = true`, as Python's `IS_INSTANCE = True`.
                daemon = if meta.input.peek(syn::Token![=]) {
                    meta.value()?.parse::<syn::LitBool>()?.value
                } else {
                    true
                };
                Ok(())
            } else {
                Err(meta.error(match marker {
                    Marker::Action => "expected name or daemon",
                    Marker::Connector => "expected name or instance",
                    Marker::Executor => "expected name",
                }))
            }
        })
        .map_err(|e| diagnostic(file, e.span(), e))?;
    } else if !matches!(attr.meta, syn::Meta::Path(_)) {
        return Err(diagnostic(
            file,
            attr.span(),
            "use #[make_action], #[make_connector] or #[make_executor] with optional (...) options",
        ));
    }
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(diagnostic(
            file,
            attr.span(),
            "names must contain only letters, digits, or underscores",
        ));
    }
    if marker == Marker::Connector && ["plugin", "sql", "api", "valkey"].contains(&name.as_str()) {
        return Err(diagnostic(
            file,
            attr.span(),
            format!("connector type '{name}' is reserved"),
        ));
    }
    if marker == Marker::Executor
        && ["local", "systemd", "api", "none"].contains(&name.to_lowercase().as_str())
    {
        return Err(diagnostic(
            file,
            attr.span(),
            format!("executor type '{name}' is reserved"),
        ));
    }
    Ok((name, daemon))
}

fn clear_attribute(source: &str, attr: &Attribute, edits: &mut Vec<Edit>) {
    let range = attr.span().byte_range();
    // Keep newlines and byte offsets intact for compiler diagnostics.
    let blank = source[range.clone()]
        .bytes()
        .map(|c| if c == b'\n' { '\n' } else { ' ' })
        .collect();
    edits.push(Edit(range, blank));
}

fn plain_type(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.segments.last().is_some_and(|s| s.ident == name && matches!(s.arguments, syn::PathArguments::None)))
}
fn reference_type(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Reference(r) if r.mutability.is_none() && plain_type(&r.elem, name))
}
fn generic_type(ty: &Type, outer: &str, inner: &str) -> bool {
    let Type::Path(path) = ty else { return false };
    let Some(segment) = path.path.segments.last() else {
        return false;
    };
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return false;
    };
    segment.ident == outer
        && args.args.len() == 1
        && matches!(args.args.first(), Some(syn::GenericArgument::Type(t)) if plain_type(t, inner))
}

/// Whether a function returns a `Result` (its adapter then applies `?`).
fn returns_result(sig: &syn::Signature) -> bool {
    matches!(&sig.output, syn::ReturnType::Type(_, ty)
        if matches!(ty.as_ref(), Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Result")))
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Fetch,
    Sync,
    Register,
    Setup,
    Subscribe,
    Action,
    PreHook,
    PostHook,
    Route,
    Tool,
    Resource,
    Prompt,
    Page,
    /// Instance methods answering a `SuccessTuple` (`register_pipe`, `sync_pipe`, ...).
    InstanceSuccess,
    /// `get_pipe_data`.
    InstanceData,
    /// Instance methods answering a JSON value (`pipe_exists`, `fetch_pipes_keys`, ...).
    InstanceValue,
}

impl Kind {
    fn data(self) -> bool {
        matches!(
            self,
            Kind::Fetch | Kind::Sync | Kind::Register | Kind::Subscribe
        )
    }
    fn hook(self) -> bool {
        matches!(self, Kind::PreHook | Kind::PostHook)
    }
    /// API routes, MCP entries and console pages: no Pipe or kwargs.
    fn server(self) -> bool {
        matches!(
            self,
            Kind::Route | Kind::Tool | Kind::Resource | Kind::Prompt | Kind::Page
        )
    }
    fn instance(self) -> bool {
        matches!(
            self,
            Kind::InstanceSuccess | Kind::InstanceData | Kind::InstanceValue
        )
    }
    /// The parameters a function of this kind accepts, for diagnostics.
    fn parameters(self) -> &'static str {
        match self {
            Kind::Action => "actions accept action: Vec<String> and kwargs: &Value",
            Kind::Setup => "setup accepts kwargs: &Value",
            Kind::PreHook => "pre-sync hooks accept pipe: &Pipe and kwargs: &Value",
            Kind::PostHook => {
                "post-sync hooks accept pipe: &Pipe, success_tuple: SuccessTuple, sync_duration: f64, and kwargs: &Value"
            }
            Kind::Subscribe => {
                "subscribe accepts pipe: &Pipe, connector: &Connector and kwargs: &Value"
            }
            Kind::Route => "routes accept request: &RouteRequest",
            Kind::Tool => "tools accept call: &ToolCall and arguments: &Value",
            Kind::Resource => "resources accept resource: &ResourceRead",
            Kind::Prompt => "prompts accept prompt: &PromptGet and arguments: &Value",
            Kind::Page => "pages accept page: &PageRequest",
            Kind::InstanceSuccess | Kind::InstanceData | Kind::InstanceValue => {
                "instance methods accept pipe: &Pipe, connector: &Connector, data: Option<Frame> (or Vec<Value>), context: &Value, and the call's fields (query, options, params, begin, end, select_columns, omit_columns, limit, order, check_existing, chunksize, extra, newest, patch, no_policy, full, chunk_minutes, holder, ttl_seconds)"
            }
            _ => {
                "use pipe: &Pipe, connector: &Connector, kwargs: &Value, begin/end: Option<Bound> (fetch/sync), or data: Option<Frame> (sync)"
            }
        }
    }
}

/// The checks every generated entry point shares: a plain synchronous function.
fn check_signature(file: &Path, function: &ItemFn) -> Result<()> {
    let sig = &function.sig;
    let name = sig.ident.unraw().to_string();
    if sig.asyncness.is_some()
        || sig.unsafety.is_some()
        || sig.abi.is_some()
        || !sig.generics.params.is_empty()
        || sig.generics.where_clause.is_some()
        || sig.variadic.is_some()
    {
        return Err(diagnostic(
            file,
            sig.span(),
            format!("{name} must be a synchronous, safe, non-generic Rust function"),
        ));
    }
    if function
        .attrs
        .iter()
        .any(|a| a.path().is_ident("cfg") || a.path().is_ident("cfg_attr"))
    {
        return Err(diagnostic(
            file,
            sig.span(),
            format!("conditional {name} functions are unsupported"),
        ));
    }
    Ok(())
}

/// A typed, named parameter: `(name without leading underscores, the argument)`.
fn parameters<'a>(file: &Path, function: &'a ItemFn) -> Result<Vec<(String, &'a syn::PatType)>> {
    let name = function.sig.ident.unraw().to_string();
    let mut seen = HashSet::new();
    let mut found = Vec::new();
    for arg in &function.sig.inputs {
        let syn::FnArg::Typed(arg) = arg else {
            return Err(diagnostic(
                file,
                arg.span(),
                "plugin functions cannot have self parameters",
            ));
        };
        let syn::Pat::Ident(ident) = arg.pat.as_ref() else {
            return Err(diagnostic(
                file,
                arg.span(),
                "plugin parameters must have names (for example pipe: &Pipe)",
            ));
        };
        if ident.by_ref.is_some() || ident.subpat.is_some() {
            return Err(diagnostic(
                file,
                ident.span(),
                "use a simple parameter name",
            ));
        }
        let param = ident.ident.to_string().trim_start_matches('_').to_owned();
        if !seen.insert(param.clone()) {
            return Err(diagnostic(
                file,
                arg.span(),
                format!("duplicate {name} parameter '{param}'"),
            ));
        }
        found.push((param, arg));
    }
    Ok(found)
}

/// Every adapter is compiled in the same module as the user's private function.
fn adapter(file: &Path, function: &ItemFn, kind: Kind, id: &str) -> Result<String> {
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
            "pipe" if kind.data() || kind.hook() || kind.instance() => {
                needs_pipe = true;
                (
                    "&Pipe",
                    reference_type(&arg.ty, "Pipe"),
                    "&__mrsm_pipe".to_owned(),
                )
            }
            "connector" if kind.data() || kind.instance() => {
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
            "context" if kind.instance() => (
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
        Kind::InstanceValue => {
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

/// The plugin functions recognized by name at the top level.
fn function_kind(name: &str) -> Option<Kind> {
    match name {
        "fetch" => Some(Kind::Fetch),
        "sync" => Some(Kind::Sync),
        "register" => Some(Kind::Register),
        "setup" => Some(Kind::Setup),
        "subscribe" => Some(Kind::Subscribe),
        _ => None,
    }
}

/// The names a marked function (action, hook, route, MCP entry) may not take.
fn reserved(name: &str) -> bool {
    ["fetch", "sync", "register", "setup"].contains(&name)
}

/// The methods recognized by name inside a `#[make_connector]` module.
fn connector_kind(name: &str) -> Option<Kind> {
    function_kind(name).filter(|kind| kind.data())
}

/// The instance methods the host calls (`meerschaum_plugin::host`'s `InstanceConnector`), by
/// what they answer.
const INSTANCE_METHODS: &[(&str, Kind)] = &[
    ("register_pipe", Kind::InstanceSuccess),
    ("delete_pipe", Kind::InstanceSuccess),
    ("drop_pipe", Kind::InstanceSuccess),
    ("edit_pipe", Kind::InstanceSuccess),
    ("sync_pipe", Kind::InstanceSuccess),
    ("clear_pipe", Kind::InstanceSuccess),
    ("deduplicate_pipe", Kind::InstanceSuccess),
    ("drop_pipe_indices", Kind::InstanceSuccess),
    ("create_pipe_indices", Kind::InstanceSuccess),
    ("compress_pipe", Kind::InstanceSuccess),
    ("decompress_pipe", Kind::InstanceSuccess),
    ("analyze_pipe", Kind::InstanceSuccess),
    ("vacuum_pipe", Kind::InstanceSuccess),
    ("partition_pipe", Kind::InstanceSuccess),
    ("get_pipe_data", Kind::InstanceData),
    ("test_connection", Kind::InstanceValue),
    ("fetch_pipes_keys", Kind::InstanceValue),
    ("pipe_exists", Kind::InstanceValue),
    ("get_pipe_id", Kind::InstanceValue),
    ("get_pipe_attributes", Kind::InstanceValue),
    ("get_sync_time", Kind::InstanceValue),
    ("get_pipe_rowcount", Kind::InstanceValue),
    ("get_pipe_columns_types", Kind::InstanceValue),
    ("get_pipe_size", Kind::InstanceValue),
    ("get_pipe_index_names", Kind::InstanceValue),
    ("get_pipe_columns_indices", Kind::InstanceValue),
    ("acquire_pipe_lease", Kind::InstanceValue),
    ("release_pipe_lease", Kind::InstanceValue),
];

/// The fields of an instance call's context a parameter may name (the host's `query`, `options`
/// and per-method fields; `Request::field` also looks inside `query` and `options`).
const INSTANCE_FIELDS: &[&str] = &[
    "query",
    "options",
    "params",
    "begin",
    "end",
    "newest",
    "patch",
    "no_policy",
    "full",
    "chunk_minutes",
    "holder",
    "ttl_seconds",
    "check_existing",
    "chunksize",
    "extra",
    "select_columns",
    "omit_columns",
    "limit",
    "order",
];

fn instance_kind(name: &str) -> Option<Kind> {
    INSTANCE_METHODS
        .iter()
        .find(|(m, _)| *m == name)
        .map(|(_, kind)| *kind)
}

/// `#[pre_sync_hook]` / `#[post_sync_hook]` on a top-level function, in source order.
fn hook_markers(attrs: &[Attribute]) -> Vec<(&Attribute, Kind)> {
    attrs
        .iter()
        .filter_map(|attr| {
            if attr.path().is_ident("pre_sync_hook") {
                Some((attr, Kind::PreHook))
            } else if attr.path().is_ident("post_sync_hook") {
                Some((attr, Kind::PostHook))
            } else {
                None
            }
        })
        .collect()
}

/// Whether a `fn setup` / `fn subscribe` is the plugin's: `setup` takes nothing or only `kwargs`;
/// `subscribe` takes only `pipe`, `connector` and `kwargs` and returns a subscription
/// ([`subscription_return`]). Others are ordinary helpers, as before these functions existed.
fn plugin_signature(function: &ItemFn, kind: Kind, subscriptions: &HashSet<String>) -> bool {
    let allowed: &[&str] = match kind {
        Kind::Setup => &["kwargs"],
        Kind::Subscribe => &["pipe", "connector", "kwargs"],
        _ => return true,
    };
    (kind != Kind::Subscribe || subscription_return(&function.sig, subscriptions))
        && function.sig.inputs.iter().all(|arg| {
            matches!(arg, syn::FnArg::Typed(arg) if matches!(arg.pat.as_ref(),
                syn::Pat::Ident(ident) if allowed.contains(&ident.ident.to_string().trim_start_matches('_'))))
        })
}

/// Whether a `subscribe` returns a subscription, optionally in a `Result`: `impl FnMut(...)`,
/// `impl Subscription`, `Box<dyn Subscription>` (or `Box<dyn FnMut(...)>`), or a type the file
/// implements `Subscription` for.
fn subscription_return(sig: &syn::Signature, subscriptions: &HashSet<String>) -> bool {
    fn names_trait(
        bounds: &syn::punctuated::Punctuated<syn::TypeParamBound, syn::Token![+]>,
    ) -> bool {
        bounds.iter().any(|bound| {
            matches!(bound, syn::TypeParamBound::Trait(t)
                if t.path.segments.last().is_some_and(|s| s.ident == "FnMut" || s.ident == "Subscription"))
        })
    }
    fn single_argument(segment: &syn::PathSegment) -> Option<&Type> {
        match &segment.arguments {
            syn::PathArguments::AngleBracketed(args) if args.args.len() == 1 => {
                match args.args.first() {
                    Some(syn::GenericArgument::Type(ty)) => Some(ty),
                    _ => None,
                }
            }
            _ => None,
        }
    }
    let syn::ReturnType::Type(_, ty) = &sig.output else {
        return false;
    };
    let mut ty = ty.as_ref();
    if let Type::Path(path) = ty
        && let Some(segment) = path.path.segments.last()
        && segment.ident == "Result"
        && let Some(inner) = single_argument(segment)
    {
        ty = inner;
    }
    match ty {
        Type::ImplTrait(found) => names_trait(&found.bounds),
        Type::Path(path) => path.path.segments.last().is_some_and(|segment| {
            if segment.ident == "Box" {
                matches!(single_argument(segment), Some(Type::TraitObject(object)) if names_trait(&object.bounds))
            } else {
                subscriptions.contains(&segment.ident.to_string())
            }
        }),
        _ => false,
    }
}

/// The types a file implements `Subscription` for (`impl Subscription for Pulses`), anywhere.
fn subscription_types(items: &[Item], found: &mut HashSet<String>) {
    for item in items {
        match item {
            Item::Impl(block) => {
                if let Some((_, path, _)) = &block.trait_
                    && path
                        .segments
                        .last()
                        .is_some_and(|s| s.ident == "Subscription")
                    && let Type::Path(ty) = block.self_ty.as_ref()
                    && let Some(segment) = ty.path.segments.last()
                {
                    found.insert(segment.ident.to_string());
                }
            }
            Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    subscription_types(items, found);
                }
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct Generated {
    functions: Vec<String>,
    actions: Vec<Value>,
    connectors: serde_json::Map<String, Value>,
    arguments: Vec<Value>,
    pre_sync_hooks: Vec<String>,
    post_sync_hooks: Vec<String>,
    api_routes: Vec<Value>,
    mcp_tools: Vec<Value>,
    mcp_resources: Vec<Value>,
    mcp_prompts: Vec<Value>,
    web_pages: Vec<Value>,
    executors: serde_json::Map<String, Value>,
    /// The types the file implements `Subscription` for.
    subscriptions: HashSet<String>,
    adapters: String,
    dispatch: String,
    executor_dispatch: String,
    edits: Vec<Edit>,
}
impl Generated {
    /// `#[pre_sync_hook]` / `#[post_sync_hook]`: named after the function, like Python's hooks.
    fn hook(
        &mut self,
        file: &Path,
        source: &str,
        function: &ItemFn,
        attr: &Attribute,
        kind: Kind,
    ) -> Result<()> {
        let name = function.sig.ident.unraw().to_string();
        if reserved(&name) {
            return Err(diagnostic(
                file,
                attr.span(),
                "fetch, sync, register, and setup are reserved plugin functions; give your hook another function name",
            ));
        }
        clear_attribute(source, attr, &mut self.edits);
        let (method, hooks) = if kind == Kind::PreHook {
            ("pre_sync_hook", &mut self.pre_sync_hooks)
        } else {
            ("post_sync_hook", &mut self.post_sync_hooks)
        };
        let id = format!("__mrsm_{method}_{}", hooks.len());
        hooks.push(name.clone());
        self.adapters.push_str(&adapter(file, function, kind, &id)?);
        self.dispatch.push_str(&format!("if request.method == {method:?} && request.context[\"name\"] == {name:?} {{ return user::{id}(request, data); }}"));
        Ok(())
    }

    /// `#[add_plugin_argument(...)]` on a function or `#![add_plugin_argument(...)]` on the file.
    fn argument(&mut self, file: &Path, source: &str, attr: &Attribute) -> Result<()> {
        let argument = plugin_argument(file, attr)?;
        for option in argument["option_strings"].as_array().into_iter().flatten() {
            if self.arguments.iter().any(|a| {
                a["option_strings"]
                    .as_array()
                    .is_some_and(|o| o.contains(option))
            }) {
                return Err(diagnostic(
                    file,
                    attr.span(),
                    format!("duplicate plugin argument {option}"),
                ));
            }
        }
        if self.arguments.iter().any(|a| a["dest"] == argument["dest"]) {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("duplicate plugin argument dest {}", argument["dest"]),
            ));
        }
        clear_attribute(source, attr, &mut self.edits);
        self.arguments.push(argument);
        Ok(())
    }

    fn connector(
        &mut self,
        file: &Path,
        source: &str,
        plugin_name: &str,
        module: &ItemMod,
        attr: &Attribute,
    ) -> Result<()> {
        let (name, instance) = options(file, attr, Marker::Connector, &module.ident.to_string())?;
        if self.connectors.contains_key(&name) {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("duplicate connector type '{name}'"),
            ));
        }
        let (brace, items) = inline_module(file, module, "connector", "readings")?;
        clear_attribute(source, attr, &mut self.edits);
        let mut methods = Vec::new();
        let mut adapters = String::new();
        let mut dispatch = String::new();
        // A `subscribe` that takes other parameters or returns no subscription is a helper, as it
        // was before streaming connectors.
        let subscribe = items.iter().find_map(|item| match item {
            Item::Fn(f) if f.sig.ident == "subscribe" => Some(f),
            _ => None,
        });
        let streaming =
            subscribe.is_some_and(|f| plugin_signature(f, Kind::Subscribe, &self.subscriptions));
        let mut ack = "None".to_owned();
        for item in items {
            reject_misplaced_marker(file, item, false)?;
            let Item::Fn(function) = item else { continue };
            let method = function.sig.ident.to_string();
            // `ack` is the streaming connector's acknowledgement (a helper otherwise).
            if streaming && method == "ack" {
                adapters.push_str(&ack_adapter(file, function)?);
                let forward = format!("__mrsm_connector_{name}_ack");
                self.adapters.push_str(&format!("pub(super) fn {forward}(request: &meerschaum_plugin::Request, tokens: &[u64]) -> meerschaum_plugin::Result<()> {{ {}::__mrsm_ack(request, tokens) }}", module.ident));
                ack = format!("Some(user::{forward})");
                continue;
            }
            let kind = match connector_kind(&method) {
                Some(Kind::Subscribe) if !streaming => None,
                Some(kind) => Some(kind),
                None if instance => instance_kind(&method),
                None => None,
            };
            let Some(kind) = kind else {
                continue;
            };
            if methods.contains(&method) {
                return Err(diagnostic(
                    file,
                    function.sig.span(),
                    format!("duplicate connector method '{method}'"),
                ));
            }
            let id = format!("__mrsm_{method}");
            adapters.push_str(&adapter(file, function, kind, &id)?);
            if kind == Kind::Subscribe {
                capture_nothing(&function.sig, &mut self.edits);
            }
            let forward = format!("__mrsm_connector_{name}_{method}");
            let returns = if kind == Kind::Subscribe {
                "Box<dyn meerschaum_plugin::subscribe::Subscription>"
            } else {
                "meerschaum_plugin::Reply"
            };
            self.adapters.push_str(&format!("pub(super) fn {forward}(request: &meerschaum_plugin::Request, data: Option<meerschaum_plugin::Frame>) -> meerschaum_plugin::Result<{returns}> {{ {}::{id}(request, data) }}", module.ident));
            if kind == Kind::Subscribe {
                dispatch.push_str(&format!("{method:?} => meerschaum_plugin::subscribe::open(user::{forward}(request, data)?, __MRSM_ACK__),"));
            } else {
                dispatch.push_str(&format!("{method:?} => user::{forward}(request, data),"));
            }
            methods.push(method);
        }
        if instance {
            if !methods.iter().any(|m| instance_kind(m).is_some()) {
                return Err(diagnostic(
                    file,
                    module.span(),
                    format!(
                        "an instance connector must define its instance methods: {}",
                        INSTANCE_METHODS
                            .iter()
                            .map(|(m, _)| *m)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        } else if !methods
            .iter()
            .any(|m| m == "fetch" || m == "sync" || m == "subscribe")
        {
            // Say why a `subscribe` here is not the streaming one.
            if let Some(function) = subscribe {
                adapter(file, function, Kind::Subscribe, "__mrsm_subscribe")?;
                return Err(diagnostic(
                    file,
                    function.sig.output.span(),
                    "subscribe must return impl FnMut(Duration) -> Batch, impl Subscription, Box<dyn Subscription> or a type that implements Subscription (optionally in a Result)",
                ));
            }
            return Err(diagnostic(
                file,
                module.span(),
                "a source connector must define fetch, sync and/or subscribe",
            ));
        }
        let dispatch = dispatch.replace("__MRSM_ACK__", &ack);
        // One-line insertions retain all original source line numbers.
        self.edits.push(Edit(
            brace.span.close().byte_range().start..brace.span.close().byte_range().start,
            format!(
                "#[allow(unused_imports)] use meerschaum_plugin::sdk::fetch_prelude::*; {adapters}"
            ),
        ));
        self.dispatch.push_str(&format!("if request.connector_type == {name:?} {{ return match request.method.as_str() {{ {dispatch} _ => Err(meerschaum_plugin::Error::Unsupported(request.method.clone())) }}; }}"));
        let mut connector = json!({
            "type": name, "plugin": plugin_name, "class_name": module.ident.to_string(),
            "is_instance": instance, "is_thread_safe": false, "methods": methods,
            "doc": docs(&module.attrs)
        });
        if streaming {
            connector["is_streaming"] = json!(true);
        }
        self.connectors.insert(name.clone(), connector);
        Ok(())
    }

    /// `#[make_executor]`: a module whose functions are the methods of the executor type.
    fn executor(
        &mut self,
        file: &Path,
        source: &str,
        plugin_name: &str,
        module: &ItemMod,
        attr: &Attribute,
    ) -> Result<()> {
        let (name, _) = options(file, attr, Marker::Executor, &module.ident.to_string())?;
        if self.executors.contains_key(&name) {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("duplicate executor type '{name}'"),
            ));
        }
        let (brace, items) = inline_module(file, module, "executor", "procs")?;
        clear_attribute(source, attr, &mut self.edits);
        let mut methods = Vec::new();
        let mut adapters = String::new();
        let mut arms = String::new();
        for item in items {
            reject_misplaced_marker(file, item, false)?;
            let Item::Fn(function) = item else { continue };
            let method = function.sig.ident.unraw().to_string();
            if !EXECUTOR_METHODS.iter().any(|(m, _, _)| *m == method) {
                continue;
            }
            let id = format!("__mrsm_executor_{method}");
            adapters.push_str(&executor_adapter(file, function, &id)?);
            if method == "monitor_logs" {
                capture_nothing(&function.sig, &mut self.edits);
            }
            arms.push_str(&format!(
                "{method:?} => {id}(__mrsm_executor, __mrsm_context),"
            ));
            methods.push(method);
        }
        if methods.is_empty() {
            return Err(diagnostic(
                file,
                module.span(),
                format!(
                    "an executor must define at least one of its methods: {}",
                    EXECUTOR_METHODS
                        .iter()
                        .map(|(m, _, _)| *m)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        adapters.push_str(&format!("pub(super) fn __mrsm_executor(__mrsm_method: &str, __mrsm_executor: &meerschaum_plugin::executor::ExecutorInfo, __mrsm_context: &meerschaum_plugin::serde_json::Value) -> meerschaum_plugin::Result<meerschaum_plugin::executor::ExecutorReply> {{ match __mrsm_method {{ {arms} other => Err(meerschaum_plugin::executor::unsupported(other)) }} }}"));
        self.edits.push(Edit(
            brace.span.close().byte_range().start..brace.span.close().byte_range().start,
            format!(
                "#[allow(unused_imports)] use meerschaum_plugin::sdk::fetch_prelude::*; {adapters}"
            ),
        ));
        let forward = format!("__mrsm_executor_{name}");
        self.adapters.push_str(&format!("pub(super) fn {forward}(method: &str, executor: &meerschaum_plugin::executor::ExecutorInfo, context: &meerschaum_plugin::serde_json::Value) -> meerschaum_plugin::Result<meerschaum_plugin::executor::ExecutorReply> {{ {}::__mrsm_executor(method, executor, context) }}", module.ident));
        // Executor calls are routed before connector types, which share the request fields.
        self.executor_dispatch.push_str(&format!("if request.connector_type == {name:?} && request.method.starts_with(\"executor.\") {{ return meerschaum_plugin::executor::dispatch(request, user::{forward}); }}"));
        self.executors.insert(
            name.clone(),
            json!({"type": name, "plugin": plugin_name, "class_name": module.ident.to_string(),
                "methods": methods, "doc": docs(&module.attrs)}),
        );
        Ok(())
    }

    /// `#[api_route]`, `#[mcp_tool]`, `#[mcp_resource]`, `#[mcp_prompt]` or `#[web_page]` on a
    /// function.
    fn server(
        &mut self,
        file: &Path,
        source: &str,
        function: &ItemFn,
        attr: &Attribute,
        kind: Kind,
    ) -> Result<()> {
        let rust_name = function.sig.ident.unraw().to_string();
        let marker = attr.path().to_token_stream().to_string();
        if reserved(&rust_name) {
            return Err(diagnostic(
                file,
                attr.span(),
                format!(
                    "fetch, sync, register, and setup are reserved plugin functions; give your #[{marker}] another function name"
                ),
            ));
        }
        let (entry, method, list) = match kind {
            Kind::Route => (
                route_entry(file, attr, &rust_name, &docs(&function.attrs))?,
                "api_route",
                &mut self.api_routes,
            ),
            Kind::Tool => (
                tool_entry(file, attr, &rust_name, &docs(&function.attrs))?,
                "mcp_tool",
                &mut self.mcp_tools,
            ),
            Kind::Resource => (
                resource_entry(file, attr, &rust_name, &docs(&function.attrs))?,
                "mcp_resource",
                &mut self.mcp_resources,
            ),
            Kind::Page => (
                page_entry(file, attr, &rust_name)?,
                "page",
                &mut self.web_pages,
            ),
            _ => (
                prompt_entry(file, attr, &rust_name, &docs(&function.attrs))?,
                "mcp_prompt",
                &mut self.mcp_prompts,
            ),
        };
        // A page call names its function (the manifest's `function`), the others their name.
        let key = if kind == Kind::Page {
            "function"
        } else {
            "name"
        };
        let name = entry[key].as_str().unwrap_or_default().to_owned();
        if kind == Kind::Page && list.iter().any(|e| e["path"] == entry["path"]) {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("duplicate #[web_page] path {}", entry["path"]),
            ));
        }
        if list.iter().any(|e| e[key] == entry[key]) {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("duplicate #[{marker}] name '{name}'"),
            ));
        }
        if kind == Kind::Resource {
            let uri = |e: &Value| e.get("uri").or_else(|| e.get("uri_template")).cloned();
            if list.iter().any(|e| uri(e) == uri(&entry)) {
                return Err(diagnostic(
                    file,
                    attr.span(),
                    format!("duplicate resource URI {}", uri(&entry).unwrap_or_default()),
                ));
            }
        }
        let id = format!("__mrsm_{method}_{}", list.len());
        list.push(entry);
        clear_attribute(source, attr, &mut self.edits);
        self.adapters.push_str(&adapter(file, function, kind, &id)?);
        self.dispatch.push_str(&format!("if request.method == {method:?} && request.context[{key:?}] == {name:?} {{ return user::{id}(request, data); }}"));
        Ok(())
    }
}

/// `#[web_page]` or `#[web_page("my/page", login_required = false, ...)]`, as Python's
/// `@web_page`: the path defaults to the function's name and is served under `/dash/`.
fn page_entry(file: &Path, attr: &Attribute, rust_name: &str) -> Result<Value> {
    let items = server_items(file, attr)?;
    let mut path = rust_name.to_owned();
    let mut path_span = attr.span();
    let mut entry = json!({"function": rust_name, "login_required": true, "skip_navbar": false,
        "dark_theme": true, "mode": "html"});
    for (i, item) in items.iter().enumerate() {
        let Some(key) = &item.key else {
            if i != 0 {
                return Err(diagnostic(
                    file,
                    item.value.span(),
                    "the page path comes first: #[web_page(\"my/page\", login_required = false)]",
                ));
            }
            path = text_option(file, item, "the page path")?;
            path_span = item.value.span();
            continue;
        };
        let key = key.unraw().to_string();
        match key.as_str() {
            "login_required" | "skip_navbar" | "dark_theme" => {
                entry[&key] = json!(bool_option(file, item, &key)?)
            }
            "page_group" => entry[&key] = json!(text_option(file, item, &key)?),
            "host_scopes" => entry[&key] = json!(host_scopes_option(file, item)?),
            _ => {
                return Err(unknown_option(
                    file,
                    item,
                    "web_page",
                    "a path, login_required, skip_navbar, page_group, dark_theme or host_scopes",
                ));
            }
        }
    }
    // PARITY: `@web_page` strips slashes and serves the page under `/dash/`.
    let trimmed = path.trim().trim_matches('/');
    let trimmed = trimmed
        .strip_prefix("dash/")
        .unwrap_or(trimmed)
        .trim_matches('/');
    // The console's rules (`meerschaum_web::native_pages`): relative segments, no wildcards.
    if trimmed.is_empty()
        || trimmed == "dash"
        || trimmed.chars().any(|c| c.is_control() || c.is_whitespace())
        || trimmed.contains(['{', '}', '*', '?', '#', '%', '\\'])
        || trimmed
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
    {
        return Err(diagnostic(
            file,
            path_span,
            format!(
                "the page path {path:?} is not a console page path: use segments of letters, digits, '-' or '_' (for example \"my-plugin/report\")"
            ),
        ));
    }
    entry["path"] = json!(format!("/dash/{trimmed}"));
    // PARITY: `@web_page`'s page_key: the path's words, capitalized.
    entry["page_key"] = json!(
        trimmed
            .replace(['-', '_'], " ")
            .split(' ')
            .map(|word| {
                let mut chars = word.chars();
                chars.next().map_or_else(String::new, |c| {
                    c.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                })
            })
            .collect::<Vec<_>>()
            .join(" ")
    );
    Ok(entry)
}

/// `impl Trait` in a return type captures every parameter lifetime in Rust 2024, but the closure
/// a `subscribe` or `monitor_logs` returns outlives the call: it captures none (`+ use<>`), so a
/// closure that borrows a parameter is a compile error at the user's line instead of in the
/// adapter. A user's own `use<...>` bound is kept.
fn capture_nothing(sig: &syn::Signature, edits: &mut Vec<Edit>) {
    fn visit(ty: &Type, edits: &mut Vec<Edit>) {
        match ty {
            Type::ImplTrait(found) => {
                if !found
                    .bounds
                    .iter()
                    .any(|b| b.to_token_stream().to_string().starts_with("use"))
                {
                    let end = found.span().byte_range().end;
                    edits.push(Edit(end..end, " + use<>".into()));
                }
            }
            Type::Path(path) => {
                for segment in &path.path.segments {
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        for arg in &args.args {
                            if let syn::GenericArgument::Type(ty) = arg {
                                visit(ty, edits);
                            }
                        }
                    }
                }
            }
            Type::Paren(inner) => visit(&inner.elem, edits),
            Type::Group(inner) => visit(&inner.elem, edits),
            _ => {}
        }
    }
    if let syn::ReturnType::Type(_, ty) = &sig.output {
        visit(ty, edits);
    }
}

/// An unconditional inline module's brace and items.
fn inline_module<'a>(
    file: &Path,
    module: &'a ItemMod,
    what: &str,
    example: &str,
) -> Result<(&'a syn::token::Brace, &'a Vec<Item>)> {
    if module
        .attrs
        .iter()
        .any(|a| a.path().is_ident("cfg") || a.path().is_ident("cfg_attr"))
    {
        return Err(diagnostic(
            file,
            module.span(),
            format!("conditional {what} modules are unsupported"),
        ));
    }
    let Some((brace, items)) = &module.content else {
        let marker = if what == "executor" {
            "make_executor"
        } else {
            "make_connector"
        };
        return Err(diagnostic(
            file,
            module.span(),
            format!("#[{marker}] needs an inline module: mod {example} {{ ... }}"),
        ));
    };
    Ok((brace, items))
}

/// A streaming connector's `fn ack(tokens: &[u64])` (optionally `connector: &Connector`).
fn ack_adapter(file: &Path, function: &ItemFn) -> Result<String> {
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
enum Returns {
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
const EXECUTOR_METHODS: &[(&str, &[&str], Returns)] = &[
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
fn executor_adapter(file: &Path, function: &ItemFn, id: &str) -> Result<String> {
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

/// What a single-file plugin declares about itself: `const VERSION: &str = "1.2.0";` and
/// `const REQUIRED: &[&str] = &["package", "plugin:name"];`, the Rust spelling of Python's
/// `__version__` and `required`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SingleFileMetadata {
    /// `VERSION`, when declared.
    pub version: Option<String>,
    /// `REQUIRED` (empty when not declared).
    pub required: Vec<String>,
}

fn declared_metadata(file: &Path, parsed: &syn::File) -> Result<SingleFileMetadata> {
    let mut metadata = SingleFileMetadata::default();
    for item in &parsed.items {
        let Item::Const(constant) = item else {
            continue;
        };
        // Only the documented types declare metadata; a `const VERSION: u32` stays a helper.
        match constant.ident.to_string().as_str() {
            "VERSION" if str_type(&constant.ty) => {
                let version = string_literal(&constant.expr)
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| {
                        diagnostic(
                            file,
                            constant.expr.span(),
                            "VERSION must be a non-empty string literal, e.g. const VERSION: &str = \"1.0.0\";",
                        )
                    })?;
                metadata.version = Some(version);
            }
            "REQUIRED" if str_slice_type(&constant.ty) => {
                let invalid = || {
                    diagnostic(
                        file,
                        constant.expr.span(),
                        "REQUIRED must be a list of string literals, e.g. const REQUIRED: &[&str] = &[\"requests\"];",
                    )
                };
                let Expr::Reference(reference) = constant.expr.as_ref() else {
                    return Err(invalid());
                };
                let Expr::Array(array) = reference.expr.as_ref() else {
                    return Err(invalid());
                };
                metadata.required = array
                    .elems
                    .iter()
                    .map(|e| string_literal(e).ok_or_else(invalid))
                    .collect::<Result<_>>()?;
            }
            _ => {}
        }
    }
    Ok(metadata)
}

/// `&str` / `&'static str`.
fn str_type(ty: &Type) -> bool {
    matches!(ty, Type::Reference(r) if r.mutability.is_none() && plain_type(&r.elem, "str"))
}

/// `&[&str]` / `&'static [&'static str]`.
fn str_slice_type(ty: &Type) -> bool {
    matches!(ty, Type::Reference(r) if r.mutability.is_none()
        && matches!(r.elem.as_ref(), Type::Slice(slice) if str_type(&slice.elem)))
}

/// Read a single-file plugin's `VERSION` and `REQUIRED` without generating or building it.
pub(crate) fn metadata(file: &Path) -> Result<SingleFileMetadata> {
    let source = std::fs::read_to_string(file).map_err(error)?;
    let parsed = syn::parse_file(&source)
        .map_err(|e| diagnostic(file, e.span(), format!("invalid Rust plugin: {e}")))?;
    declared_metadata(file, &parsed)
}

/// Optional Cargo dependencies in a Rust doc-comment fence; never infer dependencies from use.
fn dependencies(parsed: &syn::File, file: &Path) -> Result<toml::Table> {
    dependencies_for(parsed, file, false)
}

fn dependencies_for(parsed: &syn::File, file: &Path, confined: bool) -> Result<toml::Table> {
    let mut block = None;
    let mut current: Option<String> = None;
    let doc_lines: Vec<String> = parsed
        .attrs
        .iter()
        .filter_map(|attr| {
            if attr.path().is_ident("doc")
                && let syn::Meta::NameValue(meta) = &attr.meta
                && let syn::Expr::Lit(expr) = &meta.value
                && let syn::Lit::Str(text) = &expr.lit
            {
                return Some(text.value());
            }
            None
        })
        .collect();
    for doc in doc_lines.iter().flat_map(|doc| doc.lines()) {
        let doc = doc.strip_prefix(' ').unwrap_or(doc);
        if doc.trim() == "```cargo" {
            if current.is_some() || block.is_some() {
                return Err(error("only one ```cargo dependency block is allowed"));
            }
            current = Some(String::new());
        } else if doc.trim() == "```" && current.is_some() {
            block = current.take();
        } else if let Some(text) = &mut current {
            text.push_str(doc);
            text.push('\n');
        }
    }
    if current.is_some() {
        return Err(error("unclosed ```cargo dependency block"));
    }
    let Some(block) = block else {
        return Ok(toml::Table::new());
    };
    let mut cargo: toml::Table = toml::from_str(&block).map_err(|e| {
        error(format!(
            "invalid Cargo dependency block in '{}': {e}",
            file.display()
        ))
    })?;
    if cargo.keys().any(|k| k != "dependencies") {
        return Err(error(
            "single-file Cargo blocks support only [dependencies]; use a packaged plugin for other Cargo settings",
        ));
    }
    let Some(deps) = cargo.remove("dependencies") else {
        return Ok(toml::Table::new());
    };
    let mut deps = deps
        .as_table()
        .cloned()
        .ok_or_else(|| error("dependencies must be a Cargo table"))?;
    for (name, spec) in &mut deps {
        if ["meerschaum_plugin", "meerschaum-plugin"].contains(&name.as_str()) {
            return Err(error(format!(
                "'{name}' is supplied by Meerschaum; do not declare it in [dependencies]"
            )));
        }
        if !(spec.is_str() || spec.is_table()) {
            return Err(error(format!(
                "dependency '{name}' needs a version string or a Cargo dependency table"
            )));
        }
        if let Some(table) = spec.as_table_mut() {
            if confined
                && [
                    "path",
                    "git",
                    "branch",
                    "tag",
                    "rev",
                    "registry",
                    "index",
                    "workspace",
                ]
                .iter()
                .any(|key| table.contains_key(*key))
            {
                return Err(error(format!(
                    "confined dependency '{name}' must use the host's pinned offline registry inputs"
                )));
            }
            if table.contains_key("workspace")
                || table.get("optional").and_then(toml::Value::as_bool) == Some(true)
            {
                return Err(error(format!(
                    "dependency '{name}' cannot use workspace or optional; use a packaged plugin"
                )));
            }
            if let Some(path) = table.get_mut("path") {
                let relative = path
                    .as_str()
                    .ok_or_else(|| error("dependency path must be a string"))?;
                let resolved = file.parent().unwrap_or(Path::new(".")).join(relative);
                *path = toml::Value::String(
                    resolved
                        .canonicalize()
                        .map_err(|e| error(format!("dependency '{name}': {e}")))?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    Ok(deps)
}

/// Generate the crate for `file` into `directory` and return what the file declares about
/// itself (its version is the plugin's version).
pub(crate) fn prepare(file: &Path, name: &str, directory: &Path) -> Result<SingleFileMetadata> {
    prepare_with_compiler(file, name, directory, None)
}

pub(crate) fn prepare_with_compiler(
    file: &Path,
    name: &str,
    directory: &Path,
    compiler: Option<&dyn crate::source::compiler::SourceCompiler>,
) -> Result<SingleFileMetadata> {
    let source = std::fs::read_to_string(file).map_err(error)?;
    let parsed = syn::parse_file(&source)
        .map_err(|e| diagnostic(file, e.span(), format!("invalid Rust plugin: {e}")))?;
    let declared = declared_metadata(file, &parsed)?;
    let version = declared.version.clone().unwrap_or_else(|| "0.0.0".into());
    let mut generated = Generated::default();
    subscription_types(&parsed.items, &mut generated.subscriptions);
    for attr in parsed
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("add_plugin_argument"))
    {
        generated.argument(file, &source, attr)?;
    }
    for item in &parsed.items {
        reject_misplaced_marker(file, item, true)?;
        match item {
            Item::Fn(function) => {
                for attr in function
                    .attrs
                    .iter()
                    .filter(|a| a.path().is_ident("add_plugin_argument"))
                {
                    generated.argument(file, &source, attr)?;
                }
                let rust_name = function.sig.ident.unraw().to_string();
                // PARITY: Python's decorators stack, so one function may be an action and
                // both kinds of hook; each marker registers it once.
                let hooks = hook_markers(&function.attrs);
                for (attr, kind) in &hooks {
                    generated.hook(file, &source, function, attr, *kind)?;
                }
                let servers = server_markers(&function.attrs);
                for (attr, kind) in &servers {
                    generated.server(file, &source, function, attr, *kind)?;
                }
                if let Some(attr) = marker(&function.attrs, "make_action") {
                    if reserved(&rust_name) {
                        return Err(diagnostic(
                            file,
                            attr.span(),
                            "fetch, sync, register, and setup are reserved plugin functions; give your action another function name",
                        ));
                    }
                    let (action_name, daemon) = options(file, attr, Marker::Action, &rust_name)?;
                    if generated.actions.iter().any(|a| a["name"] == action_name) {
                        return Err(diagnostic(
                            file,
                            attr.span(),
                            format!("duplicate action '{action_name}'"),
                        ));
                    }
                    clear_attribute(&source, attr, &mut generated.edits);
                    let id = format!("__mrsm_action_{}", generated.actions.len());
                    generated
                        .adapters
                        .push_str(&adapter(file, function, Kind::Action, &id)?);
                    generated.dispatch.push_str(&format!("if request.method == \"action\" && request.context[\"name\"] == {action_name:?} {{ return user::{id}(request, data); }}"));
                    generated.actions.push(json!({"name": action_name, "doc": docs(&function.attrs), "daemon": daemon}));
                } else if hooks.is_empty()
                    && servers.is_empty()
                    && let Some(kind) = function_kind(&rust_name)
                        .filter(|kind| plugin_signature(function, *kind, &generated.subscriptions))
                {
                    if generated.functions.contains(&rust_name) {
                        return Err(diagnostic(
                            file,
                            function.sig.span(),
                            format!("duplicate {rust_name} function"),
                        ));
                    }
                    let id = format!("__mrsm_{rust_name}");
                    generated
                        .adapters
                        .push_str(&adapter(file, function, kind, &id)?);
                    let condition = match kind {
                        Kind::Sync => "request.connector_type == \"plugin\" && matches!(request.method.as_str(), \"sync\" | \"sync_pipe\")".to_owned(),
                        // The host calls setup outside any connector.
                        Kind::Setup => "request.method == \"setup\"".to_owned(),
                        _ => format!("request.connector_type == \"plugin\" && request.method == {rust_name:?}"),
                    };
                    let call = if kind == Kind::Subscribe {
                        capture_nothing(&function.sig, &mut generated.edits);
                        format!(
                            "meerschaum_plugin::subscribe::open(user::{id}(request, data)?, None)"
                        )
                    } else {
                        format!("user::{id}(request, data)")
                    };
                    generated
                        .dispatch
                        .push_str(&format!("if {condition} {{ return {call}; }}"));
                    generated.functions.push(rust_name);
                }
            }
            Item::Mod(module) => match (
                marker(&module.attrs, "make_connector"),
                marker(&module.attrs, "make_executor"),
            ) {
                (Some(_), Some(attr)) => {
                    return Err(diagnostic(
                        file,
                        attr.span(),
                        "a module is either a #[make_connector] or a #[make_executor]",
                    ));
                }
                (Some(attr), None) => generated.connector(file, &source, name, module, attr)?,
                (None, Some(attr)) => generated.executor(file, &source, name, module, attr)?,
                (None, None) => {}
            },
            _ => {}
        }
    }
    if generated.functions.is_empty()
        && generated.actions.is_empty()
        && generated.connectors.is_empty()
        && generated.pre_sync_hooks.is_empty()
        && generated.post_sync_hooks.is_empty()
        && generated.executors.is_empty()
        && generated.api_routes.is_empty()
        && generated.mcp_tools.is_empty()
        && generated.mcp_resources.is_empty()
        && generated.mcp_prompts.is_empty()
        && generated.web_pages.is_empty()
    {
        return Err(error(
            "a single-file plugin must define fetch, sync, register, setup, subscribe, #[make_action], #[make_connector], #[make_executor], #[pre_sync_hook], #[post_sync_hook], #[api_route], #[mcp_tool], #[mcp_resource], #[mcp_prompt] or #[web_page]",
        ));
    }
    // Referencing the declarations keeps them used and type-checks them in the user's module.
    if declared.version.is_some() {
        generated.adapters.push_str("const _: &str = VERSION;");
    }
    if parsed
        .items
        .iter()
        .any(|i| matches!(i, Item::Const(c) if c.ident == "REQUIRED" && str_slice_type(&c.ty)))
    {
        generated.adapters.push_str("const _: &[&str] = REQUIRED;");
    }
    // include! rejects inner documentation. Convert actual file doc attributes only;
    // leave lookalike text inside strings and examples untouched.
    for attr in parsed.attrs.iter().filter(|a| a.path().is_ident("doc")) {
        let range = attr.span().byte_range();
        let text = &source[range.clone()];
        if text.starts_with("//!") || text.starts_with("/*!") {
            generated
                .edits
                .push(Edit(range.start + 2..range.start + 3, " ".into()));
        } else {
            clear_attribute(&source, attr, &mut generated.edits);
        }
    }
    generated
        .edits
        .sort_by_key(|e| std::cmp::Reverse(e.0.start));
    let mut user_source = source.clone();
    for Edit(range, replacement) in generated.edits {
        user_source.replace_range(range, &replacement);
    }
    let mut plugin_entry = json!({"name": name, "version": version, "required": declared.required,
        "data_functions": generated.functions, "actions": generated.actions,
        "arguments": generated.arguments, "pre_sync_hooks": generated.pre_sync_hooks,
        "post_sync_hooks": generated.post_sync_hooks,
        "connectors": generated.connectors.keys().collect::<Vec<_>>()});
    for (key, entries) in [
        ("api_routes", generated.api_routes),
        ("mcp_tools", generated.mcp_tools),
        ("mcp_resources", generated.mcp_resources),
        ("mcp_prompts", generated.mcp_prompts),
        ("web_pages", generated.web_pages),
    ] {
        if !entries.is_empty() {
            plugin_entry[key] = Value::Array(entries);
        }
    }
    let with_hooks = |hooks: &[String]| {
        if hooks.is_empty() {
            json!([])
        } else {
            json!([name])
        }
    };
    let metadata = json!({"schema_version": crate::MANIFEST_SCHEMA_VERSION,
        "meerschaum_version": env!("CARGO_PKG_VERSION"), "python_version": "", "fingerprint": null,
        "plugins": {name: plugin_entry}, "connector_types": generated.connectors,
        "executor_types": generated.executors,
        "pre_sync_hooks": with_hooks(&generated.pre_sync_hooks),
        "post_sync_hooks": with_hooks(&generated.post_sync_hooks)});
    let metadata = serde_json::to_string(&metadata).map_err(error)?;
    let wrapper = format!(
        r#"
#![deny(unsafe_op_in_unsafe_fn)]
extern crate self as meerschaum_plugin;
pub use serde_json;
#[allow(dead_code)] mod abi;
#[allow(dead_code)] mod rows_abi;
#[allow(dead_code)] mod runtime;
pub use runtime::*;
pub const MANIFEST_SCHEMA_VERSION: u32 = {schema};
mod user {{
    #[allow(unused_imports)] use meerschaum_plugin::sdk::fetch_prelude::*;
    include!("user.rs");
    {adapters}
}}
#[derive(Default)] struct SingleFilePlugin;
impl meerschaum_plugin::Plugin for SingleFilePlugin {{
    const NAME: &'static str = {name:?};
    const VERSION: &'static str = {version:?};
    fn manifest(&self) -> meerschaum_plugin::serde_json::Value {{
        meerschaum_plugin::serde_json::from_str({metadata:?}).expect("generated manifest")
    }}
    fn call(&self, request: &meerschaum_plugin::Request, data: Option<meerschaum_plugin::Frame>) -> meerschaum_plugin::Result<meerschaum_plugin::Reply> {{
        {dispatch}
        Err(meerschaum_plugin::Error::Unsupported(request.method.clone()))
    }}
}}
#[unsafe(no_mangle)] pub extern "C" fn mrsm_plugin_v1() -> *const abi::MrsmPluginV1 {{ &runtime::Export::<SingleFilePlugin>::VTABLE }}
#[unsafe(no_mangle)] pub unsafe extern "C" fn mrsm_plugin_invoke_rows_v1(bytes: abi::Bytes, host: *const rows_abi::RowsHostV1) -> abi::CallResult {{ unsafe {{ runtime::Export::<SingleFilePlugin>::invoke(bytes, host) }} }}
"#,
        schema = crate::MANIFEST_SCHEMA_VERSION,
        adapters = generated.adapters,
        dispatch = generated.executor_dispatch + &generated.dispatch
    );
    let mut cargo: toml::Table = toml::from_str(&format!(
        r#"
[package]
name = {name:?}
version = "0.0.0"
edition = "2024"
[workspace]
[lib]
path = "lib.rs"
crate-type = ["cdylib"]
[dependencies]
serde = "1"
serde_json = {{ version = "1", features = ["preserve_order", "float_roundtrip"] }}
[profile.release]
panic = "unwind"
opt-level = 0
debug = false
strip = "debuginfo"
"#
    ))
    .map_err(error)?;
    let mut deps = if compiler.is_some() {
        dependencies_for(&parsed, file, true)?
    } else {
        dependencies(&parsed, file)?
    };
    // Users may enable Serde features, but the runtime's JSON semantics remain required.
    if let Some(json) = deps.get_mut("serde_json") {
        let mut spec = if let Some(version) = json.as_str() {
            toml::Table::from_iter([("version".into(), toml::Value::String(version.into()))])
        } else {
            json.as_table().cloned().unwrap()
        };
        let features = spec
            .entry("features")
            .or_insert_with(|| toml::Value::Array(Vec::new()));
        let features = features
            .as_array_mut()
            .ok_or_else(|| error("serde_json features must be an array"))?;
        for required in ["preserve_order", "float_roundtrip"] {
            if !features.iter().any(|f| f.as_str() == Some(required)) {
                features.push(toml::Value::String(required.into()));
            }
        }
        *json = toml::Value::Table(spec);
    }
    cargo["dependencies"].as_table_mut().unwrap().extend(deps);
    let lock = compiler
        .map(|compiler| compiler.lockfile(name).map_err(error))
        .transpose()?;
    std::fs::create_dir_all(directory).map_err(error)?;
    for (filename, contents) in [
        (
            "mrsm-source-path.txt",
            file.canonicalize()
                .map_err(error)?
                .to_string_lossy()
                .into_owned(),
        ),
        ("Cargo.toml", toml::to_string(&cargo).map_err(error)?),
        ("lib.rs", wrapper),
        ("user.rs", user_source),
        (
            "runtime.rs",
            include_str!("single_file_runtime.rs").to_owned(),
        ),
        ("rows_abi.rs", include_str!("rows_abi.rs").to_owned()),
        (
            "abi.rs",
            include_str!("abi.rs").replace(
                "use arrow::ffi_stream::FFI_ArrowArrayStream;",
                "#[allow(non_camel_case_types)]\ntype FFI_ArrowArrayStream = std::ffi::c_void;",
            ),
        ),
    ] {
        let path = directory.join(filename);
        if std::fs::read_to_string(&path).ok().as_ref() != Some(&contents) {
            std::fs::write(path, contents).map_err(error)?;
        }
    }
    if let Some(lock) = lock {
        let path = directory.join("Cargo.lock");
        if std::fs::read_to_string(&path).ok().as_ref() != Some(&lock) {
            std::fs::write(path, lock).map_err(error)?;
        }
    }
    Ok(declared)
}

/// The embedded runtime, compiled into this crate for its unit tests.
#[cfg(test)]
#[allow(dead_code, unused_imports)]
#[path = "single_file_runtime.rs"]
mod runtime;

#[cfg(test)]
#[path = "single_file_tests.rs"]
mod tests_suite;
