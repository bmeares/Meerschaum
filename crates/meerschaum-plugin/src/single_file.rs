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

mod adapters;
mod declared;
mod kinds;

use adapters::{EXECUTOR_METHODS, ack_adapter, adapter, executor_adapter};
pub use declared::SingleFileMetadata;
pub(crate) use declared::metadata;
use declared::{declared_metadata, dependencies, dependencies_for, str_slice_type};
use kinds::{
    INSTANCE_FIELDS, INSTANCE_METHODS, Kind, check_signature, connector_kind, function_kind,
    generic_type, hook_markers, instance_kind, parameters, plain_type, plugin_signature,
    reference_type, reserved, returns_result, subscription_types,
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
            let method_attr = function.attrs.iter().find(|a| a.path().is_ident("method"));
            if let Some(attr) = method_attr {
                clear_attribute(source, attr, &mut self.edits);
            }
            let kind = match connector_kind(&method) {
                Some(Kind::Subscribe) if !streaming => method_attr.map(|_| Kind::CustomMethod),
                Some(kind) => Some(kind),
                None if instance => {
                    instance_kind(&method).or_else(|| method_attr.map(|_| Kind::CustomMethod))
                }
                None if method == "test_connection" => Some(Kind::InstanceValue),
                None => method_attr.map(|_| Kind::CustomMethod),
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
        } else if methods.is_empty() {
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
                "a connector must define fetch, sync, subscribe, test_connection, and/or #[method] custom methods",
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
