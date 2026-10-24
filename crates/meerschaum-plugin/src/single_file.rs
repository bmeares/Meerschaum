//! Generate a lightweight crate from beginner-facing Rust plugin shorthand.
//! Source edits preserve the user's line numbers; manifests and dispatch are generated.
use crate::{Result, error};
use proc_macro2::Span;
use quote::ToTokens;
use serde_json::{Value, json};
use std::{collections::HashSet, ops::Range, path::Path};
use syn::{Attribute, Item, ItemFn, ItemMod, Type, spanned::Spanned};

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

/// The only options belong to the shorthand, not the generated Cargo project.
fn options(file: &Path, attr: &Attribute, action: bool, default: &str) -> Result<(String, bool)> {
    let mut name = default.to_owned();
    let mut daemon = true;
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
            } else {
                Err(meta.error(if action {
                    "expected name or daemon"
                } else {
                    "expected name; instance connectors require a packaged plugin"
                }))
            }
        })
        .map_err(|e| diagnostic(file, e.span(), e))?;
    } else if !matches!(attr.meta, syn::Meta::Path(_)) {
        return Err(diagnostic(
            file,
            attr.span(),
            "use #[make_action] or #[make_connector] with optional (...) options",
        ));
    }
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(diagnostic(
            file,
            attr.span(),
            "names must contain only letters, digits, or underscores",
        ));
    }
    if !action && ["plugin", "sql", "api", "valkey"].contains(&name.as_str()) {
        return Err(diagnostic(
            file,
            attr.span(),
            format!("connector type '{name}' is reserved"),
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

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Fetch,
    Sync,
    Register,
    Action,
}

/// Every adapter is compiled in the same module as the user's private function.
fn adapter(file: &Path, function: &ItemFn, kind: Kind, id: &str) -> Result<String> {
    let sig = &function.sig;
    let name = sig.ident.to_string();
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
    let mut args = Vec::new();
    let mut seen = HashSet::new();
    let mut needs_pipe = false;
    let mut needs_connector = false;
    for arg in &sig.inputs {
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
        let param = ident.ident.to_string();
        let param = param.trim_start_matches('_');
        if !seen.insert(param.to_owned()) {
            return Err(diagnostic(
                file,
                arg.span(),
                format!("duplicate {name} parameter '{param}'"),
            ));
        }
        let (expected, valid, value) = match param {
            "pipe" if kind != Kind::Action => {
                needs_pipe = true;
                (
                    "&Pipe",
                    reference_type(&arg.ty, "Pipe"),
                    "&__mrsm_pipe".to_owned(),
                )
            }
            "connector" if kind != Kind::Action => {
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
            "kwargs" => (
                "&Value",
                reference_type(&arg.ty, "Value"),
                "__mrsm_request.kwargs()".to_owned(),
            ),
            _ => {
                return Err(diagnostic(
                    file,
                    arg.span(),
                    format!(
                        "unsupported {name} parameter '{param}'; {}",
                        if kind == Kind::Action {
                            "actions accept action: Vec<String> and kwargs: &Value"
                        } else {
                            "use pipe: &Pipe, connector: &Connector, kwargs: &Value, begin/end: Option<Bound> (fetch/sync), or data: Option<Frame> (sync)"
                        }
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
    let pipe = if needs_pipe {
        "let __mrsm_pipe = __mrsm_request.pipe()?;"
    } else {
        ""
    };
    let connector = if needs_connector {
        "let __mrsm_connector = __mrsm_request.connector();"
    } else {
        ""
    };
    let call = format!("{}({})", sig.ident, args.join(", "));
    let output = match kind {
        Kind::Fetch => format!(
            "meerschaum_plugin::sdk::FetchOutput::into_frame({call}).map(meerschaum_plugin::Reply::data)"
        ),
        Kind::Sync => format!(
            "meerschaum_plugin::sdk::SyncOutput::into_success({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Action => format!(
            "meerschaum_plugin::sdk::ActionOutput::into_action({call}).map(meerschaum_plugin::Reply::json)"
        ),
        Kind::Register => format!(
            "meerschaum_plugin::sdk::RegisterOutput::into_parameters({call}).map(meerschaum_plugin::Reply::json)"
        ),
    };
    Ok(format!(
        "pub(super) fn {id}(__mrsm_request: &meerschaum_plugin::Request, __mrsm_data: Option<meerschaum_plugin::Frame>) -> meerschaum_plugin::Result<meerschaum_plugin::Reply> {{ {pipe} {connector} {output} }}"
    ))
}

fn kind(name: &str) -> Option<Kind> {
    match name {
        "fetch" => Some(Kind::Fetch),
        "sync" => Some(Kind::Sync),
        "register" => Some(Kind::Register),
        _ => None,
    }
}

struct Generated {
    functions: Vec<String>,
    actions: Vec<Value>,
    connectors: serde_json::Map<String, Value>,
    adapters: String,
    dispatch: String,
    edits: Vec<Edit>,
}
impl Generated {
    fn connector(
        &mut self,
        file: &Path,
        source: &str,
        plugin_name: &str,
        module: &ItemMod,
        attr: &Attribute,
    ) -> Result<()> {
        let (name, _) = options(file, attr, false, &module.ident.to_string())?;
        if self.connectors.contains_key(&name) {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("duplicate connector type '{name}'"),
            ));
        }
        if module
            .attrs
            .iter()
            .any(|a| a.path().is_ident("cfg") || a.path().is_ident("cfg_attr"))
        {
            return Err(diagnostic(
                file,
                module.span(),
                "conditional connector modules are unsupported",
            ));
        }
        let Some((brace, items)) = &module.content else {
            return Err(diagnostic(
                file,
                module.span(),
                "#[make_connector] needs an inline module: mod readings { ... }",
            ));
        };
        clear_attribute(source, attr, &mut self.edits);
        let mut methods = Vec::new();
        let mut adapters = String::new();
        let mut dispatch = String::new();
        for item in items {
            reject_misplaced_marker(file, item, false)?;
            if let Item::Fn(function) = item
                && let Some(kind) = kind(&function.sig.ident.to_string())
            {
                let method = function.sig.ident.to_string();
                if methods.contains(&method) {
                    return Err(diagnostic(
                        file,
                        function.sig.span(),
                        format!("duplicate connector method '{method}'"),
                    ));
                }
                let id = format!("__mrsm_{method}");
                adapters.push_str(&adapter(file, function, kind, &id)?);
                let forward = format!("__mrsm_connector_{name}_{method}");
                self.adapters.push_str(&format!("pub(super) fn {forward}(request: &meerschaum_plugin::Request, data: Option<meerschaum_plugin::Frame>) -> meerschaum_plugin::Result<meerschaum_plugin::Reply> {{ {}::{id}(request, data) }}", module.ident));
                dispatch.push_str(&format!("{method:?} => user::{forward}(request, data),"));
                methods.push(method);
            }
        }
        if !methods.iter().any(|m| m == "fetch" || m == "sync") {
            return Err(diagnostic(
                file,
                module.span(),
                "a source connector must define fetch and/or sync",
            ));
        }
        // One-line insertions retain all original source line numbers.
        self.edits.push(Edit(
            brace.span.close().byte_range().start..brace.span.close().byte_range().start,
            format!(
                "#[allow(unused_imports)] use meerschaum_plugin::sdk::fetch_prelude::*; {adapters}"
            ),
        ));
        self.dispatch.push_str(&format!("if request.connector_type == {name:?} {{ return match request.method.as_str() {{ {dispatch} _ => Err(meerschaum_plugin::Error::Unsupported(request.method.clone())) }}; }}"));
        self.connectors.insert(
            name.clone(),
            json!({
                "type": name, "plugin": plugin_name, "class_name": module.ident.to_string(),
                "is_instance": false, "is_thread_safe": false, "methods": methods,
                "doc": docs(&module.attrs)
            }),
        );
        Ok(())
    }
}

/// Reject misplaced annotations rather than silently treating them as helpers.
fn reject_misplaced_marker(file: &Path, item: &Item, top_level: bool) -> Result<()> {
    let attrs = match item {
        Item::Fn(i) => &i.attrs,
        Item::Mod(i) => &i.attrs,
        Item::Struct(i) => &i.attrs,
        Item::Enum(i) => &i.attrs,
        Item::Impl(i) => &i.attrs,
        Item::Use(i) => &i.attrs,
        Item::Const(i) => &i.attrs,
        Item::Static(i) => &i.attrs,
        Item::Trait(i) => &i.attrs,
        Item::Type(i) => &i.attrs,
        _ => return Ok(()),
    };
    for attr in attrs {
        let action = attr.path().is_ident("make_action");
        let connector = attr.path().is_ident("make_connector");
        if (action || connector)
            && (!top_level
                || (action && !matches!(item, Item::Fn(_)))
                || (connector && !matches!(item, Item::Mod(_))))
        {
            return Err(diagnostic(
                file,
                attr.span(),
                "#[make_action] belongs on a top-level function; #[make_connector] belongs on a top-level inline module",
            ));
        }
        if (action || connector)
            && attrs
                .iter()
                .filter(|a| a.path().is_ident("make_action") || a.path().is_ident("make_connector"))
                .count()
                != 1
        {
            return Err(diagnostic(
                file,
                attr.span(),
                "each item may have only one plugin annotation",
            ));
        }
    }
    Ok(())
}

/// Optional Cargo dependencies in a Rust doc-comment fence; never infer dependencies from use.
fn dependencies(parsed: &syn::File, file: &Path) -> Result<toml::Table> {
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

pub(crate) fn prepare(file: &Path, name: &str, directory: &Path) -> Result<()> {
    let source = std::fs::read_to_string(file).map_err(error)?;
    let parsed = syn::parse_file(&source)
        .map_err(|e| diagnostic(file, e.span(), format!("invalid Rust plugin: {e}")))?;
    let mut generated = Generated {
        functions: Vec::new(),
        actions: Vec::new(),
        connectors: serde_json::Map::new(),
        adapters: String::new(),
        dispatch: String::new(),
        edits: Vec::new(),
    };
    for item in &parsed.items {
        reject_misplaced_marker(file, item, true)?;
        match item {
            Item::Fn(function) => {
                let rust_name = function.sig.ident.to_string();
                if let Some(attr) = marker(&function.attrs, "make_action") {
                    if kind(&rust_name).is_some() {
                        return Err(diagnostic(
                            file,
                            attr.span(),
                            "fetch, sync, and register are reserved plugin functions; give your action another function name",
                        ));
                    }
                    let (action_name, daemon) = options(file, attr, true, &rust_name)?;
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
                } else if let Some(kind) = kind(&rust_name) {
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
                    let condition = if rust_name == "sync" {
                        "matches!(request.method.as_str(), \"sync\" | \"sync_pipe\")".to_owned()
                    } else {
                        format!("request.method == {rust_name:?}")
                    };
                    generated.dispatch.push_str(&format!("if request.connector_type == \"plugin\" && {condition} {{ return user::{id}(request, data); }}"));
                    generated.functions.push(rust_name);
                }
            }
            Item::Mod(module) => {
                if let Some(attr) = marker(&module.attrs, "make_connector") {
                    generated.connector(file, &source, name, module, attr)?;
                }
            }
            _ => {}
        }
    }
    if generated.functions.is_empty()
        && generated.actions.is_empty()
        && generated.connectors.is_empty()
    {
        return Err(error(
            "a single-file plugin must define fetch, sync, register, #[make_action], or #[make_connector]",
        ));
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
    let plugin_entry = json!({"name": name, "version": "0.0.0", "data_functions": generated.functions,
        "actions": generated.actions, "connectors": generated.connectors.keys().collect::<Vec<_>>()});
    let metadata = json!({"schema_version": crate::MANIFEST_SCHEMA_VERSION,
        "meerschaum_version": env!("CARGO_PKG_VERSION"), "python_version": "", "fingerprint": null,
        "plugins": {name: plugin_entry}, "connector_types": generated.connectors});
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
    const VERSION: &'static str = "0.0.0";
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
        dispatch = generated.dispatch
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
    let mut deps = dependencies(&parsed, file)?;
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_blocks_accept_cargo_forms_and_ignore_code_strings() {
        let parsed = syn::parse_file(
            r#"//! ```cargo
//! [dependencies]
//! regex = "1"
//! uuid = { version = "1", features = ["v4"], default-features = false }
//! ```
fn helper() {}
"#,
        )
        .unwrap();
        let deps = dependencies(&parsed, Path::new("example.rs")).unwrap();
        assert_eq!(deps["regex"].as_str(), Some("1"));
        assert_eq!(deps["uuid"]["features"][0].as_str(), Some("v4"));
        let parsed = syn::parse_file(
            "const EXAMPLE: &str = r#\"\n//! ```cargo\n//! bad = true\n//! ```\n\"#;",
        )
        .unwrap();
        assert!(
            dependencies(&parsed, Path::new("example.rs"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn invalid_dependency_blocks_fail_before_building() {
        for (block, expected) in [
            ("//! ```cargo\n//! [dependencies]", "unclosed"),
            (
                "//! ```cargo\n//! [dependencies]\n//! x = \n//! ```",
                "invalid Cargo",
            ),
            (
                "//! ```cargo\n//! [package]\n//! name = 'oops'\n//! ```",
                "only [dependencies]",
            ),
            (
                "//! ```cargo\n//! [dependencies]\n//! x = true\n//! ```",
                "version string",
            ),
            (
                "//! ```cargo\n//! [dependencies]\n//! x = { workspace = true }\n//! ```",
                "workspace or optional",
            ),
            (
                "//! ```cargo\n//! [dependencies]\n//! meerschaum-plugin = '5'\n//! ```",
                "supplied by Meerschaum",
            ),
            ("//! ```cargo\n//! ```\n//! ```cargo\n//! ```", "only one"),
        ] {
            let parsed = syn::parse_file(block).unwrap();
            let err = dependencies(&parsed, Path::new("example.rs"))
                .unwrap_err()
                .to_string();
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn transformed_source_preserves_lines_and_only_strips_shorthand_attributes() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("sample.rs");
        let source = "//! Introductory notes.\n#[make_action(\n name = \"greet\"\n)]\nfn hello() {}\n#[make_connector]\nmod readings {\n    #![allow(dead_code)]\n    fn fetch() -> Vec<Value> { vec![] }\n}\n";
        std::fs::write(&file, source).unwrap();
        let output = root.path().join("generated");
        prepare(&file, "sample", &output).unwrap();
        let transformed = std::fs::read_to_string(output.join("user.rs")).unwrap();
        assert!(!transformed.contains("#[make_action"));
        assert!(!transformed.contains("#[make_connector"));
        assert!(transformed.lines().nth(4).unwrap().starts_with("fn hello"));
        assert!(transformed.lines().nth(8).unwrap().contains("fn fetch"));
        assert!(syn::parse_file(&transformed).is_ok());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), source);
    }
    #[test]
    fn generated_source_keeps_doc_comment_text_inside_raw_strings() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("sample.rs");
        let source = r##"//! Actual file documentation.
#[make_action]
fn hello() {
    let example = r#"
//! ```cargo
//! this is an example, not a dependency
//! ```
"#;
    assert!(example.contains("//! this is an example"));
}
"##;
        std::fs::write(&file, source).unwrap();
        let output = root.path().join("generated");
        prepare(&file, "sample", &output).unwrap();
        let transformed = std::fs::read_to_string(output.join("user.rs")).unwrap();
        assert!(transformed.contains("//! this is an example, not a dependency"));
        assert!(transformed.starts_with("//  Actual file documentation."));
    }
}
