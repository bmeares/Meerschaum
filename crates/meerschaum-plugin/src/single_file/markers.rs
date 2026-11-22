//! Server, MCP, and argument marker parsing for single-file Rust plugins.

use super::*;

/// The `key = value` options (and positional values) of a server marker.
pub(super) fn server_items(file: &Path, attr: &Attribute) -> Result<Vec<ArgumentItem>> {
    let marker = attr.path().to_token_stream().to_string();
    match &attr.meta {
        syn::Meta::Path(_) => Ok(Vec::new()),
        syn::Meta::List(_) => {
            let items = attr
                .parse_args_with(
                    syn::punctuated::Punctuated::<ArgumentItem, syn::Token![,]>::parse_terminated,
                )
                .map_err(|e| diagnostic(file, e.span(), format!("invalid #[{marker}]: {e}")))?;
            let mut seen = HashSet::new();
            for item in &items {
                if let Some(key) = &item.key {
                    let key = key.unraw().to_string();
                    if !seen.insert(key.clone()) {
                        return Err(diagnostic(
                            file,
                            key_span(item),
                            format!("duplicate option '{key}'"),
                        ));
                    }
                }
            }
            Ok(items.into_iter().collect())
        }
        syn::Meta::NameValue(_) => Err(diagnostic(
            file,
            attr.span(),
            format!("use #[{marker}(...)]"),
        )),
    }
}

/// A string literal option.
pub(super) fn text_option(file: &Path, item: &ArgumentItem, key: &str) -> Result<String> {
    string_literal(&item.value).ok_or_else(|| {
        diagnostic(
            file,
            item.value.span(),
            format!("{key} must be a string literal"),
        )
    })
}

/// A `true` / `false` option.
pub(super) fn bool_option(file: &Path, item: &ArgumentItem, key: &str) -> Result<bool> {
    match &item.value {
        Expr::Lit(syn::ExprLit {
            lit: Lit::Bool(b), ..
        }) => Ok(b.value),
        other => Err(diagnostic(
            file,
            other.span(),
            format!("{key} must be true or false"),
        )),
    }
}

/// A `["a", "b"]` option.
pub(super) fn strings_option(file: &Path, item: &ArgumentItem, key: &str) -> Result<Vec<String>> {
    let invalid = |span: Span| {
        diagnostic(
            file,
            span,
            format!("{key} must be an array of string literals"),
        )
    };
    let Expr::Array(array) = &item.value else {
        return Err(invalid(item.value.span()));
    };
    array
        .elems
        .iter()
        .map(|e| string_literal(e).ok_or_else(|| invalid(e.span())))
        .collect()
}

/// `host_scopes = ["pipes:read", ...]`: named scopes only (`*` would give anonymous callers the
/// whole host).
pub(super) fn host_scopes_option(file: &Path, item: &ArgumentItem) -> Result<Vec<String>> {
    let scopes = strings_option(file, item, "host_scopes")?;
    if scopes.iter().any(|s| s == "*") {
        return Err(diagnostic(
            file,
            item.value.span(),
            "host_scopes names scopes such as \"pipes:read\"; \"*\" is not accepted",
        ));
    }
    Ok(scopes)
}

/// A JSON option written as a string literal (`r#"{...}"#`).
pub(super) fn json_option(file: &Path, item: &ArgumentItem, key: &str) -> Result<Value> {
    let text = text_option(file, item, key)?;
    serde_json::from_str(&text).map_err(|e| {
        diagnostic(
            file,
            item.value.span(),
            format!("{key} is not valid JSON: {e}"),
        )
    })
}

pub(super) fn unknown_option(
    file: &Path,
    item: &ArgumentItem,
    marker: &str,
    expected: &str,
) -> crate::Error {
    match &item.key {
        Some(key) => diagnostic(
            file,
            key_span(item),
            format!(
                "unknown #[{marker}] option '{}'; expected {expected}",
                key.unraw()
            ),
        ),
        None => diagnostic(
            file,
            item.value.span(),
            format!("#[{marker}] takes only key = value options ({expected})"),
        ),
    }
}

/// `#[api_route(GET, "/path/{id}", scopes = [...], public = true, name = "...")]`.
pub(super) fn route_entry(
    file: &Path,
    attr: &Attribute,
    rust_name: &str,
    doc: &str,
) -> Result<Value> {
    let items = server_items(file, attr)?;
    let positional: Vec<&ArgumentItem> = items.iter().filter(|i| i.key.is_none()).collect();
    let (method, path) = match positional.as_slice() {
        [path] => ("GET".to_owned(), *path),
        [method, path] => {
            let text = match &method.value {
                Expr::Path(p) => p.path.get_ident().map(|i| i.to_string()),
                other => string_literal(other),
            }
            .filter(|m| m == "*" || (!m.is_empty() && m.chars().all(|c| c.is_ascii_alphabetic())))
            .ok_or_else(|| {
                diagnostic(
                    file,
                    method.value.span(),
                    "the method is an HTTP method such as GET or \"POST\", or \"*\" for any method",
                )
            })?;
            (text.to_uppercase(), *path)
        }
        _ => {
            return Err(diagnostic(
                file,
                attr.span(),
                "use #[api_route(GET, \"/my/path/{id}\")] (the method defaults to GET)",
            ));
        }
    };
    let path = string_literal(&path.value)
        .filter(|p| p.starts_with('/'))
        .ok_or_else(|| {
            diagnostic(
                file,
                path.value.span(),
                "the path is a string literal starting with '/'",
            )
        })?;
    // The server's own rules: a refused path would only be a notice at `mrsm start api`.
    if let Err(reason) = crate::api::check_route_path(&path) {
        return Err(diagnostic(
            file,
            positional[positional.len() - 1].value.span(),
            format!("the route path {path:?} is refused: {reason}"),
        ));
    }
    let mut entry = json!({"name": rust_name, "method": method, "path": path,
        "scopes": [], "public": false});
    for item in items.iter().filter(|i| i.key.is_some()) {
        let key = item
            .key
            .as_ref()
            .map(|k| k.unraw().to_string())
            .unwrap_or_default();
        match key.as_str() {
            "name" => entry["name"] = json!(text_option(file, item, &key)?),
            "scopes" => entry["scopes"] = json!(strings_option(file, item, &key)?),
            "public" => entry["public"] = json!(bool_option(file, item, &key)?),
            "host_scopes" => entry["host_scopes"] = json!(host_scopes_option(file, item)?),
            "description" => entry["description"] = json!(text_option(file, item, &key)?),
            _ => {
                return Err(unknown_option(
                    file,
                    item,
                    "api_route",
                    "name, scopes, public, host_scopes or description",
                ));
            }
        }
    }
    if entry["public"] == true && entry["scopes"].as_array().is_some_and(|s| !s.is_empty()) {
        return Err(diagnostic(
            file,
            attr.span(),
            "a public route needs no credentials, so it cannot require scopes",
        ));
    }
    if entry.get("description").is_none() && !doc.is_empty() {
        entry["description"] = json!(doc);
    }
    Ok(entry)
}

/// The description of an MCP entry: `description = "..."` or the function's doc comment.
pub(super) fn described(
    file: &Path,
    attr: &Attribute,
    entry: &mut Value,
    doc: &str,
    required: bool,
) -> Result<()> {
    if entry.get("description").is_none() {
        if !doc.is_empty() {
            entry["description"] = json!(doc);
        } else if required {
            let marker = attr.path().to_token_stream().to_string();
            return Err(diagnostic(
                file,
                attr.span(),
                format!("#[{marker}] needs a description: a doc comment or description = \"...\""),
            ));
        }
    }
    Ok(())
}

/// `#[mcp_tool(description = "...", input_schema = r#"{...}"#, read_only = true, ...)]`.
pub(super) fn tool_entry(
    file: &Path,
    attr: &Attribute,
    rust_name: &str,
    doc: &str,
) -> Result<Value> {
    let mut entry = json!({"name": rust_name,
        "input_schema": {"type": "object", "properties": {}}, "scopes": []});
    for item in server_items(file, attr)? {
        let key = item
            .key
            .as_ref()
            .map(|k| k.unraw().to_string())
            .unwrap_or_default();
        match key.as_str() {
            "name" | "description" | "title" => {
                entry[&key] = json!(text_option(file, &item, &key)?)
            }
            "scopes" => entry["scopes"] = json!(strings_option(file, &item, &key)?),
            "input_schema" | "output_schema" => {
                let schema = json_option(file, &item, &key)?;
                if !schema.is_object() {
                    return Err(diagnostic(
                        file,
                        item.value.span(),
                        format!("{key} must be a JSON object"),
                    ));
                }
                jsonschema::validator_for(&schema).map_err(|e| {
                    diagnostic(
                        file,
                        item.value.span(),
                        format!("{key} is not a valid JSON Schema: {e}"),
                    )
                })?;
                entry[&key] = schema;
            }
            "annotations" => {
                let annotations = json_option(file, &item, &key)?;
                if !annotations.is_object() {
                    return Err(diagnostic(
                        file,
                        item.value.span(),
                        "annotations must be a JSON object",
                    ));
                }
                let mut merged = annotations;
                if let Some(read_only) = entry["annotations"].get("readOnlyHint") {
                    merged["readOnlyHint"] = read_only.clone();
                }
                entry["annotations"] = merged;
            }
            "read_only" => {
                if !entry["annotations"].is_object() {
                    entry["annotations"] = json!({});
                }
                entry["annotations"]["readOnlyHint"] = json!(bool_option(file, &item, &key)?);
            }
            _ => {
                return Err(unknown_option(
                    file,
                    &item,
                    "mcp_tool",
                    "name, description, title, input_schema, output_schema, scopes, read_only or annotations",
                ));
            }
        }
    }
    described(file, attr, &mut entry, doc, true)?;
    Ok(entry)
}

/// `#[mcp_resource(uri = "x://logo")]` or `#[mcp_resource(uri_template = "x://items/{id}")]`.
pub(super) fn resource_entry(
    file: &Path,
    attr: &Attribute,
    rust_name: &str,
    doc: &str,
) -> Result<Value> {
    let mut entry = json!({"name": rust_name, "scopes": []});
    for item in server_items(file, attr)? {
        let key = item
            .key
            .as_ref()
            .map(|k| k.unraw().to_string())
            .unwrap_or_default();
        match key.as_str() {
            "name" | "description" | "title" | "uri" | "uri_template" | "mime_type" => {
                entry[&key] = json!(text_option(file, &item, &key)?)
            }
            "scopes" => entry["scopes"] = json!(strings_option(file, &item, &key)?),
            _ => {
                return Err(unknown_option(
                    file,
                    &item,
                    "mcp_resource",
                    "uri or uri_template, name, description, title, mime_type or scopes",
                ));
            }
        }
    }
    if entry.get("uri").is_some() == entry.get("uri_template").is_some() {
        return Err(diagnostic(
            file,
            attr.span(),
            "#[mcp_resource] needs exactly one of uri = \"...\" or uri_template = \"...\"",
        ));
    }
    described(file, attr, &mut entry, doc, true)?;
    Ok(entry)
}

/// `#[mcp_prompt(description = "...", arguments = ["who"])]`: listed arguments are required;
/// `arguments = r#"[{"name": "who", "description": "...", "required": false}]"#` spells them out.
pub(super) fn prompt_entry(
    file: &Path,
    attr: &Attribute,
    rust_name: &str,
    doc: &str,
) -> Result<Value> {
    let mut entry = json!({"name": rust_name, "arguments": [], "scopes": []});
    for item in server_items(file, attr)? {
        let key = item
            .key
            .as_ref()
            .map(|k| k.unraw().to_string())
            .unwrap_or_default();
        match key.as_str() {
            "name" | "description" | "title" => {
                entry[&key] = json!(text_option(file, &item, &key)?)
            }
            "scopes" => entry["scopes"] = json!(strings_option(file, &item, &key)?),
            "arguments" => {
                entry["arguments"] = if matches!(item.value, Expr::Array(_)) {
                    strings_option(file, &item, &key)?
                        .into_iter()
                        .map(|name| json!({"name": name, "required": true}))
                        .collect()
                } else {
                    let arguments = json_option(file, &item, &key)?;
                    let valid = arguments
                        .as_array()
                        .is_some_and(|items| items.iter().all(|a| a["name"].is_string()));
                    if !valid {
                        return Err(diagnostic(
                            file,
                            item.value.span(),
                            "arguments must be [\"name\", ...] or a JSON array of {\"name\", \"description\"?, \"required\"?}",
                        ));
                    }
                    arguments
                };
            }
            _ => {
                return Err(unknown_option(
                    file,
                    &item,
                    "mcp_prompt",
                    "name, description, title, arguments or scopes",
                ));
            }
        }
    }
    described(file, attr, &mut entry, doc, false)?;
    Ok(entry)
}

/// `#[api_route]` / `#[mcp_tool]` / `#[mcp_resource]` / `#[mcp_prompt]` on a function.
pub(super) fn server_markers(attrs: &[Attribute]) -> Vec<(&Attribute, Kind)> {
    attrs
        .iter()
        .filter_map(|attr| {
            let kind = match attr.path().get_ident()?.to_string().as_str() {
                "api_route" => Kind::Route,
                "mcp_tool" => Kind::Tool,
                "mcp_resource" => Kind::Resource,
                "mcp_prompt" => Kind::Prompt,
                "web_page" => Kind::Page,
                _ => return None,
            };
            Some((attr, kind))
        })
        .collect()
}

/// Item markers and where each belongs. A function may stack `make_action`, the hooks, a route
/// and MCP entries, as Python's decorators stack.
pub(super) const ITEM_MARKERS: &[(&str, &str)] = &[
    ("make_action", "a top-level function"),
    ("make_connector", "a top-level inline module"),
    ("make_executor", "a top-level inline module"),
    ("pre_sync_hook", "a top-level function"),
    ("post_sync_hook", "a top-level function"),
    ("api_route", "a top-level function"),
    ("mcp_tool", "a top-level function"),
    ("mcp_resource", "a top-level function"),
    ("mcp_prompt", "a top-level function"),
    ("web_page", "a top-level function"),
];

/// Reject misplaced annotations rather than silently treating them as helpers.
pub(super) fn reject_misplaced_marker(file: &Path, item: &Item, top_level: bool) -> Result<()> {
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
        if attr.path().is_ident("add_plugin_argument")
            && !(top_level && matches!(item, Item::Fn(_)))
        {
            return Err(diagnostic(
                file,
                attr.span(),
                "#[add_plugin_argument] belongs on a top-level function or at the top of the file as #![add_plugin_argument(...)]",
            ));
        }
        let Some((name, place)) = ITEM_MARKERS.iter().find(|(m, _)| attr.path().is_ident(m)) else {
            continue;
        };
        let placed = match *name {
            "make_connector" | "make_executor" => matches!(item, Item::Mod(_)),
            _ => matches!(item, Item::Fn(_)),
        };
        if !top_level || !placed {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("#[{name}] belongs on {place}"),
            ));
        }
        if attrs.iter().filter(|a| a.path().is_ident(name)).count() != 1 {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("duplicate #[{name}]"),
            ));
        }
        if (*name == "pre_sync_hook" || *name == "post_sync_hook")
            && !matches!(attr.meta, syn::Meta::Path(_))
        {
            return Err(diagnostic(
                file,
                attr.span(),
                format!("#[{name}] takes no options"),
            ));
        }
    }
    Ok(())
}

/// One `add_plugin_argument` entry: a positional option string or `key = value`.
pub(super) struct ArgumentItem {
    pub(super) key: Option<syn::Ident>,
    pub(super) value: Expr,
}
impl syn::parse::Parse for ArgumentItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // `type` and `const` are keywords, so keys are parsed as any identifier.
        if input.peek(syn::Ident::peek_any) && input.peek2(syn::Token![=]) {
            let key = syn::Ident::parse_any(input)?;
            input.parse::<syn::Token![=]>()?;
            return Ok(Self {
                key: Some(key),
                value: input.parse()?,
            });
        }
        Ok(Self {
            key: None,
            value: input.parse()?,
        })
    }
}

/// A literal as JSON: strings, integers, floats (optionally negated) and booleans.
pub(super) fn literal_json(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Str(s) => Some(json!(s.value())),
            Lit::Int(i) => i.base10_parse::<i64>().ok().map(Value::from),
            Lit::Float(f) => f.base10_parse::<f64>().ok().map(Value::from),
            Lit::Bool(b) => Some(json!(b.value)),
            _ => None,
        },
        Expr::Unary(unary) if matches!(unary.op, syn::UnOp::Neg(_)) => {
            match literal_json(&unary.expr)? {
                Value::Number(n) if n.is_i64() => n.as_i64().map(|i| json!(-i)),
                Value::Number(n) => n.as_f64().map(|f| json!(-f)),
                _ => None,
            }
        }
        Expr::Paren(paren) => literal_json(&paren.expr),
        _ => None,
    }
}

pub(super) fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Lit(syn::ExprLit {
            lit: Lit::Str(s), ..
        }) => Some(s.value()),
        _ => None,
    }
}

/// argparse's action classes, as the Python worker's manifest names them.
pub(super) fn argparse_action(action: &str) -> Option<&'static str> {
    Some(match action {
        "store" => "_StoreAction",
        "store_true" => "_StoreTrueAction",
        "store_false" => "_StoreFalseAction",
        "store_const" => "_StoreConstAction",
        "append" => "_AppendAction",
        "append_const" => "_AppendConstAction",
        "count" => "_CountAction",
        _ => return None,
    })
}

/// `#[add_plugin_argument("--flag", "-f", action = "store_true", help = "...")]`: the manifest
/// entry Python's `add_plugin_argument` (argparse's `add_argument`) records.
pub(super) fn plugin_argument(file: &Path, attr: &Attribute) -> Result<Value> {
    let fail = |span: Span, message: String| diagnostic(file, span, message);
    let items = attr
        .parse_args_with(
            syn::punctuated::Punctuated::<ArgumentItem, syn::Token![,]>::parse_terminated,
        )
        .map_err(|e| fail(e.span(), format!("invalid add_plugin_argument: {e}")))?;
    let mut options = Vec::new();
    let mut action = "store".to_owned();
    let (mut dest, mut help, mut metavar) = (None, None, Value::Null);
    let (mut default, mut constant, mut type_name, mut choices) =
        (None, None, Value::Null, Value::Null);
    let mut required = false;
    let mut seen = HashSet::new();
    for item in &items {
        let span = item.value.span();
        let Some(key) = &item.key else {
            let option = string_literal(&item.value)
                .filter(|o| o.len() > 1 && o.starts_with('-') && !o.contains(char::is_whitespace))
                .ok_or_else(|| {
                    fail(
                        span,
                        "option strings are quoted flags such as \"--name\" or \"-n\"".into(),
                    )
                })?;
            options.push(option);
            continue;
        };
        let key = key.unraw().to_string();
        if !seen.insert(key.clone()) {
            return Err(fail(key_span(item), format!("duplicate option '{key}'")));
        }
        let text = || {
            string_literal(&item.value)
                .ok_or_else(|| fail(span, format!("{key} must be a string literal")))
        };
        match key.as_str() {
            "action" => action = text()?,
            "help" => help = Some(text()?),
            "dest" => dest = Some(text()?),
            "metavar" => metavar = json!(text()?),
            "default" => {
                default = Some(literal_json(&item.value).ok_or_else(|| {
                    fail(
                        span,
                        "default must be a string, number or bool literal".into(),
                    )
                })?)
            }
            "const" => {
                constant = Some(literal_json(&item.value).ok_or_else(|| {
                    fail(
                        span,
                        "const must be a string, number or bool literal".into(),
                    )
                })?)
            }
            "type" => {
                let name = match &item.value {
                    Expr::Path(path) => path.path.get_ident().map(|i| i.to_string()),
                    other => string_literal(other),
                };
                type_name = match name.as_deref() {
                    Some(name @ ("int" | "float" | "str")) => json!(name),
                    _ => return Err(fail(span, "type must be int, float or str".into())),
                };
            }
            "choices" => {
                let Expr::Array(array) = &item.value else {
                    return Err(fail(span, "choices must be an array of literals".into()));
                };
                choices = array
                    .elems
                    .iter()
                    .map(|e| {
                        literal_json(e)
                            .ok_or_else(|| fail(e.span(), "choices must be literals".into()))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into();
            }
            "required" => {
                required = matches!(&item.value, Expr::Lit(syn::ExprLit { lit: Lit::Bool(b), .. }) if b.value);
                if !matches!(
                    &item.value,
                    Expr::Lit(syn::ExprLit {
                        lit: Lit::Bool(_),
                        ..
                    })
                ) {
                    return Err(fail(span, "required must be true or false".into()));
                }
            }
            "nargs" => {
                return Err(fail(
                    key_span(item),
                    "nargs is unsupported; use action = \"append\" to collect several values"
                        .into(),
                ));
            }
            other => {
                return Err(fail(
                    key_span(item),
                    format!(
                        "unknown option '{other}'; expected action, help, dest, default, type, choices, metavar, const or required"
                    ),
                ));
            }
        }
    }
    if options.is_empty() {
        return Err(fail(
            attr.span(),
            "add_plugin_argument needs at least one flag such as \"--name\"".into(),
        ));
    }
    let class = argparse_action(&action).ok_or_else(|| {
        fail(
            attr.span(),
            format!(
                "unsupported action '{action}'; use store, store_true, store_false, store_const, append, append_const or count"
            ),
        )
    })?;
    let takes_value = matches!(action.as_str(), "store" | "append");
    if !takes_value && (!type_name.is_null() || !choices.is_null() || !metavar.is_null()) {
        return Err(fail(
            attr.span(),
            format!("type, choices and metavar need a value-taking action, not '{action}'"),
        ));
    }
    if constant.is_some() != matches!(action.as_str(), "store_const" | "append_const") {
        return Err(fail(
            attr.span(),
            "const is required by (and only allowed for) store_const and append_const".into(),
        ));
    }
    // argparse: the first long option names the attribute, dashes become underscores.
    let dest = dest.unwrap_or_else(|| {
        options
            .iter()
            .find(|o| o.starts_with("--"))
            .unwrap_or(&options[0])
            .trim_start_matches('-')
            .replace('-', "_")
    });
    if dest.is_empty() || !dest.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(fail(
            attr.span(),
            format!("'{dest}' is not a valid argument name; set dest = \"...\""),
        ));
    }
    let (default, constant) = match action.as_str() {
        "store_true" => (default.unwrap_or(json!(false)), json!(true)),
        "store_false" => (default.unwrap_or(json!(true)), json!(false)),
        _ => (
            default.unwrap_or(Value::Null),
            constant.unwrap_or(Value::Null),
        ),
    };
    Ok(json!({
        "option_strings": options, "dest": dest, "action": class, "nargs": null,
        "const": constant, "default": default, "type": type_name, "choices": choices,
        "required": required, "help": help, "metavar": metavar,
    }))
}

pub(super) fn key_span(item: &ArgumentItem) -> Span {
    item.key
        .as_ref()
        .map(|k| k.span())
        .unwrap_or_else(|| item.value.span())
}
