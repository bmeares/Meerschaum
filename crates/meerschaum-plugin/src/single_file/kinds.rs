//! Which plugin function, connector method or hook a user function is (its [`Kind`]), recognized
//! by name, marker and signature, and the checks its signature must pass.

use super::*;

pub(super) fn plain_type(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.segments.last().is_some_and(|s| s.ident == name && matches!(s.arguments, syn::PathArguments::None)))
}
pub(super) fn reference_type(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Reference(r) if r.mutability.is_none() && plain_type(&r.elem, name))
}
pub(super) fn generic_type(ty: &Type, outer: &str, inner: &str) -> bool {
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
pub(super) fn returns_result(sig: &syn::Signature) -> bool {
    matches!(&sig.output, syn::ReturnType::Type(_, ty)
        if matches!(ty.as_ref(), Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Result")))
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Kind {
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
    /// Custom connector methods marked with `#[method]` inside `#[make_connector]`.
    CustomMethod,
}

impl Kind {
    pub(super) fn data(self) -> bool {
        matches!(
            self,
            Kind::Fetch | Kind::Sync | Kind::Register | Kind::Subscribe
        )
    }
    pub(super) fn hook(self) -> bool {
        matches!(self, Kind::PreHook | Kind::PostHook)
    }
    /// API routes, MCP entries and console pages: no Pipe or kwargs.
    pub(super) fn server(self) -> bool {
        matches!(
            self,
            Kind::Route | Kind::Tool | Kind::Resource | Kind::Prompt | Kind::Page
        )
    }
    pub(super) fn instance(self) -> bool {
        matches!(
            self,
            Kind::InstanceSuccess | Kind::InstanceData | Kind::InstanceValue
        )
    }
    /// The parameters a function of this kind accepts, for diagnostics.
    pub(super) fn parameters(self) -> &'static str {
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
            Kind::CustomMethod => {
                "custom connector methods accept connector: &Connector, pipe: &Pipe, kwargs: &Value, and context: &Value"
            }
            _ => {
                "use pipe: &Pipe, connector: &Connector, kwargs: &Value, begin/end: Option<Bound> (fetch/sync), or data: Option<Frame> (sync)"
            }
        }
    }
}

/// The checks every generated entry point shares: a plain synchronous function.
pub(super) fn check_signature(file: &Path, function: &ItemFn) -> Result<()> {
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
pub(super) fn parameters<'a>(
    file: &Path,
    function: &'a ItemFn,
) -> Result<Vec<(String, &'a syn::PatType)>> {
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

/// The plugin functions recognized by name at the top level.
pub(super) fn function_kind(name: &str) -> Option<Kind> {
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
pub(super) fn reserved(name: &str) -> bool {
    ["fetch", "sync", "register", "setup"].contains(&name)
}

/// The methods recognized by name inside a `#[make_connector]` module.
pub(super) fn connector_kind(name: &str) -> Option<Kind> {
    function_kind(name).filter(|kind| kind.data())
}

/// The instance methods the host calls (`meerschaum_plugin::host`'s `InstanceConnector`), by
/// what they answer.
pub(super) const INSTANCE_METHODS: &[(&str, Kind)] = &[
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
pub(super) const INSTANCE_FIELDS: &[&str] = &[
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

pub(super) fn instance_kind(name: &str) -> Option<Kind> {
    INSTANCE_METHODS
        .iter()
        .find(|(m, _)| *m == name)
        .map(|(_, kind)| *kind)
}

/// `#[pre_sync_hook]` / `#[post_sync_hook]` on a top-level function, in source order.
pub(super) fn hook_markers(attrs: &[Attribute]) -> Vec<(&Attribute, Kind)> {
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
pub(super) fn plugin_signature(
    function: &ItemFn,
    kind: Kind,
    subscriptions: &HashSet<String>,
) -> bool {
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
pub(super) fn subscription_types(items: &[Item], found: &mut HashSet<String>) {
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
