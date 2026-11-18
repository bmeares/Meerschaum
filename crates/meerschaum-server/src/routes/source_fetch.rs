//! Main-owned, canonical native SQL source reads. This never dispatches user code.

use crate::json::Bounded;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::post;
use meerschaum_connector::mutation::MutationCheck as _;
use meerschaum_connector::{InstanceConnector, PipesKeysQuery};
use meerschaum_connector_api::source_fetch::{self, Request};
use meerschaum_core::template::{TemplateResolver, referenced_pipes_in_parameters};
use meerschaum_core::{Map, Pipe, Value};

use super::Ctx;
use crate::access::{self, Access};
use crate::backends::SourceFetchError;
use crate::error::{ApiError, ApiResult};
use crate::sql_guard::{self, HiddenTables};
use crate::state::AppState;

/// The route belongs to the ordinary pipes group and has its own small body bound.
pub const ROUTE: &str = "/pipes/{connector_keys}/{metric_key}/{location_key}/fetch";
const MAX_METADATA_PIPES: usize = 4096;
const MAX_METADATA_BYTES: usize = 4 * 1024 * 1024;
const MAX_PARAMETERS_BYTES: usize = 256 * 1024;
const MAX_REFERENCES: usize = 64;

/// Only this module can construct a plan. It contains a frozen, authorized definition, not
/// credentials or a caller-supplied pipe. Native backends must not resolve more references.
pub struct Plan {
    request: Request,
    pipe: Pipe,
    hidden: HiddenTables,
    table: Option<RegisteredTable>,
    binding: Binding,
    receipt_fingerprint: String,
}

/// A connector operator approved this exact registration ID and physical name.
#[derive(Clone, serde::Serialize)]
pub struct RegisteredTable {
    pub registration: i64,
    pub schema: String,
    pub table: String,
}

/// Main-owned snapshot proof. Digests and registered identities never come from request JSON.
#[derive(Clone)]
pub struct Binding {
    /// Stable original caller identity; callback rotation retains grant and launch IDs.
    pub owner: String,
    /// Fingerprint for cleanup by a credential that lost authority after its last page.
    pub credential: String,
    /// Exact request, registered/raw/resolved dependencies and trusted connector snapshot.
    pub fingerprint: String,
}

/// Only this route constructs combined source/destination authority for native replacement.
/// Its callback retains the exact original request credential and reloads both resources.
pub struct ReplacementPlan {
    source: Plan,
    destination: Pipe,
    binding: Binding,
    authority: Arc<dyn meerschaum_connector::mutation::MutationCheck>,
    receipt_authority: Arc<dyn meerschaum_connector::mutation::MutationCheck>,
    enforce_dtypes: bool,
    request_identity: String,
}
impl ReplacementPlan {
    /// Canonical SQL source; never selected by a client definition.
    pub fn source(&self) -> &Plan {
        &self.source
    }
    /// Original raw registered destination parameters, before template expansion.
    pub fn destination(&self) -> &Pipe {
        &self.destination
    }
    /// Stable original actor and exact prepared operation proof.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
    /// Fresh original-caller authority for the captured native writer transaction.
    pub fn authority(&self) -> Arc<dyn meerschaum_connector::mutation::MutationCheck> {
        self.authority.clone()
    }
    /// Fresh role/resource checks for a completed receipt; inferred metadata from the
    /// successful operation does not invalidate its exact original structured request.
    pub fn receipt_authority(&self) -> Arc<dyn meerschaum_connector::mutation::MutationCheck> {
        self.receipt_authority.clone()
    }
    /// Exact structured operation/resource identity for an idempotent completion receipt.
    pub fn request_identity(&self) -> &str {
        &self.request_identity
    }
    /// The sync caller's dtype enforcement choice.
    pub fn enforce_dtypes(&self) -> bool {
        self.enforce_dtypes
    }
}

impl Plan {
    /// Validated query and transport bounds.
    pub fn request(&self) -> &Request {
        &self.request
    }
    /// Canonical parameters, fully resolved against an authorized metadata snapshot.
    pub fn pipe(&self) -> &Pipe {
        &self.pipe
    }
    /// Positive authority for direct table reads, separate from SQL definitions.
    pub fn registered_table(&self) -> Option<&RegisteredTable> {
        self.table.as_ref()
    }
    /// Private proof for a backend's bounded snapshot registry.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
    /// Check the final generated query as well as the raw and resolved source definitions.
    pub fn check_sql(&self, query: &str) -> Result<(), String> {
        check_sql(query, &self.hidden)
    }
}

/// Register without enabling any fallback implementation.
pub fn routes(app: Router<AppState>, _config: &crate::config::ServerConfig) -> Router<AppState> {
    app.route(ROUTE, post(fetch))
        .route(&format!("{ROUTE}/pages"), post(page))
        .route(&format!("{ROUTE}/replace"), post(replacement))
}

fn bad(message: impl Into<String>) -> ApiError {
    ApiError::new(400, message)
}
fn forbidden(message: impl Into<String>) -> ApiError {
    ApiError::new(403, message)
}

fn backend_error(error: SourceFetchError) -> ApiError {
    match error {
        SourceFetchError::InProgress => ApiError::new(
            409,
            "The replacement is still running; retry its same lease.",
        ),
        SourceFetchError::Uncertain => ApiError::new(
            410,
            "The replacement lease or completion receipt has expired; the previous commit may have completed.",
        ),
        SourceFetchError::Unsupported => ApiError::new(
            503,
            "A native SQL source broker is not available for this instance.",
        ),
        SourceFetchError::Unavailable => {
            ApiError::new(503, "The native SQL source is unavailable.")
        }
        SourceFetchError::TooLarge => ApiError::new(
            413,
            "The source fetch result exceeds its byte or column bound.",
        ),
        SourceFetchError::Forbidden => {
            forbidden("The final source query is not allowed by the SQL broker.")
        }
        SourceFetchError::Failed => {
            ApiError::new(422, "The canonical source query could not be read.")
        }
        SourceFetchError::Stale => {
            ApiError::new(409, "The source snapshot changed; start a fresh fetch.")
        }
        SourceFetchError::Closed => ApiError::new(
            410,
            "The source snapshot is unavailable or expired; start a fresh fetch.",
        ),
    }
}

fn served(state: &AppState, caller: &Access, keys: &str) -> Result<(), ApiError> {
    let (typ, _) = meerschaum_core::team_scope::api_execution::split_keys(keys).map_err(bad)?;
    if typ != "sql" {
        return Err(bad("This broker requires an exact native SQL instance."));
    }
    if (!state.config.allow_multiple_instances && keys != state.config.instance_keys)
        || !state
            .config
            .allowed_instance_keys
            .iter()
            .any(|pattern| crate::config::fnmatch(keys, pattern))
    {
        return Err(forbidden(
            "The SQL source instance is not served by this API.",
        ));
    }
    access::guard::check_connector_teams(state, caller, keys)
}

async fn native(state: &AppState, keys: &str) -> Result<Arc<dyn InstanceConnector>, ApiError> {
    let conn = state
        .source_fetch
        .native_instance(keys)
        .await
        .map_err(backend_error)?;
    if conn.connector_type() != "sql" || conn.connector_keys() != keys {
        return Err(bad(
            "The native source backend returned a different logical instance.",
        ));
    }
    Ok(conn)
}

async fn metadata(
    conn: &Arc<dyn InstanceConnector>,
    defaults: &meerschaum_core::pipe::PipeDefaults,
) -> Result<Vec<Pipe>, ApiError> {
    let registered = conn
        .registered_pipes(&PipesKeysQuery::default())
        .await
        .map_err(|_| ApiError::new(503, "Canonical source metadata is unavailable."))?;
    if registered.len() > MAX_METADATA_PIPES {
        return Err(ApiError::new(
            503,
            "The source metadata snapshot exceeds its bound.",
        ));
    }
    let mut total = 0usize;
    let mut pipes = Vec::with_capacity(registered.len());
    for (identity, entry) in registered {
        let mut pipe = Pipe::builder(&entry.connector_keys, &entry.metric_key)
            .location(entry.location_key.as_deref())
            .instance(&conn.connector_keys())
            .defaults(defaults.clone())
            .build()
            .map_err(|_| bad("Invalid canonical pipe metadata."))?;
        let parameters = match entry.extra {
            Some(Value::Map(map)) => map,
            Some(Value::Str(text)) => Value::from_json_str(&text)
                .ok()
                .and_then(|v| v.as_map().cloned())
                .ok_or_else(|| bad("Invalid canonical pipe parameters."))?,
            _ => conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|_| ApiError::new(503, "Canonical pipe parameters are unavailable."))?
                .get("parameters")
                .map(Value::to_map)
                .unwrap_or_default(),
        };
        let bytes = Value::Map(parameters.clone()).to_json_string().len();
        total = total.saturating_add(bytes);
        if bytes > MAX_PARAMETERS_BYTES || total > MAX_METADATA_BYTES {
            return Err(ApiError::new(
                503,
                "The source metadata snapshot exceeds its byte bound.",
            ));
        }
        pipe.set_parameters(parameters);
        pipe.set_registration_id(identity.map(|id| id.to_string()));
        pipes.push(pipe);
    }
    Ok(pipes)
}

fn hidden(conn: &dyn InstanceConnector, pipes: &[Pipe], caller: &Access) -> HiddenTables {
    HiddenTables::new(
        pipes
            .iter()
            .filter(|pipe| !caller.can_see_pipe(pipe))
            .flat_map(|pipe| access::pipe_targets_on(conn, pipe)),
    )
    .with_identifier_byte_limit(conn.sql_identifier_byte_limit())
}

fn canonical<'a>(pipes: &'a [Pipe], ck: &str, mk: &str, lk: Option<&str>) -> Option<&'a Pipe> {
    pipes.iter().find(|pipe| {
        pipe.connector_keys() == ck && pipe.metric_key() == mk && pipe.location_key() == lk
    })
}

/// No config reference anywhere in the dependency graph may be expanded, including an
/// innocently named field subsequently read by `{{self.parameters[...]}}`.
fn parameters_safe(parameters: &Map) -> Result<(), ApiError> {
    if Value::Map(parameters.clone())
        .to_json_string()
        .contains("MRSM{")
    {
        return Err(forbidden(
            "Config references are not available to the source broker.",
        ));
    }
    Ok(())
}

struct Snapshot {
    defaults: meerschaum_core::pipe::PipeDefaults,
    entries: Vec<(Pipe, String)>,
}

impl TemplateResolver for Snapshot {
    fn defaults(&self) -> meerschaum_core::pipe::PipeDefaults {
        self.defaults.clone()
    }
    fn load_pipe(&self, pipe: &Pipe) -> Pipe {
        self.entries
            .iter()
            .find(|(stored, _)| stored == pipe)
            .map_or_else(|| pipe.clone(), |(stored, _)| stored.clone())
    }
    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        self.entries
            .iter()
            .find(|(stored, _)| stored == pipe)
            .map(|(_, target)| target.clone())
            .ok_or_else(|| "This template was not in the authorized source snapshot.".into())
    }
}

fn resolve(
    state: &AppState,
    caller: &Access,
    conn: &dyn InstanceConnector,
    pipes: &[Pipe],
    root: &Pipe,
    source_hidden: &HiddenTables,
) -> Result<(Pipe, Vec<Pipe>), ApiError> {
    let defaults = state.config.pipe_defaults.clone();
    let mut snapshot = Snapshot {
        defaults: defaults.clone(),
        entries: Vec::new(),
    };
    let mut queue = vec![root.clone()];
    while let Some(pipe) = queue.pop() {
        if snapshot.entries.iter().any(|(stored, _)| stored == &pipe) {
            continue;
        }
        if snapshot.entries.len() >= MAX_REFERENCES {
            return Err(bad("The SQL source has too many template dependencies."));
        }
        if pipe.instance_keys() != root.instance_keys() {
            return Err(forbidden(
                "Cross-instance source templates are not supported by this broker.",
            ));
        }
        let stored = canonical(
            pipes,
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        )
        .ok_or_else(ApiError::not_found)?;
        if !caller.can_see_pipe(stored) {
            return Err(ApiError::not_found());
        }
        parameters_safe(stored.raw_parameters())?;
        let target = conn
            .pipe_table_reference(stored)
            .ok_or_else(|| bad("The source template has no native table reference."))?;
        let mut dependencies =
            referenced_pipes_in_parameters(stored.raw_parameters(), Some(stored), &defaults);
        let inherited = stored
            .references(&defaults)
            .map_err(|_| bad("Invalid canonical source inheritance."))?;
        dependencies.extend(inherited.into_iter().flatten());
        for dependency in dependencies {
            if dependency.instance_keys() != root.instance_keys()
                || !dependency.raw_parameters().is_empty()
            {
                return Err(forbidden(
                    "Source templates require exact same-instance registered identities without parameter overrides.",
                ));
            }
            queue.push(dependency);
        }
        snapshot.entries.push((stored.clone(), target));
    }
    for (pipe, _) in &snapshot.entries {
        for definition in sql_guard::definitions(pipe.raw_parameters()) {
            // Before expansion the SQL/table guard runs on the original content. Function
            // validation follows expansion, so `{{Pipe(...)}}` is not treated as a SQL call.
            sql_guard::check_definition(&definition, source_hidden).map_err(forbidden)?;
        }
    }
    let mut resolved = root.clone();
    let warnings = resolved
        .resolve_parameters_with(&snapshot, None)
        .map_err(|_| bad("The canonical SQL source templates could not be resolved."))?;
    if !warnings.is_empty() {
        return Err(bad(
            "The canonical SQL source templates did not resolve completely.",
        ));
    }
    let mut parameters = resolved.parameters().clone();
    parameters_safe(&parameters)?;
    if Value::Map(parameters.clone())
        .to_json_string()
        .contains("{{")
    {
        return Err(bad(
            "The canonical SQL source contains unresolved templates.",
        ));
    }
    for definition in sql_guard::definitions(&parameters) {
        check_sql(&definition, source_hidden).map_err(forbidden)?;
    }
    if sql_guard::definitions(&parameters).is_empty() {
        return Err(bad("The registered SQL source has no definition."));
    }
    parameters.shift_remove("reference");
    parameters.shift_remove("references");
    resolved.set_parameters(parameters);
    Ok((
        resolved,
        snapshot.entries.into_iter().map(|(pipe, _)| pipe).collect(),
    ))
}

fn check_sql(query: &str, hidden: &HiddenTables) -> Result<(), String> {
    crate::mcp::security::is_read_only_query(query)?;
    sql_guard::check_definition(query, hidden)?;
    let tokens = sql_guard::tokens(query);
    // Database file/network readers and user-defined functions can escape table-name guards.
    // This broker deliberately accepts only a small set of ordinary pure SQL functions.
    const PURE: &[&str] = &[
        "abs",
        "avg",
        "cast",
        "ceil",
        "ceiling",
        "char_length",
        "coalesce",
        "concat",
        "concat_ws",
        "convert",
        "count",
        "date",
        "datetime",
        "dense_rank",
        "extract",
        "floor",
        "ifnull",
        "julianday",
        "lag",
        "lead",
        "length",
        "lower",
        "ltrim",
        "max",
        "min",
        "nullif",
        "rank",
        "replace",
        "round",
        "row_number",
        "rtrim",
        "strftime",
        "substr",
        "substring",
        "sum",
        "time",
        "trim",
        "unixepoch",
        "upper",
        "in",
        "exists",
        "as",
        "over",
        "filter",
        "when",
        "not",
        "by",
        "values",
        "from",
        "where",
        "select",
        "with",
        "and",
        "or",
        "top",
        "char",
        "varchar",
        "nvarchar",
        "nchar",
        "decimal",
        "numeric",
        "number",
        "varbinary",
        "binary",
        "timestamp",
        "timestamptz",
        "timetz",
        "datetime2",
        "datetimeoffset",
    ];
    for (index, token) in tokens.iter().enumerate() {
        if let sql_guard::Token::Ident { name, .. } = token {
            let name = name.to_ascii_lowercase();
            if matches!(tokens.get(index + 1), Some(sql_guard::Token::Punct('(')))
                && (!PURE.contains(&name.as_str())
                    || matches!(
                        index.checked_sub(1).and_then(|i| tokens.get(i)),
                        Some(sql_guard::Token::Punct('.'))
                    ))
            {
                return Err(
                    "The source broker does not allow this SQL function or table function.".into(),
                );
            }
            if matches!(name.as_str(), "from" | "join")
                && matches!(tokens.get(index + 1), Some(sql_guard::Token::Str(_)))
            {
                return Err("The source broker does not read file paths as SQL tables.".into());
            }
        }
    }
    Ok(())
}

fn owner(caller: &Access) -> Result<String, ApiError> {
    use crate::auth::Principal;
    match caller.auth.as_ref() {
        Some(Principal::Execution(identity)) => Ok(format!(
            "execution:{}:{}",
            identity.grant.id, identity.grant.launch_id
        )),
        Some(Principal::Token(token)) => token
            .id
            .map(|id| format!("token:{id}"))
            .ok_or_else(|| forbidden("The source caller has no registered identity.")),
        Some(Principal::User(user)) => user
            .user_id
            .as_ref()
            .filter(|id| !id.is_null())
            .map(|id| {
                format!(
                    "user:{}:{}",
                    id.py_str(),
                    caller.credential.as_deref().unwrap_or_default()
                )
            })
            .ok_or_else(|| forbidden("The source caller has no registered identity.")),
        None => Err(forbidden(
            "The canonical source broker requires authentication.",
        )),
    }
}

fn credential(ctx: &Ctx) -> String {
    crate::auth::credential_fingerprint(
        ctx.state.secret(),
        ctx.headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
    )
}

fn digest(value: &impl serde::Serialize) -> Result<String, ApiError> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value).map_err(|_| ApiError::internal())?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn source_parameters(pipe: &Pipe) -> Map {
    let mut parameters = pipe.raw_parameters().clone();
    // Inferred destination dtypes may be persisted between chunks of this same source.
    // The native backend separately binds the exact generated source SQL, including any
    // dtype-sensitive predicate or cast. Resolved templates remain in this proof.
    parameters.shift_remove("dtypes");
    parameters
}

fn metadata_proof(pipes: &[Pipe], root: &Pipe) -> Vec<(String, Option<String>, Map)> {
    let mut proof: Vec<_> = pipes
        .iter()
        .map(|pipe| {
            (
                pipe.pipe_ref(),
                pipe.registration_id(),
                if pipe.pipe_ref() == root.pipe_ref() {
                    source_parameters(pipe)
                } else {
                    pipe.raw_parameters().clone()
                },
            )
        })
        .collect();
    proof.sort_by(|a, b| a.0.cmp(&b.0));
    proof
}

fn receipt_metadata(pipes: &[Pipe], root: &Pipe) -> Vec<(String, Option<String>, Map)> {
    let mut proof = metadata_proof(pipes, root);
    if let Some((_, _, parameters)) = proof
        .iter_mut()
        .find(|(identity, _, _)| identity == &root.pipe_ref())
    {
        parameters.shift_remove("autoincrement");
    }
    proof
}

fn validate_route(
    ctx: &Ctx,
    request: &Request,
    ck: &str,
    mk: &str,
    lk: &str,
) -> Result<(), ApiError> {
    request.validate().map_err(bad)?;
    if request.pipe.connector_keys != ck
        || request.pipe.metric_key != mk
        || request.pipe.location_key != crate::state::normalize_location(Some(lk))
        || ctx
            .query
            .opt_str("instance_keys")
            .is_some_and(|keys| keys != request.instance_keys)
    {
        return Err(bad(
            "The source fetch identity does not match its route or instance.",
        ));
    }
    Ok(())
}

fn registered_table(
    conn: &dyn InstanceConnector,
    attributes: Option<&Value>,
    pipe: &Pipe,
    hidden: &HiddenTables,
) -> Result<RegisteredTable, ApiError> {
    let record = pipe.registration_id().ok_or_else(ApiError::not_found)?;
    let registration = record
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(ApiError::not_found)?;
    let config = attributes
        .and_then(|value| value.get("registered_source_tables"))
        .and_then(|approved| approved.get(&record))
        .ok_or_else(ApiError::not_found)?;
    let label = |name: &str| {
        config.get(name).and_then(Value::as_str).filter(|name| {
            !name.is_empty()
                && name.len() <= 63
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
    };
    let schema = label("schema").ok_or_else(ApiError::not_found)?;
    let table = label("table").ok_or_else(ApiError::not_found)?;
    let parameters = pipe.raw_parameters();
    parameters_safe(parameters)?;
    if parameters.get("target").and_then(Value::as_str) != Some(table)
        || conn.pipe_table_schema(pipe).as_deref() != Some(schema)
        || !sql_guard::definitions(parameters).is_empty()
        || parameters.contains_key("reference")
        || parameters.contains_key("references")
        || Value::Map(parameters.clone())
            .to_json_string()
            .contains("{{")
    {
        return Err(forbidden(
            "The registered physical source differs from its approved table.",
        ));
    }
    access::guard::check_pipe_target(conn, pipe, hidden)?;
    Ok(RegisteredTable {
        registration,
        schema: schema.into(),
        table: table.into(),
    })
}

async fn prepare(ctx: &Ctx, request: Request) -> Result<Plan, ApiError> {
    prepare_scoped(ctx, request, false)
        .await
        .map(|(plan, _)| plan)
}

/// Final resource metadata is bounded and loaded before the last original-caller reload.
/// The remaining canonical resolution and destination checks are synchronous.
async fn prepare_scoped(
    ctx: &Ctx,
    request: Request,
    replace: bool,
) -> Result<(Plan, Option<Pipe>), ApiError> {
    if ctx.state.config.no_auth {
        return Err(forbidden(
            "The canonical source broker requires authentication.",
        ));
    }
    let original = crate::executor::pipe_operation::ReadRequest::capture(ctx, &[]).await?;
    let admitted = original.source(ctx, &request, replace).await?;
    let caller = admitted.caller;
    served(&ctx.state, &caller, &request.instance_keys)?;
    served(&ctx.state, &caller, &request.source_keys)?;
    access::guard::check_source_connector(&ctx.state, &caller, &request.source_keys)?;
    let home = native(&ctx.state, &request.instance_keys).await?;
    let source = if request.source_keys == request.instance_keys {
        home.clone()
    } else {
        native(&ctx.state, &request.source_keys).await?
    };
    let home_pipes = metadata(&home, &ctx.state.config.pipe_defaults).await?;
    let stored = canonical(
        &home_pipes,
        &request.pipe.connector_keys,
        &request.pipe.metric_key,
        request.pipe.location_key.as_deref(),
    )
    .ok_or_else(ApiError::not_found)?;
    if !caller.can_see_pipe(stored) {
        return Err(ApiError::not_found());
    }
    let source_pipes = if request.source_keys == request.instance_keys {
        home_pipes.clone()
    } else {
        metadata(&source, &ctx.state.config.pipe_defaults).await?
    };
    let source_hidden = hidden(source.as_ref(), &source_pipes, &caller);
    let table_source =
        source.supports_registered_source_table() || home.supports_registered_source_table();
    if table_source {
        if replace || request.source_keys != request.instance_keys {
            return Err(backend_error(SourceFetchError::Unsupported));
        }
    } else {
        resolve(
            &ctx.state,
            &caller,
            home.as_ref(),
            &home_pipes,
            stored,
            &source_hidden,
        )?;
    }
    // Re-read stored ACL and parameter snapshots. The final identity load follows every
    // metadata await so revocation during the last read is observed before SQL is admitted.
    let home_pipes = metadata(&home, &ctx.state.config.pipe_defaults).await?;
    let source_pipes = if request.source_keys == request.instance_keys {
        home_pipes.clone()
    } else {
        metadata(&source, &ctx.state.config.pipe_defaults).await?
    };
    let admitted = original.source(ctx, &request, replace).await?;
    let caller = admitted.caller;
    let window = admitted.window;
    served(&ctx.state, &caller, &request.instance_keys)?;
    served(&ctx.state, &caller, &request.source_keys)?;
    access::guard::check_source_connector(&ctx.state, &caller, &request.source_keys)?;
    let stored = canonical(
        &home_pipes,
        &request.pipe.connector_keys,
        &request.pipe.metric_key,
        request.pipe.location_key.as_deref(),
    )
    .ok_or_else(ApiError::not_found)?;
    if !caller.can_see_pipe(stored) {
        return Err(ApiError::not_found());
    }
    let source_hidden = hidden(source.as_ref(), &source_pipes, &caller);
    let connectors = ctx.state.connectors();
    let attributes = |keys: &str| {
        keys.split_once(':')
            .and_then(|(typ, label)| connectors.get(typ)?.get(label))
            .cloned()
    };
    let (pipe, dependencies, table) = if table_source {
        let table = registered_table(
            source.as_ref(),
            attributes(&request.source_keys).as_ref(),
            stored,
            &source_hidden,
        )?;
        (stored.clone(), vec![stored.clone()], Some(table))
    } else {
        let (pipe, dependencies) = resolve(
            &ctx.state,
            &caller,
            home.as_ref(),
            &home_pipes,
            stored,
            &source_hidden,
        )?;
        (pipe, dependencies, None)
    };
    let destination = if replace {
        if stored.registration_id().is_none() {
            return Err(ApiError::not_found());
        }
        let home_hidden = hidden(home.as_ref(), &home_pipes, &caller);
        super::pipes::check_data_destination(&caller, home.as_ref(), stored, &home_hidden)?;
        // Templates may change the actual target; it must pass the same hidden/internal
        // table guard as the canonical raw registration.
        access::guard::check_pipe_target(home.as_ref(), &pipe, &home_hidden)?;
        if let Some(error) = access::guard::lock_conflict(stored, access::rules::LockOp::Replace) {
            return Err(error);
        }
        Some(stored.clone())
    } else {
        None
    };
    let binding = Binding {
        owner: owner(&caller)?,
        credential: credential(ctx),
        fingerprint: digest(&(
            &request,
            &window,
            metadata_proof(&dependencies, stored),
            source_parameters(&pipe),
            table.as_ref().map(|_| pipe.raw_parameters()),
            &table,
            attributes(&request.source_keys),
            attributes(&request.instance_keys),
        ))?,
    };
    let receipt_fingerprint = digest(&(
        &request,
        &window,
        receipt_metadata(&dependencies, stored),
        attributes(&request.source_keys),
        attributes(&request.instance_keys),
    ))?;
    Ok((
        Plan {
            request,
            pipe,
            hidden: source_hidden,
            table,
            binding,
            receipt_fingerprint,
        },
        destination,
    ))
}

struct ReplacementParts {
    source: Plan,
    destination: Pipe,
    binding: Binding,
    request_identity: String,
}
async fn replacement_parts(
    ctx: &Ctx,
    request: &meerschaum_connector_api::source_replace::Request,
) -> Result<ReplacementParts, ApiError> {
    let (source, destination) = prepare_scoped(ctx, request.source.clone(), true).await?;
    let destination = destination.ok_or_else(ApiError::not_found)?;
    let binding = Binding {
        owner: source.binding().owner.clone(),
        credential: source.binding().credential.clone(),
        fingerprint: digest(&(
            source.binding().fingerprint.clone(),
            destination.registration_id(),
            destination.raw_parameters(),
            request.enforce_dtypes,
        ))?,
    };
    let request_identity = digest(&(
        source.receipt_fingerprint.clone(),
        destination.registration_id(),
        source
            .pipe()
            .target(None)
            .map_err(|_| bad("Invalid canonical destination target."))?,
        request.enforce_dtypes,
        &binding.owner,
    ))?;
    Ok(ReplacementParts {
        source,
        destination,
        binding,
        request_identity,
    })
}

#[derive(Clone)]
struct ReplacementAuthority {
    ctx: Arc<Ctx>,
    request: meerschaum_connector_api::source_replace::Request,
    expected: String,
    receipt: bool,
    destination: Pipe,
    request_identity: String,
    data: Arc<dyn crate::pipe_methods::SyncAdmission>,
}
#[async_trait::async_trait]
impl meerschaum_connector::mutation::MutationCheck for ReplacementAuthority {
    fn native_batch(&self) -> Option<meerschaum_connector::mutation::NativeBatch> {
        self.data.native_batch()
    }
    async fn write_window(
        &self,
    ) -> Result<Option<meerschaum_connector::mutation::DataWindow>, String> {
        self.check().await?;
        let conn = self
            .ctx
            .state
            .get_api_connector(Some(self.destination.instance_keys()))
            .map_err(|e| e.detail_text())?;
        self.data
            .write_window(&conn, &self.destination)
            .await
            .map_err(|e| e.detail_text())
    }
    async fn prepare_data_commit(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<Option<Arc<dyn meerschaum_connector::mutation::PendingDataCommit>>, String> {
        self.check().await?;
        let conn = self
            .ctx
            .state
            .get_api_connector(Some(self.destination.instance_keys()))
            .map_err(|e| e.detail_text())?;
        Ok(self
            .data
            .prepare_data_commit(&conn, &self.destination, effect)
            .await
            .map_err(|e| e.detail_text())?
            .map(|inner| {
                Arc::new(ReplacementPending {
                    authority: self.clone(),
                    inner,
                }) as Arc<dyn meerschaum_connector::mutation::PendingDataCommit>
            }))
    }
    fn tracks_metadata_inference(&self) -> bool {
        !self.receipt
    }

    async fn metadata_inferred(
        &self,
        change: meerschaum_connector::mutation::MetadataInference,
    ) -> Result<(), String> {
        change.validate()?;
        if self.receipt
            || change.pipe.pipe_ref() != self.destination.pipe_ref()
            || change.before != *self.destination.raw_parameters()
            || self.destination.registration_id().as_deref()
                != Some(change.registration.py_str().as_str())
        {
            return Err(
                "Native replacement inference differs from its captured destination.".into(),
            );
        }
        let conn = self
            .ctx
            .state
            .get_api_connector(Some(self.destination.instance_keys()))
            .map_err(|error| error.detail_text())?;
        let attributes = conn
            .get_pipe_attributes(&change.pipe)
            .await
            .map_err(|_| "Could not confirm committed replacement inference.")?;
        if attributes.get("pipe_id") != Some(&change.registration)
            || crate::state::parameters_of(&attributes).as_ref() != Some(&change.after)
        {
            return Err(
                "Native replacement inference no longer matches its registered metadata.".into(),
            );
        }
        // The old full operation snapshot deliberately cannot authorize after its own
        // metadata commit. Reconstruct the original source and validate only this exact
        // native transition before Main's private canonical CAS advances that member.
        let caller = access::require_fresh(&self.ctx.state, &self.ctx.headers, &["pipes:write"])
            .await
            .map_err(|error| error.detail_text())?;
        access::guard::check_parameters(
            &self.ctx.state,
            &caller,
            &conn,
            &change.pipe,
            Some(&change.before),
            &change.after,
        )
        .await
        .map_err(|error| error.detail_text())?;
        self.ctx
            .state
            .actions
            .accept_pipe_inference(&caller, change)
            .await?;
        let fresh = replacement_parts(&self.ctx, &self.request)
            .await
            .map_err(|error| error.detail_text())?;
        if fresh.request_identity != self.request_identity {
            return Err(
                "The original replacement source or destination changed after commit.".into(),
            );
        }
        Ok(())
    }

    async fn check(&self) -> Result<(), String> {
        let fresh = replacement_parts(&self.ctx, &self.request)
            .await
            .map_err(|error| {
                let authorization = self
                    .ctx
                    .headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                let token = authorization.strip_prefix("Bearer ").unwrap_or_default();
                let now = chrono::Utc::now();
                let credential = match crate::auth::jwt_decode(token, self.ctx.state.secret(), now)
                {
                    Ok(payload) => format!(
                        "signed JWT valid; {} seconds remaining",
                        payload
                            .get("exp")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(now.timestamp())
                            - now.timestamp()
                    ),
                    Err(crate::auth::JwtError::Expired) => "signed JWT expired".into(),
                    Err(_) => "signed JWT validation failed".into(),
                };
                format!("{} ({credential})", error.detail_text())
            })?;
        let identity = if self.receipt {
            &fresh.request_identity
        } else {
            &fresh.binding.fingerprint
        };
        if identity != &self.expected {
            return Err("The canonical source or destination changed during replacement.".into());
        }
        Ok(())
    }
}

struct ReplacementPending {
    authority: ReplacementAuthority,
    inner: Arc<dyn meerschaum_connector::mutation::PendingDataCommit>,
}
#[async_trait::async_trait]
impl meerschaum_connector::mutation::PendingDataCommit for ReplacementPending {
    async fn before_commit(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<(), String> {
        self.authority.check().await?;
        self.inner.before_commit(effect).await
    }
    async fn committed(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<(), String> {
        self.inner.committed(effect).await
    }
    async fn acknowledge(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<(), String> {
        let mut receipt = self.authority.clone();
        receipt.receipt = true;
        receipt.expected = receipt.request_identity.clone();
        receipt.check().await?;
        self.inner.acknowledge(effect).await
    }
    async fn rolled_back(&self) -> Result<(), String> {
        self.inner.rolled_back().await
    }
}
fn replacement_plan(
    ctx: &Ctx,
    request: &meerschaum_connector_api::source_replace::Request,
    parts: ReplacementParts,
    batch: Option<Arc<super::graph_batch::WriteBatch>>,
) -> ReplacementPlan {
    let copied = Ctx {
        state: ctx.state.clone(),
        headers: ctx.headers.clone(),
        query: ctx.query.clone(),
        team_instance: Default::default(),
        engine_selection: Default::default(),
    };
    let copied = Arc::new(copied);
    let authority = Arc::new(ReplacementAuthority {
        ctx: copied.clone(),
        request: request.clone(),
        expected: parts.binding.fingerprint.clone(),
        receipt: false,
        destination: parts.destination.clone(),
        request_identity: parts.request_identity.clone(),
        data: super::pipes::replacement_admission(
            &ctx.state,
            ctx.headers.clone(),
            &parts.destination,
            batch.clone(),
        ),
    });
    let receipt_authority = Arc::new(ReplacementAuthority {
        ctx: copied,
        request: request.clone(),
        expected: parts.request_identity.clone(),
        receipt: true,
        destination: parts.destination.clone(),
        request_identity: parts.request_identity.clone(),
        data: super::pipes::replacement_admission(
            &ctx.state,
            ctx.headers.clone(),
            &parts.destination,
            batch,
        ),
    });
    ReplacementPlan {
        source: parts.source,
        destination: parts.destination,
        binding: parts.binding,
        authority,
        receipt_authority,
        enforce_dtypes: request.enforce_dtypes,
        request_identity: parts.request_identity,
    }
}
fn replacement_error(error: SourceFetchError) -> ApiError {
    match error {
        SourceFetchError::Unsupported => ApiError::new(
            503,
            "Atomic source replacement is not supported for this instance.",
        ),
        SourceFetchError::Forbidden => {
            forbidden("The source or destination no longer permits this replacement.")
        }
        other => backend_error(other),
    }
}

async fn replacement(
    ctx: Ctx,
    Path((ck, mk, lk)): Path<(String, String, String)>,
    body: Body,
) -> ApiResult {
    use meerschaum_connector_api::source_replace::{self, Phase};
    let batch_request = super::graph_batch::WriteRequest::capture(&ctx).await?;
    let bytes = super::body_bytes(body, Some(source_fetch::MAX_REQUEST_BYTES)).await?;
    let request: source_replace::Request =
        serde_json::from_slice(&bytes).map_err(|_| bad("Invalid source replacement request."))?;
    request.validate().map_err(bad)?;
    validate_route(&ctx, &request.source, &ck, &mk, &lk)?;
    if batch_request.is_some() && request.phase != Phase::Commit {
        return Err(bad(
            "A native batch selector is valid only on replacement commit.",
        ));
    }
    let cleanup_owner = access::require_fresh(&ctx.state, &ctx.headers, &[])
        .await
        .ok()
        .and_then(|caller| owner(&caller).ok())
        .or_else(|| {
            crate::executor::callbacks::current(&ctx.headers).and_then(|caller| owner(&caller).ok())
        });
    let credential = credential(&ctx);
    let close = |id: String, owner: Option<String>, credential: String| {
        let backend = ctx.state.source_fetch.clone();
        async move {
            backend
                .close_replacement(&id, owner.as_deref(), &credential)
                .await
        }
    };
    if request.phase == Phase::Close {
        close(
            request.id.clone().expect("validated lease"),
            cleanup_owner,
            credential,
        )
        .await
        .map_err(replacement_error)?;
        return Ok(crate::json::ok_json(
            &serde_json::to_value(source_replace::Response::Closed)
                .map_err(|_| ApiError::internal())?,
        ));
    }
    let parts = match replacement_parts(&ctx, &request).await {
        Ok(parts) => parts,
        Err(error) => {
            // An uncertain captured commit may still own its frozen snapshot/receipt.
            // Automatic cleanup here must not cancel or erase that original result.
            if batch_request.is_none()
                && let Some(id) = &request.id
            {
                let _ = close(id.clone(), cleanup_owner, credential).await;
            }
            return Err(error);
        }
    };
    let caller = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    if let Some(read) = ctx
        .state
        .actions
        .pipe_operation_read(&caller)
        .await
        .map_err(|reason| ApiError::new(409, reason))?
    {
        if request.phase == Phase::Commit && batch_request.is_none() {
            return Err(ApiError::new(
                409,
                "A captured graph replacement commit requires its Main-issued batch selector.",
            ));
        }
        if batch_request.is_none() && read.pending_nodes.contains(&parts.destination.pipe_ref()) {
            return Err(ApiError::new(
                409,
                "This replacement node has an unfinished native commit; inspect its original operation.",
            ));
        }
    }
    let expected = parts.binding.clone();
    let batch = match batch_request {
        Some(request) => Some(
            request
                .claim(
                    &ctx,
                    &parts.destination.pipe_ref(),
                    "source_replace",
                    &bytes,
                )
                .await?,
        ),
        None => None,
    };
    let plan = replacement_plan(&ctx, &request, parts, batch.clone());
    let response = match request.phase {
        Phase::Prepare => {
            let response = ctx
                .state
                .source_fetch
                .prepare_replacement(plan)
                .await
                .map_err(replacement_error)?;
            let source_replace::Response::Prepared { id, .. } = &response else {
                return Err(ApiError::internal());
            };
            let refreshed = replacement_parts(&ctx, &request).await;
            if refreshed.as_ref().is_err()
                || refreshed.as_ref().is_ok_and(|parts| {
                    parts.binding.fingerprint != expected.fingerprint
                        || parts.binding.owner != expected.owner
                })
            {
                let _ = close(id.clone(), Some(expected.owner), credential).await;
                return Err(refreshed
                    .err()
                    .unwrap_or_else(|| backend_error(SourceFetchError::Stale)));
            }
            response
        }
        Phase::Commit => {
            let id = request.id.as_deref().expect("validated lease");
            let result = ctx
                .state
                .source_fetch
                .commit_replacement(plan, id)
                .await
                .map_err(replacement_error)?;
            // Cache refresh cannot turn a committed transaction into a failed operation.
            ctx.state.access_cache().forget_pipes();
            let _ = ctx
                .state
                .pipes(Some(&request.source.instance_keys), true)
                .await;
            source_replace::Response::Committed { result }
        }
        Phase::Close => unreachable!(),
    };
    let mut writer = Bounded {
        bytes: Vec::new(),
        limit: source_replace::MAX_RESPONSE_BYTES,
    };
    if let Some(batch) = batch {
        let receipt = batch.receipt(&ctx.state).await?;
        serde_json::to_writer(
            &mut writer,
            &serde_json::json!({"result": response, "receipt": receipt}),
        )
    } else {
        serde_json::to_writer(&mut writer, &response)
    }
    .map_err(|_| backend_error(SourceFetchError::TooLarge))?;
    Ok(crate::json::raw_json(
        axum::http::StatusCode::OK,
        String::from_utf8(writer.bytes).map_err(|_| ApiError::internal())?,
    ))
}

async fn page(
    ctx: Ctx,
    Path((ck, mk, lk)): Path<(String, String, String)>,
    body: Body,
) -> ApiResult {
    // Revoked credentials may still close their own snapshot below; retain an
    // initial proof (or its error) without changing that cleanup contract.
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await;
    let bytes = super::body_bytes(body, Some(source_fetch::MAX_REQUEST_BYTES)).await?;
    let request: source_fetch::PageRequest =
        serde_json::from_slice(&bytes).map_err(|_| bad("Invalid source snapshot request."))?;
    request.validate().map_err(bad)?;
    validate_route(&ctx, &request.request, &ck, &mk, &lk)?;
    let cleanup_owner = access::require_fresh(&ctx.state, &ctx.headers, &[])
        .await
        .ok()
        .and_then(|caller| owner(&caller).ok())
        .or_else(|| {
            crate::executor::callbacks::current(&ctx.headers).and_then(|caller| owner(&caller).ok())
        });
    let credential = credential(&ctx);
    if request.close {
        ctx.state
            .source_fetch
            .close_snapshot(
                request.cursor.as_ref().expect("validated close"),
                cleanup_owner.as_deref(),
                &credential,
            )
            .await
            .map_err(backend_error)?;
        return Ok(crate::json::ok_json(&serde_json::json!({"success":true})));
    }
    let original = original?;
    let original_request = request.request.clone();
    let plan = match prepare(&ctx, request.request).await {
        Ok(plan) => plan,
        Err(error) => {
            if let Some(cursor) = &request.cursor {
                let _ = ctx
                    .state
                    .source_fetch
                    .close_snapshot(cursor, cleanup_owner.as_deref(), &credential)
                    .await;
            }
            return Err(error);
        }
    };
    let expected = plan.binding().clone();
    let result = ctx
        .state
        .source_fetch
        .page(plan, request.cursor.clone())
        .await
        .map_err(backend_error)?;
    // Snapshot construction and file/connection waits do not retain an earlier caller's
    // authority. Recheck immediately before releasing any page data to the HTTP client.
    let refreshed = prepare(&ctx, original_request.clone()).await;
    if refreshed.is_err()
        || refreshed.as_ref().is_ok_and(|plan| {
            plan.binding().owner != expected.owner
                || plan.binding().fingerprint != expected.fingerprint
        })
    {
        if let Some(cursor) = result.response.cursor.as_ref().or(request.cursor.as_ref()) {
            let _ = ctx
                .state
                .source_fetch
                .close_snapshot(cursor, Some(&expected.owner), &credential)
                .await;
        }
        return Err(refreshed
            .err()
            .unwrap_or_else(|| backend_error(SourceFetchError::Stale)));
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        limit: source_fetch::MAX_PAGE_BYTES,
    };
    serde_json::to_writer(&mut writer, &result.response)
        .map_err(|_| backend_error(SourceFetchError::TooLarge))?;
    original.source(&ctx, &original_request, false).await?;
    let payload = PagePayload {
        bytes: writer.bytes,
        _lease: result.lease,
    };
    Ok(axum::response::IntoResponse::into_response((
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        bytes::Bytes::from_owner(payload),
    )))
}

struct PagePayload {
    bytes: Vec<u8>,
    _lease: Box<dyn std::any::Any + Send + Sync>,
}
impl AsRef<[u8]> for PagePayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

async fn fetch(
    ctx: Ctx,
    Path((ck, mk, lk)): Path<(String, String, String)>,
    body: Body,
) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await?;
    let bytes = super::body_bytes(body, Some(source_fetch::MAX_REQUEST_BYTES)).await?;
    let request: Request = serde_json::from_slice(&bytes)
        .map_err(|_| bad("Invalid canonical source fetch request."))?;
    validate_route(&ctx, &request, &ck, &mk, &lk)?;
    let plan = prepare(&ctx, request.clone()).await?;
    let expected = plan.binding().clone();
    let result = ctx
        .state
        .source_fetch
        .fetch(plan)
        .await
        .map_err(backend_error)?;
    if result.version != source_fetch::VERSION
        || result.rows.len() > source_fetch::MAX_ROWS
        || result.dtypes.len() > source_fetch::MAX_COLUMNS
    {
        return Err(backend_error(SourceFetchError::TooLarge));
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        limit: source_fetch::MAX_RESPONSE_BYTES,
    };
    serde_json::to_writer(&mut writer, &result)
        .map_err(|_| backend_error(SourceFetchError::TooLarge))?;
    let text = String::from_utf8(writer.bytes).map_err(|_| ApiError::internal())?;
    let refreshed = prepare(&ctx, request.clone()).await?;
    if refreshed.binding().owner != expected.owner
        || refreshed.binding().fingerprint != expected.fingerprint
    {
        return Err(backend_error(SourceFetchError::Stale));
    }
    original.source(&ctx, &request, false).await?;
    Ok(crate::json::raw_json(axum::http::StatusCode::OK, text))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_sql_never_reaches_internal_tables_or_external_functions() {
        let hidden = HiddenTables::new(["sibling_private"]);
        for query in [
            "SELECT * FROM mrsm_users",
            "SELECT * FROM sibling_private",
            "SELECT readfile('/secret')",
            "SELECT * FROM read_csv('/secret')",
            "SELECT * FROM '/secret.csv'",
            "SELECT dangerous_user_function()",
            "SELECT public.count(*) FROM safe",
        ] {
            assert!(check_sql(query, &hidden).is_err(), "{query}");
        }
        check_sql(
            "SELECT count(*), max(id) FROM safe WHERE id IN (1, 2)",
            &hidden,
        )
        .unwrap();
        check_sql(
            "WITH definition AS (SELECT id, created FROM safe) SELECT * FROM definition \
             WHERE created >= CAST('2026-01-02 00:00:00+00:00' AS TIMESTAMPTZ(9)) \
             AND created < CAST('2026-01-04 00:00:00+00:00' AS TIMESTAMPTZ(9)) \
             AND id IN ('2', '3')",
            &hidden,
        )
        .unwrap();
    }
}
