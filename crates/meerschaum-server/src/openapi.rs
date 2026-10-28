//! The OpenAPI document (`/openapi.json`), as a `utoipa::openapi::OpenApi`.
//!
//! The operation table below lists every route with the same tags, parameters (name, location,
//! requiredness, type) and request bodies as the v4 FastAPI document, plus the v5 `/engine/*`
//! family. FastAPI quirks are kept: routes whose auth dependency is `None` when the server is
//! not private expose a `curr_user` query parameter, and the `sync_time` / `rowcount` routes take
//! an optional JSON body for `params`. `tests/openapi_parity.rs` diffs this table against the
//! recorded Python document.

use serde_json::{Value as Json, json};
use utoipa::openapi::path::{HttpMethod, OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::schema::{AnyOfBuilder, ObjectBuilder, Schema, SchemaType, Type};
use utoipa::openapi::{ContentBuilder, OpenApi, Paths, Required, ResponseBuilder};

use crate::config::ServerConfig;

/// When FastAPI exposes the `curr_user` dependency as a query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrUser {
    /// Never (the route always authenticates, or never does).
    Never,
    /// `... if private else None`: a query parameter unless `--private`.
    UnlessPrivate,
    /// `... if not no_auth else None`: a query parameter under `--no-auth`.
    IfNoAuth,
}

/// One parameter: `(name, in, required, type)` where the type is a `|`-joined set of JSON
/// schema types (`any` for an untyped parameter).
pub type Param = (&'static str, &'static str, bool, &'static str);

/// One operation of the document.
#[derive(Debug, Clone)]
pub struct Operation {
    /// The HTTP method (lower case).
    pub method: &'static str,
    /// The path template.
    pub path: String,
    /// The route group (`api:permissions:routes:allowlist`), or `login` / `docs`.
    pub group: &'static str,
    /// The tags.
    pub tags: Vec<&'static str>,
    /// The parameters.
    pub params: Vec<Param>,
    /// The request body: content types and whether it is required.
    pub body: Option<(Vec<&'static str>, bool)>,
    /// The `curr_user` quirk.
    pub curr_user: CurrUser,
    /// The summary (FastAPI derives it from the function name).
    pub summary: &'static str,
    /// What the route does (the v5 routes; v4's have none, as FastAPI derived none).
    pub description: Option<&'static str>,
    /// The OAuth2 scopes the route requires (listed as its `security`; the v5 routes).
    pub scopes: Vec<&'static str>,
}

impl Operation {
    /// This operation with a description and the scopes it requires.
    pub fn described(mut self, description: &'static str, scopes: &[&'static str]) -> Operation {
        self.description = Some(description);
        self.scopes = scopes.to_vec();
        self
    }
}

const IK: Param = ("instance_keys", "query", false, "null|string");
const CK: Param = ("connector_keys", "path", true, "string");
const MK: Param = ("metric_key", "path", true, "string");
const LK: Param = ("location_key", "path", true, "string");
const BEGIN: Param = ("begin", "query", false, "integer|null|string");
const END: Param = ("end", "query", false, "integer|null|string");
const PARAMS: Param = ("params", "query", false, "null|string");
const NAME: Param = ("name", "path", true, "string");
const USERNAME: Param = ("username", "path", true, "string");
const TOKEN_ID: Param = ("token_id", "path", true, "string");
const JSON_BODY: &str = "application/json";
const FORM_BODY: &str = "application/x-www-form-urlencoded";

#[allow(clippy::too_many_arguments)]
fn op(
    method: &'static str,
    path: &str,
    group: &'static str,
    tags: &[&'static str],
    summary: &'static str,
    params: &[Param],
    body: Option<(&[&'static str], bool)>,
    curr_user: CurrUser,
) -> Operation {
    Operation {
        method,
        path: path.to_string(),
        group,
        tags: tags.to_vec(),
        params: params.to_vec(),
        body: body.map(|(c, r)| (c.to_vec(), r)),
        curr_user,
        summary,
        description: None,
        scopes: Vec::new(),
    }
}

fn pipe_op(
    method: &'static str,
    suffix: &'static str,
    tag: &'static str,
    summary: &'static str,
    extra: &[Param],
    body: Option<(&[&'static str], bool)>,
) -> Operation {
    let mut params = vec![CK, MK, LK, IK];
    params.extend_from_slice(extra);
    let path = format!("/pipes/{{connector_keys}}/{{metric_key}}/{{location_key}}{suffix}");
    op(
        method,
        &path,
        "pipes",
        &[tag],
        summary,
        &params,
        body,
        CurrUser::Never,
    )
}

/// Every operation the Rust server always serves (except `/dash/*`; [`optional_operations`] has
/// `/mcp` and `/webterm`).
pub fn operations() -> Vec<Operation> {
    use CurrUser::*;
    const ATTR: &str = "Pipes: Attributes";
    const DATA: &str = "Pipes: Data";
    let mut ops = vec![
        op("get", "/", "index", &[], "Index", &[], None, Never),
        op(
            "post",
            "/login",
            "login",
            &["Users"],
            "Login",
            &[("authorization", "header", false, "null|string")],
            Some((&[FORM_BODY], false)),
            Never,
        ),
        op(
            "get",
            "/actions",
            "actions",
            &["Actions"],
            "Get Actions",
            &[],
            None,
            UnlessPrivate,
        ),
        op(
            "post",
            "/actions/{action}",
            "actions",
            &["Actions"],
            "Do Action Legacy",
            &[("action", "path", true, "string")],
            Some((&[JSON_BODY], true)),
            Never,
        ),
        // v5 (D21): the dashboard's Execute console runs an action line as an ephemeral job.
        op(
            "post",
            "/execute",
            "actions",
            &["Actions"],
            "Execute Action",
            &[],
            Some((&[JSON_BODY], true)),
            Never,
        )
        .described(
            "Run an action line (`{\"sysargs\": [...]}`) as a temporary background job owned by \
             the caller. Answers `{\"success\", \"message\", \"job\", \"sysargs\", \"stream\", \
             \"logs\"}`: `stream` (`/logs/{job}/stream`, Server-Sent Events) is `null` when this \
             server does not serve it, and `logs` (`/logs/{job}`) when the `jobs` routes are \
             off. Non-administrators run only what the console offers.",
            &["actions:execute", "jobs:execute", "jobs:write"],
        ),
        op(
            "get",
            "/execute/policy",
            "actions",
            &["Actions"],
            "Get Execute Policy",
            &[],
            None,
            UnlessPrivate,
        )
        .described(
            "What the console offers and what non-administrators may run: `{\"success\", \
             \"message\", \"omit_actions\", \"omit_flags\", ...}`.",
            &[],
        ),
        op(
            "get",
            "/connectors",
            "connectors",
            &["Connectors"],
            "Get Connectors",
            &[("type", "query", false, "null|string")],
            None,
            Never,
        ),
        op(
            "get",
            "/connectors/{type}",
            "connectors",
            &["Connectors"],
            "Get Connectors By Type",
            &[("type", "path", true, "string")],
            None,
            Never,
        ),
        op(
            "get",
            "/favicon.ico",
            "misc",
            &["Misc"],
            "Get Favicon",
            &[],
            None,
            Never,
        ),
        op(
            "get",
            "/chaining",
            "misc",
            &["Misc"],
            "Get Chaining Status",
            &[],
            None,
            UnlessPrivate,
        ),
        op(
            "get",
            "/info",
            "misc",
            &["Misc"],
            "Get Instance Info",
            &[IK],
            None,
            UnlessPrivate,
        ),
        op(
            "get",
            "/healthcheck",
            "misc",
            &["Misc"],
            "Get Healtheck",
            &[IK],
            None,
            Never,
        ),
        op(
            "get",
            "/jobs",
            "jobs",
            &["Jobs"],
            "Get Jobs",
            &[],
            None,
            Never,
        ),
        op(
            "get",
            "/jobs/{name}",
            "jobs",
            &["Jobs"],
            "Get Job",
            &[NAME],
            None,
            Never,
        ),
        op(
            "post",
            "/jobs/{name}",
            "jobs",
            &["Jobs"],
            "Create Job",
            &[NAME],
            Some((&[JSON_BODY], true)),
            Never,
        ),
        op(
            "delete",
            "/jobs/{name}",
            "jobs",
            &["Jobs"],
            "Delete Job",
            &[NAME],
            None,
            Never,
        ),
        op(
            "get",
            "/jobs/{name}/exists",
            "jobs",
            &["Jobs"],
            "Get Job Exists",
            &[NAME],
            None,
            Never,
        ),
        op(
            "get",
            "/jobs/{name}/is_blocking_on_stdin",
            "jobs",
            &["Jobs"],
            "Get Is Blocking On Stdin",
            &[NAME],
            None,
            Never,
        ),
        op(
            "post",
            "/jobs/{name}/pause",
            "jobs",
            &["Jobs"],
            "Pause Job",
            &[NAME],
            None,
            Never,
        ),
        op(
            "post",
            "/jobs/{name}/start",
            "jobs",
            &["Jobs"],
            "Start Job",
            &[NAME],
            None,
            Never,
        ),
        op(
            "post",
            "/jobs/{name}/stop",
            "jobs",
            &["Jobs"],
            "Stop Job",
            &[NAME],
            None,
            Never,
        ),
        op(
            "get",
            "/jobs/{name}/stop_time",
            "jobs",
            &["Jobs"],
            "Get Stop Time",
            &[NAME],
            None,
            Never,
        ),
        op(
            "get",
            "/jobs{name}/prompt_kwargs",
            "jobs",
            &["Jobs"],
            "Get Prompt Kwargs",
            &[NAME],
            None,
            Never,
        ),
        op(
            "get",
            "/logs/{name}",
            "jobs",
            &["Jobs"],
            "Get Logs",
            &[NAME],
            None,
            Never,
        ),
        op(
            "post",
            "/metadata",
            "pipes",
            &["Misc"],
            "Create Metadata",
            &[IK],
            None,
            Never,
        ),
        op(
            "get",
            "/pipes",
            "pipes",
            &[ATTR],
            "Get Pipes",
            &[
                ("connector_keys", "query", false, "string"),
                ("metric_keys", "query", false, "string"),
                ("location_keys", "query", false, "string"),
                ("instance_keys", "query", false, "string"),
            ],
            None,
            Never,
        ),
        op(
            "get",
            "/pipes/keys",
            "pipes",
            &[ATTR],
            "Fetch Pipes Keys",
            &[
                ("connector_keys", "query", false, "string"),
                ("metric_keys", "query", false, "string"),
                ("location_keys", "query", false, "string"),
                IK,
                ("tags", "query", false, "string"),
                ("params", "query", false, "string"),
                ("as_dict", "query", false, "boolean"),
            ],
            None,
            Never,
        ),
        op(
            "get",
            "/pipes/{connector_keys}",
            "pipes",
            &[ATTR],
            "Get Pipes By Connector",
            &[CK, IK],
            None,
            Never,
        ),
        op(
            "get",
            "/pipes/{connector_keys}/{metric_key}",
            "pipes",
            &[ATTR],
            "Get Pipes By Connector And Metric",
            &[CK, MK, IK],
            None,
            Never,
        ),
        pipe_op(
            "get",
            "",
            ATTR,
            "Get Pipe By Connector And Metric And Location",
            &[],
            None,
        ),
        pipe_op(
            "post",
            "/register",
            ATTR,
            "Register Pipe",
            &[],
            Some((&[JSON_BODY], false)),
        ),
        pipe_op(
            "patch",
            "/edit",
            ATTR,
            "Edit Pipe",
            &[("patch", "query", false, "boolean")],
            Some((&[JSON_BODY], true)),
        ),
        pipe_op("delete", "/delete", ATTR, "Delete Pipe", &[], None),
        pipe_op(
            "get",
            "/sync_time",
            DATA,
            "Get Sync Time",
            &[
                ("newest", "query", false, "boolean"),
                ("remote", "query", false, "boolean"),
                ("round_down", "query", false, "boolean"),
            ],
            Some((&[JSON_BODY], false)),
        ),
        pipe_op(
            "post",
            "/data",
            DATA,
            "Sync Pipe",
            &[
                ("check_existing", "query", false, "boolean"),
                ("blocking", "query", false, "boolean"),
                ("force", "query", false, "boolean"),
                ("workers", "query", false, "integer|null"),
                ("columns", "query", false, "null|string"),
            ],
            Some((&[JSON_BODY, "text/plain"], true)),
        ),
        pipe_op(
            "get",
            "/data",
            DATA,
            "Get Pipe Data",
            &[
                ("select_columns", "query", false, "null|string"),
                ("omit_columns", "query", false, "null|string"),
                BEGIN,
                END,
                PARAMS,
                ("limit", "query", false, "integer"),
                ("order", "query", false, "string"),
                ("date_format", "query", false, "string"),
                ("date_unit", "query", false, "string"),
                ("double_precision", "query", false, "integer"),
                ("geometry_format", "query", false, "string"),
            ],
            None,
        ),
        pipe_op(
            "get",
            "/docs",
            DATA,
            "Get Pipe Docs",
            &[
                ("select_columns", "query", false, "null|string"),
                ("omit_columns", "query", false, "null|string"),
                BEGIN,
                END,
                PARAMS,
                ("limit", "query", false, "integer"),
                ("order", "query", false, "string"),
            ],
            None,
        ),
        pipe_op(
            "get",
            "/chunk_bounds",
            DATA,
            "Get Pipe Chunk Bounds",
            &[
                BEGIN,
                END,
                ("bounded", "query", false, "boolean"),
                ("chunk_interval_minutes", "query", false, "integer|null"),
            ],
            None,
        ),
        pipe_op("delete", "/drop", DATA, "Drop Pipe", &[], None),
        pipe_op(
            "delete",
            "/clear",
            DATA,
            "Clear Pipe",
            &[BEGIN, END, PARAMS],
            None,
        ),
        pipe_op("delete", "/cache", DATA, "Delete Pipe Cache", &[], None),
        pipe_op(
            "get",
            "/csv",
            DATA,
            "Get Pipe Csv",
            &[BEGIN, END, PARAMS],
            None,
        ),
        pipe_op("get", "/id", ATTR, "Get Pipe Id", &[], None),
        pipe_op("get", "/attributes", ATTR, "Get Pipe Attributes", &[], None),
        pipe_op("get", "/exists", DATA, "Get Pipe Exists", &[], None),
        pipe_op(
            "get",
            "/rowcount",
            DATA,
            "Get Pipe Rowcount",
            &[
                BEGIN,
                END,
                ("remote", "query", false, "boolean"),
                ("estimate", "query", false, "boolean"),
                PARAMS,
            ],
            Some((&[JSON_BODY], false)),
        )
        .described(
            "The number of rows (optionally in `[begin, end)` and matching `params`, a JSON \
             object as the body or the `params` query string). With `estimate=true`, the \
             database's row estimate (catalog statistics, no table scan) where it keeps one, else \
             the exact count.",
            &["pipes:read"],
        ),
        pipe_op(
            "get",
            "/columns/types",
            DATA,
            "Get Pipe Columns Types",
            &[],
            None,
        ),
        pipe_op(
            "get",
            "/columns/indices",
            DATA,
            "Get Pipe Columns Indices",
            &[],
            None,
        ),
        pipe_op(
            "get",
            "/indices/names",
            DATA,
            "Get Pipe Index Names",
            &[],
            None,
        ),
        pipe_op("get", "/size", DATA, "Get Pipe Size", &[], None),
        pipe_op("post", "/compress", DATA, "Compress Pipe", &[], None),
        pipe_op(
            "post",
            "/decompress",
            DATA,
            "Decompress Pipe",
            &[("no_policy", "query", false, "boolean")],
            None,
        ),
        pipe_op(
            "post",
            "/vacuum",
            DATA,
            "Vacuum Pipe",
            &[("full", "query", false, "boolean")],
            None,
        ),
        pipe_op("post", "/analyze", DATA, "Analyze Pipe", &[], None),
        pipe_op(
            "post",
            "/partition",
            DATA,
            "Partition Pipe",
            &[("chunk_minutes", "query", false, "integer|null")],
            None,
        ),
        op(
            "get",
            "/plugins",
            "plugins",
            &["Plugins"],
            "Get Plugins",
            &[
                ("user_id", "query", false, "integer|null"),
                ("search_term", "query", false, "null|string"),
            ],
            None,
            UnlessPrivate,
        ),
        op(
            "post",
            "/plugins/{name}",
            "plugins",
            &["Plugins"],
            "Register Plugin",
            &[
                NAME,
                ("version", "query", false, "string"),
                ("attributes", "query", false, "string"),
            ],
            Some((&["multipart/form-data"], true)),
            Never,
        ),
        op(
            "get",
            "/plugins/{name}",
            "plugins",
            &["Plugins"],
            "Get Plugin",
            &[NAME],
            None,
            UnlessPrivate,
        ),
        op(
            "delete",
            "/plugins/{name}",
            "plugins",
            &["Plugins"],
            "Delete Plugin",
            &[NAME],
            None,
            Never,
        ),
        op(
            "get",
            "/plugins/{name}/attributes",
            "plugins",
            &["Plugins"],
            "Get Plugin Attributes",
            &[NAME],
            None,
            UnlessPrivate,
        ),
        op(
            "get",
            "/plugins/{name}/version",
            "plugins",
            &[],
            "Get Plugin Version",
            &[NAME],
            None,
            UnlessPrivate,
        ),
        op(
            "get",
            "/tokens",
            "tokens",
            &["Tokens"],
            "Get Tokens",
            &[("labels", "query", false, "string")],
            None,
            IfNoAuth,
        ),
        op(
            "post",
            "/tokens/register",
            "tokens",
            &["Tokens"],
            "Register Token",
            &[],
            Some((&[JSON_BODY], true)),
            IfNoAuth,
        ),
        op(
            "post",
            "/tokens/validate",
            "tokens",
            &["Tokens"],
            "Validate Api Key",
            &[],
            None,
            IfNoAuth,
        ),
        op(
            "get",
            "/tokens/{token_id}",
            "tokens",
            &["Tokens"],
            "Get Token Model",
            &[TOKEN_ID],
            None,
            IfNoAuth,
        ),
        op(
            "delete",
            "/tokens/{token_id}",
            "tokens",
            &["Tokens"],
            "Delete Token",
            &[TOKEN_ID],
            None,
            IfNoAuth,
        ),
        op(
            "post",
            "/tokens/{token_id}/edit",
            "tokens",
            &["Tokens"],
            "Edit Token",
            &[TOKEN_ID],
            Some((&[JSON_BODY], true)),
            IfNoAuth,
        ),
        op(
            "post",
            "/tokens/{token_id}/invalidate",
            "tokens",
            &["Tokens"],
            "Invalidate Token",
            &[TOKEN_ID],
            None,
            IfNoAuth,
        ),
        op(
            "get",
            "/users",
            "users",
            &["Users"],
            "Get Users",
            &[],
            None,
            UnlessPrivate,
        ),
        op(
            "get",
            "/users/me",
            "users",
            &["Users"],
            "Read Current User",
            &[],
            None,
            IfNoAuth,
        ),
        op(
            "post",
            "/users/register",
            "users",
            &["Users"],
            "Register User",
            &[],
            Some((&[FORM_BODY], false)),
            UnlessPrivate,
        ),
        op(
            "post",
            "/users/edit",
            "users",
            &["Users"],
            "Edit User",
            &[],
            Some((&[FORM_BODY], false)),
            Never,
        ),
        op(
            "delete",
            "/users/{username}",
            "users",
            &["Users"],
            "Delete User",
            &[USERNAME],
            None,
            IfNoAuth,
        ),
        op(
            "get",
            "/users/{username}/attributes",
            "users",
            &["Users"],
            "Get User Attributes",
            &[USERNAME],
            None,
            UnlessPrivate,
        ),
        op(
            "get",
            "/users/{username}/id",
            "users",
            &["Users"],
            "Get User Id",
            &[USERNAME],
            None,
            Never,
        ),
        op(
            "get",
            "/users/{username}/password_hash",
            "users",
            &["Users"],
            "Get User Password Hash",
            &[USERNAME],
            None,
            Never,
        ),
        op(
            "get",
            "/users/{username}/type",
            "users",
            &["Users"],
            "Get User Type",
            &[USERNAME],
            None,
            Never,
        ),
        op(
            "get",
            "/version",
            "version",
            &["Version"],
            "Get Api Version",
            &[],
            None,
            UnlessPrivate,
        ),
        op(
            "get",
            "/version/mrsm",
            "version",
            &["Version"],
            "Get Meerschaum Version",
            &[],
            None,
            UnlessPrivate,
        ),
    ];
    ops.extend([
        op(
            "post",
            "/execute/validate",
            "actions",
            &["Actions"],
            "Validate Terminal Command",
            &[],
            Some((&[JSON_BODY], true)),
            Never,
        )
        .described(
            "Whether the caller may run an action line (`{\"sysargs\": [...]}`) in the web \
             terminal: `200` or `403` with the reason.",
            &["actions:execute", "jobs:execute", "jobs:write"],
        ),
        op(
            "get",
            "/execute/runs",
            "actions",
            &["Runs"],
            "Get Retained Execution Summaries",
            &[],
            None,
            Never,
        )
        .described(
            "The summaries of finished temporary executions the caller may see (`runs`).",
            &["jobs:read"],
        ),
        op(
            "post",
            "/execute/runs/{name}/retain",
            "actions",
            &["Runs"],
            "Keep Completed Execution As Job",
            &[NAME],
            None,
            Never,
        )
        .described(
            "Keep a finished temporary execution as a persistent job (`409` while it runs).",
            &["jobs:write"],
        ),
        op(
            "get",
            "/pipelines",
            "pipelines",
            &["Pipelines"],
            "List Saved Pipelines",
            &[IK],
            None,
            Never,
        )
        .described(
            "The saved Pipelines of the instance: `{\"success\", \"message\", \"pipelines\": \
             [<definition>, ...]}`.",
            &["pipes:read"],
        ),
        op(
            "get",
            "/pipelines/keys",
            "pipelines",
            &["Pipelines: Metadata"],
            "Get Pipeline Registration Keys",
            &[
                IK,
                ("location_keys", "query", false, "null|string"),
                ("tags", "query", false, "null|string"),
                PARAMS,
            ],
            None,
            Never,
        )
        .described(
            "The registration keys of the saved Pipelines (`keys`).",
            &["pipes:read"],
        ),
        op(
            "get",
            "/pipelines/{name}",
            "pipelines",
            &["Pipelines"],
            "Get Pipeline",
            &[NAME, IK],
            None,
            Never,
        )
        .described(
            "One saved Pipeline: `pipeline` (its definition), `members` (each target pipe with \
             whether it is registered) and `last_run` (its latest run, or `null`). `404` when it \
             is not registered.",
            &["pipes:read"],
        ),
        op(
            "post",
            "/pipelines/{name}/plan",
            "pipelines",
            &["Pipelines"],
            "Plan Pipeline",
            &[NAME, IK],
            Some((&[JSON_BODY], false)),
            Never,
        )
        .described(
            "What a run of the saved Pipeline would do, without moving data or registering \
             anything: `plan` (each pipe's action, reason and window), `text` (the plan table) \
             and `warnings`. The optional body bounds the window: `{\"begin\": ..., \"end\": \
             ...}` (ISO datetimes or integers). `400` when the Pipeline cannot be planned as it \
             is.",
            &["pipes:read"],
        ),
        op(
            "post",
            "/pipelines/{name}/sync",
            "pipelines",
            &["Pipelines"],
            "Sync Pipeline",
            &[NAME, IK],
            Some((&[JSON_BODY], false)),
            Never,
        )
        .described(
            "Start a run of the saved Pipeline as a background job (`sync pipelines NAME`, like \
             the console): `{\"success\", \"message\", \"run_id\", \"job\", \"stream\", \
             \"log_stream\", \"logs\"}`, where `stream` follows the run's events \
             (`/engine/runs/{run_id}/events/stream`). The optional body takes `begin`, `end` and \
             `run_id`. Non-administrators need the server to let them run actions and access to \
             every instance the Pipeline touches.",
            &["pipes:write", "jobs:execute"],
        ),
    ]);
    for (method, suffix, summary, body, description, scopes) in [
        (
            "post",
            "register",
            "Register Pipeline Metadata",
            true,
            "Register a Pipeline definition (`{\"pipeline\": {...}}`, the raw parameters the CLI \
             and SDK write). `409` when it is registered.",
            &["pipes:write"][..],
        ),
        (
            "patch",
            "edit",
            "Edit Raw Pipeline Metadata",
            true,
            "Replace (or with `patch=true` merge) a Pipeline definition. `404` when it is not \
             registered.",
            &["pipes:write"][..],
        ),
        (
            "delete",
            "delete",
            "Delete Pipeline Metadata",
            false,
            "Delete a Pipeline definition (its pipes and their data stay). `409` while scheduled \
             jobs run it.",
            &["pipes:delete"][..],
        ),
        (
            "get",
            "id",
            "Get Pipeline Registration Id",
            false,
            "The registration's ID (`id`). `404` when it is not registered.",
            &["pipes:read"][..],
        ),
        (
            "get",
            "attributes",
            "Get Raw Pipeline Registration Attributes",
            false,
            "The registration's raw attributes (`attributes`), with the scheduled jobs that run \
             it. `404` when it is not registered.",
            &["pipes:read"][..],
        ),
    ] {
        let mut params = vec![NAME, IK];
        if suffix == "edit" {
            params.push(("patch", "query", false, "boolean"));
        }
        ops.push(
            op(
                method,
                &format!("/pipelines/{{name}}/{suffix}"),
                "pipelines",
                &["Pipelines: Metadata"],
                summary,
                &params,
                body.then_some((&[JSON_BODY][..], true)),
                Never,
            )
            .described(description, scopes),
        );
    }
    for (method, summary) in [
        ("post", "Acquire Pipeline Metadata Lease"),
        ("delete", "Release Pipeline Metadata Lease"),
    ] {
        ops.push(
            op(
                method,
                "/pipelines/leases",
                "pipelines",
                &["Pipelines: Metadata"],
                summary,
                &[IK],
                Some((&[JSON_BODY], true)),
                Never,
            )
            .described(
                "Acquire or release the lease of a Pipeline metadata edit. `409` when another \
                 holder has it.",
                &["pipes:write"],
            ),
        );
    }
    const READ: &[&str] = &["pipes:read"];
    const WRITE: &[&str] = &["pipes:write"];
    let engine = [
        (
            "post",
            "/engine/ledger",
            "Post Engine Ledger",
            vec![],
            true,
            "Append dirty-range ledger entries.",
            WRITE,
        ),
        (
            "get",
            "/engine/ledger",
            "Get Engine Ledger",
            vec![
                ("child_ref", "query", true, "string"),
                ("after_seq", "query", false, "integer|null"),
            ],
            false,
            "The ledger entries of a child's parents after a sequence number (`entries`).",
            READ,
        ),
        (
            "put",
            "/engine/cursors",
            "Put Engine Cursors",
            vec![],
            true,
            "Upsert edge cursors.",
            WRITE,
        ),
        (
            "get",
            "/engine/state",
            "Get Engine State",
            vec![("pipe_ref", "query", true, "string")],
            false,
            "A pipe's engine state (`state`); `404` when it has none.",
            READ,
        ),
        (
            "put",
            "/engine/state",
            "Put Engine State",
            vec![],
            true,
            "Upsert pipes' engine state.",
            WRITE,
        ),
        (
            "post",
            "/engine/runs",
            "Post Engine Runs",
            vec![],
            true,
            "Record runs and their per-pipe results.",
            WRITE,
        ),
        (
            "get",
            "/engine/runs",
            "Get Engine Runs",
            vec![
                ("run_id", "query", false, "null|string"),
                ("pipeline", "query", false, "null|string"),
                ("pipe", "query", false, "null|string"),
                ("status", "query", false, "null|string"),
                ("since", "query", false, "null|string"),
                ("limit", "query", false, "integer|null"),
                ("instance_keys", "query", false, "null|string"),
            ],
            false,
            "Runs, newest first (`runs`): filter by `pipeline` (a saved Pipeline's name), `pipe` \
             (a pipe reference `instance|connector|metric|location`), `status` (`success`, \
             `failed` or `running`), `since` (an ISO datetime) and `limit` (default 100, at most \
             1000); with `run_id`, that run and its pipes' rows. `instance_keys` names the \
             instance that keeps the history.",
            READ,
        ),
        (
            "get",
            "/engine/runs/{run_id}",
            "Get Engine Run",
            vec![
                ("run_id", "path", true, "string"),
                ("instance_keys", "query", false, "null|string"),
            ],
            false,
            "One run (`run`) with each pipe's result (`nodes`); `404` when it is not found.",
            READ,
        ),
        (
            "get",
            "/engine/runs/{run_id}/events/stream",
            "Stream Engine Run Events",
            vec![
                ("run_id", "path", true, "string"),
                ("after_seq", "query", false, "integer|null"),
                ("wait", "query", false, "null|number"),
                ("instance_keys", "query", false, "null|string"),
            ],
            false,
            "Follow a run's events as Server-Sent Events until it finishes.",
            READ,
        ),
        (
            "post",
            "/engine/events",
            "Post Engine Events",
            vec![],
            true,
            "Append a batch of run events.",
            WRITE,
        ),
        (
            "post",
            "/engine/leases",
            "Acquire Engine Lease",
            vec![],
            true,
            "Acquire or extend a pipe's lease; `409` when another holder has it.",
            WRITE,
        ),
        (
            "delete",
            "/engine/leases",
            "Release Engine Lease",
            vec![],
            true,
            "Release a pipe's lease; `409` when the holder does not hold it.",
            WRITE,
        ),
        (
            "post",
            "/engine/pipes/{metric}/read",
            "Read Engine State Pipe",
            vec![("metric", "path", true, "string")],
            true,
            "Rows of an engine state pipe (`rows`).",
            READ,
        ),
        (
            "post",
            "/engine/pipes/{metric}/write",
            "Write Engine State Pipe",
            vec![("metric", "path", true, "string")],
            true,
            "Append (or upsert) rows of an engine state pipe.",
            WRITE,
        ),
        (
            "post",
            "/engine/pipes/{metric}/clear",
            "Clear Engine State Pipe",
            vec![("metric", "path", true, "string")],
            true,
            "Delete rows of an engine state pipe, selected by `params` (or `end`).",
            WRITE,
        ),
        (
            "get",
            "/engine/pipes/{metric}/exists",
            "Engine State Pipe Exists",
            vec![("metric", "path", true, "string")],
            false,
            "Whether an engine state pipe's table exists (`exists`).",
            READ,
        ),
        (
            "get",
            "/engine/sync-status",
            "Get Sync Statuses",
            vec![
                ("pipe_ref", "query", false, "null|string"),
                ("instance_keys", "query", false, "null|string"),
            ],
            false,
            "The latest recorded sync of every pipe on the instance (`statuses`), or of the pipe \
             `pipe_ref` names.",
            READ,
        ),
        (
            "post",
            "/engine/sync-status",
            "Record Sync Status",
            vec![("instance_keys", "query", false, "null|string")],
            true,
            "Merge a client's sync outcomes of one pipe into its status (`status`).",
            WRITE,
        ),
        (
            "get",
            "/engine/metrics",
            "Get Sync Performance",
            vec![
                IK,
                ("begin", "query", false, "null|string"),
                ("end", "query", false, "null|string"),
                ("pipe_ref", "query", false, "null|string"),
                ("pipe", "query", false, "null|string"),
                ("pipeline", "query", false, "null|string"),
                ("limit", "query", false, "integer|null"),
                ("buckets", "query", false, "integer|null"),
                ("visible_refs", "query", false, "null|string"),
                ("visible_pipelines", "query", false, "null|string"),
            ],
            false,
            "Numeric sync and Pipeline performance (`metrics`), filtered by current scopes, \
             teams and object access before aggregation. Bounds are ISO-8601 timestamps with \
             timezones; the default window is 24 hours and the maximum is 30 days. `limit` \
             defaults to 1000 (maximum 5000), `buckets` to 48 (maximum 200). A bounded view \
             reports `truncated`. Durations are seconds, timestamps UTC and `success_rate` \
             a fraction. Rows affected include inserted, updated, upserted and deleted rows. \
             Optional visibility ceilings are JSON string arrays and may only narrow access.",
            READ,
        ),
        (
            "post",
            "/engine/metrics",
            "Record Sync Performance",
            vec![IK],
            true,
            "Persist a client batch (`metrics`) of 1 through 128 numeric outcomes, at most \
             1 MiB. Requires current edit access and matching Pipe registration identities; \
             stale identities are refused. Event IDs deduplicate transport retries.",
            WRITE,
        ),
        (
            "post",
            "/engine/metrics/initialize",
            "Initialize Sync Performance Storage",
            vec![IK],
            false,
            "Create empty native metrics, runs and nodes storage and indexes. Requires a \
             full administrator. Ordinary performance reads never create storage.",
            WRITE,
        ),
    ];
    for (method, path, summary, params, body, description, scopes) in engine {
        ops.push(
            op(
                method,
                path,
                "engine",
                &["Engine"],
                summary,
                &params,
                body.then_some((&[JSON_BODY][..], true)),
                Never,
            )
            .described(description, scopes),
        );
    }
    ops
}

/// The operations that exist only under some configurations: `/mcp` (when `api:mcp:enabled` and the
/// `mcp` route group is allowed, like the router registers it) and `/webterm/{session_id}` (when the
/// console and its terminal are served: neither `--no-dash` nor `--no-webterm`). v4.1.4 lists both
/// in its document (`/mcp` always, `/webterm/{session_id}` unless `--no-webterm`; tags `MCP` and
/// `Webterm`); `/websocket/{session_id}` is a websocket route and is in neither document.
pub fn optional_operations(cfg: &ServerConfig) -> Vec<Operation> {
    use CurrUser::Never;
    let mut ops = Vec::new();
    if cfg.mcp.enabled && cfg.route_group_is_allowed("mcp") {
        ops.push(op(
            "get",
            "/mcp",
            "mcp",
            &["MCP"],
            "Mcp Info",
            &[],
            None,
            Never,
        ));
        ops.push(op(
            "post",
            "/mcp",
            "mcp",
            &["MCP"],
            "Mcp Endpoint",
            &[],
            None,
            Never,
        ));
    }
    if !cfg.no_dash && !cfg.no_webterm {
        ops.push(op(
            "get",
            "/webterm/{session_id}",
            "webterm",
            &["Webterm"],
            "Get Webterm",
            &[("session_id", "path", true, "string")],
            None,
            Never,
        ));
    }
    ops
}

/// Whether an operation is served under this configuration.
pub fn is_served(op: &Operation, cfg: &ServerConfig) -> bool {
    match op.group {
        "login" => !cfg
            .routes_disabled
            .iter()
            .any(|pattern| crate::config::fnmatch("login", pattern)),
        "docs" => true,
        // The public preset keeps only `/healthcheck` and `/favicon.ico` of `misc`.
        "misc" => {
            cfg.route_group_is_allowed("misc")
                && (cfg.serves_full_misc()
                    || matches!(op.path.as_str(), "/healthcheck" | "/favicon.ico"))
        }
        g => cfg.route_group_is_allowed(g),
    }
}

/// The parameters of an operation under this configuration (adds `curr_user` per the quirk).
pub fn effective_params(op: &Operation, cfg: &ServerConfig) -> Vec<Param> {
    let mut params = op.params.clone();
    let curr_user = match op.curr_user {
        CurrUser::Never => false,
        CurrUser::UnlessPrivate => !cfg.private,
        CurrUser::IfNoAuth => cfg.no_auth,
    };
    if curr_user {
        params.push(("curr_user", "query", false, "any"));
    }
    params
}

fn type_of(t: &str) -> Type {
    match t {
        "string" => Type::String,
        "integer" => Type::Integer,
        "boolean" => Type::Boolean,
        "number" => Type::Number,
        "null" => Type::Null,
        "array" => Type::Array,
        _ => Type::Object,
    }
}

fn schema_of(ty: &str, title: Option<String>) -> Schema {
    let types: Vec<&str> = ty.split('|').collect();
    match types.as_slice() {
        ["any"] => Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::AnyValue)
                .title(title)
                .build(),
        ),
        [one] => Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(type_of(one)))
                .title(title)
                .build(),
        ),
        many => {
            let _ = title;
            let mut b = AnyOfBuilder::new();
            for t in many {
                b = b.item(Schema::Object(
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(type_of(t)))
                        .build(),
                ));
            }
            Schema::AnyOf(b.build())
        }
    }
}

fn title_case(name: &str) -> String {
    name.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn http_method(m: &str) -> HttpMethod {
    match m {
        "post" => HttpMethod::Post,
        "put" => HttpMethod::Put,
        "patch" => HttpMethod::Patch,
        "delete" => HttpMethod::Delete,
        _ => HttpMethod::Get,
    }
}

fn param_in(loc: &str) -> ParameterIn {
    match loc {
        "path" => ParameterIn::Path,
        "header" => ParameterIn::Header,
        "cookie" => ParameterIn::Cookie,
        _ => ParameterIn::Query,
    }
}

/// The OpenAPI document for this configuration.
pub fn document(cfg: &ServerConfig) -> OpenApi {
    let mut paths = Paths::new();
    let optional = optional_operations(cfg);
    let served = operations()
        .into_iter()
        .filter(|o| is_served(o, cfg))
        .chain(optional);
    for op in served {
        let op = &op;
        let mut b = OperationBuilder::new()
            .summary(Some(op.summary))
            .operation_id(Some(format!(
                "{}_{}",
                op.summary.to_lowercase().replace(' ', "_"),
                op.method
            )))
            .response(
                "200",
                ResponseBuilder::new()
                    .description("Successful Response")
                    .build(),
            );
        if !op.tags.is_empty() {
            b = b.tags(Some(op.tags.clone()));
        }
        if let Some(description) = op.description {
            b = b.description(Some(description));
        }
        if !op.scopes.is_empty() {
            b = b.securities(Some(["OAuth2PasswordBearer", "APIKey"].map(|scheme| {
                utoipa::openapi::security::SecurityRequirement::new(
                    scheme,
                    op.scopes.iter().copied(),
                )
            })));
        }
        for (name, loc, required, ty) in effective_params(op, cfg) {
            b = b.parameter(
                ParameterBuilder::new()
                    .name(name)
                    .parameter_in(param_in(loc))
                    .required(if required {
                        Required::True
                    } else {
                        Required::False
                    })
                    .schema(Some(schema_of(ty, Some(title_case(name)))))
                    .build(),
            );
        }
        if let Some((types, required)) = &op.body {
            let mut rb = RequestBodyBuilder::new().required(Some(if *required {
                Required::True
            } else {
                Required::False
            }));
            for t in types {
                rb = rb.content(
                    *t,
                    ContentBuilder::new()
                        .schema(Some(schema_of("any", None)))
                        .build(),
                );
            }
            b = b.request_body(Some(rb.build()));
        }
        paths.add_path_operation(&op.path, vec![http_method(op.method)], b.build());
    }
    let scopes: serde_json::Map<String, Json> = meerschaum_core::static_config()
        .scope_names()
        .into_iter()
        .map(|s| (s, json!("")))
        .collect();
    let doc = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Meerschaum API",
            "description": "The Meerschaum Web API lets you manage your pipes over the Internet.",
            "version": cfg.version,
            "contact": {"name": "Bennett Meares", "url": "https://meerschaum.io"},
            "license": {
                "name": "Apache 2.0",
                "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
            },
        },
        "paths": {},
        "components": {
            "securitySchemes": {
                "OAuth2PasswordBearer": {
                    "type": "oauth2",
                    "flows": {"password": {"tokenUrl": "/login", "scopes": scopes}},
                },
                "APIKey": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "mrsm-key:{client_id}:{client_secret}",
                    "description": "Authentication using a Meerschaum API Key.",
                },
            },
        },
        "security": [{"OAuth2PasswordBearer": []}, {"APIKey": []}],
    });
    let mut openapi: OpenApi = serde_json::from_value(doc).expect("a valid OpenAPI document");
    openapi.paths = paths;
    openapi
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_builds_and_round_trips() {
        let cfg = ServerConfig::for_root("sql:main", std::path::Path::new("/tmp"));
        let doc = document(&cfg);
        let j = serde_json::to_value(&doc).unwrap();
        assert!(j["paths"]["/pipes/keys"]["get"]["parameters"].is_array());
        assert!(j["paths"]["/version"]["get"]["parameters"][0]["name"] == "curr_user");
        assert!(j["components"]["securitySchemes"]["APIKey"].is_object());
    }
}
