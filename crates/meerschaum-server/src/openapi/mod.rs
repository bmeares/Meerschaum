//! The OpenAPI document (`/openapi.json`), as a `utoipa::openapi::OpenApi`.
//!
//! The operation table ([`operations`], in `table.rs`) lists every route with the same tags,
//! parameters (name, location, requiredness, type) and request bodies as the v4 FastAPI
//! document, plus the v5 `/engine/*` family. FastAPI quirks are kept: routes whose auth
//! dependency is `None` when the server is not private expose a `curr_user` query parameter, and
//! the `sync_time` / `rowcount` routes take an optional JSON body for `params`.
//! `tests/openapi_parity.rs` diffs this table against the recorded Python document.

use serde_json::{Value as Json, json};
use utoipa::openapi::path::{HttpMethod, OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::schema::{AnyOfBuilder, ObjectBuilder, Schema, SchemaType, Type};
use utoipa::openapi::{ContentBuilder, OpenApi, Paths, Required, ResponseBuilder};

use crate::config::ServerConfig;

mod table;

pub use table::{operations, optional_operations};

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

/// Whether an operation is served under this configuration.
pub fn is_served(op: &Operation, cfg: &ServerConfig) -> bool {
    match op.group {
        "login" => !cfg
            .routes_disabled
            .iter()
            .any(|pattern| crate::config::fnmatch("login", pattern)),
        "docs" => true,
        // The public preset keeps only `/healthcheck` (and its `/ready`) and `/favicon.ico` of
        // `misc`.
        "misc" => {
            cfg.route_group_is_allowed("misc")
                && (cfg.serves_full_misc()
                    || matches!(
                        op.path.as_str(),
                        "/healthcheck" | "/healthcheck/ready" | "/favicon.ico"
                    ))
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
