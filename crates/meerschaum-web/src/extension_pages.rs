//! Confined pages rendered in an opaque frame; only the trusted typed bridge runs script.

use super::*;
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    response::{Html, IntoResponse, Response},
    routing::post,
};
use meerschaum_server::{
    access,
    error::ApiError,
    executor::{plugin_handlers, plugins},
};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

const MAX_PAGE: usize = 4 * 1024 * 1024;
const MAX_SHELL: usize = 12 * 1024 * 1024;
const MAX_CALLBACK: usize = 1024 * 1024;
const GRANT_KIND: &str = "extension_page";
static RENDER_SLOTS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(2)));
static CALLBACK_SLOTS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(8)));
const CHILD_SCRIPT: &str = include_str!("../assets/extension_frame.js");
#[cfg(test)]
const PLOTLY_PROVENANCE: &str = include_str!("extension_plotly.json");

pub(crate) fn routes(router: Router<WebState>) -> Router<WebState> {
    router
        .route("/plugins/extensions/pages/{*path}", get(page))
        .route("/plugins/extensions/page-callback", post(callback))
        .route("/plugins/extensions/assets/plotly.js", get(plotly))
}

fn script_json(value: &Json) -> Result<String, ApiError> {
    let text = serde_json::to_string(value)
        .map_err(|_| ApiError::new(500, "Invalid isolated page data."))?;
    Ok(text
        .replace('<', "\\u003c")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029"))
}

fn inert_html(input: &str) -> Result<String, ApiError> {
    if input.len() > 2 * 1024 * 1024 {
        return Err(ApiError::new(
            413,
            "The page fragment exceeds its byte limit.",
        ));
    }
    let mut sanitizer = ammonia::Builder::empty();
    sanitizer
        .tags(HashSet::from([
            "a",
            "abbr",
            "article",
            "aside",
            "b",
            "bdi",
            "bdo",
            "blockquote",
            "br",
            "caption",
            "cite",
            "code",
            "col",
            "colgroup",
            "dd",
            "del",
            "details",
            "dfn",
            "div",
            "dl",
            "dt",
            "em",
            "figcaption",
            "figure",
            "footer",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "header",
            "hr",
            "i",
            "ins",
            "kbd",
            "li",
            "main",
            "mark",
            "nav",
            "ol",
            "p",
            "pre",
            "q",
            "s",
            "samp",
            "section",
            "small",
            "span",
            "strong",
            "sub",
            "summary",
            "sup",
            "table",
            "tbody",
            "td",
            "tfoot",
            "th",
            "thead",
            "time",
            "tr",
            "u",
            "ul",
            "var",
            "wbr",
        ]))
        .clean_content_tags(HashSet::from([
            "script", "style", "svg", "math", "iframe", "object", "embed", "template", "form",
            "audio", "video",
        ]))
        .generic_attributes(HashSet::from(["id", "title", "class", "lang", "dir"]))
        .tag_attributes(HashMap::from([
            ("div", HashSet::from(["data-mrsm-figure"])),
            ("figure", HashSet::from(["data-mrsm-figure"])),
        ]))
        .url_schemes(HashSet::new())
        .url_relative(ammonia::UrlRelative::Deny)
        .link_rel(None)
        .strip_comments(true);
    let html = sanitizer.clean(input).to_string();
    if html.len() > MAX_PAGE {
        return Err(ApiError::new(
            413,
            "The normalized page exceeds its byte limit.",
        ));
    }
    Ok(html)
}

async fn browser_source(
    state: &WebState,
    headers: &HeaderMap,
) -> Result<(HeaderMap, Json, String), ApiError> {
    if !state.server.config.route_group_is_allowed("plugins") {
        return Err(ApiError::not_found());
    }
    let data = session(state, headers).await.map_err(|status| {
        ApiError::new(
            status.as_u16(),
            "A current signed-in console session is required.",
        )
    })?;
    let id = cookie(headers).ok_or_else(ApiError::invalid_credentials)?;
    let mut api = api_headers_for(&state.server, &data, CONSOLE_BEARER_SECONDS)
        .await
        .map_err(|status| {
            ApiError::new(status.as_u16(), "The page session's source is unavailable.")
        })?;
    // Main's typed request resolver must bind the same verified console session. Preserve
    // only its original cookie beside Main's freshly bound API header; never adopt a
    // browser-supplied Authorization header. Neither header enters the child Invocation.
    if let Some(cookie) = headers.get(header::COOKIE) {
        api.insert(header::COOKIE, cookie.clone());
    }
    access::require_fresh(&state.server, &api, &["plugins:read"]).await?;
    Ok((api, data, id))
}

fn session_binding(state: &WebState, id: &str, data: &Json) -> Result<String, ApiError> {
    // Keep the real session cookie and credential binding on Main. Only a domain-separated
    // MAC enters the parent page grant; even its decoded claims disclose no session secret.
    let private: Map = serde_json::from_value(json!({
        "domain":"mrsm-extension-page-session-v1", "session":id,
        "username":data["username"], "credential":data["credential"],
    }))
    .map_err(|_| ApiError::invalid_credentials())?;
    auth::jwt_encode(&private, state.server.secret())
        .rsplit('.')
        .next()
        .filter(|signature| !signature.is_empty())
        .map(str::to_owned)
        .ok_or_else(ApiError::invalid_credentials)
}

async fn source_catalog(
    state: &WebState,
    headers: &HeaderMap,
) -> Result<plugins::Validated, ApiError> {
    meerschaum_server::routes::extensions::console_catalog(&state.server, headers).await
}

async fn page(
    State(state): State<WebState>,
    Path(path): Path<String>,
    request: Request,
) -> Response {
    match render_page(state, path, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn render_page(
    state: WebState,
    path: String,
    request: Request,
) -> Result<Response, ApiError> {
    let permit = RENDER_SLOTS
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::new(503, "The isolated page render limit is busy."))?;
    let (api, data, session_id) = browser_source(&state, request.headers()).await?;
    let catalog = source_catalog(&state, &api).await?;
    let mut matching = Vec::new();
    for (name, source) in &catalog.catalog().plugins {
        if let Some(registry) = source
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.registry.as_ref())
        {
            for handler in registry.handlers() {
                if let plugin_handlers::Selector::Page {
                    path: declared,
                    renderer,
                } = &handler.selector
                    && declared == &path
                {
                    matching.push((name.clone(), handler.clone(), *renderer));
                }
            }
        }
    }
    if matching.len() != 1 {
        return Err(ApiError::not_found());
    }
    let (plugin, handler, renderer) = matching.remove(0);
    let digest = catalog
        .source_digest()
        .map_err(|_| ApiError::new(500, "Invalid page source identity."))?;
    let invocation = plugin_handlers::Invocation {
        catalog_sha256: digest.clone(),
        plugin: plugin.clone(),
        handler_id: handler.id.clone(),
        request: plugin_handlers::Request::Page {
            query: meerschaum_server::routes::parse_form(
                request.uri().query().unwrap_or_default().as_bytes(),
            ),
        },
    };
    let reply =
        meerschaum_server::routes::extensions::execute_console(&state.server, &api, invocation)
            .await?;
    let plugin_handlers::Reply::Page { fragment, status } = reply else {
        return Err(ApiError::new(
            502,
            "The page handler returned another result kind.",
        ));
    };
    // execute_console revalidates this exact cookie, credential and captured source
    // after invocation. No awaited work follows that proof before page publication.
    let registry = catalog.catalog().plugins[&plugin]
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.registry.as_ref())
        .ok_or_else(ApiError::not_found)?;
    let handlers: Vec<_> = registry
        .handlers()
        .filter(|handler| {
            matches!(
                &handler.selector,
                plugin_handlers::Selector::DashCallback { .. }
            )
        })
        .cloned()
        .collect();
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let claims = json!({"sub":nonce,"mrsm_principal":GRANT_KIND,"exp":chrono::Utc::now().timestamp()+1800,
        "source_catalog_sha256":digest,"plugin":plugin,"page":handler.id,"callbacks":handlers.iter().map(|handler|handler.id.clone()).collect::<Vec<_>>(),
        "session_binding":session_binding(&state,&session_id,&data)?});
    let claims: Map = serde_json::from_value(claims)
        .map_err(|_| ApiError::new(500, "Invalid page bridge claims."))?;
    let grant = auth::jwt_encode(&claims, state.server.secret());
    let mut child = json!({"nonce":nonce,"handlers":handlers,"renderer":renderer});
    match renderer {
        plugins::PageRenderer::Html => child["html"] = inert_html(&fragment)?.into(),
        plugins::PageRenderer::Dash => {
            if fragment.len() > 2 * 1024 * 1024 {
                return Err(ApiError::new(
                    413,
                    "The Dash layout exceeds its byte limit.",
                ));
            }
            child["layout"] = serde_json::from_str(&fragment)
                .map_err(|_| ApiError::new(502, "Invalid typed Dash layout."))?;
        }
    }
    let child_json = script_json(&child)?;
    let frame = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"referrer\" content=\"no-referrer\"><meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; connect-src 'none'; img-src 'none'; font-src 'none'; frame-src 'none'; child-src 'none'; worker-src 'none'; object-src 'none'; form-action 'none'; base-uri 'none'\"><style id=\"plotly.js-style-global\" nonce=\"{nonce}\">body{{margin:0;padding:1rem;font-family:system-ui,sans-serif;color-scheme:light dark}}button,input,textarea,select{{font:inherit}}#mrsm-extension-root{{min-height:12rem}}</style><script nonce=\"{nonce}\" crossorigin=\"anonymous\" integrity=\"sha256-O24V1F27f8pb0glCkelh3cVHLNiHAJ5gCaVtq2aNch8=\" src=\"/plugins/extensions/assets/plotly.js\"></script></head><body><main id=\"mrsm-extension-root\"></main><script nonce=\"{nonce}\">window.__MRSM_EXTENSION__={child_json};{CHILD_SCRIPT}</script></body></html>"
    );
    if frame.len() > MAX_SHELL {
        return Err(ApiError::new(
            413,
            "The isolated frame exceeds its byte limit.",
        ));
    }
    let parent = script_json(
        &json!({"nonce":nonce,"grant":grant,"frame":frame,"handlers":child["handlers"]}),
    )?;
    let html = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"referrer\" content=\"no-referrer\"><title>Meerschaum plugin page</title><style nonce=\"{nonce}\">#mrsm-extension-frame{{width:100%;height:90vh;border:0}}</style></head><body><iframe id=\"mrsm-extension-frame\" sandbox=\"allow-scripts\" referrerpolicy=\"no-referrer\"></iframe><p id=\"mrsm-extension-status\" role=\"status\"></p><script nonce=\"{nonce}\">const page={parent};const frame=document.getElementById('mrsm-extension-frame');const status=document.getElementById('mrsm-extension-status');let inFlight=0;window.addEventListener('message',async event=>{{if(event.source!==frame.contentWindow||event.origin!=='null'||!event.data||event.data.nonce!==page.nonce)return;const message=event.data;if(message.kind==='ready'){{status.textContent='';return;}}if(message.kind==='error'){{status.textContent=String(message.message||'Unsupported page content');return;}}if(message.kind!=='callback'||inFlight>=2||!Number.isSafeInteger(message.serial)||!page.handlers.some(handler=>handler.id===message.handler))return;const text=JSON.stringify({{grant:page.grant,handler:message.handler,inputs:message.inputs}});if(text.length>{MAX_CALLBACK})return;inFlight++;try{{const response=await fetch('/plugins/extensions/page-callback',{{method:'POST',credentials:'same-origin',headers:{{'content-type':'application/json','x-mrsm-extension-page':'1'}},body:text}});const body=await response.json();frame.contentWindow.postMessage({{kind:'callback_result',nonce:page.nonce,serial:message.serial,updates:response.ok?body:null,error:response.ok?null:(body.detail||'The callback source is no longer authorized.')}},'*');}}catch(error){{frame.contentWindow.postMessage({{kind:'callback_result',nonce:page.nonce,serial:message.serial,error:'The page callback is unavailable.'}},'*');}}finally{{inFlight--;}}}});frame.srcdoc=page.frame;</script></body></html>"
    );
    if html.len() > MAX_SHELL {
        return Err(ApiError::new(
            413,
            "The isolated page shell exceeds its byte limit.",
        ));
    }
    let mut response = Html(html).into_response();
    *response.status_mut() = StatusCode::from_u16(status)
        .map_err(|_| ApiError::new(502, "Invalid page response status."))?;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    response.headers_mut().insert(header::CONTENT_SECURITY_POLICY,format!("default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; frame-src 'self' about:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'self'").parse().unwrap());
    drop(permit);
    Ok(response)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Callback {
    grant: String,
    handler: String,
    inputs: Json,
}

async fn callback(State(state): State<WebState>, request: Request) -> Response {
    match callback_result(state, request).await {
        Ok(value) => axum::Json(value).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn callback_result(state: WebState, request: Request) -> Result<Json, ApiError> {
    let _permit = CALLBACK_SLOTS
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::new(503, "The isolated callback limit is busy."))?;
    if request
        .headers()
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
        != Some("same-origin")
        || request
            .headers()
            .get("x-mrsm-extension-page")
            .and_then(|value| value.to_str().ok())
            != Some("1")
    {
        return Err(ApiError::new(
            403,
            "The page bridge requires a same-origin console request.",
        ));
    }
    let (parts, body) = request.into_parts();
    let (api, data, session_id) = browser_source(&state, &parts.headers).await?;
    let bytes = to_bytes(body, MAX_CALLBACK)
        .await
        .map_err(|_| ApiError::new(413, "The callback request exceeds its byte limit."))?;
    let request: Callback = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::new(422, "Invalid typed page callback."))?;
    let claims = auth::jwt_decode(&request.grant, state.server.secret(), chrono::Utc::now())
        .map_err(|_| ApiError::invalid_credentials())?;
    if claims["mrsm_principal"] != GRANT_KIND
        || claims["session_binding"] != session_binding(&state, &session_id, &data)?
        || claims["callbacks"]
            .as_array()
            .is_none_or(|ids| !ids.iter().any(|id| id == &request.handler))
    {
        return Err(ApiError::new(
            403,
            "The callback does not belong to this exact page session.",
        ));
    }
    let invocation = plugin_handlers::Invocation {
        catalog_sha256: claims["source_catalog_sha256"]
            .as_str()
            .ok_or_else(ApiError::invalid_credentials)?
            .into(),
        plugin: claims["plugin"]
            .as_str()
            .ok_or_else(ApiError::invalid_credentials)?
            .into(),
        handler_id: request.handler,
        request: plugin_handlers::Request::DashCallback {
            inputs: request.inputs,
        },
    };
    let reply =
        meerschaum_server::routes::extensions::execute_console(&state.server, &api, invocation)
            .await?;
    // The shared console-only executor already rechecked this exact session and source.
    match reply {
        plugin_handlers::Reply::DashCallback { updates } => Ok(updates),
        _ => Err(ApiError::new(
            502,
            "The callback returned another result kind.",
        )),
    }
}

async fn plotly(request: Request) -> Response {
    let mut response = assets::serve(request).await;
    response
        .headers_mut()
        .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
    response.headers_mut().insert(
        "cross-origin-resource-policy",
        "cross-origin".parse().unwrap(),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_plotly_provenance_matches_the_embedded_runtime() {
        let recorded: Json = serde_json::from_str(PLOTLY_PROVENANCE).unwrap();
        let asset = assets::lookup("plotly.js").unwrap();
        assert_eq!(recorded["raw_bytes"].as_u64(), Some(asset.raw_len as u64));
        assert!(recorded["sha256"].as_str().unwrap().starts_with(asset.hash));
        assert_eq!(recorded["version"], "4.1.1");
    }

    #[test]
    fn html_parser_keeps_inert_content_and_removes_navigation_script_and_request_elements() {
        let source = r#"<base href='https://fixture.invalid/'><meta http-equiv=refresh content='0;url=https://fixture.invalid/nav'><script>parent.location='https://fixture.invalid/script'</script><iframe src='https://fixture.invalid/frame'></iframe><form action='https://fixture.invalid/form'><button>send</button></form><svg><a xlink:href='https://fixture.invalid/svg'>go</a></svg><a href='https://fixture.invalid/link' ping='https://fixture.invalid/ping' onclick='location="https://fixture.invalid/event"'>read</a><p style='background:url(https://fixture.invalid/style)' class='note'>inert</p><img src='https://fixture.invalid/image'><div data-mrsm-figure='{"data":[]}'></div>"#;
        let html = inert_html(source).unwrap();
        assert!(html.contains("<p class=\"note\">inert</p>"), "{html}");
        assert!(html.contains("data-mrsm-figure"), "{html}");
        assert!(!html.contains("fixture.invalid"), "{html}");
        for tag in [
            "<script", "<svg", "<form", "<iframe", "<meta", "<base", "<img",
        ] {
            assert!(!html.contains(tag), "{html}");
        }
    }
}
