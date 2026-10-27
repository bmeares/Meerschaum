//! `/openapi.json`, `/docs` (Swagger UI), `/docs/oauth2-redirect` and `/redoc`, rendered like
//! FastAPI's `get_swagger_ui_html` / `get_redoc_html`. The docs pages are omitted in production
//! unless `api:endpoints:docs_in_production`.

use std::sync::Arc;

use axum::Router;
use axum::http::header;
use axum::response::{Html, IntoResponse};
use axum::routing::get;

use crate::config::ServerConfig;
use crate::state::AppState;

/// FastAPI's Swagger UI page for the `Meerschaum API` title.
pub const SWAGGER_HTML: &str = r##"
    <!DOCTYPE html>
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link type="text/css" rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
    <link rel="shortcut icon" href="https://fastapi.tiangolo.com/img/favicon.png">
    <title>Meerschaum API - Swagger UI</title>
    </head>
    <body>
    <div id="swagger-ui">
    </div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <!-- `SwaggerUIBundle` is now available on the page -->
    <script>
    const ui = SwaggerUIBundle({
        url: '/openapi.json',
    "dom_id": "#swagger-ui",
"layout": "BaseLayout",
"deepLinking": true,
"showExtensions": true,
"showCommonExtensions": true,
oauth2RedirectUrl: window.location.origin + '/docs/oauth2-redirect',
    presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIBundle.SwaggerUIStandalonePreset
        ],
    })
    </script>
    </body>
    </html>
    "##;

const REDOC_HTML: &str = r##"
    <!DOCTYPE html>
    <html>
    <head>
    <title>Meerschaum API - ReDoc</title>
    <!-- needed for adaptive design -->
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
    <link rel="shortcut icon" href="https://fastapi.tiangolo.com/img/favicon.png">
    <!--
    ReDoc doesn't change outer page styles
    -->
    <style>
      body {
        margin: 0;
        padding: 0;
      }
    </style>
    </head>
    <body>
    <noscript>
        ReDoc requires Javascript to function. Please enable it to browse the documentation.
    </noscript>
    <redoc spec-url="/openapi.json"></redoc>
    <script src="https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js"> </script>
    </body>
    </html>
    "##;

const OAUTH2_REDIRECT_HTML: &str = r##"<!doctype html>
<html lang="en-US">
<head>
    <title>Swagger UI: OAuth2 Redirect</title>
</head>
<body>
<script>
    'use strict';
    function run () {
        var oauth2 = window.opener.swaggerUIRedirectOauth2;
        var sentState = oauth2.state;
        var redirectUrl = oauth2.redirectUrl;
        var isValid, qp, arr;
        if (/code|token|error/.test(window.location.hash)) {
            qp = window.location.hash.substring(1).replace('?', '&');
        } else {
            qp = location.search.substring(1);
        }
        arr = qp.split("&");
        arr.forEach(function (v,i,_arr) { _arr[i] = '"' + v.replace('=', '":"') + '"';});
        qp = qp ? JSON.parse('{' + arr.join() + '}',
                function (key, value) {
                    return key === "" ? value : decodeURIComponent(value);
                }
        ) : {};
        isValid = qp.state === sentState;
        if ((oauth2.auth.schema.get("flow") === "accessCode" ||
             oauth2.auth.schema.get("flow") === "authorizationCode" ||
             oauth2.auth.schema.get("flow") === "authorization_code"
            ) && !oauth2.auth.code) {
            if (!isValid) {
                oauth2.errCb({authId: oauth2.auth.name, source: "auth", level: "warning",
                    message: "Authorization may be unsafe, passed state was changed in server. The passed state wasn't returned from auth server."});
            }
            if (qp.code) {
                delete oauth2.state;
                oauth2.auth.code = qp.code;
                oauth2.callback({auth: oauth2.auth, redirectUrl: redirectUrl});
            } else {
                oauth2.errCb({authId: oauth2.auth.name, source: "auth", level: "error",
                    message: "[Authorization failed]: no accessCode received from the server."});
            }
        } else {
            oauth2.callback({auth: oauth2.auth, token: qp, isValid: isValid, redirectUrl: redirectUrl});
        }
        window.close();
    }
    if (document.readyState !== 'loading') {
        run();
    } else {
        document.addEventListener('DOMContentLoaded', function () {
            run();
        });
    }
</script>
</body>
</html>
"##;

/// Register `/openapi.json` and (when enabled) the docs pages. The public preset serves neither
/// unless the allowlist names the `docs` group.
pub fn routes(app: Router<AppState>, cfg: &ServerConfig) -> Router<AppState> {
    if !cfg.openapi_enabled {
        return app;
    }
    let doc = Arc::new(
        crate::openapi::document(cfg)
            .to_json()
            .unwrap_or_else(|_| "{}".into()),
    );
    let app =
        app.route(
            meerschaum_connector_api::routes::OPENAPI,
            get(move || {
                let doc = doc.clone();
                async move {
                    ([(header::CONTENT_TYPE, "application/json")], (*doc).clone()).into_response()
                }
            }),
        );
    if !cfg.docs_enabled {
        return app;
    }
    app.route(
        meerschaum_connector_api::routes::DOCS,
        get(|| async { Html(SWAGGER_HTML) }),
    )
    .route(
        "/docs/oauth2-redirect",
        get(|| async { Html(OAUTH2_REDIRECT_HTML) }),
    )
    .route(
        meerschaum_connector_api::routes::REDOC,
        get(|| async { Html(REDOC_HTML) }),
    )
}
