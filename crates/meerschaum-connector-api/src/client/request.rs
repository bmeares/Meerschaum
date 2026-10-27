//! `make_request()` and the verb wrappers (`meerschaum/connectors/api/_request.py`), encoding
//! requests the way `requests` does.

use std::time::Duration;

use crate::traits::{ConnectorError, ConnectorResult};
use crate::wire::{QueryValue, encode_form, encode_query};

use super::APIConnector;

/// An HTTP method (`METHODS` in `_request.py`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// `GET`
    Get,
    /// `OPTIONS`
    Options,
    /// `HEAD`
    Head,
    /// `POST`
    Post,
    /// `PUT`
    Put,
    /// `PATCH`
    Patch,
    /// `DELETE`
    Delete,
}

impl Method {
    /// The method name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Options => "OPTIONS",
            Method::Head => "HEAD",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Patch => "PATCH",
            Method::Delete => "DELETE",
        }
    }

    fn to_reqwest(self) -> reqwest::Method {
        match self {
            Method::Get => reqwest::Method::GET,
            Method::Options => reqwest::Method::OPTIONS,
            Method::Head => reqwest::Method::HEAD,
            Method::Post => reqwest::Method::POST,
            Method::Put => reqwest::Method::PUT,
            Method::Patch => reqwest::Method::PATCH,
            Method::Delete => reqwest::Method::DELETE,
        }
    }
}

/// A request body, as `requests` sends each kind.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum Body {
    /// No body.
    #[default]
    None,
    /// `json=obj`: the `json.dumps` text, with `Content-Type: application/json`.
    Json(String),
    /// `data='...'`: a string body without a `Content-Type`.
    Text(String),
    /// `data={...}`: `application/x-www-form-urlencoded` (`None` values already dropped).
    Form(Vec<(String, String)>),
    /// `files={name: file}`: one multipart file part.
    File {
        /// The field name.
        field: String,
        /// The file name (`os.path.basename(fp.name)`).
        file_name: String,
        /// The content.
        data: Vec<u8>,
    },
}

/// The keyword arguments of `make_request`.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestOptions {
    /// `params=`: the query, in order (`None` values are dropped).
    pub query: Vec<(String, QueryValue)>,
    /// `headers=`.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Body,
    /// `use_token`: add `Authorization: Bearer {token}`.
    pub use_token: bool,
    /// `timeout=` (default `STATIC_CONFIG['api']['default_timeout']`).
    pub timeout: Option<Duration>,
}

impl Default for RequestOptions {
    fn default() -> Self {
        RequestOptions {
            query: Vec::new(),
            headers: Vec::new(),
            body: Body::None,
            use_token: true,
            timeout: None,
        }
    }
}

impl RequestOptions {
    /// Options with a query.
    pub fn query(pairs: Vec<(&str, QueryValue)>) -> RequestOptions {
        RequestOptions {
            query: pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            ..Default::default()
        }
    }

    /// Add a body.
    pub fn with_body(mut self, body: Body) -> RequestOptions {
        self.body = body;
        self
    }

    /// Without the token.
    pub fn without_token(mut self) -> RequestOptions {
        self.use_token = false;
        self
    }
}

/// A response: status, `Content-Type` and the body.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiResponse {
    /// The status code.
    pub status: u16,
    /// The `Content-Type` header.
    pub content_type: String,
    /// The raw body.
    pub bytes: Vec<u8>,
}

impl ApiResponse {
    /// `bool(response)` / `response.ok`: status below 400.
    pub fn ok(&self) -> bool {
        self.status < 400
    }

    /// `response.text`.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }

    /// `response.json()`.
    pub fn json(&self) -> ConnectorResult<serde_json::Value> {
        Ok(serde_json::from_slice(&self.bytes)?)
    }

    /// The body of a v5-only route (`/pipelines/*`, `/engine/*`, `/execute*`), which answers
    /// `{"success": bool, "message": str, ...}` with real status codes: the object when it
    /// succeeded, else [`ConnectorError::Http`] with the status and the server's message. A body
    /// in an older shape (a server before 5.0.0rc15) comes back as it is when the status is a
    /// success, and as an error otherwise.
    pub fn envelope(&self) -> ConnectorResult<serde_json::Value> {
        let parsed = serde_json::from_slice::<serde_json::Value>(&self.bytes);
        if let Ok(serde_json::Value::Object(o)) = &parsed
            && let Some(success) = o.get("success").and_then(serde_json::Value::as_bool)
            && let Some(message) = o.get("message").and_then(serde_json::Value::as_str)
        {
            if success && self.ok() {
                return Ok(serde_json::Value::Object(o.clone()));
            }
            return Err(ConnectorError::Http {
                status: self.status,
                body: message.to_string(),
            });
        }
        if !self.ok() {
            if let Ok(serde_json::Value::Object(o)) = &parsed
                && let Some(detail) = o.get("detail")
            {
                return Err(ConnectorError::Http {
                    status: self.status,
                    body: match detail {
                        serde_json::Value::String(s) => s.clone(),
                        other => crate::models::detail_repr(other),
                    },
                });
            }
            return Err(ConnectorError::Http {
                status: self.status,
                body: self.text(),
            });
        }
        Ok(parsed?)
    }

    /// One field of an [`envelope`](Self::envelope) (`null` when absent); from a server before
    /// 5.0.0rc15, the whole (older) body.
    pub fn envelope_field(&self, key: &str) -> ConnectorResult<serde_json::Value> {
        let j = self.envelope()?;
        match j {
            serde_json::Value::Object(mut o)
                if o.get("success").is_some_and(serde_json::Value::is_boolean)
                    && o.get("message").is_some_and(serde_json::Value::is_string) =>
            {
                Ok(o.remove(key).unwrap_or(serde_json::Value::Null))
            }
            other => Ok(other),
        }
    }

    /// `response.json()` with the body's `{"detail": ...}` turned into an error.
    pub fn json_or_detail(&self) -> ConnectorResult<serde_json::Value> {
        let j = self.json()?;
        if let Some(d) = j.as_object().and_then(|o| o.get("detail")) {
            return Err(ConnectorError::Detail(match d {
                serde_json::Value::String(s) => s.clone(),
                other => crate::models::detail_repr(other),
            }));
        }
        Ok(j)
    }
}

/// urllib3's path encoding (`_encode_invalid_chars(path, PATH_CHARS)`) followed by `requests`'
/// `unquote_unreserved`: characters outside `A-Za-z0-9-._~!$&'()*+,;=:@/` are percent-encoded
/// (so `[None]` becomes `%5BNone%5D`), valid escapes are upper-cased and kept.
pub fn encode_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let is_hex = |b: u8| b.is_ascii_hexdigit();
    let valid_escapes = (0..bytes.len())
        .filter(|&i| {
            bytes[i] == b'%' && i + 2 < bytes.len() && is_hex(bytes[i + 1]) && is_hex(bytes[i + 2])
        })
        .count();
    let all_percent_valid = valid_escapes == bytes.iter().filter(|b| **b == b'%').count();
    let allowed = |b: u8| b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/".contains(&b);
    let mut out = String::with_capacity(path.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && all_percent_valid {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("00");
            let v = u8::from_str_radix(hex, 16).unwrap_or(0);
            // `unquote_unreserved`: decode escapes of unreserved characters.
            if v.is_ascii_alphanumeric() || b"-._~".contains(&v) {
                out.push(v as char);
            } else {
                out.push('%');
                out.push_str(&hex.to_uppercase());
            }
            i += 3;
            continue;
        }
        if b < 128 && allowed(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
        i += 1;
    }
    out
}

impl APIConnector {
    /// The full request URL for a relative path and an encoded query.
    pub fn request_url(&self, r_url: &str, query: &[(String, QueryValue)]) -> String {
        let pairs: Vec<(&str, QueryValue)> =
            query.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let q = encode_query(&pairs);
        let mut url = format!("{}{}", self.url, encode_path(r_url));
        if !q.is_empty() {
            url.push('?');
            url.push_str(&q);
        }
        url
    }

    /// `make_request(method, r_url, headers, use_token, **kwargs)`.
    pub async fn make_request(
        &self,
        method: Method,
        r_url: &str,
        opts: RequestOptions,
    ) -> ConnectorResult<ApiResponse> {
        let url = self.request_url(r_url, &opts.query);
        let mut req = self
            .http
            .request(method.to_reqwest(), &url)
            .timeout(opts.timeout.unwrap_or(self.timeout));
        for (k, v) in &opts.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if opts.use_token {
            // PARITY: an unavailable token is sent as `Bearer None` (`f'Bearer {self.token}'`).
            // Boxed: `token()` may log in, which posts through `make_request`.
            let token = Box::pin(self.token())
                .await
                .unwrap_or_else(|| "None".into());
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        req = match opts.body {
            Body::None => req,
            Body::Json(s) => req.header("Content-Type", "application/json").body(s),
            Body::Text(s) => req.body(s),
            Body::Form(fields) => {
                let pairs: Vec<(&str, &str)> = fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                req.header("Content-Type", "application/x-www-form-urlencoded")
                    .body(encode_form(&pairs))
            }
            Body::File {
                field,
                file_name,
                data,
            } => {
                let part = reqwest::multipart::Part::bytes(data).file_name(file_name);
                req.multipart(reqwest::multipart::Form::new().part(field, part))
            }
        };
        let response = req.send().await.map_err(|e| {
            if let Some(message) = self.connect_failure(&e) {
                return ConnectorError::Backend(message);
            }
            let mut msg = e.to_string();
            let mut source = std::error::Error::source(&e);
            while let Some(s) = source {
                msg.push_str(&format!(": {s}"));
                source = s.source();
            }
            ConnectorError::Backend(msg)
        })?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| ConnectorError::Backend(e.to_string()))?
            .to_vec();
        Ok(ApiResponse {
            status,
            content_type,
            bytes,
        })
    }

    /// `get(r_url, **kwargs)`.
    pub async fn get(&self, r_url: &str, opts: RequestOptions) -> ConnectorResult<ApiResponse> {
        self.make_request(Method::Get, r_url, opts).await
    }

    /// `post(r_url, **kwargs)`.
    pub async fn post(&self, r_url: &str, opts: RequestOptions) -> ConnectorResult<ApiResponse> {
        self.make_request(Method::Post, r_url, opts).await
    }

    /// `put(r_url, **kwargs)`.
    pub async fn put(&self, r_url: &str, opts: RequestOptions) -> ConnectorResult<ApiResponse> {
        self.make_request(Method::Put, r_url, opts).await
    }

    /// `patch(r_url, **kwargs)`.
    pub async fn patch(&self, r_url: &str, opts: RequestOptions) -> ConnectorResult<ApiResponse> {
        self.make_request(Method::Patch, r_url, opts).await
    }

    /// `delete(r_url, **kwargs)`.
    pub async fn delete(&self, r_url: &str, opts: RequestOptions) -> ConnectorResult<ApiResponse> {
        self.make_request(Method::Delete, r_url, opts).await
    }

    /// `wget(r_url)`: download a resource (with the token) and return its bytes.
    pub async fn wget(&self, r_url: &str) -> ConnectorResult<Vec<u8>> {
        Ok(self.get(r_url, RequestOptions::default()).await?.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urllib3_paths() {
        assert_eq!(
            encode_path("/pipes/sql:main/weather/[None]"),
            "/pipes/sql:main/weather/%5BNone%5D"
        );
        assert_eq!(encode_path("/pipes/a b/é"), "/pipes/a%20b/%C3%A9");
        assert_eq!(encode_path("/x/%41%2f"), "/x/A%2F");
        assert_eq!(encode_path("/x/100%"), "/x/100%25");
    }
}
