//! URI parsing (`SQLConnector.parse_uri` as `APIConnector` uses it, and
//! `APIConnector.from_uri`).

use meerschaum_core::{Map, Value};

use crate::traits::{ConnectorError, ConnectorResult};

/// Percent-decoding (`urllib.parse.unquote`, as SQLAlchemy's `make_url` decodes credentials).
pub fn unquote(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(v) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `parse_uri(uri)`: `flavor://[username[:password]@]host[:port][/database][?key=value...]` into
/// `host`, `port`, `username`, `password`, `database`, `flavor` and the query parameters
/// (first values).
pub fn parse_uri(uri: &str) -> ConnectorResult<Map> {
    let mut params = Map::new();
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| ConnectorError::Invalid(format!("Could not parse URI '{uri}'.")))?;
    let (main, query) = match rest.split_once('?') {
        Some((m, q)) => (m, Some(q)),
        None => (rest, None),
    };
    let (authority, database) = match main.split_once('/') {
        Some((a, d)) => (a, Some(d)),
        None => (main, None),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    if let Some(u) = userinfo {
        match u.split_once(':') {
            Some((user, pass)) => {
                params.insert("username".into(), Value::Str(unquote(user)));
                params.insert("password".into(), Value::Str(unquote(pass)));
            }
            None => {
                params.insert("username".into(), Value::Str(unquote(u)));
            }
        }
    }
    let (host, port) = if let Some(stripped) = hostport.strip_prefix('[') {
        match stripped.split_once(']') {
            Some((h, rest)) => (h.to_string(), rest.strip_prefix(':').map(String::from)),
            None => (hostport.to_string(), None),
        }
    } else {
        match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), Some(p.to_string())),
            None => (hostport.to_string(), None),
        }
    };
    if !host.is_empty() {
        params.insert("host".into(), Value::Str(host));
    }
    if let Some(p) = port.filter(|p| !p.is_empty()) {
        let port: i64 = p
            .parse()
            .map_err(|_| ConnectorError::Invalid(format!("Invalid port '{p}' in URI.")))?;
        params.insert("port".into(), Value::Int(port));
    }
    if let Some(db) = database.filter(|d| !d.is_empty()) {
        params.insert("database".into(), Value::Str(unquote(db)));
    }
    let mut flavor = scheme.split('+').next().unwrap_or(scheme).to_string();
    if flavor == "postgres" {
        flavor = "postgresql".into();
    }
    params.insert("flavor".into(), Value::Str(flavor));
    if let Some(q) = query {
        for (k, v) in crate::wire::parse_query(q) {
            if !params.contains_key(&k) || !["host", "port", "flavor"].contains(&k.as_str()) {
                params.entry(k).or_insert(Value::Str(v));
            }
        }
    }
    Ok(params)
}

/// `APIConnector.from_uri(uri, label, as_dict=True)`: the parsed attributes with `protocol` (the
/// scheme) and `label` (`[username@]host`, lower-cased, unless given).
pub fn from_uri_params(uri: &str, label: Option<&str>) -> ConnectorResult<Map> {
    let mut params = parse_uri(uri)?;
    if !params.contains_key("host") {
        return Err(ConnectorError::Invalid(
            "No host was found in the provided URI.".into(),
        ));
    }
    let flavor = params.shift_remove("flavor").unwrap_or(Value::Null);
    params.insert("protocol".into(), flavor);
    let label = match label {
        Some(l) => l.to_string(),
        None => {
            let user = params
                .get("username")
                .map(|u| format!("{}@", meerschaum_core::pyfmt::py_str(u)))
                .unwrap_or_default();
            let host = params
                .get("host")
                .map(meerschaum_core::pyfmt::py_str)
                .unwrap_or_default();
            (user + &host).to_lowercase()
        }
    };
    params.insert("label".into(), Value::Str(label));
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_api_uris() {
        let p = parse_uri("http://user:p%40ss@localhost:8989/db?x=1").unwrap();
        assert_eq!(p["username"], Value::Str("user".into()));
        assert_eq!(p["password"], Value::Str("p@ss".into()));
        assert_eq!(p["host"], Value::Str("localhost".into()));
        assert_eq!(p["port"], Value::Int(8989));
        assert_eq!(p["database"], Value::Str("db".into()));
        assert_eq!(p["flavor"], Value::Str("http".into()));
        assert_eq!(p["x"], Value::Str("1".into()));
        let p = from_uri_params("https://Example.com", None).unwrap();
        assert_eq!(p["label"], Value::Str("example.com".into()));
        assert_eq!(p["protocol"], Value::Str("https".into()));
        let p = from_uri_params("http://U@h:1", None).unwrap();
        assert_eq!(p["label"], Value::Str("u@h".into()));
        assert!(from_uri_params("http://", None).is_err());
    }
}
