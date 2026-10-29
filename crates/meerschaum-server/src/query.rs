//! Query-string parameters with FastAPI / pydantic v2 semantics: the last repeated key wins, lax
//! boolean and integer parsing, and 422 validation errors in pydantic's shape.

use serde_json::Value as Json;

use crate::error::ApiError;

/// A request's query parameters.
#[derive(Debug, Clone, Default)]
pub struct Query {
    pairs: Vec<(String, String)>,
}

/// pydantic's lax boolean parsing of strings.
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "1" | "on" | "t" | "true" | "y" | "yes" => Some(true),
        "0" | "off" | "f" | "false" | "n" | "no" => Some(false),
        _ => None,
    }
}

/// pydantic's lax integer parsing of strings (surrounding whitespace, a sign, `_` separators
/// between digits).
pub fn parse_int(s: &str) -> Option<i64> {
    let t = s.trim();
    if t.contains("__") || t.ends_with('_') || t.starts_with('_') {
        return None;
    }
    t.replace('_', "").parse::<i64>().ok()
}

impl Query {
    /// Parse a raw query string (`parse_qsl(keep_blank_values=True)`).
    pub fn parse(raw: Option<&str>) -> Query {
        Query {
            pairs: raw
                .map(meerschaum_connector_api::wire::parse_query)
                .unwrap_or_default(),
        }
    }

    /// The raw value of `key` (the last one wins, like Starlette's `QueryParams`).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Every value of `key`, in order.
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        self.pairs
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    /// An optional string parameter.
    pub fn opt_str(&self, key: &str) -> Option<String> {
        self.get(key).map(String::from)
    }

    /// A string parameter with a default.
    pub fn str_or(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or(default).to_string()
    }

    /// A boolean parameter with a default.
    pub fn bool(&self, key: &str, default: bool) -> Result<bool, ApiError> {
        match self.get(key) {
            None => Ok(default),
            Some(v) => parse_bool(v).ok_or_else(|| {
                ApiError::validation(vec![ApiError::field_error(
                    "bool_parsing",
                    &["query", key],
                    "Input should be a valid boolean, unable to interpret input",
                    Json::String(v.to_string()),
                    None,
                )])
            }),
        }
    }

    /// An optional integer parameter.
    pub fn opt_int(&self, key: &str) -> Result<Option<i64>, ApiError> {
        match self.get(key) {
            None => Ok(None),
            Some(v) => parse_int(v).map(Some).ok_or_else(|| int_error(key, v)),
        }
    }

    /// An integer parameter with a default.
    pub fn int(&self, key: &str, default: i64) -> Result<i64, ApiError> {
        Ok(self.opt_int(key)?.unwrap_or(default))
    }
}

fn int_error(key: &str, v: &str) -> ApiError {
    ApiError::validation(vec![ApiError::field_error(
        "int_parsing",
        &["query", key],
        "Input should be a valid integer, unable to parse string as an integer",
        Json::String(v.to_string()),
        None,
    )])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lax_parsing() {
        let q = Query::parse(Some("a=True&a=no&n=+12&x=1_000&bad=1.5"));
        assert!(!q.bool("a", true).unwrap());
        assert_eq!(q.int("n", 0).unwrap(), 12);
        assert_eq!(q.int("x", 0).unwrap(), 1000);
        assert!(q.opt_int("bad").is_err());
        assert!(q.bool("missing", true).unwrap());
        assert!(q.bool("n", true).is_err());
    }
}
