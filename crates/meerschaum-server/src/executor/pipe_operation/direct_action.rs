//! One registered destination selected by a built-in action's original typed keywords.
use super::{ApiError, PipeOperation};
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};

fn unsupported() -> ApiError {
    ApiError::unavailable(
        "This sync or verify action requires an exact Main capture of its selectors, arguments and source defaults.",
    )
}

/// The same CLI option also reaches catalog selection. Restrict it to ordinary row
/// columns which SQLite's registered-pipe selector leaves untouched.
pub(super) fn validate_row_params(params: &Map) -> Result<(), ApiError> {
    if params.is_empty()
        || params.len() > 32
        || params.iter().any(|(name, value)| {
            name.is_empty()
                || name.len() > 64
                || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                || matches!(
                    name.to_ascii_lowercase().as_str(),
                    "pipe_id" | "connector_keys" | "metric_key" | "location_key" | "parameters"
                )
                || !matches!(value, Value::Int(_))
        })
    {
        return Err(unsupported());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectAction {
    action: String,
    keywords: Map,
}
impl DirectAction {
    /// Built-in maintenance subactions do not select a data graph.
    pub fn graph_candidate(action: &str, keywords: &Map) -> bool {
        if !matches!(action, "sync" | "verify") {
            return false;
        }
        !(action == "verify"
            && matches!(keywords.get("action"),Some(Value::List(words)) if matches!(words.as_slice(),[word] if matches!(word.as_str(),Some("plugins"|"packages"|"venvs")))))
    }

    /// Every unrelated action retains its original dispatch. For sync/verify an unsupported
    /// argument set is an explicit refusal; dropping a keyword can widen a data window.
    pub fn capture(action: &str, keywords: &Map) -> Result<Option<Self>, ApiError> {
        if !Self::graph_candidate(action, keywords) {
            return Ok(None);
        }
        if keywords.len() > 9
            || keywords.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "action"
                        | "mrsm_instance"
                        | "connector_keys"
                        | "metric_keys"
                        | "location_keys"
                        | "begin"
                        | "end"
                        | "debug"
                        | "params"
                )
            })
        {
            return Err(unsupported());
        }
        if !matches!(keywords.get("debug"), None | Some(Value::Bool(false)))
            || !matches!(keywords.get("action"),Some(Value::List(words)) if words.len()==1 && words[0].as_str()==Some("pipes"))
        {
            return Err(unsupported());
        }
        let input = Self {
            action: action.into(),
            keywords: keywords.clone(),
        };
        input.selector("connector_keys")?.ok_or_else(unsupported)?;
        input.selector("metric_keys")?.ok_or_else(unsupported)?;
        input.selector("mrsm_instance")?;
        input.selector("location_keys")?;
        input.bound("begin")?;
        input.bound("end")?;
        input.row_params()?;
        Ok(Some(input))
    }

    pub(super) fn selector(&self, key: &str) -> Result<Option<&str>, ApiError> {
        let text = match (key, self.keywords.get(key)) {
            (_, None) | ("mrsm_instance", Some(Value::Null)) => return Ok(None),
            ("mrsm_instance", Some(Value::Str(text))) => text,
            ("location_keys", Some(Value::List(words))) if words.is_empty() => return Ok(None),
            ("location_keys", Some(Value::List(words))) if words.len() == 1 => match &words[0] {
                Value::Null => return Ok(Some("None")),
                Value::Str(text) if text == "*" => return Ok(None),
                Value::Str(text) if text == "[None]" => return Ok(Some("None")),
                Value::Str(text) => text,
                _ => return Err(unsupported()),
            },
            (_, Some(Value::List(words))) if words.len() == 1 => {
                words[0].as_str().ok_or_else(unsupported)?
            }
            _ => return Err(unsupported()),
        };
        if text.is_empty()
            || text.len() > 256
            || !text.as_bytes()[0].is_ascii_alphanumeric()
            || !text
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b':' | b'_' | b'.' | b'-'))
        {
            return Err(unsupported());
        }
        Ok(Some(text))
    }
    fn bound(&self, key: &str) -> Result<Option<String>, ApiError> {
        match self.keywords.get(key) {
            None => Ok(None),
            Some(Value::Int(number)) => Ok(Some(number.to_string())),
            Some(Value::Str(text))
                if !text.is_empty() && text.len() <= 256 && !text.chars().any(char::is_control) =>
            {
                Ok(Some(text.clone()))
            }
            _ => Err(unsupported()),
        }
    }
    fn row_params(&self) -> Result<Option<Map>, ApiError> {
        self.keywords
            .get("params")
            .map(|value| {
                let params = value.as_map().ok_or_else(unsupported)?;
                validate_row_params(params)?;
                Ok(params.clone())
            })
            .transpose()
    }

    /// Resolve omitted selectors once on the original Main. An explicit null location
    /// selects the unlocated pipe; an omitted location can match any registration.
    pub async fn resolve(
        &self,
        state: &crate::AppState,
        caller: &crate::access::Access,
    ) -> Result<PipeOperation, ApiError> {
        use crate::access::guard;
        let instance = self
            .selector("mrsm_instance")?
            .unwrap_or(&state.config.instance_keys);
        let connector = self.selector("connector_keys")?.ok_or_else(unsupported)?;
        let metric = self.selector("metric_keys")?.ok_or_else(unsupported)?;
        if let Some(location) = self.selector("location_keys")? {
            return self.selected(instance, location);
        }
        caller.require(&["pipes:write"], &[])?;
        if !instance
            .split_once(':')
            .is_some_and(|(kind, _)| matches!(kind, "sql" | "api" | "valkey"))
            || !guard::may_use_connector(state, caller, instance)
        {
            return Err(unsupported());
        }
        guard::check_source_connector(state, caller, connector)?;
        let conn = state.get_api_connector(Some(instance))?;
        let keys = conn
            .fetch_pipes_keys(&meerschaum_connector::PipesKeysQuery {
                connector_keys: vec![connector.into()],
                metric_keys: vec![metric.into()],
                params: self.row_params()?,
                ..Default::default()
            })
            .await
            .map_err(|_| unsupported())?;
        let entries = keys.entries();
        let [entry] = entries.as_slice() else {
            return Err(ApiError::unavailable(
                "This action requires exactly one registered pipe. Supply its location when the connector and metric select more than one.",
            ));
        };
        if entry.connector_keys != connector || entry.metric_key != metric {
            return Err(unsupported());
        }
        self.selected(instance, entry.location_key.as_deref().unwrap_or("None"))
    }
    /// The stored worker uses each original explicit selector and only fills the
    /// omitted ones from the exact identity which Main previously resolved.
    pub(super) fn request_for(&self, reference: &str) -> Result<PipeOperation, ApiError> {
        let parts = reference.split('|').collect::<Vec<_>>();
        let [instance, _, _, location] = parts.as_slice() else {
            return Err(unsupported());
        };
        let request = self.selected(instance, location)?;
        if request.pipe_ref != reference {
            return Err(unsupported());
        }
        Ok(request)
    }
    pub fn request(&self) -> Result<PipeOperation, ApiError> {
        self.selected(
            self.selector("mrsm_instance")?.ok_or_else(unsupported)?,
            self.selector("location_keys")?.ok_or_else(unsupported)?,
        )
    }
    fn selected(&self, instance: &str, location: &str) -> Result<PipeOperation, ApiError> {
        Ok(PipeOperation {
            row_params: self.row_params()?,
            pipe_ref: [
                self.selector("mrsm_instance")?.unwrap_or(instance),
                self.selector("connector_keys")?.ok_or_else(unsupported)?,
                self.selector("metric_keys")?.ok_or_else(unsupported)?,
                self.selector("location_keys")?.unwrap_or(location),
            ]
            .join("|"),
            registration: Value::Null,
            verify: self.action == "verify",
            begin: self.bound("begin")?,
            end: self.bound("end")?,
            parents: None,
            children: None,
            metadata: None,
        })
    }
}

/// The immutable action input and canonical native operation are both retained.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionPipe {
    pub input: DirectAction,
    pub normalized_at: i64,
    pub pipe: PipeOperation,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn keywords() -> Map {
        Value::from_json_str(r#"{"action":["pipes"],"connector_keys":["sql:db"],"metric_keys":["orders"],"location_keys":["None"],"mrsm_instance":"sql:main","begin":-2,"end":25}"#).unwrap().to_map()
    }
    #[test]
    fn captures_original_typed_bounds_and_refuses_additional_or_missing_selectors() {
        for action in ["sync", "verify"] {
            let mut words = keywords();
            let input = DirectAction::capture(action, &words).unwrap().unwrap();
            let request = input.request().unwrap();
            assert_eq!(request.begin.as_deref(), Some("-2"));
            assert_eq!(request.end.as_deref(), Some("25"));
            assert_eq!(request.verify, action == "verify");
            assert_eq!(input.keywords, words);
            words.insert("params".into(), Value::Map(Map::new()));
            assert!(DirectAction::capture(action, &words).is_err());
            words.shift_remove("params");
            words.shift_remove("location_keys");
            let unresolved = DirectAction::capture(action, &words).unwrap().unwrap();
            assert!(unresolved.request().is_err());
            assert_eq!(
                unresolved
                    .request_for("sql:main|sql:db|orders|west")
                    .unwrap()
                    .pipe_ref,
                "sql:main|sql:db|orders|west"
            );
            words.shift_remove("metric_keys");
            assert!(DirectAction::capture(action, &words).is_err());
        }
    }
    #[test]
    fn ordinary_show_action_keeps_its_own_keyword_grammar() {
        let words =
            Value::from_json_str(r#"{"action":["version"],"custom_version_format":"long"}"#)
                .unwrap()
                .to_map();
        assert!(DirectAction::capture("show", &words).unwrap().is_none());
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    use crate::executor::pipe_operation::GraphOperation;

    #[test]
    fn stored_direct_capture_retains_both_original_permissions_and_its_canonical_argv() {
        let keywords=Value::from_json_str(r#"{"action":["pipes"],"connector_keys":["sql:db"],"metric_keys":["a"],"location_keys":["None"],"mrsm_instance":"sql:main","begin":0,"end":10}"#).unwrap().to_map();
        let input = DirectAction::capture("verify", &keywords).unwrap().unwrap();
        let mut pipe = input.request().unwrap();
        pipe.registration = Value::Int(42);
        let original = GraphOperation::Direct(ActionPipe {
            input,
            normalized_at: 1,
            pipe,
        });
        let restored: GraphOperation =
            serde_json::from_value(serde_json::to_value(&original).unwrap()).unwrap();
        assert_eq!(restored, original);
        assert!(restored.sysargs().unwrap().contains(&"--begin=0".into()));
        assert_eq!(
            restored.required_scopes(),
            std::collections::BTreeSet::from(["actions:execute".into(), "pipes:write".into()])
        );
    }
}
