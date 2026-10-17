//! The MCP registry: tools, resources and prompts as data (`MCPTool`, `MCPResource`, `MCPPrompt`
//! in `meerschaum/mcp/_registry.py`).
//!
//! The built-in entries are v4's, embedded from `registry.json`. Plugin entries come from the
//! Python API host ([`super::federation`]) in the same shape.

use std::sync::OnceLock;

use meerschaum_core::{Map, Value};

use super::protocol::{map_of, s};

/// Where an entry is implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A Rust handler in this crate.
    Builtin,
    /// A Python plugin, hosted by the API host.
    Host,
}

fn strings(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_list)
        .map(|l| l.iter().map(Value::py_str).collect())
        .unwrap_or_default()
}

fn opt_str(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::Str(s)) => Some(s.clone()),
        Some(other) => Some(other.py_str()),
    }
}

fn flag(v: Option<&Value>) -> bool {
    v.is_some_and(Value::truthy)
}

/// An MCP tool (`MCPTool`).
#[derive(Debug, Clone, PartialEq)]
pub struct Tool {
    /// The tool name.
    pub name: String,
    /// The description shown to the model.
    pub description: String,
    /// The human title.
    pub title: Option<String>,
    /// The scopes required to see and call it.
    pub scopes: Vec<String>,
    /// `readOnlyHint`.
    pub read_only: bool,
    /// `destructiveHint`.
    pub destructive: bool,
    /// `idempotentHint`.
    pub idempotent: bool,
    /// `openWorldHint`.
    pub open_world: bool,
    /// The input JSON Schema.
    pub input_schema: Value,
    /// The output JSON Schema (enables `structuredContent`).
    pub output_schema: Option<Value>,
    /// Who implements it.
    pub source: Source,
}

impl Tool {
    /// Parse a registry entry (the fields of `MCPTool.__init__`).
    pub fn from_value(v: &Value, source: Source) -> Option<Tool> {
        Some(Tool {
            name: opt_str(v.get("name"))?,
            description: opt_str(v.get("description")).unwrap_or_default(),
            title: opt_str(v.get("title")),
            scopes: strings(v.get("scopes")),
            read_only: flag(v.get("read_only")),
            destructive: flag(v.get("destructive")),
            idempotent: flag(v.get("idempotent")),
            open_world: flag(v.get("open_world")),
            input_schema: match v.get("input_schema") {
                Some(Value::Null) | None => Value::Map(map_of([
                    ("type", s("object")),
                    ("properties", Value::new_map()),
                ])),
                Some(schema) => schema.clone(),
            },
            output_schema: v.get("output_schema").filter(|o| o.truthy()).cloned(),
            source,
        })
    }

    /// The `required` arguments of the input schema.
    pub fn required(&self) -> Vec<String> {
        strings(self.input_schema.get("required"))
    }

    /// `MCPTool.annotations`.
    pub fn annotations(&self) -> Value {
        let mut a = map_of([
            ("readOnlyHint", Value::Bool(self.read_only)),
            ("idempotentHint", Value::Bool(self.idempotent)),
            ("openWorldHint", Value::Bool(self.open_world)),
        ]);
        if let Some(t) = self.title.as_ref().filter(|t| !t.is_empty()) {
            a.insert("title".into(), s(t.clone()));
        }
        // Per the MCP spec, `destructiveHint` is only meaningful when not read-only.
        if !self.read_only {
            a.insert("destructiveHint".into(), Value::Bool(self.destructive));
        }
        Value::Map(a)
    }

    /// `MCPTool.to_dict()`: the `tools/list` representation.
    pub fn to_dict(&self) -> Value {
        let mut d = map_of([
            ("name", s(self.name.clone())),
            ("description", s(self.description.clone())),
            ("inputSchema", self.input_schema.clone()),
            ("annotations", self.annotations()),
        ]);
        if let Some(t) = self.title.as_ref().filter(|t| !t.is_empty()) {
            d.insert("title".into(), s(t.clone()));
        }
        if let Some(o) = &self.output_schema {
            d.insert("outputSchema".into(), o.clone());
        }
        Value::Map(d)
    }
}

/// An MCP resource or resource template (`MCPResource`).
#[derive(Debug, Clone, PartialEq)]
pub struct Resource {
    /// The resource name.
    pub name: String,
    /// The description.
    pub description: String,
    /// The human title.
    pub title: Option<String>,
    /// A static URI.
    pub uri: Option<String>,
    /// An RFC 6570 template (`{var}` segments).
    pub uri_template: Option<String>,
    /// The MIME type of `text`.
    pub mime_type: String,
    /// The scopes required to see and read it.
    pub scopes: Vec<String>,
    /// Who implements it.
    pub source: Source,
}

impl Resource {
    /// Parse a registry entry.
    pub fn from_value(v: &Value, source: Source) -> Option<Resource> {
        let uri = opt_str(v.get("uri")).filter(|u| !u.is_empty());
        let uri_template = opt_str(v.get("uri_template")).filter(|u| !u.is_empty());
        if uri.is_none() && uri_template.is_none() {
            return None;
        }
        Some(Resource {
            name: opt_str(v.get("name"))?,
            description: opt_str(v.get("description")).unwrap_or_default(),
            title: opt_str(v.get("title")),
            uri,
            uri_template,
            mime_type: opt_str(v.get("mime_type")).unwrap_or_else(|| "application/json".into()),
            scopes: strings(v.get("scopes")),
            source,
        })
    }

    /// `is_template`.
    pub fn is_template(&self) -> bool {
        self.uri_template.is_some()
    }

    /// `match(uri)`: the template variables when `uri` addresses this resource (an empty map
    /// for a static URI). Each `{var}` matches `[^/]+`.
    pub fn matches(&self, uri: &str) -> Option<Map> {
        let template = self
            .uri_template
            .as_deref()
            .or(self.uri.as_deref())
            .unwrap_or("");
        let mut vars = Map::new();
        let mut rest = uri;
        let mut t = template;
        loop {
            match t.find('{') {
                None => return (rest == t).then_some(vars),
                Some(open) => {
                    let close = t[open..].find('}').map(|c| open + c);
                    let name = close.map(|c| &t[open + 1..c]).filter(|n| is_identifier(n));
                    let (Some(close), Some(name)) = (close, name) else {
                        // Not a variable: match literally up to and including the brace.
                        let lit = &t[..=open];
                        rest = rest.strip_prefix(lit)?;
                        t = &t[open + 1..];
                        continue;
                    };
                    rest = rest.strip_prefix(&t[..open])?;
                    t = &t[close + 1..];
                    // `[^/]+` is greedy and the regex backtracks; with a literal after the
                    // variable, take the longest prefix without `/` that lets the rest match.
                    let seg_end = rest.find('/').unwrap_or(rest.len());
                    if seg_end == 0 {
                        return None;
                    }
                    let next_lit_end = t.find('{').unwrap_or(t.len());
                    let next_lit = &t[..next_lit_end];
                    let mut chosen = None;
                    for end in (1..=seg_end).rev() {
                        if !rest.is_char_boundary(end) {
                            continue;
                        }
                        let after = &rest[end..];
                        if after.starts_with(next_lit) && (next_lit_end < t.len() || after == t) {
                            chosen = Some(end);
                            break;
                        }
                    }
                    let end = chosen?;
                    vars.insert(name.to_string(), s(rest[..end].to_string()));
                    rest = &rest[end..];
                }
            }
        }
    }

    /// `to_dict()`: the `resources/list` or `resources/templates/list` representation.
    pub fn to_dict(&self) -> Value {
        let mut d = map_of([
            ("name", s(self.name.clone())),
            ("description", s(self.description.clone())),
            ("mimeType", s(self.mime_type.clone())),
        ]);
        if let Some(t) = self.title.as_ref().filter(|t| !t.is_empty()) {
            d.insert("title".into(), s(t.clone()));
        }
        match (&self.uri_template, &self.uri) {
            (Some(t), _) => d.insert("uriTemplate".into(), s(t.clone())),
            (None, u) => d.insert("uri".into(), u.clone().map(Value::Str).unwrap_or_default()),
        };
        Value::Map(d)
    }
}

fn is_identifier(n: &str) -> bool {
    let mut chars = n.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// An MCP prompt (`MCPPrompt`).
#[derive(Debug, Clone, PartialEq)]
pub struct Prompt {
    /// The prompt name.
    pub name: String,
    /// The description.
    pub description: String,
    /// The human title.
    pub title: Option<String>,
    /// The declared arguments (`{name, description, required}`).
    pub arguments: Vec<Value>,
    /// The scopes required to see and get it.
    pub scopes: Vec<String>,
    /// Who implements it.
    pub source: Source,
}

impl Prompt {
    /// Parse a registry entry.
    pub fn from_value(v: &Value, source: Source) -> Option<Prompt> {
        Some(Prompt {
            name: opt_str(v.get("name"))?,
            description: opt_str(v.get("description")).unwrap_or_default(),
            title: opt_str(v.get("title")),
            arguments: v
                .get("arguments")
                .and_then(Value::as_list)
                .cloned()
                .unwrap_or_default(),
            scopes: strings(v.get("scopes")),
            source,
        })
    }

    /// The declared argument names.
    pub fn argument_names(&self) -> Vec<String> {
        self.arguments
            .iter()
            .filter_map(|a| a.get("name").map(Value::py_str))
            .collect()
    }

    /// `to_dict()`: the `prompts/list` representation.
    pub fn to_dict(&self) -> Value {
        let mut d = map_of([
            ("name", s(self.name.clone())),
            ("description", s(self.description.clone())),
            ("arguments", Value::List(self.arguments.clone())),
        ]);
        if let Some(t) = self.title.as_ref().filter(|t| !t.is_empty()) {
            d.insert("title".into(), s(t.clone()));
        }
        Value::Map(d)
    }
}

/// The built-in registry and constants (`registry.json`).
#[derive(Debug, Clone)]
pub struct Builtin {
    /// `LATEST_PROTOCOL_VERSION`.
    pub protocol_version: String,
    /// `SUPPORTED_PROTOCOL_VERSIONS`.
    pub supported_protocol_versions: Vec<String>,
    /// `SERVER_NAME`.
    pub server_name: String,
    /// `MRSM_PRIMER`, the `initialize` instructions.
    pub primer: String,
    /// `MAX_CURSOR_OFFSET`.
    pub max_cursor_offset: i64,
    /// `DEFAULT_ROW_LIMIT`.
    pub default_row_limit: i64,
    /// `MAX_ROW_LIMIT`.
    pub max_row_limit: i64,
    /// `DEFAULT_PAGE_SIZE`.
    pub default_page_size: usize,
    /// `PROTECTED_TABLES`.
    pub protected_tables: Vec<String>,
    /// `_READ_ONLY_STARTS`.
    pub read_only_starts: Vec<String>,
    /// `_MUTATING_KEYWORDS`.
    pub mutating_keywords: Vec<String>,
    /// `_MUTATING_FUNCTIONS`.
    pub mutating_functions: Vec<String>,
    /// `_MUTATING_WORD_PREFIXES`.
    pub mutating_word_prefixes: Vec<String>,
    /// The tools, in registration order.
    pub tools: Vec<Tool>,
    /// The resources and templates, in registration order.
    pub resources: Vec<Resource>,
    /// The prompts, in registration order.
    pub prompts: Vec<Prompt>,
}

const REGISTRY_JSON: &str = include_str!("registry.json");

/// The built-in registry, parsed once.
pub fn builtin() -> &'static Builtin {
    static CELL: OnceLock<Builtin> = OnceLock::new();
    CELL.get_or_init(|| {
        let v = Value::from_json_str(REGISTRY_JSON).expect("registry.json is valid JSON");
        let list = |k: &str| {
            v.get(k)
                .and_then(Value::as_list)
                .cloned()
                .unwrap_or_default()
        };
        let int = |k: &str| v.get(k).and_then(Value::as_i64).unwrap_or_default();
        Builtin {
            protocol_version: opt_str(v.get("protocol_version")).unwrap_or_default(),
            supported_protocol_versions: strings(v.get("supported_protocol_versions")),
            server_name: opt_str(v.get("server_name")).unwrap_or_default(),
            primer: opt_str(v.get("primer")).unwrap_or_default(),
            max_cursor_offset: int("max_cursor_offset"),
            default_row_limit: int("default_row_limit"),
            max_row_limit: int("max_row_limit"),
            default_page_size: usize::try_from(int("default_page_size")).unwrap_or(100),
            protected_tables: strings(v.get("protected_tables")),
            read_only_starts: strings(v.get("read_only_starts")),
            mutating_keywords: strings(v.get("mutating_keywords")),
            mutating_functions: strings(v.get("mutating_functions")),
            mutating_word_prefixes: strings(v.get("mutating_word_prefixes")),
            tools: list("tools")
                .iter()
                .filter_map(|t| Tool::from_value(t, Source::Builtin))
                .collect(),
            resources: list("resources")
                .iter()
                .filter_map(|r| Resource::from_value(r, Source::Builtin))
                .collect(),
            prompts: list("prompts")
                .iter()
                .filter_map(|p| Prompt::from_value(p, Source::Builtin))
                .collect(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_registry_parses() {
        let b = builtin();
        assert_eq!(b.protocol_version, "2025-06-18");
        assert_eq!(b.tools.len(), 15);
        assert_eq!(b.resources.len(), 5);
        assert_eq!(b.prompts.len(), 3);
        assert!(b.primer.starts_with("Meerschaum is an ETL framework"));
    }

    #[test]
    fn template_matching() {
        let r = builtin()
            .resources
            .iter()
            .find(|r| r.is_template())
            .unwrap();
        let vars = r.matches("mrsm://pipes/sql:main/weather/None").unwrap();
        assert_eq!(vars.get("connector_keys"), Some(&s("sql:main")));
        assert_eq!(vars.get("location_key"), Some(&s("None")));
        assert!(r.matches("mrsm://pipes/a/b").is_none());
        assert!(r.matches("mrsm://pipes/a/b/c/d").is_none());
        assert!(r.matches("mrsm://pipes//b/c").is_none());
        let docs = builtin()
            .resources
            .iter()
            .find(|r| r.uri.as_deref() == Some("mrsm://docs"))
            .unwrap();
        assert_eq!(docs.matches("mrsm://docs"), Some(Map::new()));
        assert!(docs.matches("mrsm://docsx").is_none());
    }
}
