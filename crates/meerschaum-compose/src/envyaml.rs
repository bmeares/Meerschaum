//! A port of `envyaml.EnvYAML` (v1.10.211231), which Python compose uses to read the compose
//! file with `${VAR}` / `$VAR` substitution:
//!
//! ```python
//! EnvYAML(yaml_file=path, env_file=env_file, include_environment=True, flatten=False, strict=True)
//! ```
//!
//! The configuration is the process environment, updated with the variables parsed from the env
//! file (envyaml's own parser, not python-dotenv), updated with the top-level keys of the YAML
//! document. The YAML text has column-zero comment lines removed, then every `$VAR`, `${VAR}`,
//! `$VAR|default`, `${VAR|default}` is replaced textually before parsing, and `$$` becomes `$`.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use meerschaum_core::value::{Map, Value};
use regex::Regex;

use crate::env::ProcessEnv;

/// The env var that disables strict mode (`ENVYAML_STRICT_DISABLE`).
pub const STRICT_DISABLE: &str = "ENVYAML_STRICT_DISABLE";

/// Comment lines starting at column zero (`RE_COMMENTS`).
static RE_COMMENTS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)(^#.*\n)").expect("valid regex"));

/// `RE_DOT_ENV` without the `(?!\d+)` look-ahead, which [`read_env_text`] applies by hand.
static RE_DOT_ENV: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?m)^(?P<name>[\w\-\.]+)=["']?(?P<value>(.*?))["']?$"#).expect("valid regex")
});

/// `RE_PATTERN` (written without `re.VERBOSE` whitespace).
static RE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r#"(?m)(?P<pref>["'])?"#,
        r"(\$(?:(?P<escaped>(\$|\d+))|",
        r"\{(?P<braced>(.*?))(\|(?P<braced_default>.*?))?\}|",
        r"(?P<named>[\w\-\.]+)(\|(?P<named_default>.*))?))",
        r#"(?P<post>["'])?"#,
    ))
    .expect("valid regex")
});

/// `posixpath.expandvars`'s variable pattern (ASCII `\w`).
static RE_EXPANDVARS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$([A-Za-z0-9_]+|\{[^}]*\})").expect("valid regex"));

/// The errors `EnvYAML` raises.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EnvYamlError {
    /// Strict mode: variables referenced in the YAML file are not defined (`ValueError`).
    /// The names are in order of first appearance.
    #[error("Strict mode enabled, variables {} are not defined!", dollar_join(.0))]
    Undefined(Vec<String>),
    /// Strict mode: variables defined twice in the env file (`ValueError`).
    #[error("Strict mode enabled, variables {} defined several times!", dollar_join(.0))]
    DefinedSeveralTimes(Vec<String>),
    /// A file could not be read (`FileNotFoundError` and friends).
    #[error("{0}")]
    Io(String),
    /// The substituted text is not valid YAML.
    #[error("{0}")]
    Yaml(String),
}

fn dollar_join(names: &[String]) -> String {
    names
        .iter()
        .map(|n| format!("${n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `os.path.expandvars(value)` against an environment.
pub fn expandvars(path: &str, environ: &ProcessEnv) -> String {
    if !path.contains('$') {
        return path.to_string();
    }
    let mut path = path.to_string();
    let mut i = 0;
    while let Some(caps) = RE_EXPANDVARS.captures_at(&path, i) {
        let m = caps.get(0).expect("group 0");
        let (start, end) = (m.start(), m.end());
        let mut name = caps[1].to_string();
        if name.starts_with('{') && name.ends_with('}') {
            name = name[1..name.len() - 1].to_string();
        }
        match environ.get(&name) {
            None => i = end,
            Some(value) => {
                let tail = path[end..].to_string();
                path = format!("{}{}", &path[..start], value);
                i = path.len();
                path.push_str(&tail);
            }
        }
    }
    path
}

/// `EnvYAML.__read_env_file`: parse an env file with envyaml's own regex.
pub fn read_env_text(
    content: &str,
    strict: bool,
    environ: &ProcessEnv,
) -> Result<Vec<(String, String)>, EnvYamlError> {
    let mut config: Vec<(String, String)> = Vec::new();
    let mut defined: Vec<String> = Vec::new();
    for caps in RE_DOT_ENV.captures_iter(content) {
        let name = caps["name"].to_string();
        // `(?!\d+)`: a line starting with a digit is not a variable.
        if name.chars().next().is_some_and(char::is_numeric) {
            continue;
        }
        let value = caps["value"].to_string();
        let value = if value.contains('$') {
            expandvars(&value, environ)
        } else {
            value
        };
        if let Some(existing) = config.iter_mut().find(|(k, _)| *k == name) {
            if !defined.contains(&name) {
                defined.push(name.clone());
            }
            existing.1 = value;
        } else {
            config.push((name, value));
        }
    }
    if strict && !defined.is_empty() {
        return Err(EnvYamlError::DefinedSeveralTimes(defined));
    }
    Ok(config)
}

/// `EnvYAML.__read_yaml_file`'s text processing: strip comment lines and substitute variables.
/// `cfg` holds the variables (the environment and the env file).
pub fn substitute(
    content: &str,
    cfg: &dyn Fn(&str) -> Option<String>,
    strict: bool,
) -> Result<String, EnvYamlError> {
    let original = RE_COMMENTS.replace_all(content, "").into_owned();
    let mut content = original.clone();
    let mut not_found: Vec<String> = Vec::new();
    let mut replaces: Vec<(String, String)> = Vec::new();
    let mut shifting: isize = 0;

    for caps in RE_PATTERN.captures_iter(&original) {
        let named = caps.name("named").map(|m| m.as_str());
        let braced = caps.name("braced").map(|m| m.as_str());
        let escaped = caps.name("escaped").map(|m| m.as_str());
        let mut variable: Option<&str> = None;
        let mut default: Option<&str> = None;
        let mut is_braced = false;

        if let Some(n) = named.filter(|n| !n.is_empty()) {
            variable = Some(n);
            default = caps.name("named_default").map(|m| m.as_str());
        } else if let Some(b) = braced.filter(|b| !b.is_empty()) {
            variable = Some(b);
            default = caps.name("braced_default").map(|m| m.as_str());
            is_braced = true;
        } else if let Some(e) = escaped.filter(|e| e.contains('$')) {
            // PARITY: `$$` is unescaped in place, tracking the shift between the original
            // match offsets and the edited text (envyaml.py `shifting`).
            let m = caps.get(0).expect("group 0");
            let start = (m.start() as isize + shifting) as usize;
            let end = (m.end() as isize + shifting) as usize;
            content = format!("{}{}{}", &content[..start], e, &content[end..]);
            shifting += e.len() as isize - (m.end() - m.start()) as isize;
        }

        if let Some(var) = variable {
            let replace = match cfg(var) {
                Some(v) => Some(v),
                None => match default {
                    Some(d) => Some(d.to_string()),
                    None => {
                        if !not_found.iter().any(|n| n == var) {
                            not_found.push(var.to_string());
                        }
                        None
                    }
                },
            };
            if let Some(replace) = replace {
                let mut search = if is_braced { "${" } else { "$" }.to_string();
                search.push_str(var);
                if let Some(d) = default {
                    search.push('|');
                    search.push_str(d);
                }
                if is_braced {
                    search.push('}');
                }
                if let Some(existing) = replaces.iter_mut().find(|(k, _)| *k == search) {
                    existing.1 = replace;
                } else {
                    replaces.push((search, replace));
                }
            }
        }
    }

    if strict && !not_found.is_empty() {
        return Err(EnvYamlError::Undefined(not_found));
    }

    // PARITY: replacements are applied in reverse lexicographic order of the search strings
    // (`sorted(replaces, reverse=True)`), each with `str.replace` over the whole text.
    replaces.sort_by(|a, b| b.0.cmp(&a.0));
    for (search, replace) in &replaces {
        content = content.replace(search.as_str(), replace);
    }
    Ok(content)
}

/// Inputs for [`EnvYaml::load`].
#[derive(Debug, Clone)]
pub struct EnvYamlOptions<'a> {
    /// The compose file.
    pub yaml_file: &'a Path,
    /// `env_file=`: an explicit env file (already joined to the compose directory if relative).
    pub env_file: Option<PathBuf>,
    /// The working directory (envyaml's default `.env` and `ENV_FILE` are relative to it).
    pub cwd: &'a Path,
    /// `os.environ` at construction.
    pub environ: &'a ProcessEnv,
    /// `strict=` (overridden by `ENVYAML_STRICT_DISABLE`).
    pub strict: bool,
}

/// The loaded configuration (`EnvYAML` with `flatten=False`).
#[derive(Debug, Clone, PartialEq)]
pub struct EnvYaml {
    /// The configuration: environment variables, then env-file variables, then the YAML keys.
    pub cfg: Map,
    /// The parsed YAML document alone (after substitution).
    pub document: Value,
}

impl EnvYaml {
    /// `EnvYAML(yaml_file, env_file, include_environment=True, strict, flatten=False)`.
    pub fn load(opts: &EnvYamlOptions) -> Result<EnvYaml, EnvYamlError> {
        let strict = if opts.environ.contains_key(STRICT_DISABLE) {
            false
        } else {
            opts.strict
        };
        let mut cfg: Map = opts
            .environ
            .iter()
            .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
            .collect();

        // `__get_file_path(env_file, "ENV_FILE", ".env")`.
        let env_path: Option<PathBuf> = if let Some(p) = &opts.env_file {
            Some(p.clone())
        } else if let Some(p) = opts.environ.get("ENV_FILE").filter(|p| !p.is_empty()) {
            Some(PathBuf::from(p))
        } else if opts.cwd.join(".env").exists() {
            Some(PathBuf::from(".env"))
        } else {
            None
        };
        if let Some(path) = env_path {
            let path = if path.is_absolute() {
                path
            } else {
                opts.cwd.join(path)
            };
            let content = std::fs::read_to_string(&path).map_err(|e| {
                EnvYamlError::Io(format!(
                    "[Errno 2] {}: '{}'",
                    io_message(&e),
                    path.display()
                ))
            })?;
            for (k, v) in read_env_text(&content, strict, opts.environ)? {
                cfg.insert(k, Value::Str(v));
            }
        }

        let content = std::fs::read_to_string(opts.yaml_file).map_err(|e| {
            EnvYamlError::Io(format!(
                "[Errno 2] {}: '{}'",
                io_message(&e),
                opts.yaml_file.display()
            ))
        })?;
        let lookup = |name: &str| -> Option<String> {
            cfg.get(name).map(|v| match v {
                Value::Str(s) => s.clone(),
                other => meerschaum_core::pyfmt::py_str(other),
            })
        };
        let substituted = substitute(&content, &lookup, strict)?;
        let parsed = meerschaum_core::yaml::load(&substituted)
            .map_err(|e| EnvYamlError::Yaml(e.to_string()))?;
        let document = parsed.clone();
        match parsed {
            Value::Map(m) if !m.is_empty() => {
                for (k, v) in m {
                    cfg.insert(k, v);
                }
            }
            Value::List(items) if !items.is_empty() => {
                // PARITY: a top-level list is merged with integer keys, which can never match a
                // compose key; keep them as strings for completeness.
                for (i, v) in items.into_iter().enumerate() {
                    cfg.insert(i.to_string(), v);
                }
            }
            _ => {}
        }
        Ok(EnvYaml { cfg, document })
    }
}

fn io_message(e: &std::io::Error) -> &'static str {
    match e.kind() {
        std::io::ErrorKind::NotFound => "No such file or directory",
        std::io::ErrorKind::PermissionDenied => "Permission denied",
        _ => "I/O error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> ProcessEnv {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn substitutes_like_envyaml() {
        let e = env(&[("A", "1"), ("B", ""), ("AB", "x")]);
        let lookup = |k: &str| e.get(k).cloned();
        let text = "# $NOPE\na: $A\nb: ${B}\nc: ${C|dflt}\nd: $$A\ne: $AB\nf: $C|rest of line\n";
        let out = substitute(text, &lookup, true).unwrap();
        assert_eq!(out, "a: 1\nb: \nc: dflt\nd: 1\ne: x\nf: rest of line\n");
        let err = substitute("x: $MISSING $ALSO", &lookup, true).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Strict mode enabled, variables $MISSING, $ALSO are not defined!"
        );
    }

    #[test]
    fn reads_env_files_like_envyaml() {
        let e = env(&[("HOME", "/home/x")]);
        let parsed = read_env_text("A=1\nB='q'\n1C=no\nexport D=x\nE=$HOME/y\n", true, &e).unwrap();
        assert_eq!(
            parsed,
            vec![
                ("A".into(), "1".into()),
                ("B".into(), "q".into()),
                ("E".into(), "/home/x/y".into())
            ]
        );
        assert!(matches!(
            read_env_text("A=1\nA=2\n", true, &e),
            Err(EnvYamlError::DefinedSeveralTimes(_))
        ));
        assert_eq!(expandvars("$X ${HOME} $HOME", &e), "$X /home/x /home/x");
    }
}
