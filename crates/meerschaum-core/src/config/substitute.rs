//! `MRSM{a:b:c}` symlinks: `search_and_substitute_config` and the secret references a symlink
//! may name.

use super::*;

impl ConfigState {
    /// Port of `search_and_substitute_config` for a JSON-like config.
    pub fn search_and_substitute(&mut self, config: &Map) -> Result<Map, String> {
        let substituted =
            self.search_and_substitute_lossless(&ConfigValue::map_from_value(config))?;
        Ok(ConfigValue::map_to_value(&substituted))
    }

    /// Port of `search_and_substitute_config` with the default arguments
    /// (`MRSM{` ... `}`, `:` delimiter, `!` literal prefix, `keep_symlinks=True`).
    pub fn search_and_substitute_lossless(
        &mut self,
        config: &ConfigMap,
    ) -> Result<ConfigMap, String> {
        self.search_and_substitute_with(config, &SubstituteOptions::default())
    }

    /// Port of `search_and_substitute_config`: replace every `MRSM{a:b:c}` in `config` with the
    /// value at those keys of the current config.
    ///
    /// Substitution rewrites a `json.dumps` text, as v4 does; a date, `bytes` or `set` that the
    /// text cannot hold rides through as a placeholder and comes back as itself (v4 raises a
    /// `TypeError` there, PARITY.md G7). Tuples become lists and non-string keys strings, as in
    /// v4, wherever a substitution happened.
    pub fn search_and_substitute_with(
        &mut self,
        config: &ConfigMap,
        options: &SubstituteOptions,
    ) -> Result<ConfigMap, String> {
        let needle = options.needle();
        let mut links: Vec<(Vec<String>, String)> = Vec::new();
        fn find_symlinks(
            d: &ConfigMap,
            prefix: &[String],
            needle: &str,
            out: &mut Vec<(Vec<String>, String)>,
        ) {
            for (k, v) in d {
                let mut path = prefix.to_vec();
                path.push(k.clone());
                match v {
                    ConfigValue::Map(m) => find_symlinks(m, &path, needle, out),
                    ConfigValue::Str(s) if s.contains(needle) => out.push((path, s.clone())),
                    _ => {}
                }
            }
        }
        if options.keep_symlinks {
            find_symlinks(config, &[], &needle, &mut links);
        }

        let mut parsed_config = if !map_contains_text(config, &needle) {
            config.clone()
        } else {
            let mut table: Vec<ConfigValue> = Vec::new();
            let mut haystack =
                json_dumps_map_lossless(config, JsonStyle::Compact, true, &mut table)?;
            let pattern_re = Regex::new(&format!(
                "{}(?P<pattern_keys>.*?){}",
                regex::escape(&needle),
                regex::escape(&options.end_key)
            ))
            .map_err(|e| e.to_string())?;
            let mut order: Vec<String> = Vec::new();
            let mut patterns: std::collections::HashMap<String, (ConfigValue, bool, bool)> =
                std::collections::HashMap::new();
            let mut memo: std::collections::HashMap<Vec<String>, (bool, ConfigValue)> =
                std::collections::HashMap::new();
            // 5.1: `MRSM{file:...}` / `MRSM{env:...}` (the resolved secret of each pattern).
            let mut secret_values: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
            let matches: Vec<(String, String, usize, usize)> = pattern_re
                .captures_iter(&haystack)
                .filter_map(|caps| {
                    let m = caps.get(0)?;
                    Some((
                        m.as_str().to_string(),
                        caps["pattern_keys"].to_string(),
                        m.start(),
                        m.end(),
                    ))
                })
                .collect();
            for (pattern, raw_keys, start, end) in matches {
                let prior = haystack[..start].chars().next_back();
                let after = haystack[end..].chars().next();
                let mut keys: Vec<String> = raw_keys
                    .split(options.delimiter.as_str())
                    .map(String::from)
                    .collect();
                if let Some(reference) = secret_reference(&raw_keys, options) {
                    match secrets::resolve(&reference, &self.paths.root_dir) {
                        Ok(value) => {
                            if !patterns.contains_key(&pattern)
                                && !secret_values.contains_key(&pattern)
                            {
                                order.push(pattern.clone());
                            }
                            secret_values.insert(pattern, value);
                        }
                        // The reference stays as its text: only what needs the value fails.
                        Err(error) => self.debug(error.to_string()),
                    }
                    continue;
                }
                let mut force_literal = false;
                if let Some(first) = keys.first_mut()
                    && let Some(stripped) = first.strip_prefix(options.literal_key.as_str())
                {
                    *first = stripped.to_string();
                    force_literal = true;
                }
                if keys.len() == 1 && keys[0].is_empty() {
                    keys.clear();
                }
                let (valid, value) = match memo.get(&keys) {
                    Some(v) => v.clone(),
                    None => {
                        let opts = GetOptions {
                            patch: true,
                            substitute: false,
                            write_missing: false,
                            warn: true,
                        };
                        let result = self.get_config_lossless(&keys, opts);
                        memo.insert(keys.clone(), result.clone());
                        result
                    }
                };
                if !valid {
                    continue;
                }
                if !patterns.contains_key(&pattern) {
                    order.push(pattern.clone());
                }
                // PARITY: a pattern seen both quoted and inside a longer string keeps the status
                // of its last occurrence (`_read_config.py:265`).
                patterns.insert(
                    pattern,
                    (
                        value,
                        prior == Some('"') && after == Some('"'),
                        force_literal,
                    ),
                );
            }
            for pattern in order {
                if let Some(secret) = secret_values.get(&pattern) {
                    // A string, alone or inside a longer one: the same JSON-escaped text either
                    // way (a quote or backslash in a secret never breaks the document).
                    let dumped = json_dumps_str(secret, true);
                    haystack = haystack.replace(&pattern, &dumped[1..dumped.len() - 1]);
                    continue;
                }
                let (value, isolated, literal) = &patterns[&pattern];
                if *isolated {
                    let dumped = json_dumps_lossless(value, JsonStyle::Default, true, &mut table)?;
                    haystack = haystack.replace(&json_dumps_str(&pattern, true), &dumped);
                } else if *literal {
                    // PARITY: escaping `'` as `\'` produces invalid JSON (`_read_config.py:283`).
                    let dumped = json_dumps(value, JsonStyle::Default, true)?
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\'', "\\'");
                    haystack = haystack.replace(&pattern, &dumped);
                } else {
                    haystack = haystack.replace(&pattern, &py_str(value));
                }
            }
            match restore_placeholders(json_loads(&haystack).map_err(|e| e.message)?, &table) {
                ConfigValue::Map(m) => m,
                v if !v.truthy() => ConfigMap::new(),
                _ => return Err("substituted config is not a dictionary".to_string()),
            }
        };

        if !links.is_empty() {
            let mut symlinks = ConfigMap::new();
            for (keys, pattern) in links {
                let mut s = &mut symlinks;
                for k in &keys[..keys.len() - 1] {
                    let entry = s.entry(k.clone()).or_insert_with(ConfigValue::new_map);
                    if !entry.is_dict() {
                        *entry = ConfigValue::new_map();
                    }
                    s = entry.as_map_mut().expect("map");
                }
                s.insert(keys[keys.len() - 1].clone(), ConfigValue::Str(pattern));
            }
            let symlinks_key = Self::symlinks_key();
            let merged = match parsed_config.get(symlinks_key) {
                None => ConfigValue::Map(symlinks),
                Some(existing) => apply_patch(existing, &ConfigValue::Map(symlinks)),
            };
            parsed_config.insert(symlinks_key.to_string(), merged);
        }
        Ok(parsed_config)
    }
}

/// The secret reference a symlink's keys name (5.1), read from the JSON text the substitution
/// edits (so `\\` in a Windows path is one backslash). `None` for an ordinary symlink, or with
/// a non-default syntax (another leading key or delimiter).
fn secret_reference(raw_keys: &str, options: &SubstituteOptions) -> Option<secrets::SecretRef> {
    let default = SubstituteOptions::default();
    if options.delimiter != default.delimiter || options.leading_key != default.leading_key {
        return None;
    }
    let first = raw_keys.split(':').next()?;
    let first = first
        .strip_prefix(options.literal_key.as_str())
        .unwrap_or(first);
    if !secrets::RESERVED_KEYS.contains(&first) {
        return None;
    }
    let text = match json_loads(&format!("\"{raw_keys}\"")) {
        Ok(ConfigValue::Str(text)) => text,
        _ => raw_keys.to_string(),
    };
    let mut keys: Vec<String> = text.split(':').map(String::from).collect();
    if let Some(first) = keys.first_mut()
        && let Some(stripped) = first.strip_prefix(options.literal_key.as_str())
    {
        *first = stripped.to_string();
    }
    secrets::SecretRef::from_keys(&keys)
}
