//! Where each config value came from (5.1), and the writer that persists only what may be.
//!
//! The registry records every layer it applies over the files ([`EnvLayer`]: the
//! `MRSM_<TYPE>_<LABEL>` connectors, `MRSM_CONFIG` / `MRSM_PATCH` or their `*_FILE` forms, a
//! `patch_config/` directory). [`ConfigState::origins`] replays them over a scratch registry to
//! tell, for each value of a top-level key, its [`Origin`]: a default, a config file, one of those
//! layers, or a secret reference (on top of one of the others).
//!
//! **The writer.** Every write into the config directory goes through
//! [`ConfigState::persistable`]: for each value the caller passes,
//!
//! - a value the caller changed (it differs from what the registry would hold if it loaded the key
//!   now) is written as the caller has it, except a resolved secret the file holds as a reference
//!   (a stale copy of a rotated secret), which keeps the reference;
//! - a value the caller did not change is written as the file has it (an `MRSM{...}` reference
//!   stays a reference), or, when the file does not have it, only if it is a default;
//! - a value that came only from the environment, a patch or a secret reference is never written;
//! - a value the caller dropped is dropped.
//!
//! So `register connectors`, `login`, `bootstrap connectors`, the console's writers and
//! `write_config(get_config(...))` from a plugin all write the file as written plus the caller's
//! change. Writes into another directory (a patch directory, the defaults copy) are dumps of what
//! the caller passed, as before.

use std::collections::HashMap;
use std::path::PathBuf;

use super::lossless::{ConfigMap, ConfigValue, apply_patch_maps};
use super::{ConfigState, GetOptions, revert_symlinks_lossless, secrets};

/// One layer applied over the config files, as the registry applied it.
#[derive(Debug, Clone, PartialEq)]
pub enum EnvLayer {
    /// `MRSM_<TYPE>_<LABEL>` connector variables (`(name, value)`).
    Uris(Vec<(String, String)>),
    /// `MRSM_CONFIG` / `MRSM_PATCH` texts, in order, with the variable each came from
    /// (`MRSM_CONFIG_FILE` when read from a file).
    Patches(Vec<(String, String)>),
    /// A `patch_config/` directory (applied in memory and deleted).
    PatchDir(ConfigMap),
}

/// Where a config value came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// The shipped defaults.
    Default,
    /// A config file (the path).
    File(PathBuf),
    /// An environment variable: an `MRSM_<TYPE>_<LABEL>` connector, `MRSM_CONFIG`,
    /// `MRSM_PATCH` or their `*_FILE` forms.
    Environment(String),
    /// A `patch_config/` directory.
    PatchDir,
    /// A secret reference (as written), inside a value of the origin `under`.
    Secret {
        /// The reference (`MRSM{file:/run/secrets/pg}`).
        reference: String,
        /// Where the text holding the reference came from.
        under: Box<Origin>,
    },
}

impl Origin {
    /// Whether a value of this origin may be persisted when the caller did not change it.
    fn persisted_unchanged(&self) -> bool {
        match self {
            Origin::Default | Origin::File(_) => true,
            Origin::Environment(_) | Origin::PatchDir => false,
            Origin::Secret { under, .. } => under.persisted_unchanged(),
        }
    }
}

/// The leaves of a value (`[path] -> value`); a map's entries are recursed into, anything else
/// (lists included) is one leaf, and so is an empty map.
fn leaves(value: &ConfigValue, path: &mut Vec<String>, out: &mut Vec<(Vec<String>, ConfigValue)>) {
    match value {
        ConfigValue::Map(m) if !m.is_empty() => {
            for (k, v) in m {
                path.push(k.clone());
                leaves(v, path, out);
                path.pop();
            }
        }
        other => out.push((path.clone(), other.clone())),
    }
}

/// Two values the same for the writer (also when one went through the JSON projection, a date
/// read as text by a caller that only has `Value`).
fn same(a: &ConfigValue, b: &ConfigValue) -> bool {
    a == b || a.to_value() == b.to_value()
}

/// Whether a value holds the text of a value a secret reference produced.
fn holds_known_secret(value: &ConfigValue) -> bool {
    match value {
        ConfigValue::Str(s) => secrets::mask_known(s) != *s,
        ConfigValue::List(items) | ConfigValue::Tuple(items) => {
            items.iter().any(holds_known_secret)
        }
        ConfigValue::Map(m) => m.values().any(holds_known_secret),
        _ => false,
    }
}

/// Whether a value holds a secret reference's text.
fn holds_reference(value: &ConfigValue) -> bool {
    match value {
        ConfigValue::Str(s) => secrets::contains_reference(s),
        ConfigValue::List(items) | ConfigValue::Tuple(items) => items.iter().any(holds_reference),
        ConfigValue::Map(m) => m.values().any(holds_reference),
        _ => false,
    }
}

/// What [`ConfigState::origins`] knows about one top-level key.
#[derive(Debug, Clone, Default)]
pub struct KeyOrigins {
    /// The key as its file holds it (no substitution), when it has a file.
    pub file: Option<ConfigValue>,
    /// The value the registry would hold if it loaded the key now (every layer, substituted).
    pub resolved: Option<ConfigValue>,
    /// The origin of each leaf of the key, by path below the key.
    pub leaves: HashMap<Vec<String>, Origin>,
}

impl KeyOrigins {
    /// The origin of the leaf at `path` (below the key), if known.
    pub fn origin(&self, path: &[String]) -> Option<&Origin> {
        self.leaves.get(path)
    }
}

/// The origin a replay stage gives a leaf it changed (by the leaf's path).
type OriginOf = Box<dyn Fn(&[String]) -> Origin>;

impl ConfigState {
    /// A registry with this one's paths, defaults and layers, and nothing loaded, that never
    /// writes.
    fn scratch(&self) -> ConfigState {
        ConfigState {
            paths: self.paths.clone(),
            defaults: self.defaults.clone(),
            config: ConfigMap::new(),
            allow_writes: false,
            notices: Vec::new(),
            write_error: None,
            layers: Vec::new(),
            withholds: self.withholds,
        }
    }

    /// The layers applied so far (see [`EnvLayer`]).
    pub fn layers(&self) -> &[EnvLayer] {
        &self.layers
    }

    pub(super) fn record_layer(&mut self, layer: EnvLayer) {
        if self.layers.last() != Some(&layer) {
            self.layers.push(layer);
        }
    }

    fn replay(&mut self, layer: &EnvLayer) {
        match layer {
            EnvLayer::Uris(env) => {
                self.apply_environment_uris(env, true);
            }
            EnvLayer::Patches(texts) => self.apply_patch_texts(texts),
            EnvLayer::PatchDir(patch) => self.config = apply_patch_maps(&self.config, patch),
        }
    }

    /// The key `key` of the registry's config with every `MRSM{...}` original restored.
    fn reverted_key(&self, key: &str) -> Option<ConfigValue> {
        revert_symlinks_lossless(&self.config).get(key).cloned()
    }

    /// Where each value of the top-level key `key` comes from (see the module docs): the
    /// defaults, its file, then each recorded layer in order, then the secret references.
    pub fn origins(&mut self, key: &str) -> KeyOrigins {
        let mut scratch = self.scratch();
        let file_path = scratch
            .get_keyfile_path(key, false, None)
            .filter(|p| p.exists());
        let keys = [key.to_string()];
        let raw = scratch
            .read_config_lossless(None, Some(&keys), false, false, false)
            .ok()
            .and_then(|(c, _)| c.get(key).cloned());
        let mut result = KeyOrigins {
            file: file_path.as_ref().and(raw.clone()),
            ..KeyOrigins::default()
        };
        let mut found = Vec::new();
        if let Some(defaults) = self.defaults.get(key) {
            leaves(defaults, &mut Vec::new(), &mut found);
            for (path, _) in found.drain(..) {
                result.leaves.insert(path, Origin::Default);
            }
        }
        if let (Some(path), Some(file)) = (&file_path, &result.file) {
            leaves(file, &mut Vec::new(), &mut found);
            for (leaf, _) in found.drain(..) {
                result.leaves.insert(leaf, Origin::File(path.clone()));
            }
        }
        if let Some(raw) = raw {
            scratch.config.insert(key.to_string(), raw);
        }

        // Each layer (each variable of a patch layer on its own): what it changed is its own.
        let mut stages: Vec<(EnvLayer, OriginOf)> = Vec::new();
        for layer in &self.layers {
            match layer {
                EnvLayer::Uris(env) => {
                    let env = env.clone();
                    stages.push((
                        layer.clone(),
                        Box::new(move |path: &[String]| {
                            let wanted = (path.get(1).cloned(), path.get(2).cloned());
                            let var = env
                                .iter()
                                .find(|(var, _)| {
                                    super::parse_connector_env_var(var)
                                        .is_some_and(|(t, l)| (Some(t), Some(l)) == wanted)
                                })
                                .map(|(var, _)| var.clone())
                                .unwrap_or_else(|| "MRSM_<TYPE>_<LABEL>".into());
                            Origin::Environment(var)
                        }),
                    ));
                }
                EnvLayer::Patches(texts) => {
                    for (var, text) in texts {
                        let var = var.clone();
                        stages.push((
                            EnvLayer::Patches(vec![(var.clone(), text.clone())]),
                            Box::new(move |_: &[String]| Origin::Environment(var.clone())),
                        ));
                    }
                }
                EnvLayer::PatchDir(_) => {
                    stages.push((layer.clone(), Box::new(|_: &[String]| Origin::PatchDir)));
                }
            }
        }
        let mut previous = scratch.reverted_key(key);
        for (layer, origin) in &stages {
            scratch.replay(layer);
            let current = scratch.reverted_key(key);
            let mut before = Vec::new();
            if let Some(v) = &previous {
                leaves(v, &mut Vec::new(), &mut before);
            }
            let before: HashMap<Vec<String>, ConfigValue> = before.into_iter().collect();
            if let Some(v) = &current {
                leaves(v, &mut Vec::new(), &mut found);
                for (path, value) in found.drain(..) {
                    if before.get(&path).is_none_or(|old| !same(old, &value)) {
                        let o = origin(&path);
                        result.leaves.insert(path, o);
                    }
                }
            }
            previous = current;
        }
        // Secret references, over whatever origin the text holding them has.
        if let Some(v) = &previous {
            leaves(v, &mut Vec::new(), &mut found);
            for (path, value) in found.drain(..) {
                let ConfigValue::Str(text) = &value else {
                    continue;
                };
                if let Some(reference) = secrets::references_in(text).into_iter().next() {
                    let under = result.leaves.get(&path).cloned().unwrap_or(Origin::Default);
                    result.leaves.insert(
                        path,
                        Origin::Secret {
                            reference,
                            under: Box::new(under),
                        },
                    );
                }
            }
        }
        let opts = GetOptions {
            patch: false,
            substitute: true,
            write_missing: false,
            warn: false,
        };
        let (valid, resolved) = scratch.get_config_lossless(&keys, opts);
        result.resolved = valid.then_some(resolved);
        result
    }

    /// What may be written for the top-level key `key` when a caller writes `caller` (see the
    /// module docs). `caller_links` is the caller's own `_symlinks` entry for the key (v4's
    /// `write_config` restores those originals). `None` when nothing of it may be written.
    pub fn persistable(
        &mut self,
        key: &str,
        caller: &ConfigValue,
        caller_links: Option<&ConfigValue>,
    ) -> Option<ConfigValue> {
        let origins = self.origins(key);
        let defaults = self.defaults.get(key).cloned();
        merge(
            caller,
            origins.resolved.as_ref(),
            origins.file.as_ref(),
            defaults.as_ref(),
            caller_links,
            &origins,
            &mut Vec::new(),
        )
    }

    /// [`persistable`](Self::persistable) for every top-level key of a config a caller writes
    /// (its `_symlinks` key is the caller's originals, not a key to write).
    pub fn persistable_config(&mut self, config: &ConfigMap) -> ConfigMap {
        let symlinks_key = Self::symlinks_key();
        let links = config.get(symlinks_key);
        let mut out = ConfigMap::new();
        for (k, v) in config {
            if k == symlinks_key {
                continue;
            }
            if let Some(value) = self.persistable(k, v, links.and_then(|l| l.get(k))) {
                out.insert(k.clone(), value);
            }
        }
        out
    }
}

fn merge(
    caller: &ConfigValue,
    base: Option<&ConfigValue>,
    file: Option<&ConfigValue>,
    defaults: Option<&ConfigValue>,
    links: Option<&ConfigValue>,
    origins: &KeyOrigins,
    path: &mut Vec<String>,
) -> Option<ConfigValue> {
    let is_map_or_none = |v: Option<&ConfigValue>| v.is_none_or(|v| v.as_map().is_some());
    if let ConfigValue::Map(c) = caller
        && is_map_or_none(base)
        && is_map_or_none(file)
        && !c.is_empty()
    {
        if base.is_none() && file.is_none() {
            // A new subtree: the caller's, with its own originals restored.
            return Some(restore_links(caller, links));
        }
        let mut out = ConfigMap::new();
        for (k, v) in c {
            path.push(k.clone());
            let merged = merge(
                v,
                base.and_then(|b| b.get(k)),
                file.and_then(|f| f.get(k)),
                defaults.and_then(|d| d.get(k)),
                links.and_then(|l| l.get(k)),
                origins,
                path,
            );
            path.pop();
            if let Some(merged) = merged {
                out.insert(k.clone(), merged);
            }
        }
        if out.is_empty() && file.is_none() {
            return None;
        }
        return Some(ConfigValue::Map(out));
    }
    match base {
        Some(b) if same(caller, b) => {
            // Unchanged: as the file has it, or a default; never what only a layer holds.
            if let Some(f) = file {
                return Some(f.clone());
            }
            // A default, with the caller's `MRSM{...}` originals restored (v4).
            let is_default = defaults.is_some_and(|d| same(d, caller));
            (is_default
                || origins
                    .origin(path)
                    .is_some_and(Origin::persisted_unchanged))
            .then(|| restore_links(caller, links))
        }
        _ => {
            // Changed by the caller.
            if let Some(ConfigValue::Str(original)) = links
                && original.contains("MRSM{")
            {
                return Some(ConfigValue::Str(original.clone()));
            }
            if let Some(f) = file
                && holds_reference(f)
                && holds_known_secret(caller)
            {
                // A resolved secret the file holds as a reference (a rotated secret's stale copy).
                return Some(f.clone());
            }
            if holds_known_secret(caller)
                && let Some(Origin::Secret { under, .. }) = origins.origin(path)
                && !under.persisted_unchanged()
            {
                // A secret an environment reference produced.
                return None;
            }
            Some(caller.clone())
        }
    }
}

/// `value` with the caller's `_symlinks` originals restored (v4's `revert_symlinks_config`).
fn restore_links(value: &ConfigValue, links: Option<&ConfigValue>) -> ConfigValue {
    match (value, links) {
        (_, Some(ConfigValue::Str(original))) => ConfigValue::Str(original.clone()),
        (ConfigValue::Map(m), Some(ConfigValue::Map(l))) => ConfigValue::Map(
            m.iter()
                .map(|(k, v)| (k.clone(), restore_links(v, l.get(k))))
                .collect(),
        ),
        (v, _) => v.clone(),
    }
}
