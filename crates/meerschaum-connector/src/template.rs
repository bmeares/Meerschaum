//! Instance-backed `{{ Pipe() }}` resolution (§6.3).
//!
//! `meerschaum-core` resolves `{{ self.* }}` on its own but its [`TemplateResolver`] is
//! synchronous, while a referenced pipe's registered parameters live on an instance connector
//! behind async I/O. [`prepare_resolver`] therefore walks the (transitive) references of a pipe
//! first, fetches every referenced pipe's registered attributes from *its own* instance, and
//! freezes them into a [`SnapshotResolver`]; [`resolve_pipe`] then runs the synchronous core
//! resolution against that snapshot and caches the result on the pipe
//! (`Pipe::resolve_parameters_with`).
//!
//! Python resolves lazily, so every `pipe.parameters` access re-reads the referenced pipes'
//! attributes (through the per-pipe attributes cache). The snapshot is taken once per call of
//! [`resolve_pipe`], which is the per-sync / per-fetch granularity the engine needs.

use std::sync::Arc;

use meerschaum_core::config::Config;
use meerschaum_core::pipe::{Pipe, PipeDefaults};
use meerschaum_core::template::{TemplateResolver, referenced_pipes_in_parameters};
use meerschaum_core::value::apply_patch_maps;
use meerschaum_core::{Map, Value};

use crate::traits::InstanceConnector;

/// Maps instance keys (`'sql:other'`) to connectors, for references that name an instance other
/// than the one the referencing pipe lives on.
pub trait InstanceProvider: Send + Sync {
    /// The connector for `keys`, or `None` when it cannot be built.
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>>;
}

#[derive(Debug, Clone)]
struct Entry {
    pipe: Pipe,
    /// The registered parameters (`None`: unregistered or unreachable instance).
    registered: Option<Map>,
    /// `{{ Pipe(...) }}` without an attribute chain.
    table_reference: Option<String>,
}

/// A [`TemplateResolver`] over registered state fetched ahead of time (see the module docs).
#[derive(Debug, Clone, Default)]
pub struct SnapshotResolver {
    defaults: PipeDefaults,
    config: Option<Arc<Config>>,
    entries: Vec<Entry>,
    /// Problems met while fetching referenced pipes.
    pub warnings: Vec<String>,
}

impl SnapshotResolver {
    /// The number of referenced pipes in the snapshot.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no referenced pipe was fetched.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl TemplateResolver for SnapshotResolver {
    fn defaults(&self) -> PipeDefaults {
        self.defaults.clone()
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        if let Some(Entry {
            table_reference: Some(t),
            ..
        }) = self.entries.iter().find(|e| e.pipe == *pipe)
        {
            return Ok(t.clone());
        }
        pipe.target(None).map_err(|e| e.to_string())
    }

    fn load_pipe(&self, pipe: &Pipe) -> Pipe {
        match self.entries.iter().find(|e| e.pipe == *pipe) {
            Some(Entry {
                registered: Some(registered),
                ..
            }) => {
                let mut out = pipe.clone();
                out.set_parameters(apply_patch_maps(registered, pipe.raw_parameters()));
                out
            }
            _ => pipe.clone(),
        }
    }

    fn config(&self) -> Option<&Config> {
        self.config.as_deref()
    }
}

/// Whether resolving `pipe`'s parameters can need anything from an instance: a `{{ Pipe() }}`
/// template or a `reference(s)` key. (`{{ self }}` templates and `MRSM{}` symlinks resolve
/// inside core.)
pub fn needs_instance_resolution(pipe: &Pipe) -> bool {
    let raw = pipe.raw_parameters();
    raw.contains_key("reference")
        || raw.contains_key("references")
        || Value::Map(raw.clone()).to_json_string().contains("{{")
}

fn direct_dependencies(pipe: &Pipe, parameters: &Map, defaults: &PipeDefaults) -> Vec<Pipe> {
    let mut out = referenced_pipes_in_parameters(parameters, Some(pipe), defaults);
    let mut with_raw = pipe.clone();
    with_raw.set_parameters(parameters.clone());
    if let Ok(refs) = with_raw.references(defaults) {
        for r in refs.into_iter().flatten() {
            if !out.contains(&r) {
                out.push(r);
            }
        }
    }
    out
}

/// Fetch every pipe `root` references (transitively) from its instance. `home` is the instance
/// `root` lives on; other instances come from `provider`.
pub async fn prepare_resolver(
    root: &Pipe,
    home: &dyn InstanceConnector,
    provider: Option<&dyn InstanceProvider>,
    defaults: &PipeDefaults,
    config: Option<Config>,
) -> SnapshotResolver {
    let mut resolver = SnapshotResolver {
        defaults: defaults.clone(),
        config: config.map(Arc::new),
        ..Default::default()
    };
    let mut queue: Vec<Pipe> = direct_dependencies(root, root.raw_parameters(), defaults);
    let mut seen: Vec<Pipe> = Vec::new();
    while let Some(pipe) = queue.pop() {
        if seen.contains(&pipe) {
            continue;
        }
        seen.push(pipe.clone());
        let foreign: Option<Arc<dyn InstanceConnector>>;
        let instance: Option<&dyn InstanceConnector> =
            if pipe.instance_keys() == home.connector_keys() {
                Some(home)
            } else {
                foreign = provider.and_then(|p| p.instance(pipe.instance_keys()));
                foreign.as_deref()
            };
        let mut entry = Entry {
            pipe: pipe.clone(),
            registered: None,
            table_reference: None,
        };
        match instance {
            Some(inst) => {
                match inst.get_pipe_attributes(&pipe).await {
                    Ok(attrs) => {
                        entry.registered = attrs.get("parameters").map(Value::to_map);
                    }
                    Err(e) => resolver.warnings.push(format!(
                        "Failed to fetch the registered parameters of {pipe}: {e}"
                    )),
                }
                entry.table_reference = inst.pipe_table_reference(&pipe);
            }
            None => resolver.warnings.push(format!(
                "Cannot reach the instance '{}' of the referenced pipe {pipe}.",
                pipe.instance_keys()
            )),
        }
        let params = match &entry.registered {
            Some(r) => apply_patch_maps(r, pipe.raw_parameters()),
            None => pipe.raw_parameters().clone(),
        };
        queue.extend(direct_dependencies(&pipe, &params, defaults));
        resolver.entries.push(entry);
    }
    resolver
}

/// A copy of `pipe` whose parameters are resolved against the registered state of everything it
/// references. Returns the pipe unchanged (and no warnings) when it needs no instance.
/// `Pipe::raw_parameters` of the result are untouched, so persistence keeps the templates.
pub async fn resolve_pipe(
    pipe: &Pipe,
    home: &dyn InstanceConnector,
    provider: Option<&dyn InstanceProvider>,
    defaults: &PipeDefaults,
) -> (Pipe, Vec<String>) {
    if !needs_instance_resolution(pipe) {
        return (pipe.clone(), Vec::new());
    }
    let wants_config = Value::Map(pipe.raw_parameters().clone())
        .to_json_string()
        .contains("MRSM{");
    let config = wants_config.then(|| Config::from_env().ok()).flatten();
    let resolver = prepare_resolver(pipe, home, provider, defaults, config).await;
    let mut out = pipe.clone();
    let mut warnings = resolver.warnings.clone();
    match out.resolve_parameters_with(&resolver, None) {
        Ok(w) => warnings.extend(w),
        Err(e) => warnings.push(format!("Failed to resolve the parameters of {pipe}: {e}")),
    }
    (out, warnings)
}
