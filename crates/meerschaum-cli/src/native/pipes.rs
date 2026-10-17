//! `get_pipes` (`meerschaum/utils/_get_pipes.py`) and the pipe helpers the actions share.

use meerschaum_connector::{PipeId, PipesKeysQuery};
use meerschaum_core::Pipe;
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::value::{Map, Value};

use crate::builtin::ActionError;
use crate::dispatch::Runtime;
use crate::format::PipeKeys;
use crate::format::progress::Progress;
use crate::value::{ArgsMap, PyValue};

use super::{Instance, params, session, string, words};

/// A pipe and its ID on the instance (when the instance returned one).
#[derive(Debug, Clone)]
pub struct SelectedPipe {
    /// The pipe (parameters from the instance when it returned them).
    pub pipe: Pipe,
    /// `pipe.id`.
    pub id: Option<PipeId>,
    /// Whether `parameters` came from the instance (`attributes` is cached).
    pub hydrated: bool,
}

/// The `-c`, `-m`, `-l`, `--tags` and `--params` filters.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// `connector_keys`.
    pub connector_keys: Vec<String>,
    /// `metric_keys`.
    pub metric_keys: Vec<String>,
    /// `location_keys` (`None` for null locations).
    pub location_keys: Vec<Option<String>>,
    /// `tags`.
    pub tags: Vec<String>,
    /// `params`.
    pub params: Option<Map>,
}

impl Selection {
    /// The filters from the parsed arguments.
    pub fn from_args(args: &ArgsMap) -> Selection {
        let location_keys = args
            .get("location_keys")
            .and_then(PyValue::as_list)
            .map(|l| {
                l.iter()
                    .map(|v| match v {
                        PyValue::None => None,
                        other => Some(other.py_str()),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Selection {
            connector_keys: words(args, "connector_keys"),
            metric_keys: words(args, "metric_keys"),
            location_keys,
            tags: words(args, "tags"),
            params: params(args),
        }
    }

    /// The query for `fetch_pipes_keys`.
    pub fn query(&self) -> PipesKeysQuery {
        PipesKeysQuery {
            connector_keys: self.connector_keys.clone(),
            metric_keys: self.metric_keys.clone(),
            location_keys: self.location_keys.clone(),
            tags: self.tags.clone(),
            params: self.params.clone(),
        }
    }
}

/// The pipe defaults from the config registry.
pub fn defaults(rt: &mut Runtime<'_>) -> PipeDefaults {
    PipeDefaults::from_config(rt.config())
}

/// `Pipe(ck, mk, lk, instance=instance_keys, parameters=...)`.
pub fn build_pipe(
    ck: &str,
    mk: &str,
    lk: Option<&str>,
    instance_keys: &str,
    parameters: Option<Map>,
    defaults: &PipeDefaults,
) -> Result<Pipe, ActionError> {
    let mut b = Pipe::builder(ck, mk)
        .location(lk)
        .instance(instance_keys)
        .defaults(defaults.clone());
    if let Some(p) = parameters {
        b = b.parameters(p);
    }
    b.build()
        .map_err(|e| ActionError::new("ValueError", e.to_string()))
}

/// `get_pipes(as_list=True, method='registered', **kw)`: the registered pipes matching the
/// filters, grouped by connector then metric (`flatten_pipes_dict` order).
pub fn get_pipes(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    sel: &Selection,
) -> Result<Vec<SelectedPipe>, ActionError> {
    let session = session(rt);
    let defaults = defaults(rt);
    let conn = inst.conn();
    let keys = inst.keys();
    let result = session
        .block_on(conn.fetch_pipes_keys(&sel.query()))
        .map_err(|e| ActionError::new("Exception", e.to_string()))?;
    let items: Vec<(Option<PipeId>, meerschaum_connector::PipeKeysEntry)> = match result {
        meerschaum_connector::PipesKeys::List(l) => l.into_iter().map(|e| (None, e)).collect(),
        meerschaum_connector::PipesKeys::Dict(d) => {
            d.into_iter().map(|(id, e)| (Some(id), e)).collect()
        }
    };
    let mut pipes = Vec::new();
    for (id, entry) in items {
        let lk = entry
            .location_key
            .as_deref()
            .filter(|l| *l != "None")
            .map(str::to_string);
        let parameters = match &entry.extra {
            Some(Value::Map(m)) => Some(m.clone()),
            Some(Value::Str(s)) => Value::from_json_str(s).ok().map(|v| v.to_map()),
            _ => None,
        };
        let tags = match &entry.extra {
            Some(Value::List(l)) => Some(Value::List(l.clone())),
            _ => None,
        };
        let hydrated = parameters.is_some();
        let mut params = parameters.unwrap_or_default();
        if let Some(t) = tags
            && !params.contains_key("tags")
        {
            params.insert("tags".into(), t);
        }
        let pipe = build_pipe(
            &entry.connector_keys,
            &entry.metric_key,
            lk.as_deref(),
            &keys,
            Some(params),
            &defaults,
        )?;
        pipes.push(SelectedPipe { pipe, id, hydrated });
    }
    Ok(group_pipes(pipes))
}

/// `flatten_pipes_dict(pipes_dict)` order: by first-seen connector, then metric; a repeated
/// location replaces the earlier pipe in place.
pub fn group_pipes(pipes: Vec<SelectedPipe>) -> Vec<SelectedPipe> {
    type Locs = Vec<SelectedPipe>;
    let mut grouped: Vec<(String, Vec<(String, Locs)>)> = Vec::new();
    for p in pipes {
        let ck = p.pipe.connector_keys().to_string();
        let mk = p.pipe.metric_key().to_string();
        let ci = match grouped.iter().position(|(c, _)| *c == ck) {
            Some(i) => i,
            None => {
                grouped.push((ck, Vec::new()));
                grouped.len() - 1
            }
        };
        let metrics = &mut grouped[ci].1;
        let mi = match metrics.iter().position(|(m, _)| *m == mk) {
            Some(i) => i,
            None => {
                metrics.push((mk, Vec::new()));
                metrics.len() - 1
            }
        };
        let locs = &mut metrics[mi].1;
        match locs
            .iter()
            .position(|l| l.pipe.location_key() == p.pipe.location_key())
        {
            Some(i) => locs[i] = p,
            None => locs.push(p),
        }
    }
    grouped
        .into_iter()
        .flat_map(|(_, ms)| ms.into_iter().flat_map(|(_, ls)| ls))
        .collect()
}

/// `get_pipes(method='explicit')`: every combination of the given keys (`[None]` locations when
/// none are given).
pub fn explicit_pipes(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    sel: &Selection,
) -> Result<Vec<SelectedPipe>, ActionError> {
    let defaults = defaults(rt);
    let keys = inst.keys();
    let locations: Vec<Option<String>> = if sel.location_keys.is_empty() {
        vec![None]
    } else {
        sel.location_keys.clone()
    };
    let mut pipes = Vec::new();
    for ck in &sel.connector_keys {
        for mk in &sel.metric_keys {
            for lk in &locations {
                let pipe = build_pipe(ck, mk, lk.as_deref(), &keys, None, &defaults)?;
                pipes.push(SelectedPipe {
                    pipe,
                    id: None,
                    hydrated: false,
                });
            }
        }
    }
    Ok(group_pipes(pipes))
}

/// `get_pipes(as_list=True, **kw)` for an action: the instance from `-i` and the filters.
pub fn selected_pipes(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
) -> Result<(Instance, Vec<SelectedPipe>), ActionError> {
    let _progress = Progress::start(rt, args, "Looking up pipes");
    let inst = super::get_instance(args, rt)?;
    let sel = Selection::from_args(args);
    let pipes = get_pipes(rt, &inst, &sel)?;
    Ok((inst, pipes))
}

/// Load the pipe's registered parameters (`pipe.attributes`) when they were not returned with
/// the keys.
pub fn hydrate(rt: &mut Runtime<'_>, inst: &Instance, sp: &mut SelectedPipe) {
    if sp.hydrated {
        return;
    }
    let session = session(rt);
    if let Ok(attrs) = session.block_on(inst.conn().get_pipe_attributes(&sp.pipe)) {
        if let Some(p) = attrs.get("parameters").and_then(Value::as_map) {
            let mut params = p.clone();
            for (k, v) in sp.pipe.raw_parameters() {
                params.insert(k.clone(), v.clone());
            }
            sp.pipe.set_parameters(params);
        }
        if let Some(id) = attrs.get("pipe_id") {
            sp.id = match id {
                Value::Int(i) => Some(PipeId::Int(*i)),
                Value::Str(s) => Some(PipeId::parse(s)),
                _ => sp.id.clone(),
            };
        }
    }
    sp.hydrated = true;
}

/// The keys `pipe_repr` reads.
pub fn pipe_keys(pipe: &Pipe) -> PipeKeys {
    PipeKeys {
        connector: pipe.connector_keys().to_string(),
        metric: pipe.metric_key().to_string(),
        location: pipe.location_key().map(str::to_string),
        instance: Some(pipe.instance_keys().to_string()),
    }
}

/// `pipe.__getstate__()` as JSON: `{"connector_keys", "metric_key", "location_key",
/// "parameters", "instance_keys"}`.
pub fn pipe_state(pipe: &Pipe) -> Map {
    let mut m = Map::new();
    m.insert(
        "connector_keys".into(),
        Value::Str(pipe.connector_keys().into()),
    );
    m.insert("metric_key".into(), Value::Str(pipe.metric_key().into()));
    m.insert(
        "location_key".into(),
        pipe.location_key()
            .map(|l| Value::Str(l.into()))
            .unwrap_or(Value::Null),
    );
    m.insert(
        "parameters".into(),
        Value::Map(pipe.raw_parameters().clone()),
    );
    m.insert(
        "instance_keys".into(),
        Value::Str(pipe.instance_keys().into()),
    );
    m
}

/// `pipe.meta`: the keys and the instance.
pub fn pipe_meta(pipe: &Pipe) -> Map {
    let mut m = Map::new();
    m.insert(
        "connector_keys".into(),
        Value::Str(pipe.connector_keys().into()),
    );
    m.insert("metric_key".into(), Value::Str(pipe.metric_key().into()));
    m.insert(
        "location_key".into(),
        pipe.location_key()
            .map(|l| Value::Str(l.into()))
            .unwrap_or(Value::Null),
    );
    m.insert(
        "instance_keys".into(),
        Value::Str(pipe.instance_keys().into()),
    );
    m
}

/// `str(pipe)`.
pub fn pipe_str(pipe: &Pipe) -> String {
    pipe.to_string()
}

/// Whether a connector's pipes are Python-defined (plugins and custom connectors), which the
/// worker path owns.
pub fn is_python_connector(connector_keys: &str) -> bool {
    let typ = connector_keys.split(':').next().unwrap_or_default();
    !matches!(typ, "sql" | "api" | "valkey") || connector_keys.starts_with("plugin:")
}

/// `string(args, "mrsm_instance")` for messages that print the raw argument.
pub fn raw_instance_arg(args: &ArgsMap) -> String {
    string(args, "mrsm_instance").unwrap_or_else(|| "None".into())
}
