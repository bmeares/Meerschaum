//! The live-system operations compose needs: the instance connector (pipe registrations), the
//! job supervisor, and the interactive prompt. Neither the Rust instance connectors nor the job
//! supervisor are wired into compose yet, so subactions take them through these traits.
//!
//! The in-memory fakes reproduce the semantics the parity fixtures were recorded with (see
//! `tests/fixtures/gen_compose.py`): a registry of pipes with parameters, `edit`/`delete` events,
//! and a fixed set of jobs.
//
// TODO(core-merge): implement `InstanceOps` over the Rust instance connectors (`meerschaum-sql`,
// `meerschaum-api`, `meerschaum-valkey`) and `JobOps` over `meerschaum-jobs` once their public
// APIs land in `meerschaum-core`.

use indexmap::IndexMap;
use meerschaum_core::pipe::Pipe;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

/// Pipe registrations on instance connectors.
pub trait InstanceOps {
    /// `mrsm.get_connector(keys, **attributes)` for a custom connector defined by the project
    /// (`build_custom_connectors`). The default accepts every connector.
    fn build_connector(&mut self, _keys: &str, _attributes: &Map) -> Result<(), String> {
        Ok(())
    }

    /// `mrsm.get_pipes(tags=tags, instance=instance_keys)`: the registered pipes on an instance
    /// which carry any of the tags, with their registered parameters.
    fn get_pipes(&mut self, instance_keys: &str, tags: &[String]) -> Vec<Pipe>;

    /// The registered parameters of a pipe (`pipe.attributes['parameters']` from the instance),
    /// or `None` if the pipe is not registered.
    fn get_parameters(&mut self, pipe: &Pipe) -> Option<Map>;

    /// `pipe.get_id()`.
    fn get_id(&mut self, pipe: &Pipe) -> Option<i64>;

    /// `pipe.edit()`: persist the pipe's in-memory parameters.
    fn edit_pipe(&mut self, pipe: &Pipe) -> SuccessTuple;

    /// `pipe.delete(drop=True)`.
    fn delete_pipe(&mut self, pipe: &Pipe) -> SuccessTuple;

    /// The table reference `{{ Pipe(...) }}` resolves to (the quoted, schema-qualified target on
    /// SQL instances). The default has no live connector and fails, which leaves the template
    /// as written (what Python does when the instance cannot be reached).
    fn table_reference(&mut self, pipe: &Pipe) -> Result<String, String> {
        Err(format!(
            "No instance connector is available to resolve {pipe}."
        ))
    }

    /// `pipe.sync()` for a temporary pipe (compose syncs temporary pipes in-process).
    fn sync_pipe(&mut self, pipe: &Pipe) -> SuccessTuple {
        SuccessTuple::fail(format!("Cannot sync {pipe}: no sync engine is available."))
    }

    /// `pipe.instance_connector.get_pipe_attributes(pipe)`.
    fn get_pipe_attributes(&mut self, pipe: &Pipe) -> Result<Map, String> {
        let mut out = Map::new();
        if let Some(params) = self.get_parameters(pipe) {
            out.insert("parameters".into(), Value::Map(params));
        }
        Ok(out)
    }
}

/// One registered pipe in an [`InMemoryInstance`].
#[derive(Debug, Clone, PartialEq)]
pub struct RegisteredPipe {
    /// The pipe's keys (connector, metric, location, instance).
    pub connector_keys: String,
    /// The metric key.
    pub metric_key: String,
    /// The location key.
    pub location_key: Option<String>,
    /// The instance keys.
    pub instance_keys: String,
    /// The pipe's ID.
    pub id: i64,
    /// The registered parameters.
    pub parameters: Map,
}

impl RegisteredPipe {
    fn matches(&self, pipe: &Pipe) -> bool {
        self.connector_keys == pipe.connector_keys()
            && self.metric_key == pipe.metric_key()
            && self.location_key.as_deref() == pipe.location_key()
            && self.instance_keys == pipe.instance_keys()
    }
}

/// A change made through [`InstanceOps`].
#[derive(Debug, Clone, PartialEq)]
pub enum InstanceEvent {
    /// `pipe.edit()` with the pipe's `str()` and parameters.
    Edit(String, Map),
    /// `pipe.delete()` with the pipe's `str()`.
    Delete(String),
}

/// An in-memory instance registry.
#[derive(Debug, Clone, Default)]
pub struct InMemoryInstance {
    /// The registered pipes.
    pub pipes: Vec<RegisteredPipe>,
    /// The edits and deletes, in order.
    pub events: Vec<InstanceEvent>,
    /// The custom connectors built.
    pub connectors: Vec<String>,
    /// The installed plugins: syncing a temporary `plugin:<name>` pipe whose plugin is missing
    /// fails like Python (`None` treats every plugin as installed).
    pub installed_plugins: Option<Vec<String>>,
    /// The temporary pipes synced in-process.
    pub synced: Vec<String>,
}

impl InstanceOps for InMemoryInstance {
    fn build_connector(&mut self, keys: &str, _attributes: &Map) -> Result<(), String> {
        self.connectors.push(keys.to_string());
        Ok(())
    }

    fn get_pipes(&mut self, instance_keys: &str, tags: &[String]) -> Vec<Pipe> {
        self.pipes
            .iter()
            .filter(|r| r.instance_keys == instance_keys)
            .filter(|r| {
                let rec_tags: Vec<String> = match r.parameters.get("tags") {
                    Some(Value::List(l)) => l.iter().map(meerschaum_core::pyfmt::py_str).collect(),
                    _ => Vec::new(),
                };
                tags.is_empty() || tags.iter().any(|t| rec_tags.contains(t))
            })
            .filter_map(|r| {
                Pipe::builder(&r.connector_keys, &r.metric_key)
                    .location(r.location_key.as_deref())
                    .instance(&r.instance_keys)
                    .parameters(r.parameters.clone())
                    .build()
                    .ok()
            })
            .collect()
    }

    fn get_parameters(&mut self, pipe: &Pipe) -> Option<Map> {
        self.pipes
            .iter()
            .find(|r| r.matches(pipe))
            .map(|r| r.parameters.clone())
    }

    fn get_id(&mut self, pipe: &Pipe) -> Option<i64> {
        self.pipes.iter().find(|r| r.matches(pipe)).map(|r| r.id)
    }

    fn edit_pipe(&mut self, pipe: &Pipe) -> SuccessTuple {
        self.events.push(InstanceEvent::Edit(
            pipe.to_string(),
            pipe.parameters().clone(),
        ));
        if let Some(r) = self.pipes.iter_mut().find(|r| r.matches(pipe)) {
            r.parameters = pipe.parameters().clone();
        }
        SuccessTuple::success()
    }

    fn delete_pipe(&mut self, pipe: &Pipe) -> SuccessTuple {
        self.events.push(InstanceEvent::Delete(pipe.to_string()));
        self.pipes.retain(|r| !r.matches(pipe));
        SuccessTuple::success()
    }

    fn sync_pipe(&mut self, pipe: &Pipe) -> SuccessTuple {
        self.synced.push(pipe.to_string());
        let ck = pipe.connector_keys();
        if let (Some(installed), Some(name)) = (&self.installed_plugins, ck.strip_prefix("plugin:"))
        {
            if !installed.iter().any(|p| p == name) {
                return SuccessTuple::fail(format!(
                    "{pipe} does not have a valid connector.\n    Perhaps {ck} has a syntax error?"
                ));
            }
        }
        SuccessTuple::success()
    }
}

/// A background job as compose sees it (`meerschaum.jobs.Job`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct JobInfo {
    /// `job.sysargs`.
    pub sysargs: Vec<String>,
    /// `job.env`.
    pub env: IndexMap<String, String>,
    /// `job.status`: `running`, `paused` or `stopped`.
    pub status: String,
    /// `job.restart`.
    pub restart: bool,
}

/// The job supervisor.
pub trait JobOps {
    /// `meerschaum.jobs.get_jobs()`.
    fn get_jobs(&mut self) -> IndexMap<String, JobInfo>;
}

/// A fixed set of jobs.
#[derive(Debug, Clone, Default)]
pub struct InMemoryJobs {
    /// The jobs by name.
    pub jobs: IndexMap<String, JobInfo>,
}

impl JobOps for InMemoryJobs {
    fn get_jobs(&mut self) -> IndexMap<String, JobInfo> {
        self.jobs.clone()
    }
}

/// The interactive yes/no prompt (`meerschaum.utils.prompt.yes_no` without `yes`/`noask`).
pub trait Prompter {
    /// Ask a question; `None` when no answer can be read (end of input), which Python turns
    /// into an exception.
    fn ask(&mut self, question: &str, default_yes: bool) -> Option<bool>;
}

/// A prompter with no input (stdin closed): every question fails.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoInput;

impl Prompter for NoInput {
    fn ask(&mut self, _question: &str, _default_yes: bool) -> Option<bool> {
        None
    }
}

/// A prompter that records the questions and answers them from a list.
#[derive(Debug, Clone, Default)]
pub struct ScriptedPrompter {
    /// The answers, in order (`None` for end of input).
    pub answers: Vec<Option<bool>>,
    /// The questions asked.
    pub questions: Vec<String>,
}

impl Prompter for ScriptedPrompter {
    fn ask(&mut self, question: &str, _default_yes: bool) -> Option<bool> {
        self.questions.push(question.to_string());
        if self.answers.is_empty() {
            None
        } else {
            self.answers.remove(0)
        }
    }
}
