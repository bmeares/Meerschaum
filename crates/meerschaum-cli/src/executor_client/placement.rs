//! Resolve whole-operation placement from registration metadata before any plugin is loaded.

use super::*;
use crate::native::{Session, pipes::Selection as Filters};
use crate::value::ArgsMap;
use meerschaum_core::access::PipeAccess;
use meerschaum_engine::{Graph, GraphOptions, PipeRef, Pipeline, Selection, StaticRegistry};
use std::collections::BTreeSet;

/// An operation cannot cross execution boundaries. Host records participate in this decision
/// too, so selecting a remote pipe never silently moves a mixed graph onto the main host.
fn merge(placements: &[Option<Team>]) -> Result<Option<Team>, String> {
    let Some(first) = placements.first() else {
        return Ok(None);
    };
    if placements.iter().any(|next| match (first, next) {
        (None, None) => false,
        (Some(a), Some(b)) => a.id != b.id,
        _ => true,
    }) {
        return Err("This operation spans different executors. Select pipes and Pipeline members that share one executor.".into());
    }
    Ok(first.clone())
}

struct PlacementPolicy {
    teams: Vec<Team>,
    connectors: Map,
    policy: Policy,
}

impl PlacementPolicy {
    fn team(&self, name: &str) -> Result<Option<Team>, String> {
        if same_team(Some(name), Some(EVERYONE)) {
            return Ok(None);
        }
        let team = self
            .teams
            .iter()
            .find(|t| same_team(Some(&t.name), Some(name)))
            .ok_or("A resource's execution team no longer exists.")?;
        let settings = team.execution().map_err(|e| e.to_string())?;
        if settings.mode == Mode::Host {
            self.policy.check(team, true).map_err(|e| e.to_string())?;
            Ok(None)
        } else {
            Ok(Some(team.clone()))
        }
    }

    fn instance(&self, keys: &str) -> Result<Option<Team>, String> {
        let mut matches = Vec::new();
        for team in &self.teams {
            if team.default_instance().as_deref() == Some(keys)
                || meerschaum_core::access::reserved_for(&self.connectors, &team.name)
                    .iter()
                    .any(|k| k == keys)
            {
                matches.push(self.team(&team.name)?);
            }
        }
        merge(&matches)
    }
}

impl Controller {
    async fn metadata_instance(
        &self,
        session: &Session,
        keys: &str,
    ) -> Result<Arc<dyn InstanceConnector>, String> {
        // Custom instance connectors are executable code. Their metadata belongs behind their
        // selected executor and must never be instantiated here just to choose that executor.
        if !keys
            .split_once(':')
            .is_some_and(|(typ, _)| matches!(typ, "sql" | "api" | "valkey"))
        {
            return Err("Execution placement requires native instance metadata; custom instance code cannot load on the controller.".into());
        }
        let config = self.config()?;
        let session = session.clone();
        let keys = keys.to_owned();
        tokio::task::spawn_blocking(move || {
            session
                .instance(&keys, &config)
                .map(|i| i.conn())
                .map_err(|e| e.message)
        })
        .await
        .map_err(|_| "Could not resolve execution metadata.".to_owned())?
    }

    /// Resource-owned execution also applies to administrators and bulk operations. Resolve
    /// the same graph metadata as the engine, without constructing its connector/plugin layer.
    pub async fn command_team(
        &self,
        argv: &[String],
        session: &Session,
    ) -> Result<Option<Team>, String> {
        let caller_team = self.selected(None).await?;
        let config = self.config()?;
        let policy = PlacementPolicy {
            teams: self
                .instance
                .get_teams()
                .await
                .map_err(|_| "Could not read executor placement teams.")?,
            connectors: config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            policy: policy(&config)?,
        };
        let mut placements: Vec<Option<Team>> = caller_team.iter().cloned().map(Some).collect();
        let steps = meerschaum_server::execute_policy::command_steps(argv)?;
        for step in steps {
            let mut args =
                crate::parse::parse_arguments(&step.words, &crate::types::DateContext::system())
                    .map_err(|e| e.message)?;
            args.extend(
                step.patch
                    .into_iter()
                    .map(|(key, value)| (key, crate::server::argument(value))),
            );
            let words = crate::native::words(&args, "action");
            let resource_kind = words.get(1).map(|w| w.trim_end_matches('s'));
            let registry = crate::native::string(&args, "mrsm_instance").unwrap_or_else(|| {
                caller_team
                    .as_ref()
                    .and_then(Team::default_instance)
                    .or_else(|| {
                        config
                            .get(&["meerschaum", "api_instance"])
                            .and_then(|v| v.as_str().map(str::to_owned))
                    })
                    .unwrap_or_else(|| "sql:main".into())
            });
            if !matches!(resource_kind, Some("pipe" | "pipeline")) {
                // A raw SQL action names the database directly rather than with `-i`.
                let direct = if words.first().is_some_and(|w| w == "sql") {
                    match words.get(1) {
                        Some(keys) => policy.instance(keys)?,
                        None => policy.instance(&registry)?,
                    }
                } else {
                    caller_team.clone()
                };
                placements.push(direct);
                continue;
            }
            let instance_placement = policy.instance(&registry)?;
            // Saved-definition listings / edits / copies do not resolve their members. In
            // particular, a copy's destination is a new name, not another saved definition.
            if resource_kind == Some("pipeline")
                && !matches!(
                    words.first().map(String::as_str),
                    Some("sync" | "plan" | "analyze" | "clear" | "drop")
                )
            {
                placements.push(instance_placement.or(caller_team.clone()));
                continue;
            }
            // A team database is controlled by that team. Its rows cannot choose another
            // executor by claiming a different owner, even when an administrator reads them.
            if let Some(placement) = &instance_placement {
                placements.push(Some(placement.clone()));
            }
            // A custom team's instance can only be inspected remotely. Native metadata below
            // remains available to check explicit graph/pipe selections, including admin runs.
            if !registry
                .split_once(':')
                .is_some_and(|(typ, _)| matches!(typ, "sql" | "api" | "valkey"))
            {
                placements.push(
                    instance_placement
                        .or(caller_team.clone())
                        .ok_or("A custom instance requires a selected separate executor.")
                        .map(Some)?,
                );
                continue;
            }
            let conn = self.metadata_instance(session, &registry).await?;
            let mut selections = Vec::new();
            if resource_kind == Some("pipeline") {
                let mut names = words[2..].to_vec();
                if names.is_empty() {
                    let query = meerschaum_connector::PipesKeysQuery {
                        connector_keys: vec!["mrsm".into()],
                        metric_keys: vec!["pipeline".into()],
                        ..Default::default()
                    };
                    let keys = conn
                        .fetch_pipes_keys(&query)
                        .await
                        .map_err(|_| "Could not list Pipeline metadata.")?;
                    names.extend(
                        keys.entries()
                            .into_iter()
                            .filter_map(|e| e.location_key.clone()),
                    );
                }
                if names.len() > 10_000 {
                    return Err("Execution exceeds its Pipeline metadata limit.".into());
                }
                for name in names {
                    meerschaum_core::pipeline::validate_name(&name)?;
                    let pipe = meerschaum_core::Pipe::builder("mrsm", "pipeline")
                        .location(Some(&name))
                        .instance(&registry)
                        .build()
                        .map_err(|e| e.to_string())?;
                    let attributes = conn
                        .get_pipe_attributes(&pipe)
                        .await
                        .map_err(|_| "Could not read Pipeline metadata.")?;
                    if let Some(parameters) = attributes.get("parameters").and_then(Value::as_map) {
                        let owner = PipeAccess::from_parameters(parameters).team;
                        if let Some(owner) = owner
                            .as_deref()
                            .filter(|name| !same_team(Some(name), Some(EVERYONE)))
                        {
                            placements.push(policy.team(owner)?);
                        }
                    }
                    let definition = attributes
                        .get("parameters")
                        .and_then(Value::as_map)
                        .and_then(|p| p.get("pipeline"))
                        .ok_or("The Pipeline is missing its registered definition.")?;
                    let definition: crate::native::pipelines::Definition =
                        serde_json::from_str(&definition.to_json_string())
                            .map_err(|_| "The Pipeline has invalid registered metadata.")?;
                    selections.push(
                        if matches!(words.first().map(String::as_str), Some("clear" | "drop")) {
                            // Destructive Pipeline operations freeze explicit members, excluding
                            // both inherited and command-line traversal, like native lifecycle.
                            Selection::refs(
                                definition.targets.into_iter().map(PipeRef::from_string),
                            )
                        } else {
                            pipeline_selection(definition, &args)
                        },
                    );
                }
            } else {
                let keys = conn
                    .fetch_pipes_keys(&Filters::from_args(&args).query())
                    .await
                    .map_err(|_| "Could not read pipe placement metadata.")?;
                let refs = keys
                    .entries()
                    .into_iter()
                    .filter(|e| e.connector_keys != "mrsm")
                    .map(|e| {
                        PipeRef::new(
                            &registry,
                            &e.connector_keys,
                            &e.metric_key,
                            e.location_key.as_deref(),
                        )
                    });
                selections.push(
                    Selection::refs(refs)
                        .with_parents(depth(&args, "parents"))
                        .with_children(depth(&args, "children")),
                );
            }
            if selections.is_empty() {
                placements.push(instance_placement.clone().or(caller_team.clone()));
            }
            for selection in selections {
                let graph = self
                    .metadata_graph(session, &registry, &selection, &config)
                    .await?;
                let subgraph = Pipeline::new(selection)
                    .resolve(&graph)
                    .map_err(|e| e.to_string())?;
                if subgraph.order.is_empty() {
                    placements.push(instance_placement.clone().or(caller_team.clone()));
                }
                for reference in subgraph.order {
                    let node = graph
                        .node(&reference)
                        .ok_or("Execution metadata changed while selecting its graph.")?;
                    let owner = PipeAccess::of_pipe(&node.pipe).team;
                    let placement = match owner.as_deref() {
                        Some(name) if !same_team(Some(name), Some(EVERYONE)) => {
                            policy.team(name)?
                        }
                        _ => policy
                            .instance(reference.parts().instance_keys)?
                            .or(caller_team.clone()),
                    };
                    placements.push(placement);
                }
            }
        }
        merge(&placements)
    }

    async fn metadata_graph(
        &self,
        session: &Session,
        home: &str,
        selection: &Selection,
        config: &Config,
    ) -> Result<Graph, String> {
        let defaults = meerschaum_engine::EngineConfig::from_config(config, Some(&self.root));
        let options = GraphOptions {
            defaults: defaults.defaults.clone(),
            implicit_edges: defaults.implicit_edges,
            default_backtrack_minutes: defaults.default_backtrack_minutes as f64,
        };
        let mut visited = BTreeSet::new();
        let mut pending = selection.target_instances();
        pending.insert(home.into());
        let mut records = Vec::new();
        loop {
            for keys in std::mem::take(&mut pending) {
                if !visited.insert(keys.clone()) {
                    continue;
                }
                if visited.len() > 64 {
                    return Err("Execution graph exceeds its instance limit.".into());
                }
                let conn = self.metadata_instance(session, &keys).await?;
                let registry = StaticRegistry::new().with_instance(&keys, conn);
                records.extend(
                    meerschaum_engine::engine::fetch_records(
                        &registry,
                        &defaults.defaults,
                        &keys,
                        &Default::default(),
                    )
                    .await?,
                );
                if records.len() > 100_000 {
                    return Err("Execution graph exceeds its metadata limit.".into());
                }
            }
            let graph = Graph::build(records.clone(), &options);
            pending = selection
                .referenced_instances(&graph)
                .difference(&visited)
                .cloned()
                .collect();
            if pending.is_empty() {
                return Ok(graph);
            }
        }
    }
}

fn depth(args: &ArgsMap, key: &str) -> Option<meerschaum_engine::Depth> {
    crate::native::sync::depth(args, key)
}

fn pipeline_selection(
    definition: crate::native::pipelines::Definition,
    args: &ArgsMap,
) -> Selection {
    let inherited_depth = |key, fallback: &serde_json::Value| {
        if args.contains_key(key) {
            depth(args, key)
        } else {
            depth(
                &ArgsMap::from_iter([(key.into(), crate::native_plugins::json_to_py(fallback))]),
                key,
            )
        }
    };
    let parents = inherited_depth("parents", &definition.parents);
    let children = inherited_depth("children", &definition.children);
    Selection::refs(definition.targets.into_iter().map(PipeRef::from_string))
        .with_parents(parents)
        .with_children(children)
        .with_graph(definition.graph.unwrap_or(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cached_clients_cannot_sign_after_retirement_or_generation_rotation() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new(root.path());
        let mut team = Team::new("A");
        let id = Uuid::new_v4();
        team.id = Some(id.to_string());
        team.attributes.insert("isolated".into(), true.into());
        team.attributes.insert(
            "execution".into(),
            Value::Map(Map::from_iter([("mode".into(), "container".into())])),
        );
        registry.reconcile(&[team.clone()]).unwrap();
        let (_, authority) = registry.authority(id).unwrap();
        let signer = Signer {
            registry_root: root.path().into(),
            authority,
            actor: Actor {
                owner: "operator".into(),
                username: Some("operator".into()),
                permissions: ["*".into()].into_iter().collect(),
                administrator: false,
            },
            revision: "0".repeat(64),
        };
        let request = || Request {
            method: "GET",
            path: "/version",
            body: Some(&[]),
        };
        assert!(signer.authorization(request()).is_ok());
        registry.reconcile(&[]).unwrap();
        assert!(signer.authorization(request()).is_err());
        registry.reconcile(&[team]).unwrap();
        assert!(signer.authorization(request()).is_err());
    }
    #[test]
    fn a_whole_operation_cannot_mix_hosts_or_team_identities() {
        let mut a = Team::new("A");
        a.id = Some(Uuid::new_v4().to_string());
        let mut b = Team::new("B");
        b.id = Some(Uuid::new_v4().to_string());
        assert_eq!(merge(&[None, None]).unwrap(), None);
        assert!(merge(&[Some(a.clone()), None]).is_err());
        assert!(merge(&[Some(a.clone()), Some(b)]).is_err());
        let mut renamed = a.clone();
        renamed.name = "renamed".into();
        assert_eq!(
            merge(&[Some(a.clone()), Some(renamed)])
                .unwrap()
                .unwrap()
                .id,
            a.id
        );
    }
}
