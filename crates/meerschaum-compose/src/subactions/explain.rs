//! `compose explain`: what the project defines, where each pipe is registered and how its
//! parameters resolve.

use super::*;

/// The registration status shown by `explain`.
fn registration_status(
    temporary: bool,
    registered: bool,
    local: &Map,
    remote: &Map,
) -> (&'static str, bool) {
    if temporary {
        return ("🔳 Temporary", false);
    }
    if !registered {
        return ("⭕ Not registered", false);
    }
    let local_str = json_sorted_compact(&Value::Map(local.clone()));
    let remote_str = json_sorted_compact(&Value::Map(remote.clone()));
    if local_str == remote_str {
        return ("✅ Up-to-date", false);
    }
    // `{**local, **remote} != remote`: local keys missing from the remote parameters.
    let mut merged = local.clone();
    for (k, v) in remote {
        merged.insert(k.clone(), v.clone());
    }
    if py_eq(&Value::Map(merged), &Value::Map(remote.clone())) {
        ("🟨 Params added", true)
    } else {
        ("❌ Outdated", true)
    }
}

fn json_rich(params: &Map) -> String {
    json_dumps(&Value::Map(params.clone()), JsonStyle::Indent(2), false)
}

/// `compose explain`: the defined pipes and their registration status, plus the lint.
///
/// Side-effect free: nothing is created or installed, the file is not cached and no plugin is
/// fetched. An instance that cannot be reached, a connector that is not defined and a plugin
/// that is not installed are findings, and the table is still printed.
pub(super) fn compose_explain(
    ctx: &mut Ctx,
    project: &Project,
    pc: &ProjectConfig,
    tree_pipes: &[PipeKey],
) -> Result<SuccessTuple, ComposeError> {
    let defaults = pc.pipe_defaults();
    let default_instance = project
        .config_block()
        .get("meerschaum")
        .and_then(Value::as_map)
        .and_then(|m| m.get("instance"))
        .map(py_str)
        .unwrap_or_else(|| defaults.instance_keys.clone());
    let mut findings = lint(
        &LintInput {
            document: &project.document,
            compose_file: &project.file,
            defaults: &defaults,
            default_instance: &default_instance,
            also_defined: tree_pipes,
        },
        ctx.instance,
    );

    let project_name = project.project_name();
    // The lint is reported even when the pipes cannot be built.
    if ctx.multi {
        for f in &mut findings {
            f.location = format!("{project_name}: {}", f.location);
        }
    }
    ctx.out.lint.append(&mut findings);
    build_custom_connectors(ctx, project)?;
    let pipes = defined_pipes(ctx, project, pc)?;
    let mut findings = project_findings(ctx, project, pc);
    if ctx.multi {
        for f in &mut findings {
            f.location = format!("{project_name}: {}", f.location);
        }
    }
    let groups = instance_pipes(&pipes);
    let tags = vec![project_name.clone()];
    let mut remote: IndexMap<String, Vec<Pipe>> = IndexMap::new();
    let mut unreachable: IndexMap<String, String> = IndexMap::new();
    for inst in groups.keys() {
        match ctx.instance.try_get_pipes(inst, &tags) {
            Ok(r) => {
                remote.insert(inst.clone(), r);
            }
            Err(e) => {
                findings.push(LintWarning::warning(
                    "unreachable-instance",
                    format!("{project_name}: {inst}"),
                    format!(
                        "The instance '{inst}' cannot be reached, so registrations are unknown: {}",
                        e.trim()
                    ),
                ));
                unreachable.insert(inst.clone(), e);
            }
        }
    }
    ctx.out.lint.extend(findings);

    let width = ctx.args.width.unwrap_or(80);
    let mut model = ExplainModel {
        project_name: project_name.clone(),
        width,
        default_instance: default_instance.clone(),
        pipelines: crate::pipelines::defined(project, pc)?,
        instances: Vec::new(),
    };
    let mut rows: Vec<(Vec<Renderable>, bool, bool)> = Vec::new();
    for (inst, idxs) in &groups {
        rows.push((
            vec![
                Renderable::Panel {
                    body: inst.clone(),
                    title: "Instance".into(),
                },
                Renderable::text(""),
                Renderable::text(""),
            ],
            false,
            true,
        ));
        let mut explained = ExplainInstance {
            keys: inst.clone(),
            pipes: Vec::new(),
        };
        for (i, ix) in idxs.iter().enumerate() {
            let pipe = &pipes[*ix];
            let is_unreachable = unreachable.contains_key(inst);
            let registered = remote.get(inst).is_some_and(|rs| {
                rs.iter().any(|r| {
                    r.connector_keys() == pipe.connector_keys()
                        && r.metric_key() == pipe.metric_key()
                        && r.location_key() == pipe.location_key()
                })
            });
            let remote_params = if is_unreachable {
                Map::new()
            } else {
                ctx.instance.get_parameters(pipe).unwrap_or_default()
            };
            let local_params = pipe.raw_parameters().clone();
            let (status, include_remote) = if is_unreachable && !pipe.temporary {
                ("❔ Unknown (instance unreachable)", false)
            } else {
                registration_status(pipe.temporary, registered, &local_params, &remote_params)
            };
            let end_section = i == idxs.len() - 1;
            rows.push((
                vec![
                    Renderable::text(format!("{pipe}\n\n{status}\n")),
                    Renderable::json(json_rich(&local_params)),
                    if include_remote {
                        Renderable::json(json_rich(&remote_params))
                    } else {
                        Renderable::text("")
                    },
                ],
                end_section,
                true,
            ));
            explained.pipes.push(ExplainPipe {
                connector: pipe.connector_keys().to_string(),
                metric: pipe.metric_key().to_string(),
                location: pipe.location_key().map(String::from),
                instance: pipe.instance_keys().to_string(),
                status: status.to_string(),
                local: local_params,
                remote: include_remote.then_some(remote_params),
            });
        }
        model.instances.push(explained);
    }

    // PARITY: the instance rows carry an empty remote cell (not `None`), so the "Remote
    // Parameters" column is shown whenever there is at least one pipe (explain.py:249).
    let include_remote_col = rows.iter().any(|(_, _, has_remote)| *has_remote);
    let mut headers = vec![
        "Defined Pipes".to_string(),
        "Compose Parameters".to_string(),
    ];
    if include_remote_col {
        headers.push("Remote Parameters".into());
    }
    let mut table_rows = Vec::new();
    for (mut cells, end_section, _) in rows {
        if !include_remote_col {
            cells.truncate(2);
        }
        table_rows.push(Row { cells, end_section });
        if !end_section {
            table_rows.push(Row {
                cells: vec![Renderable::text(""), Renderable::text("")],
                end_section: false,
            });
        }
    }
    let table = Table {
        title: format!("Pipes in '{project_name}'"),
        headers,
        rows: table_rows,
    };
    let mut rendered = table.render(width);
    if !model.pipelines.is_empty() {
        rendered.push_str(&format!("\nPipelines in '{project_name}':\n"));
        for spec in &model.pipelines {
            rendered.push_str(&format!(
                "{} (registry {})\n{}\n",
                spec.name,
                spec.instance,
                json_rich(&spec.definition)
            ));
        }
    }
    match &mut ctx.out.explain {
        Some(existing) => {
            existing.push('\n');
            existing.push_str(&rendered);
        }
        none => *none = Some(rendered),
    }
    ctx.out.explain_models.push(model);
    Ok(SuccessTuple::success())
}
