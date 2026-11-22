//! `compose init`: write a new project's compose file and initialize an existing project.

use super::*;

/// The compose file `compose init` writes for a new project (`_write_new_compose_file`).
pub fn new_compose_file_text(project_name: &str, pipes: &[Map]) -> String {
    let mut doc = Map::new();
    doc.insert("project_name".into(), Value::Str(project_name.into()));
    doc.insert("root_dir".into(), Value::Str("./root".into()));
    doc.insert(
        "plugins_dir".into(),
        Value::List(vec![Value::Str("./plugins".into())]),
    );
    doc.insert(
        "pipes".into(),
        Value::List(pipes.iter().cloned().map(Value::Map).collect()),
    );
    doc.insert("plugins".into(), Value::List(Vec::new()));
    let mut sql = Map::new();
    sql.insert(
        "main".into(),
        Value::Str("MRSM{meerschaum:connectors:sql:main}".into()),
    );
    let mut connectors = Map::new();
    connectors.insert("sql".into(), Value::Map(sql));
    let mut mrsm = Map::new();
    mrsm.insert("connectors".into(), Value::Map(connectors));
    let mut config = Map::new();
    config.insert("meerschaum".into(), Value::Map(mrsm));
    doc.insert("config".into(), Value::Map(config));
    doc.insert("environment".into(), Value::new_map());
    format!(
        "### See https://meerschaum.io/reference/compose/ for the compose schema.\n\n{}",
        meerschaum_core::yaml::dump(&Value::Map(doc))
            .expect("the compose template is a fixed document the YAML emitter accepts")
    )
}

/// The `pipes:` entry `compose init` writes for a registered pipe.
fn pipe_entry(pipe: &Pipe) -> Map {
    let mut entry = Map::new();
    entry.insert("connector".into(), Value::Str(pipe.connector_keys().into()));
    entry.insert("metric".into(), Value::Str(pipe.metric_key().into()));
    entry.insert(
        "location".into(),
        pipe.location_key()
            .map_or(Value::Null, |l| Value::Str(l.into())),
    );
    entry.insert("instance".into(), Value::Str(pipe.instance_keys().into()));
    entry.insert(
        "parameters".into(),
        Value::Map(pipe.raw_parameters().clone()),
    );
    entry
}

/// `compose init`: create a project (if none exists) and install its plugins' requirements.
pub(super) fn compose_init(ctx: &mut Ctx) -> Result<SuccessTuple, ComposeError> {
    let mut file = ctx.args.file.clone();
    let found = infer_compose_file_path(file.as_deref(), &ctx.host.cwd, &mut ctx.out.messages);
    if found.is_none() {
        let cwd = ctx.host.cwd.clone();
        let compose_path = cwd.join("mrsm-compose.yaml");
        file = Some(compose_path.clone());
        ctx.out.messages.info("No compose project could be found.");
        let dir_name = cwd
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let create = ctx
            .yes_no(
                &format!(
                    "Create new project '{dir_name}'?\n    ({})",
                    compose_path.display()
                ),
                true,
                false,
            )
            .unwrap_or(false);
        if !create {
            return Ok(SuccessTuple::fail("No files were created."));
        }
        // `-c/-m/-l/-t/-i` add the matching registered pipes.
        let filters = PipeFilters {
            connector_keys: ctx.args.connector_keys.clone(),
            metric_keys: ctx.args.metric_keys.clone(),
            location_keys: ctx.args.location_keys.clone(),
            tags: ctx.args.tags.clone(),
            instance: ctx.args.mrsm_instance.clone(),
        };
        let mut entries: Vec<Map> = Vec::new();
        if filters.is_empty() {
            ctx.out
                .messages
                .info("Run `compose init` with filters (-c, -m, -l, -t, -i) to add pipes.");
        } else if ctx.yes_no(
            "Add pipes from the given filters to the compose file?",
            true,
            false,
        )? {
            ctx.instance.configure_host(ctx.host);
            let pipes = ctx
                .instance
                .get_pipes_matching(&filters)
                .map_err(|e| ComposeError::new(ErrorKind::Exception, e))?;
            ctx.out.messages.info(if pipes.is_empty() {
                "No pipes match the given filters.".to_string()
            } else {
                format!(
                    "Pipes matching the given filters:\n{}",
                    pipes
                        .iter()
                        .map(|p| format!("  {p}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            });
            if ctx.yes_no("Add these pipes to the compose file?", true, false)? {
                entries = pipes.iter().map(pipe_entry).collect();
            } else {
                ctx.out.messages.info("No pipes will be added.");
            }
        }
        if compose_path.exists() {
            return Err(ComposeError::new(
                ErrorKind::FileExists,
                format!("File '{}' already exists.", compose_path.display()),
            ));
        }
        let text = new_compose_file_text(&dir_name, &entries);
        if std::fs::write(&compose_path, &text).is_err() {
            return Ok(SuccessTuple::fail(format!(
                "Failed to create file '{}'.",
                compose_path.display()
            )));
        }
        ctx.out.written_files.push((compose_path, text));
    }

    let run = load_run_tree(ctx, file.as_deref(), false, true)?;
    ctx.multi = run.selected.len() > 1;
    let mut results: Vec<(String, SuccessTuple)> = Vec::new();
    for ix in &run.selected {
        let project = run.tree.nodes[*ix].project.clone();
        ctx.primary = *ix == run.primary;
        let result = init_project(ctx, &project)?;
        let ok = result.success;
        results.push((project.project_name(), result));
        if !ok {
            break;
        }
    }
    Ok(combine_results(results, ctx.multi))
}

/// `compose init` for one project of the run.
fn init_project(ctx: &mut Ctx, project: &Project) -> Result<SuccessTuple, ComposeError> {
    init_root(ctx, project)?;
    let pc = ProjectConfig::new(ctx.host, project)?;
    let project_name = project.project_name();
    let existing = get_installed_plugins(project)?;
    let r = check_and_install_plugins(
        project,
        &pc,
        Some(existing.clone()),
        ctx.args.debug,
        &mut ctx.runner,
        &mut ctx.out.messages,
    )?;
    if !r.success {
        return Ok(r);
    }
    let installed = if existing.is_empty() {
        get_installed_plugins(project)?
    } else {
        existing
    };
    if !installed.is_empty() {
        let r = ctx.run(
            &["install".into(), "required".into()],
            project,
            RunOptions {
                subprocess: Some(true),
                ..RunOptions::default()
            },
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to install required packages for project '{project_name}':\n{}",
                r.message
            )));
        }
        let r = ctx.run(
            &["setup".into(), "plugins".into()],
            project,
            RunOptions {
                subprocess: Some(true),
                ..RunOptions::default()
            },
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to setup plugins for project '{project_name}':\n{}",
                r.message
            )));
        }
    }
    Ok(SuccessTuple::ok(format!(
        "Finished initializing project '{project_name}'."
    )))
}
