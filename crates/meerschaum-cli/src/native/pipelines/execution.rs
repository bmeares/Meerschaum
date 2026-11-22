//! Running Pipelines: planning and syncing their members once, in a loop or listening, and
//! the captured run other hosts start.

use super::*;

/// The saved Pipelines a command names, resolved and ready to run.
struct PreparedRun {
    /// Their definitions, as read now.
    definitions: Vec<Definition>,
    /// The pipes of their selections.
    pipes: Vec<SelectedPipe>,
    /// The engine over those pipes.
    engine: meerschaum_engine::Engine,
    /// The run's options (the exact selection and the definitions it runs).
    options: meerschaum_engine::RunOptions,
    /// The command's arguments with the first Pipeline's execution defaults filled in.
    effective: ArgsMap,
}

pub(super) struct CapturedPipeline {
    definition: Definition,
    roots: BTreeSet<String>,
    authority: std::sync::Arc<super::graph_plan::MainGraph>,
}

/// Entered from the built-in dispatcher only after Main confirmed the exact argv.
pub(crate) fn run_captured(
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    name: &str,
    mut authority: super::graph_plan::MainGraph,
) -> ActionResult {
    let instance = super::get_instance(args, rt)?;
    let registry = instance.keys();
    let run_id = super::string(args, "run_id")
        .ok_or_else(|| error("The captured Pipeline requires its run ID."))?;
    let (definition, roots) = session(rt)
        .block_on(authority.pipeline(&registry, name, &run_id))
        .map_err(error)?;
    let expected =
        meerschaum_server::executor::pipe_operation::pipeline_targets(&definition, &registry, name)
            .map_err(|reason| error(reason.detail_text()))?;
    let count = roots.len();
    let roots: BTreeSet<_> = roots.into_iter().collect();
    if roots.len() != count || roots != expected {
        return Err(error(
            "The issued Pipeline roots differ from its saved definition.",
        ));
    }
    let captured = CapturedPipeline {
        definition: serde_json::from_value(definition).map_err(error)?,
        roots,
        authority: std::sync::Arc::new(authority),
    };
    execute("sync", args, rt, &instance, Some(captured))
}

/// Read and resolve the Pipelines `args` name (`None`: they select no pipes). `continuous`: for
/// the engine's continuous mode, whose stream sources get the loop's options (and `--listen`
/// keeps v4-pattern connectors subscribed).
fn prepare_run(
    action: &str,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    instance: &Instance,
    continuous: bool,
    captured: Option<CapturedPipeline>,
) -> Result<Option<PreparedRun>, ActionError> {
    let (definitions, captured_roots, authority) = match captured {
        Some(capture) => (
            vec![capture.definition],
            Some(capture.roots),
            Some(capture.authority),
        ),
        None => (names(rt, instance, args, true)?, None, None),
    };
    let graph = definitions[0].graph.unwrap_or(true);
    if definitions
        .iter()
        .any(|definition| definition.graph.unwrap_or(true) != graph)
    {
        return Err(error(
            "Combined Pipelines have conflicting graph settings; run them separately.",
        ));
    }
    let default_keys: BTreeSet<_> = definitions
        .iter()
        .flat_map(|definition| definition.defaults.keys())
        .collect();
    for key in default_keys {
        if !args.contains_key(key)
            && definitions
                .iter()
                .skip(1)
                .any(|definition| definition.defaults.get(key) != definitions[0].defaults.get(key))
        {
            return Err(error(format!(
                "Combined Pipelines have conflicting execution default '{key}'; provide an explicit --{} override.",
                key.replace('_', "-")
            )));
        }
    }
    let mut dependency_order = graph;
    let refs = if let Some(roots) = captured_roots {
        roots
    } else {
        let mut refs = BTreeSet::new();
        for definition in &definitions {
            dependency_order |=
                !definition_selection(definition, &effective_args(args, definition)?).is_flat();
            refs.extend(resolve(rt, instance, definition, args)?);
        }
        refs
    };
    if refs.is_empty() {
        return Ok(None);
    }
    let refs: Vec<_> = refs.into_iter().collect();
    let pipes = selected(rt, &refs)?;
    let session = session(rt);
    if action != "plan" {
        super::sync::prepare_live(rt, args);
    }
    let effective = effective_args(args, &definitions[0])?;
    let (listen, stream_kwargs) = if continuous {
        (
            flag(&effective, "listen"),
            Some(super::sync::stream_kwargs(&effective)),
        )
    } else {
        (false, None)
    };
    let engine =
        super::sync::engine::build(rt, &session, instance, &pipes, true, listen, stream_kwargs)?;
    let engine = match authority {
        Some(authority) => engine.with_plan_authority(authority),
        None => engine,
    };
    let mut options = super::sync::run_options(
        &effective,
        instance,
        &pipes,
        &super::sync::SyncArgs::from_args(&effective),
    );
    options.selection =
        Some(Selection::refs(refs.iter().map(PipeRef::from_string)).with_graph(dependency_order));
    options.pipeline_definitions = definitions
        .iter()
        .map(|d| serde_json::to_value(d).unwrap())
        .collect();
    Ok(Some(PreparedRun {
        definitions,
        pipes,
        engine,
        options,
        effective,
    }))
}

/// The name a run of `definitions` is given (`daily-2026-...`; `combined` for several).
fn run_name(definitions: &[Definition]) -> &str {
    if definitions.len() == 1 {
        definitions[0].name.as_str()
    } else {
        "combined"
    }
}

pub(super) fn execute(
    action: &str,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    instance: &Instance,
    captured: Option<CapturedPipeline>,
) -> ActionResult {
    if action == "sync" && (flag(args, "loop") || flag(args, "listen")) {
        if captured.is_some() {
            return Err(error("The captured Pipeline was admitted for one run."));
        }
        return execute_loop(args, rt, instance);
    }
    let Some(PreparedRun {
        definitions,
        engine,
        mut options,
        ..
    }) = prepare_run(action, args, rt, instance, false, captured)?
    else {
        return Ok(SuccessTuple::fail("Pipeline has no target Pipes."));
    };
    let session = session(rt);
    if action == "plan" {
        let plan = session.block_on(engine.plan(&options)).map_err(error)?;
        if flag(args, "nopretty") {
            rt.print(&format!(
                "{}\n",
                serde_json::to_string(&plan.plan).map_err(error)?
            ));
        } else {
            rt.print(&format!("{}", plan.plan));
        }
        return Ok(SuccessTuple::success());
    }
    let run_id = options
        .run_id
        .clone()
        .unwrap_or_else(|| meerschaum_engine::runs::new_named_run_id(run_name(&definitions)));
    rt.print(&format!("Run: {run_id}\n"));
    options.run_id = Some(run_id);
    // Each member's block prints as it ends (a job's log too), then the totals.
    let view = super::sync::LiveView::Nodes { instance, args };
    let (result, summary) =
        super::sync::run_reported(rt, &session, engine, options, view, flag(args, "nopretty"));
    for warning in result.warnings {
        super::warn(rt, &warning);
    }
    Ok(SuccessTuple {
        success: result.success,
        message: format!("{}{summary}", result.message),
    })
}

/// Each Pipeline's identity and revision: what an edit changes.
fn revisions(definitions: &[Definition]) -> Vec<(String, String, u64)> {
    definitions
        .iter()
        .map(|d| (d.name.clone(), d.id.clone(), d.revision))
        .collect()
}

/// `sync pipelines NAME --loop` (what `start pipeline NAME` runs) and `--listen`: the engine's
/// continuous mode over the Pipelines' members, as `sync pipes --parents --loop` runs, so a
/// member with a `schedule` syncs when it is due and a lap with nothing due records no run.
/// The loop reads the Pipelines again every `RELOAD_CHECK`: after an edit (a new revision), it
/// stops after its current lap and starts again over the new members. An edit it cannot run (a
/// member that is not registered, an unreachable instance) is retried like a failing lap.
fn execute_loop(args: &ArgsMap, rt: &mut Runtime<'_>, instance: &Instance) -> ActionResult {
    let session = session(rt);
    super::jobs::install_stop_handler(&session);
    let signal = super::jobs::stop_requested_flag();
    let mut heartbeat: Option<meerschaum_jobs::Heartbeat> = None;
    let mut retries =
        meerschaum_jobs::LapPacer::new(super::float(args, "min_seconds").unwrap_or(1.0));
    loop {
        let run = match prepare_run("sync", args, rt, instance, true, None) {
            Ok(Some(run)) => run,
            Ok(None) => return Ok(SuccessTuple::fail("Pipeline has no target Pipes.")),
            // The first start fails as `sync pipes --loop` does; a reload keeps the job alive.
            Err(e) if heartbeat.is_some() => {
                let pause = retries
                    .after_lap(std::time::Instant::now(), false)
                    .duration();
                if let Some(heartbeat) = heartbeat.as_mut() {
                    heartbeat.lap_finished(false, &e.message);
                    heartbeat.waiting(pause);
                }
                super::warn(
                    rt,
                    &format!(
                        "Cannot sync the edited Pipeline: {}\n    Trying again in {}.",
                        e.message,
                        meerschaum_jobs::health::humanize_seconds(pause.as_secs_f64())
                    ),
                );
                // Another edit (a fix) is tried at once; else the pause runs out.
                let failed = names(rt, instance, args, true).ok().map(|d| revisions(&d));
                let deadline = std::time::Instant::now()
                    .checked_add(pause)
                    .unwrap_or_else(|| std::time::Instant::now() + super::sync::RELOAD_CHECK);
                loop {
                    let step = deadline
                        .saturating_duration_since(std::time::Instant::now())
                        .min(super::sync::RELOAD_CHECK);
                    if step.is_zero() {
                        break;
                    }
                    if super::jobs::sleep_unless_stopped(step.as_secs_f64()) {
                        return Ok(SuccessTuple::fail(e.message));
                    }
                    let now = names(rt, instance, args, true).ok().map(|d| revisions(&d));
                    if now.is_some() && now != failed {
                        break;
                    }
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        retries.after_lap(std::time::Instant::now(), true);
        // Job health: the pace of the members that start the graph (their own schedules).
        let roots: BTreeSet<PipeRef> = match session.block_on(run.engine.resolve(&run.options)) {
            Ok((_, sub, _)) => sub
                .order
                .iter()
                .filter(|r| sub.flat || sub.parent_edges(r).next().is_none())
                .cloned()
                .collect(),
            Err(_) => BTreeSet::new(),
        };
        let (interval, stall) = super::jobs_laps::sync_cadence(
            &run.effective,
            run.pipes
                .iter()
                .map(|sp| &sp.pipe)
                .filter(|pipe| roots.is_empty() || roots.contains(&PipeRef::from_pipe(pipe))),
        );
        let heartbeat =
            heartbeat.get_or_insert_with(|| super::jobs_laps::heartbeat(interval, "sync"));
        heartbeat.set_interval(interval);
        heartbeat.set_stall_seconds(stall);
        let loaded = revisions(&run.definitions);
        let mut reload = |rt: &mut Runtime<'_>| -> bool {
            // An unreadable registry keeps the members the loop has.
            names(rt, instance, args, true).is_ok_and(|now| revisions(&now) != loaded)
        };
        let end = super::sync::run_continuous(
            &run.effective,
            rt,
            &session,
            instance,
            run.engine,
            run.options,
            heartbeat,
            super::sync::LoopExtras {
                run_name: Some(run_name(&run.definitions).to_string()),
                reload: Some(&mut reload),
            },
        )?;
        if !end.reload || signal.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(end.result);
        }
        let changed: Vec<String> = run.definitions.iter().map(|d| d.name.clone()).collect();
        super::info(
            rt,
            &format!(
                "Pipeline{} {} {} edited: reloading {} members.",
                if changed.len() == 1 { "" } else { "s" },
                super::items_str(&changed),
                if changed.len() == 1 { "was" } else { "were" },
                if changed.len() == 1 { "its" } else { "their" },
            ),
        );
    }
}
