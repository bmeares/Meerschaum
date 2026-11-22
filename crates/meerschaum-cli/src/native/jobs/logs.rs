//! Following a job's output: `attach jobs` (answering its prompts) and `show logs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_core::value::Value;
use meerschaum_jobs::{Job, JobOptions, MonitorControl, MonitorOptions};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::rich::text::Text;
use crate::value::ArgsMap;

use super::stop_flag::{install_stop_handler, stop_requested_flag};
use super::{executor_keys, filtered_jobs, jobs_context};
use crate::native::{flag, info, string, words};

/// `attach jobs`.
pub fn attach_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let name = string(args, "name");
    if action.is_empty() && name.is_none() {
        return Ok(SuccessTuple::fail(
            "Provide the name of the job to attach to.",
        ));
    }
    let name = name.unwrap_or_else(|| action[0].clone());
    let ctx = jobs_context(rt);
    let mut job = Job::new(
        &name,
        None,
        JobOptions {
            executor_keys: executor_keys(args),
            ..Default::default()
        },
        &ctx,
    )
    .map_err(|e| ActionError::new("ValueError", e.to_string()))?;
    match job.try_exists() {
        Err(e) => return Ok(SuccessTuple::fail(e)),
        Ok(false) => {
            return Ok(SuccessTuple::fail(format!(
                "Job '{}' does not exist.",
                job.name
            )));
        }
        Ok(true) => {}
    }
    install_stop_handler(&super::session(rt));
    // The question of a Python `prompt()` inside the job: it writes `prompt_kwargs.json` and
    // prints nothing, and v4's `attach` asked it here (with the job's styling and icon).
    let question_templates = [
        super::question_text(rt, ATTACH_QUESTION_SLOT, false),
        super::question_text(rt, ATTACH_QUESTION_SLOT, true),
    ];
    let mut probe = Job::new(
        &name,
        None,
        JobOptions {
            executor_keys: executor_keys(args),
            ..Default::default()
        },
        &ctx,
    )
    .ok();
    // A job's input is its program's: a restricted terminal forwards it only to a job whose own
    // command the server would let this user run.
    let accept_input = !crate::shell::restricted::active() || {
        let sysargs = job.sysargs();
        match crate::shell::restricted::authorize(&sysargs) {
            Ok(()) => true,
            Err(message) => {
                let text = rt.with_formatter(|f| {
                    f.warn(&format!(
                        "Showing the output of job '{}' without forwarding input: {message}",
                        job.name
                    ))
                });
                rt.eprint(&text);
                false
            }
        }
    };
    let result = std::cell::RefCell::new(SuccessTuple::ok("Success"));
    let out = std::cell::RefCell::new(&mut *rt.out);
    let stop = stop_requested_flag();
    let opts = MonitorOptions {
        stop_on_exit: true,
        strip_timestamps: true,
        accept_input,
        stop: Some(stop),
        input: Some(Box::new(move || {
            let kwargs = probe
                .as_mut()
                .map(|j| j.get_prompt_kwargs())
                .unwrap_or_default();
            let question = attach_question(&kwargs, &question_templates);
            if let Some((question, true)) = &question
                && std::io::IsTerminal::is_terminal(&std::io::stdin())
            {
                return crate::native::terminal::read_secret(question).map(|a| a + "\n");
            }
            if let Some((question, _)) = &question {
                print!("{question}");
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }
            // Wait for the answer while the job waits for it: a job which ended (or was
            // answered elsewhere) is not waited for.
            loop {
                if let Some(line) = crate::native::terminal::timed_input(Duration::from_millis(250))
                {
                    return Some(line + "\n");
                }
                if !probe.as_mut().is_some_and(|j| j.is_blocking_on_stdin()) {
                    return None;
                }
            }
        })),
        on_stop: Some(Box::new(|r: &SuccessTuple| {
            *result.borrow_mut() = r.clone();
        })),
        ..Default::default()
    };
    let _ = job.monitor_logs(
        |line: &str| {
            let mut o = out.borrow_mut();
            let _ = o.write_all(line.as_bytes());
            let _ = o.flush();
            MonitorControl::Continue
        },
        opts,
    );
    Ok(result.into_inner())
}

/// Stands for the question in the templates `attach_question` fills.
const ATTACH_QUESTION_SLOT: &str = "\u{1}MRSM_QUESTION\u{1}";

/// The question of `prompt_kwargs.json` (written by Python's `prompt()` inside a job) as the
/// job's own prompt would show it, and whether the answer is a password (v4's `prompt(**kwargs)`
/// in `attach`). `None` without a question to show (no file, or `silent`).
fn attach_question(
    kwargs: &meerschaum_core::value::Map,
    templates: &[String; 2],
) -> Option<(String, bool)> {
    let question = kwargs.get("question")?.as_str()?.to_string();
    if kwargs.get("silent").is_some_and(Value::truthy) {
        return None;
    }
    let mut text = question;
    match kwargs.get("default") {
        Some(Value::List(items)) if items.len() > 1 => {
            text.push_str(&format!(
                " (default: {} [{}])",
                items[0].py_str(),
                items[1].py_str()
            ));
        }
        Some(Value::Null) | None => {}
        Some(other) => text.push_str(&format!(" (default: {})", other.py_str())),
    }
    let detect = kwargs.get("detect_password").is_none_or(Value::truthy);
    let password = kwargs.get("is_password").is_some_and(Value::truthy)
        || (detect && text.to_lowercase().contains("password"));
    let icon = kwargs.get("icon").is_none_or(Value::truthy);
    let template = &templates[usize::from(icon)];
    Some((template.replace(ATTACH_QUESTION_SLOT, &text), password))
}

/// `datetime.strptime(prefix, format)` for the log timestamp formats (`%Y-%m-%d %H:%M` etc.).
fn parse_line_timestamp(prefix: &str, fmt: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(prefix, fmt)
        .ok()
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(prefix, fmt)
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
        })
}

/// `show logs` / `attach logs`.
pub fn show_logs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let ctx = jobs_context(rt);
    let exec_keys = executor_keys(args);
    let mut jobs = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false) {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    // A job's log keeps its styles; printed off a terminal (or under `NO_COLOR`) it is plain.
    let plain = {
        let settings = rt.settings();
        !settings.ansi || settings.no_color
    };
    if nopretty {
        for (name, job) in jobs.iter_mut() {
            let logs = match job.try_get_logs() {
                Ok(logs) => logs.unwrap_or_else(|| "None".into()),
                Err(e) => return Ok(SuccessTuple::fail(e)),
            };
            let logs = if plain {
                crate::format::remove_ansi(&logs)
            } else {
                logs
            };
            rt.print(&format!("\n-*-\nMRSM_JOB: {name}\n-*-\n{logs}\n"));
        }
        return Ok(SuccessTuple::ok("Success"));
    }

    let colors: Vec<String> = rt
        .config()
        .get(&["jobs", "logs", "colors"])
        .and_then(|v| v.as_list().map(|l| l.iter().map(Value::py_str).collect()))
        .filter(|l: &Vec<String>| !l.is_empty())
        .unwrap_or_else(|| vec!["cyan".into()]);
    let min_buffer_len = rt
        .config()
        .get(&["jobs", "logs", "min_buffer_len"])
        .and_then(|v| v.as_i64())
        .unwrap_or(10) as usize;
    let ts_fmt = ctx.config.timestamp_format.clone();
    let follow_fmt = ctx.config.follow_timestamp_format.clone();
    let ansi = rt.settings().ansi;
    if ansi {
        info(rt, "Watching logs...");
    } else {
        rt.print("Watching logs...\n");
    }

    install_stop_handler(&super::session(rt));
    let stop = stop_requested_flag();
    let (tx, rx) = std::sync::mpsc::channel::<(String, String)>();
    let mut tasks: IndexMap<String, (Arc<AtomicBool>, std::thread::JoinHandle<()>)> =
        IndexMap::new();
    let mut job_keys: IndexMap<String, String> = IndexMap::new();
    let now_len = chrono::Utc::now()
        .format(&ts_fmt)
        .to_string()
        .chars()
        .count();
    let mut previous_ts: Option<chrono::NaiveDateTime> = None;
    let mut last_refresh = Instant::now() - Duration::from_secs(2);
    let mut names: Vec<String> = jobs.keys().cloned().collect();
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        if last_refresh.elapsed() >= Duration::from_secs(1) {
            last_refresh = Instant::now();
            let current = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false)
            {
                Ok(jobs) => jobs,
                // An unreachable endpoint ends the follower with its error.
                Err(failed) => {
                    for (_, (s, h)) in tasks {
                        s.store(true, Ordering::SeqCst);
                        let _ = h.join();
                    }
                    return Ok(failed);
                }
            };
            names = current.keys().cloned().collect();
            for (n, j) in &current {
                job_keys.insert(n.clone(), j.executor_keys.clone());
            }
            for name in &names {
                if tasks.contains_key(name) {
                    continue;
                }
                let job_stop = Arc::new(AtomicBool::new(false));
                let keys = job_keys.get(name).cloned();
                let (tx, ctx, n, js) = (tx.clone(), ctx.clone(), name.clone(), job_stop.clone());
                let handle = std::thread::spawn(move || {
                    let Ok(mut job) = Job::new(
                        &n,
                        None,
                        JobOptions {
                            executor_keys: keys,
                            ..Default::default()
                        },
                        &ctx,
                    ) else {
                        return;
                    };
                    let _ = job.monitor_logs(
                        |line: &str| {
                            if tx.send((n.clone(), line.to_string())).is_err() {
                                return MonitorControl::Stop;
                            }
                            MonitorControl::Continue
                        },
                        MonitorOptions {
                            accept_input: false,
                            stop_on_exit: false,
                            stop: Some(js),
                            ..Default::default()
                        },
                    );
                });
                tasks.insert(name.clone(), (job_stop, handle));
            }
            let gone: Vec<String> = tasks
                .keys()
                .filter(|n| !names.contains(n))
                .cloned()
                .collect();
            for n in gone {
                if let Some((s, h)) = tasks.shift_remove(&n) {
                    s.store(true, Ordering::SeqCst);
                    let _ = h.join();
                }
            }
        }
        let Ok((name, line)) = rx.recv_timeout(Duration::from_millis(100)) else {
            continue;
        };
        let prefix: String = line.chars().take(now_len).collect();
        let (line_ts, rest) = match parse_line_timestamp(&prefix, &ts_fmt) {
            Some(ts) => {
                previous_ts = Some(ts);
                (Some(ts), line.chars().skip(now_len + 3).collect::<String>())
            }
            None => (previous_ts, line.clone()),
        };
        if rest.is_empty() || rest == "\n" {
            continue;
        }
        let max_len_id = names.iter().map(|n| n.chars().count()).max().unwrap_or(0) + 1;
        let buffer_len = min_buffer_len.max(max_len_id);
        let spaces = " ".repeat(buffer_len.saturating_sub(name.chars().count()));
        let spaces = if spaces.is_empty() {
            " ".into()
        } else {
            spaces
        };
        let color = names
            .iter()
            .position(|n| *n == name)
            .map(|i| colors[i % colors.len()].clone())
            .unwrap_or_else(|| colors[0].clone());
        let line_prefix = format!(
            "{spaces}{} | ",
            line_ts
                .map(|t| t.format(&follow_fmt).to_string())
                .unwrap_or_default()
        );
        let body = rest.strip_suffix('\n').unwrap_or(&rest);
        let body = if plain {
            std::borrow::Cow::Owned(crate::format::remove_ansi(body))
        } else {
            std::borrow::Cow::Borrowed(body)
        };
        let text = rt.with_formatter(|f| {
            let mut t = Text::plain_text(&name);
            t.append_str(&format!("{line_prefix}{body}"), "");
            if f.settings.ansi {
                let end = name.chars().count() + line_prefix.chars().count();
                t.stylize(color.as_str(), 0, Some(end));
            }
            f.console().print_text(&t)
        });
        rt.print(&text);
    }
    for (_, (s, h)) in tasks {
        s.store(true, Ordering::SeqCst);
        let _ = h.join();
    }
    let _ = ansi;
    Ok(SuccessTuple::ok("Success"))
}
