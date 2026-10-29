//! Formatting parity: every case in `tests/fixtures/format.json` (recorded from v4 by
//! `gen_format.py`) must render byte for byte the same in Rust.

use std::collections::BTreeMap;
use std::path::PathBuf;

use meerschaum_cli::format::rich::pretty::PyObj;
use meerschaum_cli::format::{
    Cell, FormatSettings, Formatter, Frame, OptionsArgs, PipeKeys, TupleArgs, format_dataframe,
};
use meerschaum_cli::shell;
use meerschaum_core::SuccessTuple;
use meerschaum_core::Value;
use meerschaum_core::config::{Config, ConfigOptions};

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format.json");
    // The core `Value` keeps the JSON object order (dict order matters for `pprint`).
    Value::from_json_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// A config registry with only defaults (an empty root, no writes).
pub fn default_config(root: &std::path::Path) -> Config {
    let mut opts = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
    ]);
    opts.allow_writes = false;
    opts.home = root.to_path_buf();
    Config::new(opts).unwrap()
}

fn obj(v: &Value) -> PyObj {
    PyObj::from_value(v)
}

fn idx(v: &Value, i: usize) -> &Value {
    &v.as_list().unwrap()[i]
}

fn tuple(v: &Value) -> SuccessTuple {
    SuccessTuple {
        success: idx(v, 0).as_bool().unwrap(),
        message: idx(v, 1).as_str().unwrap().to_string(),
    }
}

fn opt_usize(v: &Value, key: &str, default: usize) -> usize {
    v.get(key)
        .and_then(Value::as_i64)
        .map(|x| x as usize)
        .unwrap_or(default)
}

fn opt_bool(v: &Value, key: &str, default: bool) -> bool {
    v.get(key).and_then(Value::as_bool).unwrap_or(default)
}

fn strings(v: &Value) -> Vec<String> {
    v.as_list()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

fn pipe_keys(v: &Value) -> PipeKeys {
    PipeKeys {
        connector: v.get("connector_keys").unwrap().as_str().unwrap().into(),
        metric: v.get("metric_key").unwrap().as_str().unwrap().into(),
        location: v
            .get("location_key")
            .and_then(Value::as_str)
            .map(str::to_string),
        instance: v
            .get("instance_keys")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

fn frame(v: &Value) -> Frame {
    let cols = v.get("columns").unwrap().as_map().unwrap();
    Frame {
        columns: cols
            .iter()
            .map(|(name, values)| {
                let cells = values
                    .as_list()
                    .unwrap()
                    .iter()
                    .map(|c| match c {
                        Value::Null => Cell::Null,
                        Value::Bool(b) => Cell::Bool(*b),
                        Value::Str(s) => Cell::Str(s.clone()),
                        Value::Int(i) => Cell::Int(*i),
                        Value::Float(f) => Cell::Float(*f),
                        _ => panic!("unsupported cell"),
                    })
                    .collect();
                (name.clone(), cells)
            })
            .collect(),
    }
}

fn render(f: &Formatter<'_>, case: &Value, version: &str) -> Option<String> {
    let args = case.get("args").unwrap();
    let a = |k: &str| args.get(k).unwrap();
    Some(match case.get("fn").unwrap().as_str().unwrap() {
        "print_tuple" => f.print_tuple(
            &tuple(a("tuple")),
            TupleArgs {
                skip_common: opt_bool(args, "skip_common", true),
                common_only: opt_bool(args, "common_only", false),
                upper_padding: opt_usize(args, "upper_padding", 1),
                lower_padding: opt_usize(args, "lower_padding", 1),
                calm: opt_bool(args, "calm", false),
            },
        ),
        "format_success_tuple" => f.format_success_tuple(
            &tuple(a("tuple")),
            opt_usize(args, "upper_padding", 0),
            opt_usize(args, "lower_padding", 0),
            opt_usize(args, "left_padding", 1),
            false,
        ),
        "print_options" => f.print_options(
            &strings(a("options")),
            &OptionsArgs {
                nopretty: opt_bool(args, "nopretty", false),
                name: args
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("options")
                    .to_string(),
                header: args
                    .get("header")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                num_cols: args
                    .get("num_cols")
                    .and_then(Value::as_i64)
                    .map(|x| x as usize),
                adjust_cols: opt_bool(args, "adjust_cols", true),
                sort_options: opt_bool(args, "sort_options", false),
                number_options: opt_bool(args, "number_options", false),
            },
        ),
        "pprint" => f.pprint(&obj(a("value")), opt_bool(args, "nopretty", false)),
        "pprint_tuple" => {
            let t = tuple(a("tuple"));
            f.pprint(
                &PyObj::Tuple(vec![PyObj::Bool(t.success), PyObj::Str(t.message)]),
                false,
            )
        }
        "make_header" => f.make_header_with(
            a("message").as_str().unwrap(),
            args.get("ruler").and_then(Value::as_str).unwrap_or("─"),
            opt_usize(args, "left_pad", 2),
            opt_bool(args, "top", false),
            1,
        ),
        "highlight_pipes" => f.highlight_pipes(a("message").as_str().unwrap()),
        "pipe_repr" => f.pipe_repr(&pipe_keys(a("pipe"))),
        "fill_ansi" => f.fill_ansi(a("string").as_str().unwrap(), a("style").as_str().unwrap()),
        "colored" => f.colored(a("text").as_str().unwrap(), a("style").as_str().unwrap()),
        "pprint_pipes" => {
            let pipes: Vec<PipeKeys> = a("pipes")
                .as_list()
                .unwrap()
                .iter()
                .map(|p| PipeKeys {
                    connector: idx(p, 0).as_str().unwrap().into(),
                    metric: idx(p, 1).as_str().unwrap().into(),
                    location: idx(p, 2).as_str().map(str::to_string),
                    instance: idx(p, 3).as_str().map(str::to_string),
                })
                .collect();
            f.pprint_pipes(&pipes)
        }
        "format_dataframe" => format_dataframe(
            &frame(args),
            args.get("max_rows")
                .and_then(Value::as_i64)
                .map(|x| x as usize),
        ),
        "shell_intro" => shell::shell_intro(f, opt_bool(args, "with_color", true), version),
        "shell_prompt" => {
            let instance = a("instance").as_str().unwrap();
            let project = args.get("compose_project").and_then(Value::as_str);
            shell::render_prompt(f, &shell::PromptState::new(f, instance, project))
        }
        _ => return None,
    })
}

#[test]
fn formatting_matches_python() {
    let fixture = fixture();
    let version = fixture
        .get("version")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let tmp = tempfile::tempdir().unwrap();
    let config = default_config(tmp.path());
    let mut by_fn: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let mut failures: Vec<String> = Vec::new();
    let only = std::env::var("FORMAT_ONLY").ok();
    for case in fixture.get("cases").unwrap().as_list().unwrap() {
        let name = case.get("fn").unwrap().as_str().unwrap().to_string();
        if only.as_deref().is_some_and(|o| o != name) {
            continue;
        }
        let width = case.get("width").unwrap().as_i64().unwrap() as usize;
        let ansi = case.get("ansi").unwrap().as_bool().unwrap();
        let unicode = case.get("unicode").unwrap().as_bool().unwrap();
        let settings = FormatSettings {
            ansi,
            unicode,
            width,
            cols: width,
            no_color: false,
        };
        let f = Formatter::new(settings, &config);
        let expected = case.get("output").unwrap().as_str().unwrap();
        let entry = by_fn.entry(name.clone()).or_default();
        entry.0 += 1;
        let Some(actual) = render(&f, case, &version) else {
            failures.push(format!("{name}: not implemented"));
            continue;
        };
        if actual == expected {
            entry.1 += 1;
        } else if failures.len() < 12 {
            failures.push(format!(
                "{name} (width={width}, ansi={ansi}, unicode={unicode}) args={}\n--- python\n{expected:?}\n--- rust\n{actual:?}",
                case.get("args").unwrap().to_json_string()
            ));
        } else {
            failures.push(String::new());
        }
    }
    let total: usize = by_fn.values().map(|v| v.0).sum();
    let matched: usize = by_fn.values().map(|v| v.1).sum();
    for (name, (n, ok)) in &by_fn {
        eprintln!("{name:>22}: {ok}/{n}");
    }
    eprintln!("formatting parity: {matched}/{total} cases match byte for byte");
    let shown: Vec<&String> = failures.iter().filter(|f| !f.is_empty()).collect();
    assert!(
        failures.is_empty(),
        "{} mismatches:\n\n{}",
        failures.len(),
        shown
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n\n")
    );
}
