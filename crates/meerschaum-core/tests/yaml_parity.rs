//! Parity: the lossless YAML codec behaves like released v4.1.4 (PyYAML) on the corpus in
//! `fixtures/yaml_corpus.py`, recorded by `fixtures/gen_yaml.py` into `fixtures/yaml_parity.json`.
//!
//! - `load` / `safe_load` build the same typed tree (or fail with the same exception class and
//!   message) for every YAML text.
//! - `dump` writes the same bytes for every typed value and `yaml.dump` keyword set.
//! - The config registry's `read_config` / `write_config` agree on the config cases that do not
//!   involve typed (non-JSON) values, which the registry's JSON-like `Value` cannot hold.

mod common;

use std::collections::HashMap;
use std::rc::Rc;

use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::yaml::dumper::{DumpOptions, dump_node};
use meerschaum_core::yaml::loader::{LoadOptions, load_text};
use meerschaum_core::yaml::node::{
    Date, DateTime, Node, NodeRef, dedup_entries, dedup_set, tagged_json,
};
use meerschaum_core::yaml::scanner::{ErrorKind, PyYamlError};
use num_bigint::BigInt;
use serde_json::Value as J;

fn fixture() -> J {
    let text = std::fs::read_to_string(common::fixtures().join("yaml_parity.json"))
        .expect("yaml_parity.json (run scripts/parity/run.sh crates/meerschaum-core/tests/fixtures/gen_yaml.py)");
    serde_json::from_str(&text).expect("valid fixture")
}

fn class_name(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Reader => "ReaderError",
        ErrorKind::Scanner => "ScannerError",
        ErrorKind::Parser => "ParserError",
        ErrorKind::Composer => "ComposerError",
        ErrorKind::Constructor => "ConstructorError",
        ErrorKind::Representer => "RepresenterError",
        ErrorKind::Emitter => "EmitterError",
        ErrorKind::Value => "ValueError",
        ErrorKind::Key => "KeyError",
        ErrorKind::Index => "IndexError",
        ErrorKind::Recursion => "RecursionError",
        ErrorKind::Attribute => "AttributeError",
    }
}

fn outcome(result: Result<NodeRef, PyYamlError>) -> J {
    match result {
        Ok(node) => serde_json::json!({"ok": tagged_json(&node)}),
        Err(e) => {
            // Python's `str(KeyError(key))` is the key's repr.
            let message = if e.kind == ErrorKind::Key {
                meerschaum_core::pyfmt::str_repr(&e.message)
            } else {
                e.message
            };
            serde_json::json!({"error": class_name(e.kind), "message": message})
        }
    }
}

/// Cases where the released `yaml.load` (PyYAML's full `Loader`) builds arbitrary Python objects
/// and the native codec refuses them like `SafeLoader` does (PARITY.md, "Wave 13").
const PYTHON_TAG_DEVIATIONS: &[&str] = &[
    "python_object_tag",
    "python_apply_tag",
    "python_name_tag",
    "python_module_tag",
    "python_str_tag",
    "python_complex_tag",
    "python_dict_tag",
    "python_list_tag",
];

#[test]
fn load_cases_match_pyyaml() {
    let fixture = fixture();
    let cases = fixture["load_cases"].as_array().unwrap();
    assert!(cases.len() > 200);
    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let text = case["text"].as_str().unwrap();
        for (label, safe) in [("load", false), ("safe_load", true)] {
            let expected = &case[label];
            if !safe && PYTHON_TAG_DEVIATIONS.contains(&name) {
                // The reference constructs a Python object (or fails to tag one); we raise the
                // SafeLoader error.
                let actual = outcome(load_text(
                    text,
                    "<unicode string>",
                    true,
                    LoadOptions {
                        safe: false,
                        project: false,
                    },
                ));
                assert_eq!(actual["error"], "ConstructorError", "{name}");
                assert!(
                    actual["message"]
                        .as_str()
                        .unwrap()
                        .starts_with("could not determine a constructor for the tag"),
                    "{name}"
                );
                continue;
            }
            let actual = outcome(load_text(
                text,
                "<unicode string>",
                true,
                LoadOptions {
                    safe,
                    project: false,
                },
            ));
            if &actual != expected {
                failures.push(format!(
                    "{name} [{label}]\n  expected {expected}\n  actual   {actual}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn from_tagged(spec: &J, shared: &mut HashMap<String, NodeRef>) -> NodeRef {
    let t = spec["t"].as_str().unwrap();
    if t == "shared" {
        let id = spec["id"].as_str().unwrap().to_string();
        if spec.get("v").is_some() {
            let node = from_tagged(&spec["v"], shared);
            shared.insert(id, node.clone());
            return node;
        }
        return shared[&id].clone();
    }
    let items = |spec: &J, shared: &mut HashMap<String, NodeRef>| -> Vec<NodeRef> {
        spec["v"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| from_tagged(i, shared))
            .collect()
    };
    Rc::new(match t {
        "none" => Node::Null,
        "bool" => Node::Bool(spec["v"].as_bool().unwrap()),
        "int" => Node::Int(spec["v"].as_str().unwrap().parse::<BigInt>().unwrap()),
        "float" => Node::Float(match spec["v"].as_str().unwrap() {
            "nan" => f64::NAN,
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            other => other.parse().unwrap(),
        }),
        "str" => Node::Str(spec["v"].as_str().unwrap().to_string()),
        "bytes" => {
            let hex = spec["v"].as_str().unwrap();
            Node::Bytes(
                (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                    .collect(),
            )
        }
        "date" => Node::Date(parse_date(spec["v"].as_str().unwrap())),
        "datetime" => Node::DateTime(parse_datetime(spec["v"].as_str().unwrap())),
        "list" => Node::Seq(items(spec, shared)),
        "tuple" => Node::Tuple(items(spec, shared)),
        "set" => Node::Set(dedup_set(items(spec, shared)).expect("hashable set items")),
        "dict" => {
            let mut entries = Vec::new();
            for pair in spec["v"].as_array().unwrap() {
                let k = from_tagged(&pair[0], shared);
                let v = from_tagged(&pair[1], shared);
                entries.push((k, v));
            }
            // A Python dict collapses colliding keys (`{1: a, True: b}`) on construction.
            Node::Map(dedup_entries(entries).expect("hashable dict keys"))
        }
        other => panic!("unknown tag {other}"),
    })
}

fn parse_date(s: &str) -> Date {
    let p: Vec<i32> = s.split('-').map(|x| x.parse().unwrap()).collect();
    Date {
        year: p[0],
        month: p[1] as u32,
        day: p[2] as u32,
    }
}

fn parse_datetime(s: &str) -> DateTime {
    let (date, rest) = s.split_once('T').unwrap();
    let (time, offset) = match rest.find(['+', '-']) {
        Some(i) => (&rest[..i], Some(&rest[i..])),
        None => (rest, None),
    };
    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, f),
        None => (time, ""),
    };
    let hp: Vec<u32> = hms.split(':').map(|x| x.parse().unwrap()).collect();
    let microsecond = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}")[..6].parse().unwrap()
    };
    let utc_offset_us = offset.map(|o| {
        let sign = if o.starts_with('-') { -1 } else { 1 };
        let body = &o[1..];
        let mut parts = body.split(':');
        let h: i64 = parts.next().unwrap().parse().unwrap();
        let m: i64 = parts.next().map_or(0, |x| x.parse().unwrap());
        sign * (h * 3600 + m * 60) * 1_000_000
    });
    DateTime {
        date: parse_date(date),
        hour: hp[0],
        minute: hp[1],
        second: hp[2],
        microsecond,
        utc_offset_us,
    }
}

fn dump_options(kwargs: &J) -> DumpOptions {
    let mut opts = DumpOptions::default();
    if let Some(map) = kwargs.as_object() {
        for (k, v) in map {
            match k.as_str() {
                "sort_keys" => opts.sort_keys = v.as_bool().unwrap(),
                "default_flow_style" => opts.default_flow_style = v.as_bool(),
                "indent" => opts.indent = v.as_u64().map(|x| x as usize),
                "width" => opts.width = v.as_u64().map(|x| x as usize),
                "allow_unicode" => opts.allow_unicode = v.as_bool().unwrap(),
                "default_style" => opts.default_style = v.as_str().and_then(|s| s.chars().next()),
                "explicit_start" => opts.explicit_start = v.as_bool().unwrap(),
                "explicit_end" => opts.explicit_end = v.as_bool().unwrap(),
                "line_break" => opts.line_break = v.as_str().map(String::from),
                other => panic!("unhandled dump keyword {other}"),
            }
        }
    }
    opts
}

#[test]
fn dump_cases_match_pyyaml() {
    let fixture = fixture();
    let cases = fixture["dump_cases"].as_array().unwrap();
    assert!(cases.len() > 3000);
    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let node = from_tagged(&case["data"], &mut HashMap::new());
        let actual = match dump_node(&node, &dump_options(&case["kwargs"])) {
            Ok(text) => serde_json::json!({"ok": text}),
            Err(e) => serde_json::json!({"error": class_name(e.kind), "message": e.message}),
        };
        if actual != case["expected"] {
            failures.push(format!(
                "{name} {}\n  expected {}\n  actual   {actual}",
                case["kwargs"], case["expected"]
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn dump_then_load_round_trips_typed_values() {
    // What the typed corpus dumps loads back to the same tree (YAML 1.1 round trip), except
    // the values PyYAML does not round trip (NaN keys, sets in safe mode, ...).
    let fixture = fixture();
    let mut checked = 0;
    for case in fixture["dump_cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        if name.contains("__") || !name.starts_with("str_value_") {
            continue;
        }
        let node = from_tagged(&case["data"], &mut HashMap::new());
        let text = dump_node(&node, &DumpOptions::default()).unwrap();
        let loaded = load_text(
            &text,
            "<unicode string>",
            true,
            LoadOptions {
                safe: false,
                project: false,
            },
        );
        let loaded = loaded.unwrap_or_else(|e| panic!("{name}: {e}\n{text}"));
        assert_eq!(tagged_json(&loaded), tagged_json(&node), "{name}\n{text}");
        checked += 1;
    }
    assert!(checked > 100);
}

// ---------------------------------------------------------------------------------------------
// The config registry on the config cases
// ---------------------------------------------------------------------------------------------

fn value_to_tagged(v: &meerschaum_core::Value) -> J {
    tagged_json(&meerschaum_core::yaml::dumper::node_from_value(v))
}

/// The cases the registry's JSON-like `Value` can express (no dates, bytes, tuples, big ints).
const REGISTRY_CASES: &[&str] = &[
    "preference_yaml_over_yml_json",
    "preference_yml_over_json",
    "preference_all_keys",
    "json_only",
    "substitute_false",
    "malformed_preserved",
    "malformed_json_preserved",
    "write_header_and_order",
    "write_key_order_kept",
    "write_multiline_string",
    "write_filetype_json",
    "write_keeps_existing_yml",
    "write_keeps_existing_json",
    "write_leaves_other_keys",
    "write_symlinks_restored",
    "write_nested_symlinks_restored",
    "write_not_allowed",
];

#[test]
fn registry_config_cases_match_python() {
    let fixture = fixture();
    // `serde_json` sorts object keys; the config written must keep the caller's key order.
    let ordered = common::load_fixture("yaml_parity.json");
    let ordered_cases = ordered
        .get("config_cases")
        .and_then(|c| c.as_list())
        .unwrap();
    let mut ran = 0;
    for (case_index, case) in fixture["config_cases"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let name = case["name"].as_str().unwrap();
        if !REGISTRY_CASES.contains(&name) {
            continue;
        }
        ran += 1;
        let tmp = tempfile::tempdir().unwrap();
        let tmp_path = tmp.path().canonicalize().unwrap();
        let root = tmp_path.join("root");
        std::fs::create_dir_all(&root).unwrap();
        let plugins = tmp_path.join("plugins");
        std::fs::create_dir_all(&plugins).unwrap();
        let config = Config::new(ConfigOptions {
            env: vec![
                (
                    "MRSM_ROOT_DIR".to_string(),
                    root.to_string_lossy().to_string(),
                ),
                (
                    "MRSM_PLUGINS_DIR".to_string(),
                    plugins.to_string_lossy().to_string(),
                ),
            ],
            home: tmp_path.clone(),
            windows: false,
            package_root: None,
            cpu_count: 4,
            allow_writes: !case["steps"]
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s["kw"]["allow_write_missing"].as_bool() == Some(false)),
        })
        .unwrap();
        // The cases run in the root's own (emptied) config directory.
        let dir = config.paths().config_dir;
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for (file, text) in case["setup"].as_object().unwrap() {
            std::fs::write(dir.join(file), text.as_str().unwrap()).unwrap();
        }
        let outcome = &case["outcome"];
        let expected_results = outcome["results"].as_array().unwrap();
        let ordered_steps = ordered_cases[case_index]
            .get("steps")
            .and_then(|c| c.as_list())
            .unwrap();
        for ((step, ordered_step), expected) in case["steps"]
            .as_array()
            .unwrap()
            .iter()
            .zip(ordered_steps)
            .zip(expected_results)
        {
            match step["op"].as_str().unwrap() {
                "read" => {
                    let kw = &step["kw"];
                    let keys: Option<Vec<String>> = step["keys"]
                        .as_array()
                        .map(|a| a.iter().map(|k| k.as_str().unwrap().to_string()).collect());
                    let (read, names) = config.with_state(|s| {
                        s.read_config(
                            Some(&dir),
                            keys.as_deref(),
                            kw["write_missing"].as_bool().unwrap_or(true),
                            kw["substitute"].as_bool().unwrap_or(true),
                            kw["raise_parsing_errors"].as_bool().unwrap_or(false),
                        )
                        .unwrap()
                    });
                    let mut got = serde_json::json!({
                        "config": value_to_tagged(&meerschaum_core::Value::Map(read)),
                    });
                    if kw["with_filenames"].as_bool().unwrap_or(false) {
                        got["filenames"] = serde_json::json!(names);
                    }
                    assert_eq!(&got, expected, "case {name} read");
                }
                "write" => {
                    let Some(meerschaum_core::Value::Map(map)) =
                        ordered_step.get("config").cloned()
                    else {
                        panic!("write config is a map")
                    };
                    let ok = config.with_state(|s| s.write_config(Some(&map), Some(&dir)));
                    assert_eq!(
                        serde_json::json!({"ok": ok}),
                        *expected,
                        "case {name} write"
                    );
                }
                other => panic!("unhandled step {other}"),
            }
        }
        // The files on disk match byte for byte.
        let expected_files = outcome["files"].as_object().unwrap();
        let mut actual_files: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        actual_files.sort();
        let mut expected_names: Vec<String> = expected_files.keys().cloned().collect();
        expected_names.sort();
        assert_eq!(actual_files, expected_names, "case {name} files");
        for (file, text) in expected_files {
            let actual = std::fs::read_to_string(dir.join(file)).unwrap();
            assert_eq!(actual, text.as_str().unwrap(), "case {name} file {file}");
        }
    }
    assert!(ran >= 15, "ran {ran} registry config cases");
}
