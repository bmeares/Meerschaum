//! Unit tests for single-file Rust plugin generation and embedded runtime.

use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty `Vec` is zero rows (`[]` on the rows ABI); only an `Option` input's `None` is
    /// no data (`null`), as v4 tells a `fetch()` returning `[]` from one returning `None`.
    #[test]
    fn runtime_empty_rows_are_zero_rows_and_none_is_no_data() {
        use runtime::sdk::FetchOutput;
        let empty = Vec::<Value>::new().into_frame().unwrap().unwrap();
        assert_eq!(empty.height(), 0);
        let some = Some(Vec::<Value>::new()).into_frame().unwrap();
        assert_eq!(some.map(|f| f.height()), Some(0));
        assert!(None::<Vec<Value>>.into_frame().unwrap().is_none());
        assert!(None::<runtime::Frame>.into_frame().unwrap().is_none());
        assert!(Some(vec![json!(1)]).into_frame().is_err());
    }

    #[test]
    fn dependency_blocks_accept_cargo_forms_and_ignore_code_strings() {
        let parsed = syn::parse_file(
            r#"//! ```cargo
//! [dependencies]
//! regex = "1"
//! uuid = { version = "1", features = ["v4"], default-features = false }
//! ```
fn helper() {}
"#,
        )
        .unwrap();
        let deps = dependencies(&parsed, Path::new("example.rs")).unwrap();
        assert_eq!(deps["regex"].as_str(), Some("1"));
        assert_eq!(deps["uuid"]["features"][0].as_str(), Some("v4"));
        let parsed = syn::parse_file(
            "const EXAMPLE: &str = r#\"\n//! ```cargo\n//! bad = true\n//! ```\n\"#;",
        )
        .unwrap();
        assert!(
            dependencies(&parsed, Path::new("example.rs"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn invalid_dependency_blocks_fail_before_building() {
        for (block, expected) in [
            ("//! ```cargo\n//! [dependencies]", "unclosed"),
            (
                "//! ```cargo\n//! [dependencies]\n//! x = \n//! ```",
                "invalid Cargo",
            ),
            (
                "//! ```cargo\n//! [package]\n//! name = 'oops'\n//! ```",
                "only [dependencies]",
            ),
            (
                "//! ```cargo\n//! [dependencies]\n//! x = true\n//! ```",
                "version string",
            ),
            (
                "//! ```cargo\n//! [dependencies]\n//! x = { workspace = true }\n//! ```",
                "workspace or optional",
            ),
            (
                "//! ```cargo\n//! [dependencies]\n//! meerschaum-plugin = '5'\n//! ```",
                "supplied by Meerschaum",
            ),
            ("//! ```cargo\n//! ```\n//! ```cargo\n//! ```", "only one"),
        ] {
            let parsed = syn::parse_file(block).unwrap();
            let err = dependencies(&parsed, Path::new("example.rs"))
                .unwrap_err()
                .to_string();
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn transformed_source_preserves_lines_and_only_strips_shorthand_attributes() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("sample.rs");
        let source = "//! Introductory notes.\n#[make_action(\n name = \"greet\"\n)]\nfn hello() {}\n#[make_connector]\nmod readings {\n    #![allow(dead_code)]\n    fn fetch() -> Vec<Value> { vec![] }\n}\n";
        std::fs::write(&file, source).unwrap();
        let output = root.path().join("generated");
        prepare(&file, "sample", &output).unwrap();
        let transformed = std::fs::read_to_string(output.join("user.rs")).unwrap();
        assert!(!transformed.contains("#[make_action"));
        assert!(!transformed.contains("#[make_connector"));
        assert!(transformed.lines().nth(4).unwrap().starts_with("fn hello"));
        assert!(transformed.lines().nth(8).unwrap().contains("fn fetch"));
        assert!(syn::parse_file(&transformed).is_ok());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), source);
    }
    #[test]
    fn generated_source_keeps_doc_comment_text_inside_raw_strings() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("sample.rs");
        let source = r##"//! Actual file documentation.
#[make_action]
fn hello() {
    let example = r#"
//! ```cargo
//! this is an example, not a dependency
//! ```
"#;
    assert!(example.contains("//! this is an example"));
}
"##;
        std::fs::write(&file, source).unwrap();
        let output = root.path().join("generated");
        prepare(&file, "sample", &output).unwrap();
        let transformed = std::fs::read_to_string(output.join("user.rs")).unwrap();
        assert!(transformed.contains("//! this is an example, not a dependency"));
        assert!(transformed.starts_with("//  Actual file documentation."));
    }

    #[test]
    fn custom_connector_methods_work_without_fetch_or_sync() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("kuma.rs");
        std::fs::write(
            &file,
            r#"
#[make_connector]
mod kuma {
    #[method]
    pub fn push(connector: &Connector, kwargs: &Value) -> Result<SuccessTuple> {
        Ok(SuccessTuple::ok(format!("{}:{}", connector.label(), kwargs["status"])))
    }
}
"#,
        )
        .unwrap();
        let output = dir.path().join("generated");
        prepare(&file, "kuma", &output).unwrap();
        let user_rs = std::fs::read_to_string(output.join("user.rs")).unwrap();
        let lib_rs = std::fs::read_to_string(output.join("lib.rs")).unwrap();
        assert!(!user_rs.contains("#[method]"));
        assert!(syn::parse_file(&user_rs).is_ok());
        assert!(lib_rs.contains(r#""push" =>"#));
        assert!(lib_rs.contains(r#"\"methods\":[\"push\"]"#) || lib_rs.contains(r#""push""#));
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::runtime::{self, executor::ExecutorReply};
    use serde_json::{Value, json};

    #[test]
    fn bounds_convert_iso_timestamps_without_chrono() {
        use runtime::Bound;
        let at = |text: &str| Bound::from(text).timestamp();
        assert_eq!(at("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(at("2024-03-01T12:30:00+02:00").unwrap(), 1_709_289_000);
        assert_eq!(at("2024-03-01 10:30:00").unwrap(), 1_709_289_000);
        assert_eq!(
            at("2024-03-01T10:30:00.250000+00:00").unwrap(),
            1_709_289_000
        );
        assert_eq!(
            Bound::from("2024-03-01T10:30:00.250Z")
                .timestamp_millis()
                .unwrap(),
            1_709_289_000_250
        );
        assert_eq!(at("2024-03-01").unwrap(), 1_709_251_200);
        assert_eq!(at("1969-12-31T23:59:59-0000").unwrap(), -1);
        for bad in ["soon", "2024-13-01", "2024-01-01T10:30:00+5", ""] {
            assert!(at(bad).is_err(), "{bad}");
        }
        assert!(Bound::Int(3).timestamp().is_err());
        assert_eq!(Bound::Int(3).as_int(), Some(3));
        assert_eq!(
            Bound::from_timestamp(1_709_289_000),
            Bound::Datetime("2024-03-01T10:30:00+00:00".into())
        );
        assert_eq!(
            Bound::from_timestamp(-1).to_string(),
            "1969-12-31T23:59:59+00:00"
        );
        for seconds in [0, 951_782_400, 1_709_289_000, 4_102_444_800, -86_400 * 366] {
            assert_eq!(Bound::from_timestamp(seconds).timestamp().unwrap(), seconds);
        }
    }

    #[test]
    fn base64_round_trips_every_length() {
        for n in 0..16 {
            let bytes: Vec<u8> = (0..n).map(|i| (i * 37 + 200) as u8).collect();
            let text = runtime::base64::encode(&bytes);
            assert_eq!(text.len() % 4, 0);
            assert_eq!(runtime::base64::decode(&text).unwrap(), bytes);
        }
        assert_eq!(runtime::base64::encode(&[0, 159, 146, 150]), "AJ+Slg==");
        assert!(runtime::base64::decode("a*b").is_err());
    }

    fn request(method: &str, context: Value) -> runtime::Request {
        runtime::Request {
            method: format!("executor.{method}"),
            connector_type: "memory".into(),
            label: "x".into(),
            config: Value::Null,
            context,
            kwargs: json!({}),
        }
    }

    fn methods(
        method: &str,
        _: &runtime::executor::ExecutorInfo,
        context: &Value,
    ) -> runtime::Result<ExecutorReply> {
        match method {
            "get_job_names" => Ok(ExecutorReply::Value(json!(["a", "b"]))),
            "get_job_metadata" if context["name"] == "a" => Ok(ExecutorReply::Value(
                json!({"daemon": {"status": "paused", "properties": {"process": {"ended": "e"}}}}),
            )),
            "get_job_metadata" => Ok(ExecutorReply::Value(Value::Null)),
            other => Err(runtime::executor::unsupported(other)),
        }
    }

    #[test]
    fn executor_defaults_derive_from_names_and_metadata() {
        let call = |method: &str, context: Value| {
            runtime::executor::dispatch(&request(method, context), methods).map(|r| r.value)
        };
        assert_eq!(
            call("get_job_exists", json!({"name": "b"})).unwrap(),
            json!(true)
        );
        assert_eq!(
            call("get_job_status", json!({"name": "a"})).unwrap(),
            json!("paused")
        );
        assert_eq!(
            call("get_job_status", json!({"name": "b"})).unwrap(),
            json!("stopped")
        );
        assert_eq!(
            call("get_job_ended", json!({"name": "a"})).unwrap(),
            json!("e")
        );
        assert_eq!(
            call("get_jobs", json!({})).unwrap(),
            json!({"a": {"daemon": {"status": "paused", "properties": {"process": {"ended": "e"}}}}})
        );
        assert!(matches!(
            call("stop_job", json!({"name": "a"})),
            Err(runtime::Error::Unsupported(m)) if m == "`stop_job` is not supported by this executor."
        ));
        assert!(matches!(
            call("monitor_logs", json!({"name": "a"})),
            Err(runtime::Error::Unsupported(_))
        ));
    }
}
