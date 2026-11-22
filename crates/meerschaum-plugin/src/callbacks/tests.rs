//! Tests of the host calls: arguments, frames and pipes round-trip between a plugin and its host,
//! and older hosts answer the newer calls with the release that added them.

use super::*;
use serde_json::json;

#[test]
fn validates_the_host_prefix_before_reading_callbacks() {
    // SAFETY: `enter` checks for null before reading anything.
    assert!(unsafe { enter(std::ptr::null()) }.is_err());
    for prefix in [[99_u32, 8], [1, 8]] {
        // A short prefix is all that may be inspected when its version/size is wrong.
        // SAFETY: `prefix` is a live 8-byte, u32-aligned array, which is exactly the
        // two-u32 prefix `enter` reads before refusing a wrong version or a short size.
        assert!(unsafe { enter(prefix.as_ptr().cast()) }.is_err());
    }
}

fn request() -> Request {
    Request {
        method: "fetch".into(),
        connector_type: "plugin".into(),
        label: "test".into(),
        config: Value::Null,
        context: json!({"pipe": {"keys": {"connector_keys": "plugin:test", "metric_key": "test", "instance_keys": "sql:local"}, "parameters": {"columns": {"datetime": "ts"}}}}),
    }
}

/// Run `f` inside a host-enabled call of `services` for `request`.
fn with_host<T>(services: &dyn HostServices, request: &Request, f: impl FnOnce() -> T) -> T {
    let mut context = HostContext {
        services,
        request,
        plugin: "test",
    };
    let table = HostV1 {
        abi_version: 1,
        struct_size: std::mem::size_of::<HostV1>() as u32,
        context: (&mut context as *mut HostContext<'_>).cast(),
        invoke: invoke_host,
        free_bytes: sdk::free_bytes,
    };
    // SAFETY: `table` is a complete v1 table whose `context` points at the live `context`
    // above; both outlive `_scope`, which is declared after them and so dropped first.
    let _scope = unsafe { enter(&table) }.unwrap();
    f()
}

#[test]
fn pipe_views_cannot_reuse_callback_state_after_the_invocation() {
    struct Host;
    impl HostServices for Host {
        fn call(&self, _: &Request, _: &str, _: Option<Frame>) -> Result<Value> {
            Ok(json!(7))
        }
    }
    let request = request();
    let mut context = HostContext {
        services: &Host,
        request: &request,
        plugin: "test",
    };
    let table = HostV1 {
        abi_version: 1,
        struct_size: std::mem::size_of::<HostV1>() as u32,
        context: (&mut context as *mut HostContext<'_>).cast(),
        invoke: invoke_host,
        free_bytes: sdk::free_bytes,
    };
    // SAFETY: `table` and the `context` it points to are locals that outlive `scope`
    // (dropped explicitly below).
    let scope = unsafe { enter(&table) }.unwrap();
    let pipe = request.pipe().unwrap();
    assert_eq!(pipe.get_sync_time().unwrap(), Some(Bound::Int(7)));
    drop(scope);
    assert!(pipe.get_sync_time().is_err());
    assert!(crate::host::plugin_config().is_err());
    // SAFETY: as above; `_next` is declared after `table` and `context`, so it is dropped
    // before them.
    let _next = unsafe { enter(&table) }.unwrap();
    // Even if the host context has exactly the same address, a prior view is expired.
    assert!(pipe.get_sync_time().is_err());
    assert_eq!(
        request.pipe().unwrap().get_sync_time().unwrap(),
        Some(Bound::Int(7))
    );
}

/// A host that records every call and answers like the CLI host would.
#[derive(Default)]
struct Recorder {
    calls: std::sync::Mutex<Vec<HostCall>>,
}
impl HostServices for Recorder {
    fn call(&self, _: &Request, _: &str, _: Option<Frame>) -> Result<Value> {
        unreachable!("every call goes through host_call")
    }
    fn host_call(
        &self,
        _: &Request,
        call: &HostCall,
        data: Option<PipeInput>,
    ) -> Result<HostReply> {
        self.calls.lock().unwrap().push(call.clone());
        let rows =
            |rows: Vec<Value>| sdk::docs_to_frame(&sdk::rows_to_docs(rows).unwrap()).unwrap();
        Ok(match call.method.as_str() {
            "get_data" | "read_sql" => HostReply::data(Some(rows(vec![
                json!({"id": 1, "label": "a"}),
                json!({"id": 2, "label": "b"}),
            ]))),
            "sync" => {
                let height = match data {
                    Some(input) => crate::host::input_frame(input)?.height(),
                    None => 0,
                };
                HostReply::json(json!([true, format!("synced {height}")]))
            }
            "get_rowcount" => HostReply::json(json!(2)),
            "clear" | "exec_sql" | "write_plugin_config" | "register" => {
                HostReply::json(json!([true, call.method]))
            }
            "get_columns_types" => HostReply::json(json!({"id": "BIGINT", "label": "TEXT"})),
            "exists" => HostReply::json(json!(true)),
            "plugin_config" => HostReply::json(json!({"multiplier": 3})),
            "config" => HostReply::json(json!(call.arg("keys"))),
            "pipe" => HostReply::json(json!({
                "keys": {"connector_keys": call.arg("connector_keys"), "metric_key": call.arg("metric_key"),
                    "location_key": call.arg("location_key"), "instance_keys": "sql:other"},
                "parameters": {"columns": {"id": "id"}}, "temporary": false,
            })),
            "get_sync_time" => HostReply::json(json!("2024-01-01T00:00:00+00:00")),
            "mint_token" => HostReply::json(json!({
                "id": "00000000-0000-4000-8000-000000000001", "label": call.arg("label"),
                "scopes": call.arg("scopes"),
                "expiration": (!call.arg("expiration").is_null()).then_some("2030-03-17T17:46:40Z"),
                "secret": "synthetic-secret", "api_key": "mrsm-key:synthetic",
            })),
            "revoke_token" => HostReply::json(json!([true, call.arg("id")])),
            "draining" => HostReply::json(json!(true)),
            other => return Err(crate::Error::Unsupported(other.into())),
        })
    }
}

#[test]
fn host_calls_round_trip_arguments_frames_and_pipes() {
    let host = Recorder::default();
    let request = request();
    with_host(&host, &request, || {
        let own = request.pipe().unwrap();
        assert_eq!(own.datetime_column(), Some("ts"));
        let frame = own
            .get_data(Some(Bound::Int(1)), None, Some(&json!({"id": [1, 2]})))
            .unwrap();
        assert_eq!(frame.height(), 2);
        assert_eq!(frame.names(), ["id", "label"]);
        assert_eq!(own.get_rowcount(None, None, None).unwrap(), 2);
        assert!(
            own.clear(Some(Bound::Int(0)), Some(Bound::Int(9)), None)
                .unwrap()
                .success
        );
        assert_eq!(own.get_columns_types().unwrap()["label"], "TEXT");
        assert!(own.exists().unwrap());
        let other = crate::host::pipe("plugin:noaa", "weather", Some("atl"), None).unwrap();
        assert_eq!(other.instance_keys(), "sql:other");
        assert_eq!(other.location_key(), Some("atl"));
        assert_eq!(
            other.to_string(),
            "Pipe('plugin:noaa', 'weather', 'atl', instance='sql:other')"
        );
        assert_eq!(
            other.sync(vec![json!({"id": 1})]).unwrap().message,
            "synced 1"
        );
        assert_eq!(
            other
                .get_sync_time()
                .unwrap()
                .unwrap()
                .to_datetime()
                .unwrap()
                .to_rfc3339(),
            "2024-01-01T00:00:00+00:00"
        );
        let sql = crate::host::read_sql("sql:main", "SELECT 1").unwrap();
        assert_eq!(sql.height(), 2);
        assert!(
            crate::host::exec_sql("sql:main", "DELETE FROM t")
                .unwrap()
                .success
        );
        assert_eq!(crate::host::plugin_config().unwrap()["multiplier"], 3);
        assert!(
            crate::host::write_plugin_config(&json!({"a": 1}))
                .unwrap()
                .success
        );
        assert_eq!(
            crate::host::config(&["meerschaum", "instance"]).unwrap(),
            json!(["meerschaum", "instance"])
        );
        let registered = other
            .clone()
            .with_parameters(json!({"columns": {"datetime": "ts"}}));
        assert!(registered.register().unwrap().success);
        assert!(crate::host::available());
    });
    let calls = host.calls.lock().unwrap();
    let names: Vec<&str> = calls.iter().map(|c| c.method.as_str()).collect();
    assert_eq!(
        names,
        [
            "get_data",
            "get_rowcount",
            "clear",
            "get_columns_types",
            "exists",
            "pipe",
            "sync",
            "get_sync_time",
            "read_sql",
            "exec_sql",
            "plugin_config",
            "write_plugin_config",
            "config",
            "register"
        ]
    );
    // The request's Pipe travels as `null`; another Pipe as its metadata.
    assert!(calls[0].pipe.is_none());
    assert_eq!(calls[0].arg("begin"), &json!(1));
    assert_eq!(calls[0].arg("params"), &json!({"id": [1, 2]}));
    assert_eq!(calls[2].arg("end"), &json!(9));
    assert_eq!(
        calls[6].pipe.as_ref().unwrap()["keys"]["metric_key"],
        "weather"
    );
    assert_eq!(
        calls[13].pipe.as_ref().unwrap()["parameters"]["columns"]["datetime"],
        "ts"
    );
    // The loader names the plugin; a plugin cannot claim another plugin's config.
    assert!(calls.iter().all(|c| c.plugin == "test"));
    assert!(!crate::host::available());
}

#[test]
fn hosts_without_the_new_calls_answer_with_an_upgrade_message() {
    /// A host from before the new calls: only `get_sync_time` and `sync`.
    struct Old;
    impl HostServices for Old {
        fn call(&self, _: &Request, method: &str, _: Option<Frame>) -> Result<Value> {
            match method {
                "get_sync_time" => Ok(json!(3)),
                other => Err(crate::Error::Unsupported(format!(
                    "unsupported Pipe host operation: {other}"
                ))),
            }
        }
    }
    let request = request();
    with_host(&Old, &request, || {
        let pipe = request.pipe().unwrap();
        // The original call still works on the original wire.
        assert_eq!(pipe.get_sync_time().unwrap(), Some(Bound::Int(3)));
        let error = pipe.get_data(None, None, None).unwrap_err().to_string();
        assert!(error.contains("`pipe.get_data()`"), "{error}");
        assert!(error.contains("upgrade Meerschaum"), "{error}");
        let error = crate::host::read_sql("sql:main", "SELECT 1")
            .unwrap_err()
            .to_string();
        assert!(error.contains("`host::read_sql()`"), "{error}");
        // A filtered sync time is a new call.
        assert!(
            pipe.get_sync_time_with(Some(&json!({"id": 1})), true)
                .is_err()
        );
    });
}

#[test]
fn token_and_drain_calls_round_trip_and_hide_the_secret() {
    let host = Recorder::default();
    let request = request();
    with_host(&host, &request, || {
        let minted = crate::host::mint_token(
            "device-17",
            &["devices:ingest"],
            Some(Bound::Int(1_900_000_000)),
        )
        .unwrap();
        assert_eq!(minted.label, "device-17");
        assert_eq!(minted.scopes, ["devices:ingest"]);
        assert_eq!(minted.api_key, "mrsm-key:synthetic");
        assert_eq!(minted.expiration.as_deref(), Some("2030-03-17T17:46:40Z"));
        let debug = format!("{minted:?}");
        assert!(!debug.contains("synthetic"), "{debug}");
        let never = crate::host::mint_token("device-18", &["devices:ingest"], None).unwrap();
        assert_eq!(never.expiration, None);
        let revoked = crate::host::revoke_token(&minted.id).unwrap();
        assert!(revoked.success);
        assert_eq!(revoked.message, minted.id);
        assert!(crate::host::draining().unwrap());
    });
    let calls = host.calls.lock().unwrap();
    let names: Vec<&str> = calls.iter().map(|c| c.method.as_str()).collect();
    assert_eq!(
        names,
        ["mint_token", "mint_token", "revoke_token", "draining"]
    );
    assert_eq!(calls[0].arg("expiration"), &json!(1_900_000_000));
    assert_eq!(calls[1].arg("expiration"), &Value::Null);
    assert!(calls.iter().all(|c| c.plugin == "test" && c.pipe.is_none()));
    // Outside a plugin call they explain why they cannot run.
    let error = crate::host::draining().unwrap_err().to_string();
    assert!(error.contains("host calls are unavailable"), "{error}");
}

#[test]
fn older_hosts_name_the_release_with_the_token_and_drain_calls() {
    struct Old;
    impl HostServices for Old {
        fn call(&self, _: &Request, method: &str, _: Option<Frame>) -> Result<Value> {
            Err(crate::Error::Unsupported(format!(
                "unsupported Pipe host operation: {method}"
            )))
        }
    }
    let request = request();
    with_host(&Old, &request, || {
        for (name, error) in [
            (
                "mint_token",
                crate::host::mint_token("d", &["devices:ingest"], None).unwrap_err(),
            ),
            (
                "revoke_token",
                crate::host::revoke_token("00000000-0000-4000-8000-000000000001").unwrap_err(),
            ),
            ("draining", crate::host::draining().unwrap_err()),
        ] {
            let error = error.to_string();
            assert!(error.contains(&format!("`host::{name}()`")), "{error}");
            assert!(
                error.contains("upgrade Meerschaum to 5.0.0rc34 or later"),
                "{error}"
            );
        }
        // The earlier calls keep naming the release that brought them.
        let error = crate::host::plugin_config().unwrap_err().to_string();
        assert!(error.contains("5.0.0rc15"), "{error}");
    });
}

#[test]
fn envelopes_decode_and_bare_names_stay_legacy() {
    let legacy = HostCall::decode("p", b"sync").unwrap();
    assert!(legacy.is_legacy());
    let call = HostCall::decode(
        "p",
        br#"{"method": "clear", "pipe": null, "args": {"begin": 1, "end": ""}}"#,
    )
    .unwrap();
    assert!(!call.is_legacy());
    assert_eq!(call.bound("begin").unwrap(), Some(Bound::Int(1)));
    assert_eq!(call.bound("end").unwrap(), None);
    assert!(HostCall::decode("p", br#"{"args": {}}"#).is_err());
    assert!(HostCall::decode("p", br#"{"method": "x", "args": [1]}"#).is_err());
}

#[test]
fn bounds_convert_to_and_from_chrono_and_integers() {
    let dt = chrono::DateTime::parse_from_rfc3339("2024-03-01T12:30:00+02:00")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let bound = Bound::from(dt);
    assert_eq!(bound, Bound::Datetime("2024-03-01T10:30:00+00:00".into()));
    assert_eq!(bound.to_datetime().unwrap(), dt);
    // A timestamp without an offset is UTC; the host's space-separated form parses too.
    assert_eq!(
        Bound::from("2024-03-01 10:30:00").to_datetime().unwrap(),
        dt
    );
    assert_eq!(Bound::from(5_i64).as_int(), Some(5));
    assert!(Bound::Int(5).to_datetime().is_err());
    assert!(Bound::from("soon").to_datetime().is_err());
    assert_eq!(Bound::Int(5).to_string(), "5");
    assert_eq!(
        Query::new().begin(1).limit(2).order("desc").args()["order"],
        "desc"
    );
    assert_eq!(
        bound.to_sql_param(),
        json!({"$mrsm_sql": "timestamptz", "value": "2024-03-01T10:30:00+00:00"})
    );
    assert_eq!(Bound::Int(5).to_sql_param(), json!(5));
    assert_eq!(
        crate::host::sql_bytes(b"hello"),
        json!({"$mrsm_sql": "bytes", "value": "aGVsbG8="})
    );
}

#[test]
fn v5_1_host_calls_round_trip_sql_params_root_dir_pipe_id_and_transactions() {
    use std::sync::Mutex;
    struct RecordingHost {
        calls: Mutex<Vec<HostCall>>,
        next_tx: Mutex<u64>,
        pipe_id: Mutex<Value>,
    }
    impl HostServices for RecordingHost {
        fn call(&self, _: &Request, _: &str, _: Option<Frame>) -> Result<Value> {
            unreachable!()
        }
        fn host_call(
            &self,
            _: &Request,
            call: &HostCall,
            _: Option<PipeInput>,
        ) -> Result<HostReply> {
            self.calls.lock().unwrap().push(call.clone());
            match call.method.as_str() {
                method::ROOT_DIR => Ok(HostReply::json(json!("/srv/sra/root"))),
                method::GET_PIPE_ID => Ok(HostReply::json(self.pipe_id.lock().unwrap().clone())),
                method::IS_REGISTERED => {
                    let reg = !self.pipe_id.lock().unwrap().is_null();
                    Ok(HostReply::json(json!(reg)))
                }
                method::READ_SQL_WITH | method::TX_READ_SQL => {
                    Ok(HostReply::data(Some(Frame::empty())))
                }
                method::EXEC_SQL_WITH | method::TX_EXEC_SQL => {
                    Ok(HostReply::json(json!([true, "ok"])))
                }
                method::TX_BEGIN => {
                    let mut id = self.next_tx.lock().unwrap();
                    *id += 1;
                    Ok(HostReply::json(json!({"tx_id": *id})))
                }
                method::TX_COMMIT | method::TX_ROLLBACK => {
                    Ok(HostReply::json(json!([true, "done"])))
                }
                other => Err(crate::Error::Unsupported(other.into())),
            }
        }
    }
    let host = RecordingHost {
        calls: Mutex::new(Vec::new()),
        next_tx: Mutex::new(0),
        pipe_id: Mutex::new(Value::Null),
    };
    let request = request();
    with_host(&host, &request, || {
        assert_eq!(
            crate::host::root_dir().unwrap(),
            std::path::PathBuf::from("/srv/sra/root")
        );
        let pipe = request.pipe().unwrap();
        assert_eq!(pipe.id().unwrap(), None);
        assert!(!pipe.is_registered().unwrap());
        *host.pipe_id.lock().unwrap() = json!(42);
        assert_eq!(pipe.id().unwrap(), Some(42));
        assert!(pipe.is_registered().unwrap());

        let params = json!({
            "ids": ["a", "b"],
            "begin": crate::host::sql_timestamptz("2026-09-25T17:00:00+00:00"),
        });
        let _ = crate::host::read_sql_with("sql:main", "SELECT :ids, :begin", &params).unwrap();
        assert!(
            crate::host::exec_sql_with("sql:main", "DELETE FROM t WHERE id = ANY(:ids)", &params)
                .unwrap()
                .success
        );

        // Commit on Ok.
        let committed = crate::host::transaction("sql:main", Some("cmdq:c1"), |tx| {
            let _ = tx.read_sql_with("SELECT 1 WHERE id = :id", &json!({"id": 1}))?;
            let res = tx.exec_sql_with("INSERT INTO q VALUES (:id)", &json!({"id": 2}))?;
            assert!(res.success);
            Ok(99_i64)
        })
        .unwrap();
        assert_eq!(committed, 99);

        // Rollback on Err.
        let err = crate::host::transaction::<(), _>("sql:main", None, |tx| {
            let _ = tx.exec_sql("INSERT INTO q VALUES (3)")?;
            Err(error("boom"))
        })
        .unwrap_err();
        assert!(err.to_string().contains("boom"));

        // Rollback automatically on panic unwind via RAII Drop.
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = crate::host::transaction::<(), _>("sql:main", Some("cmdq:c2"), |tx| {
                let _ = tx.exec_sql("INSERT INTO q VALUES (4)")?;
                panic!("panic inside tx");
            });
        }));
        assert!(panicked.is_err());
    });

    let calls = host.calls.lock().unwrap();
    let names: Vec<&str> = calls.iter().map(|c| c.method.as_str()).collect();
    assert_eq!(
        names,
        [
            "root_dir",
            "get_pipe_id",
            "is_registered",
            "get_pipe_id",
            "is_registered",
            "read_sql_with",
            "exec_sql_with",
            "tx_begin",
            "tx_read_sql",
            "tx_exec_sql",
            "tx_commit",
            "tx_begin",
            "tx_exec_sql",
            "tx_rollback",
            "tx_begin",
            "tx_exec_sql",
            "tx_rollback",
        ]
    );
    assert_eq!(calls[7].arg("lock_key"), &json!("cmdq:c1"));
    assert_eq!(calls[11].arg("lock_key"), &Value::Null);
    assert_eq!(calls[14].arg("lock_key"), &json!("cmdq:c2"));
}

#[test]
fn older_hosts_name_5_1_0_for_new_host_calls() {
    struct Old;
    impl HostServices for Old {
        fn call(&self, _: &Request, method: &str, _: Option<Frame>) -> Result<Value> {
            Err(crate::Error::Unsupported(method.into()))
        }
    }
    let request = request();
    with_host(&Old, &request, || {
        let pipe = request.pipe().unwrap();
        for err in [
            crate::host::root_dir().unwrap_err(),
            crate::host::read_sql_with("sql:main", "SELECT 1", &json!({})).unwrap_err(),
            crate::host::exec_sql_with("sql:main", "SELECT 1", &json!({})).unwrap_err(),
            pipe.id().unwrap_err(),
            pipe.is_registered().unwrap_err(),
            crate::host::transaction::<(), _>("sql:main", None, |_| Ok(())).unwrap_err(),
        ] {
            let msg = err.to_string();
            assert!(
                msg.contains("upgrade Meerschaum to 5.1.0 or later"),
                "{msg}"
            );
        }
    });
}
