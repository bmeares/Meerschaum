//! The `api_route` and `mcp_*` call kinds through a real cdylib (`examples/native_api.rs`).
use meerschaum_plugin::{
    LoadedPlugin,
    api::{self, RouteRequest},
    mcp::{self, PromptGet, ResourceContents, ResourceRead, ToolCall},
    serde_json::json,
};
use std::{path::PathBuf, process::Command, sync::OnceLock};

fn artifact(name: &str) -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "meerschaum-plugin", "--examples"])
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
    });
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

#[test]
fn routes_and_mcp_entries_round_trip_through_the_abi() {
    // SAFETY: the test builds this SDK-owned example from the repository.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_api")) }.unwrap();
    let entry = &plugin.manifest["plugins"]["native_api"];

    let routes = api::routes_of(entry).unwrap();
    assert_eq!(routes.len(), 8);
    assert_eq!(routes[0].scopes, vec!["pipes:read".to_string()]);
    assert!(routes[2].public);

    let reply = plugin
        .api_route(&RouteRequest {
            name: "blob".into(),
            method: "PUT".into(),
            path: "/native-api/blobs/a/b".into(),
            path_params: [("key".to_string(), "a/b".to_string())].into(),
            body: vec![0, 255, 1],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(reply.status, 201);
    assert_eq!(reply.body, vec![0, 255, 1]);
    assert!(
        reply
            .headers
            .contains(&("x-native-key".into(), "a/b".into()))
    );
    assert_eq!(
        reply
            .headers
            .iter()
            .filter(|(k, _)| k == "set-cookie")
            .count(),
        2
    );
    // A handler error is reported as an error, not a response.
    let unknown = plugin.api_route(&RouteRequest {
        name: "missing".into(),
        ..Default::default()
    });
    assert!(unknown.unwrap_err().to_string().contains("unknown route"));

    let entries = mcp::entries_of(entry).unwrap();
    assert_eq!(entries.tools.len(), 4);
    assert!(entries.tools[0].hint("readOnlyHint"));
    assert!(entries.tools[0].validate(&json!({"a": 1})).is_err());
    let sum = plugin
        .mcp_tool(&ToolCall {
            name: "native_api_add".into(),
            arguments: json!({"a": 2, "b": 3}).as_object().unwrap().clone(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(sum, json!({"sum": 5}));

    let item = plugin
        .mcp_resource(&ResourceRead {
            name: "native_api_item".into(),
            uri: "native-api://items/7".into(),
            variables: json!({"id": "7"}).as_object().unwrap().clone(),
            ..Default::default()
        })
        .unwrap();
    let ResourceContents::Text { text, mime_type } = item else {
        panic!("text expected");
    };
    assert_eq!(mime_type.as_deref(), Some("application/json"));
    assert!(text.contains("\"7\""));
    let logo = plugin
        .mcp_resource(&ResourceRead {
            name: "native_api_logo".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(matches!(logo, ResourceContents::Blob { ref bytes, .. } if bytes.len() == 4));

    let (description, messages) = plugin
        .mcp_prompt(&PromptGet {
            name: "native_api_greet".into(),
            arguments: json!({"who": "Ada"}).as_object().unwrap().clone(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(description.as_deref(), Some("A greeting for Ada."));
    assert_eq!(messages[0]["content"]["text"], "Say hello to Ada.");
}

#[test]
fn server_calls_are_not_serialized() {
    // SAFETY: the test builds this SDK-owned example from the repository.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_api")) }.unwrap();
    let slow = RouteRequest {
        name: "slow".into(),
        method: "GET".into(),
        path: "/native-api/slow".into(),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    std::thread::scope(|scope| {
        let calls: Vec<_> = (0..3)
            .map(|_| scope.spawn(|| plugin.api_route(&slow).unwrap()))
            .collect();
        for call in calls {
            assert_eq!(call.join().unwrap().body, b"slept");
        }
    });
    // Three two-second calls in parallel, not one after another.
    assert!(
        started.elapsed() < std::time::Duration::from_millis(5000),
        "{:?}",
        started.elapsed()
    );
}
