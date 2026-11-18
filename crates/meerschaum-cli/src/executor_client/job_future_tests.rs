//! Regression for the real canonical metadata stack, without a larger Tokio thread stack.

use super::*;
use crate::executor_client::completed_result;

#[test]
fn completed_result_uses_one_finished_properties_snapshot_instead_of_an_earlier_placeholder() {
    let mut body = serde_json::json!({"status":"stopped", "result":[false,"No result available."],
        "daemon":{"properties":{"process":{"ended":"2026-11-17T08:00:00"},
            "result":[true,"Committed worker result"]}}});
    assert_eq!(
        completed_result(&body),
        Some((true, "Committed worker result"))
    );
    body["daemon"]["properties"]["result"] = serde_json::json!([false, "Worker refused"]);
    assert_eq!(completed_result(&body), Some((false, "Worker refused")));
    body["status"] = "running".into();
    assert_eq!(completed_result(&body), None);
    body["status"] = "stopped".into();
    body["daemon"]["properties"]["process"]["ended"] = serde_json::Value::Null;
    assert_eq!(completed_result(&body), None);
    body["daemon"]["properties"]["process"]["ended"] = "2026-11-17T08:00:00".into();
    body["daemon"]["properties"]["result"] = serde_json::Value::Null;
    assert_eq!(completed_result(&body), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_registration_delivery_and_metadata_futures_remain_bounded() {
    let root = tempfile::tempdir().unwrap();
    let (controller, _) = super::identity_tests::fixture(root.path()).await;
    let mut record = Record::new(
        "future-budget",
        Uuid::new_v4(),
        vec!["show".into(), "version".into()],
        Default::default(),
    );
    let registration = controller.register_job_callback(&mut record, None);
    let registration_bytes = std::mem::size_of_val(registration.as_ref().get_ref());
    drop(registration);
    let delivery = controller.deliver_job_context(&mut record, false);
    let delivery_bytes = std::mem::size_of_val(delivery.as_ref().get_ref());
    drop(delivery);
    let metadata = controller.discover_plugin_catalog(record.team_id, Default::default());
    let metadata_bytes = std::mem::size_of_val(metadata.as_ref().get_ref());
    drop(metadata);
    let pipe = meerschaum_core::Pipe::builder("plugin:fixture", "registration")
        .instance("sql:main")
        .build()
        .unwrap();
    let hook = controller.connector_registration(&pipe);
    let hook_bytes = std::mem::size_of_val(&hook);
    assert!(
        hook_bytes <= 2048,
        "The Main registration hook once again embeds its tracked worker in the HTTP caller ({hook_bytes} bytes)."
    );
    drop(hook);
    for (name, bytes) in [
        ("registration", registration_bytes),
        ("delivery", delivery_bytes),
        ("metadata", metadata_bytes),
    ] {
        assert!(
            bytes <= 128 * 1024,
            "Canonical {name} future is {bytes} bytes; box its large awaited operations rather than growing the worker stack."
        );
    }
}
