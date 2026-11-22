//! Mandatory ignored acceptance for generated per-team containers on local Docker >=28.
//!
//! Required artifact interface: MRSM_TEST_EXECUTOR_IMAGE names a locally loaded native+SDK
//! image; MRSM_TEST_EXECUTOR_NATIVE_SHA256 and MRSM_TEST_EXECUTOR_SDK_SHA256 identify the exact
//! native executable and installed ABI3 extension. The native Linux candidate workflow runs
//! the selected deployment cases from scripts/ci/docker_candidate.py against the immutable
//! full image and the sealed CLI and wheel before recording that image for promotion.
//! No image is built/pulled/published here; cleanup verifies exact fixture owner/project labels.
//! Run this bind-state fixture as UID1000, matching the generated native container user.
//! A candidate lane using another runner UID must explicitly run this gate as UID1000.
//! Host OpenSSL creates only fixture keys. The private CA/server keys stay on the fixture host
//! and TLS peer; Code mounts only the public CA read-only. Required TLS is entirely local.
//! A test-only native DNS command double supplies the inspected peer IP for fixturetls; native
//! deployment-host DNS isolation is separately covered by `--test executor_network`.
//! MRSM_TEST_EXECUTOR_PROVISION_FACTORY=1 additionally requires a locally loaded production
//! image with its complete native Factory artifact manifest. The administrator must be UID1000
//! with identity-mapped Docker file ownership for the generated 1000:1000 service.
//! MRSM_TEST_EXECUTOR_PUBLIC_TLS=1 additionally checks the public HTTPS proxy (optional).
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_cli::dispatch::{CommandOutput, CommandSpec, Host, SystemHost};
use meerschaum_cli::executor::jobs::{Catalog, JobPlacement};
use meerschaum_cli::executor::registry::Registry;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT;
use meerschaum_core::users::User;
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_server::executor::environment::Profile;
use meerschaum_server::executor::factory_attestation::{PRIVATE_PATH, PUBLIC_DIRECTORY};
use serde_json::{Value as Json, json};

#[path = "../common/mod.rs"]
mod common;
#[path = "../fixtures/container_terminal.rs"]
mod container_terminal;
use common::http::response;
use common::json_map as map;
use container_terminal::Placement;

mod docker;
mod job_checks;
mod scenario;

use job_checks::login;
use scenario::run_container;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Api,
    Team,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Terminal(Placement),
    Lifecycle,
    PipeDelivery(Role),
    InstalledProfile(Role),
    NativeMetadata,
    Recovery,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the provisioned candidate image with its public API-only terminal capability"]
async fn public_host_api_terminal_shows_version_after_reconnect() {
    run_container(Scenario::Terminal(Placement::Host)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the provisioned candidate image with its public API-only terminal capability"]
async fn public_remote_api_terminal_shows_version_after_reconnect() {
    run_container(Scenario::Terminal(Placement::Remote)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned local Docker instance and native+SDK candidate image"]
async fn generated_container_api_and_code_jobs_keep_profiles_callbacks_network_and_persistent_state()
 {
    run_container(Scenario::Lifecycle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned local Docker instance and native+SDK candidate image"]
async fn generated_container_api_job_delivers_plugin_rows() {
    run_container(Scenario::PipeDelivery(Role::Api)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned local Docker instance and native+SDK candidate image"]
async fn generated_container_team_job_delivers_plugin_rows() {
    run_container(Scenario::PipeDelivery(Role::Team)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned local Docker instance and provisioned native compiler image"]
async fn generated_container_discovers_a_granted_native_resource() {
    run_container(Scenario::NativeMetadata).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned local Docker instance and provisioned native+SDK candidate image"]
async fn generated_container_starts_api_job_and_discovers_a_python_source() {
    run_container(Scenario::InstalledProfile(Role::Api)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned local Docker instance and provisioned native+SDK candidate image"]
async fn generated_container_starts_credentialed_team_job() {
    run_container(Scenario::InstalledProfile(Role::Team)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned local Docker instance and provisioned native+SDK candidate image"]
async fn generated_executors_resume_jobs_after_outage_and_preserve_team_state_on_recreation() {
    run_container(Scenario::Recovery).await;
}

async fn version_job(client: &reqwest::Client, origin: &str, token: &str, profile: &str) {
    let name = format!("version-{profile}");
    let url = format!("{origin}/jobs/{name}");
    let (status, started) = response(
        client
            .post(&url)
            .bearer_auth(token)
            .json(&json!({"sysargs":["show","version"], "properties":{}})),
    )
    .await;
    assert_eq!(
        (status, started[0].as_bool()),
        (200, Some(true)),
        "{started}"
    );
    assert_version_completed(client, origin, token, profile).await;
}

async fn assert_version_completed(
    client: &reqwest::Client,
    origin: &str,
    token: &str,
    profile: &str,
) {
    let name = format!("version-{profile}");
    let url = format!("{origin}/jobs/{name}");
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let (status, job) = response(client.get(&url).bearer_auth(token)).await;
        assert_eq!(status, 200, "{job}");
        assert_eq!(job["executor"]["profile"], profile, "{job}");
        assert_eq!(job["executor"]["placement"], "remote", "{job}");
        if job["status"] == "stopped" && job["completion_pending"] != true {
            assert_eq!(job["result"][0], true, "{job}");
            break;
        }
        assert!(Instant::now() < deadline, "{job}");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let (status, logs) = response(
        client
            .get(format!("{origin}/logs/{name}"))
            .bearer_auth(token),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        logs.as_str().unwrap().contains(env!("CARGO_PKG_VERSION")),
        "{logs}"
    );
}
