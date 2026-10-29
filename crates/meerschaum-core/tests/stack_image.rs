//! W18PK-001: the stack's `api` service runs this release's `full` image, not the floating
//! `bmeares/meerschaum:api` tag (a prerelease never moves `:api`, so it kept naming the v4 image).

use meerschaum_core::config::{VERSION_MARKER, default_config};
use meerschaum_core::python_guard::expected_version;
use meerschaum_core::{Map, Value};

fn api_image(config: &Map) -> String {
    Value::Map(config.clone())
        .get_path(&["stack", "docker-compose.yaml", "services", "api", "image"])
        .and_then(Value::as_str)
        .expect("stack api image")
        .to_string()
}

#[test]
fn the_default_api_image_is_this_releases_full_image() {
    let config = default_config("/root", 8, false);
    let expected = format!("bmeares/meerschaum:{}-full", expected_version());
    assert_eq!(api_image(&config), expected);
    assert!(
        !expected.contains(VERSION_MARKER),
        "the marker must be resolved: {expected}"
    );
}

#[test]
fn no_default_still_holds_the_version_marker() {
    let config = Value::Map(default_config("/root", 8, false)).to_json_string();
    assert!(
        !config.contains(VERSION_MARKER) && !config.contains("__MRSM_ROOT_DIR__"),
        "an unresolved marker is left in the default config"
    );
}

#[test]
fn the_image_tag_is_a_pep440_version_docker_accepts() {
    // `docker buildx` tags images `<PEP 440 version>-full` (scripts/docker/buildx.sh).
    let version = expected_version();
    assert!(
        version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.'),
        "{version}"
    );
    assert!(!version.contains("-rc"), "Cargo spelling leaked: {version}");
}
