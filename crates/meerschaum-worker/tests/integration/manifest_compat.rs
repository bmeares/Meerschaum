//! Manifests written by older or minimal producers must load: argument fields other than the
//! option strings and the action class are optional, and `venv_warnings` is new in schema 3.

use meerschaum_worker::manifest::{MANIFEST_SCHEMA_VERSION, Manifest};

#[test]
fn argument_manifest_fields_are_optional() {
    let json = serde_json::json!({
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "meerschaum_version": "5.0.0",
        "python_version": "3.11.0",
        "fingerprint": null,
        "plugins": {
            "demo": {
                "name": "demo",
                "import_error": null,
                "arguments": [
                    {"option_strings": ["--foo"], "action": "_StoreAction"},
                    {"option_strings": ["--bar"], "action": "_StoreTrueAction", "dest": "bar",
                     "nargs": "+", "const": true, "default": false, "choices": ["a"],
                     "metavar": "BAR", "help": "bar", "type": "str", "required": true}
                ]
            }
        }
    });
    let manifest: Manifest = serde_json::from_value(json).unwrap();
    let args = &manifest.plugins["demo"].arguments;
    assert!(args[0].nargs.is_null() && args[0].choices.is_null() && args[0].metavar.is_null());
    assert!(args[0].default.is_null() && args[0].const_value.is_null());
    assert_eq!(args[1].nargs, serde_json::json!("+"));
    assert_eq!(args[1].const_value, serde_json::json!(true));
    assert!(manifest.plugins["demo"].venv_warnings.is_empty());
}

#[test]
fn venv_warnings_round_trip() {
    let json = serde_json::json!({
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "meerschaum_version": "5.0.0",
        "python_version": "3.11.0",
        "fingerprint": null,
        "plugins": {"demo": {"name": "demo", "import_error": null,
                              "venv_warnings": ["add 'pypdf' to required"]}}
    });
    let manifest: Manifest = serde_json::from_value(json).unwrap();
    assert_eq!(manifest.plugins["demo"].venv_warnings.len(), 1);
    let again: Manifest = serde_json::from_str(&serde_json::to_string(&manifest).unwrap()).unwrap();
    assert_eq!(again, manifest);
}
