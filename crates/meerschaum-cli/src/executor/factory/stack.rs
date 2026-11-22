//! Typed administrator installation projected into generated Compose, without secret bytes.

use meerschaum_core::Value;
use meerschaum_core::execution::Mode;
use meerschaum_server::executor::factory_attestation::{PRIVATE_PATH, PUBLIC_DIRECTORY};
use serde_json::json;
use std::path::Path;

use crate::executor::registry::Desired;

fn value(value: serde_json::Value) -> Result<Value, String> {
    Value::from_json_str(&value.to_string())
        .map_err(|_| "Invalid native Factory deployment model.".into())
}
fn refused() -> String {
    "The administrator's native Factory deployment does not match its protected installation."
        .into()
}

pub(crate) fn render(
    root: &Path,
    desired: &Desired,
    reference: &str,
    model: Value,
) -> Result<Value, String> {
    let Some(selected) = crate::executor::factory::docker::selected(root, reference)? else {
        return Ok(model);
    };
    let mut model = model.to_map();
    let mut services = model
        .get("services")
        .and_then(Value::as_map)
        .cloned()
        .ok_or_else(refused)?;
    let mut secrets = model
        .get("secrets")
        .and_then(Value::as_map)
        .cloned()
        .unwrap_or_default();
    let records = desired
        .teams
        .values()
        .filter(|record| !record.retired && record.settings.mode == Mode::Container)
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Ok(Value::Map(model));
    }
    for record in records {
        let Some(private_file) = crate::executor::factory::install::existing(
            root,
            record,
            &selected.image,
            &selected.identity,
        )?
        else {
            continue;
        };
        let service = services
            .get_mut(&format!("executor-{}", record.key))
            .and_then(Value::as_map_mut)
            .ok_or_else(refused)?;
        if service.get("user").and_then(Value::as_str) != Some("1000:1000")
            || service.get("image").and_then(Value::as_str) != Some(selected.image.as_str())
        {
            return Err(refused());
        }
        let key = format!("executor-{}-factory", record.key);
        let definition = value(json!({"file":private_file}))?;
        if secrets
            .get(&key)
            .is_some_and(|previous| previous != &definition)
        {
            return Err(refused());
        }
        secrets.insert(key.clone(), definition);
        let grant = value(
            json!({"source":key,"target":PRIVATE_PATH.rsplit('/').next().ok_or_else(refused)?}),
        )?;
        match service
            .entry("secrets".into())
            .or_insert_with(|| Value::List(Vec::new()))
        {
            Value::List(secrets) if !secrets.contains(&grant) => secrets.push(grant),
            Value::List(_) => {}
            _ => return Err(refused()),
        }
    }
    let api = services
        .get_mut("api")
        .and_then(Value::as_map_mut)
        .ok_or_else(refused)?;
    let mount = value(
        json!({"type":"bind","source":crate::executor::factory::install::directory(root).join("public"),
        "target":PUBLIC_DIRECTORY,"read_only":true,"bind":{"create_host_path":false}}),
    )?;
    match api
        .entry("volumes".into())
        .or_insert_with(|| Value::List(Vec::new()))
    {
        Value::List(volumes) if !volumes.contains(&mount) => volumes.push(mount),
        Value::List(_) => {}
        _ => return Err(refused()),
    }
    model.insert("services".into(), Value::Map(services));
    model.insert("secrets".into(), Value::Map(secrets));
    Ok(Value::Map(model))
}

pub(crate) fn provision(
    root: &Path,
    desired: &Desired,
    reference: &str,
    environment: &[(String, String)],
    host: &mut dyn crate::dispatch::Host,
) -> Result<bool, String> {
    let records = desired
        .teams
        .values()
        .filter(|record| !record.retired && record.settings.mode == Mode::Container)
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Ok(false);
    }
    crate::executor::factory::install::check_provisioner()?;
    let image = crate::executor::factory::docker::inspect(root, reference, environment, host)?;
    for record in records {
        crate::executor::factory::install::provision(root, record, &image)?;
    }
    crate::executor::factory::docker::select(root, reference, &image)?;
    Ok(true)
}
