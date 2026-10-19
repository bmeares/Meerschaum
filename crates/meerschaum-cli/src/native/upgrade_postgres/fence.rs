//! Fence ordinary TCP writers while the old database is dumped and stopped. Disconnecting the
//! container's networks leaves loopback and its Unix socket available to our docker-exec tools.
//! Attachments are journalled first and reconnected only after stop, or while putting back.

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use super::docker::{Docker, Res, StepError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Attachment {
    pub network: String,
    pub aliases: Vec<String>,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
}

impl Attachment {
    fn connect_args(&self, container: &str) -> Vec<String> {
        let mut args = vec!["network".to_string(), "connect".to_string()];
        for alias in &self.aliases {
            args.extend(["--alias".into(), alias.clone()]);
        }
        if let Some(ip) = &self.ipv4 {
            args.extend(["--ip".into(), ip.clone()]);
        }
        if let Some(ip) = &self.ipv6 {
            args.extend(["--ip6".into(), ip.clone()]);
        }
        args.extend([self.network.clone(), container.to_string()]);
        args
    }
}

fn settings(container: &str) -> Res<Json> {
    let done = Docker::new(&[
        "container",
        "inspect",
        "--format",
        "{{json .NetworkSettings.Networks}}",
        container,
    ])
    .check("Reading database network attachments")?;
    serde_json::from_slice(&done.stdout)
        .map_err(|e| StepError::new(format!("Cannot read database network attachments: {e}")))
}

pub(crate) fn attachments(container: &str) -> Res<Vec<Attachment>> {
    let done = Docker::new(&[
        "container",
        "inspect",
        "--format",
        "{{.HostConfig.NetworkMode}}",
        container,
    ])
    .check("Reading database network mode")?;
    let mode = done.stdout_text();
    if mode.trim() == "host" || mode.trim().starts_with("container:") {
        return Err(StepError::new(
            "The database shares a host/container network namespace, so external writes cannot be fenced safely. Use the manual procedure, or --force to explicitly accept concurrent-write risk.",
        ));
    }
    let settings = settings(container)?;
    let mut attachments = Vec::new();
    for (network, endpoint) in settings.as_object().into_iter().flatten() {
        if network == "none" {
            continue;
        }
        if endpoint
            .get("Links")
            .is_some_and(|v| v.as_array().is_some_and(|a| !a.is_empty()))
            || endpoint
                .get("DriverOpts")
                .is_some_and(|v| v.as_object().is_some_and(|m| !m.is_empty()))
            || endpoint
                .get("IPAMConfig")
                .and_then(|v| v.get("LinkLocalIPs"))
                .and_then(Json::as_array)
                .is_some_and(|a| !a.is_empty())
            || endpoint
                .get("GwPriority")
                .and_then(Json::as_i64)
                .unwrap_or(0)
                != 0
        {
            return Err(StepError::new(
                "The database has custom network links, link-local addresses, driver options or gateway priority. Its attachments cannot be restored automatically; use the manual procedure, or --force to explicitly accept concurrent-write risk.",
            ));
        }
        let ip = |key: &str| {
            endpoint
                .get("IPAMConfig")
                .and_then(|i| i.get(key))
                .and_then(Json::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        attachments.push(Attachment {
            network: network.clone(),
            aliases: endpoint
                .get("Aliases")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .filter_map(Json::as_str)
                .map(str::to_string)
                .collect(),
            ipv4: ip("IPv4Address"),
            ipv6: ip("IPv6Address"),
        });
    }
    Ok(attachments)
}

pub(crate) fn disconnect(container: &str, attachments: &[Attachment]) -> Res<()> {
    for attachment in attachments {
        Docker::new(&["network", "disconnect", &attachment.network, container])
            .check("Isolating the old database from external clients")?;
    }
    Ok(())
}

/// Idempotent: a killed reconnect may have already restored some endpoints.
pub(crate) fn reconnect(container: &str, attachments: &[Attachment]) -> Vec<String> {
    if attachments.is_empty() {
        return Vec::new();
    }
    let current = match settings(container) {
        Ok(s) => s,
        Err(e) => return vec![e.message],
    };
    let mut problems = Vec::new();
    for attachment in attachments {
        if current.get(&attachment.network).is_some() {
            continue;
        }
        let args = attachment.connect_args(container);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        if let Err(e) = Docker::new(&refs).check("Restoring the database network attachments") {
            problems.push(e.message);
        }
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_preserves_aliases_and_static_addresses_in_the_journal() {
        let attachment = Attachment {
            network: "scratch_backend".into(),
            aliases: vec!["db".into(), "scratch-db-1".into()],
            ipv4: Some("192.0.2.2".into()),
            ipv6: Some("2001:db8::2".into()),
        };
        let journal = serde_json::to_string(&attachment).unwrap();
        let recovered: Attachment = serde_json::from_str(&journal).unwrap();
        assert_eq!(
            recovered.connect_args("scratch-db-1"),
            [
                "network",
                "connect",
                "--alias",
                "db",
                "--alias",
                "scratch-db-1",
                "--ip",
                "192.0.2.2",
                "--ip6",
                "2001:db8::2",
                "scratch_backend",
                "scratch-db-1"
            ]
        );
    }
}
