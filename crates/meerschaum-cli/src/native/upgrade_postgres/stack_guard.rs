//! Refuse `stack up` when a known PostgreSQL image would open data of another major.
//! Every probe goes through the caller's host (also for isolated CLI tests); volumes are
//! inspected before mounting, read-only, and images are never pulled by this guard.

use std::path::Path;
use std::time::Duration;

use serde_json::Value as Json;

use crate::dispatch::{CommandSpec, Runtime};

use super::images::{Family, ImageRef};

fn configured_major(image: &str) -> Option<u32> {
    let image = ImageRef::parse(image);
    let tag = image.tag.as_deref()?;
    let digits = match image.family()? {
        Family::TimescaleHa => tag.strip_prefix("pg")?,
        Family::Timescale => tag.strip_prefix("latest-pg")?,
        Family::Postgres | Family::Postgis => tag,
    };
    digits
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

fn capture(rt: &mut Runtime<'_>, base: &CommandSpec, args: &[&str]) -> Result<String, String> {
    let spec = CommandSpec {
        args: args.iter().map(|s| (*s).to_string()).collect(),
        ..base.clone()
    };
    let out = rt
        .host
        .output(&spec, Some(Duration::from_secs(30)))
        .map_err(|e| format!("Cannot check the stack database: {e}"))?;
    if out.code != 0 || out.timed_out {
        return Err(format!(
            "Cannot check the stack database: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

pub(crate) fn stack_up_preflight(
    rt: &mut Runtime<'_>,
    project: &str,
    compose_path: &Path,
    env: &[(String, String)],
) -> Result<(), String> {
    let base = CommandSpec {
        program: "docker".into(),
        env: env.to_vec(),
        cwd: compose_path.parent().map(Path::to_path_buf),
        quiet: true,
        ..CommandSpec::default()
    };
    let config = capture(
        rt,
        &base,
        &[
            "compose",
            "--project-name",
            project,
            "config",
            "--format",
            "json",
        ],
    )?;
    // A host that does not support captured output (e.g. a recording host) has nothing to inspect.
    if config.is_empty() {
        return Ok(());
    }
    let resolved: Json =
        serde_json::from_str(&config).map_err(|e| format!("Cannot read the stack config: {e}"))?;
    let Some(db) = resolved.pointer("/services/db") else {
        return Ok(());
    };
    let image = db.get("image").and_then(Json::as_str).unwrap_or_default();
    let Some(major) = configured_major(image) else {
        return Ok(());
    };
    let id = capture(
        rt,
        &base,
        &["compose", "--project-name", project, "ps", "-a", "-q", "db"],
    )?;
    let mut probe_image = image.to_string();
    if !id.is_empty() {
        let text = capture(
            rt,
            &base,
            &["container", "inspect", "--format", "{{json .}}", &id],
        )?;
        let container: Json = serde_json::from_str(&text)
            .map_err(|e| format!("Cannot read the stack database container: {e}"))?;
        let labels = container
            .pointer("/Config/Labels")
            .and_then(Json::as_object)
            .map(|labels| {
                labels
                    .iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        if !super::created_from(&labels, compose_path) {
            return Err(format!(
                "The database container of project '{project}' was not created from this root's stack. Use its root or change stack:project_name."
            ));
        }
        let old_image = container
            .pointer("/Config/Image")
            .and_then(Json::as_str)
            .unwrap_or_default();
        if container.pointer("/State/Running").and_then(Json::as_bool) == Some(true)
            && old_image == image
        {
            return Ok(());
        }
        if !old_image.is_empty() {
            probe_image = old_image.to_string();
        }
    }
    let volumes = db
        .get("volumes")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default();
    for mount in volumes {
        if mount.get("type").and_then(Json::as_str) != Some("volume") {
            continue;
        }
        let Some(key) = mount.get("source").and_then(Json::as_str) else {
            continue;
        };
        let name = resolved
            .get("volumes")
            .and_then(|v| v.get(key))
            .and_then(|v| v.get("name"))
            .and_then(Json::as_str)
            .unwrap_or(key);
        // A nonexistent volume is a fresh cluster. A failed lookup must not accidentally create it.
        if capture(
            rt,
            &base,
            &["volume", "inspect", "--format", "{{.Name}}", name],
        )
        .is_err()
        {
            continue;
        }
        let mount = format!("type=volume,source={name},target=/mrsm-data,readonly");
        let result = capture(rt, &base, &["run", "--rm", "--pull", "never", "--network", "none", "--mount", &mount,
            "--entrypoint", "sh", &probe_image, "-c",
            "for f in /mrsm-data/PG_VERSION /mrsm-data/*/PG_VERSION /mrsm-data/*/*/PG_VERSION; do [ -f \"$f\" ] && cat \"$f\"; done; exit 0"])
            .map_err(|e| format!("{e}\nPin the database's previous image or load '{probe_image}' locally before starting it; --force bypasses this check."))?;
        for old in result
            .lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
        {
            if old != major {
                return Err(format!(
                    "The database volume '{name}' contains PostgreSQL {old} data (PG_VERSION), but the configured image '{image}' runs PostgreSQL {major}. Pin its PostgreSQL {old} image in `mrsm edit config stack`, start the old database, then run `mrsm upgrade postgres {major}`. `mrsm stack up --force` bypasses this check."
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_and_pinned_known_images_expose_their_major() {
        for (image, major) in [
            ("timescale/timescaledb-ha:pg18", 18),
            ("timescale/timescaledb-ha:pg17.5-ts2.20.3", 17),
            ("postgres:17.6-alpine", 17),
            ("postgis/postgis:17-3.5", 17),
            ("timescale/timescaledb:latest-pg16", 16),
        ] {
            assert_eq!(configured_major(image), Some(major), "{image}");
        }
        assert_eq!(configured_major("custom:18"), None);
        assert_eq!(configured_major("postgres:latest"), None);
    }

    struct ProbeHost {
        responses: std::collections::VecDeque<(i32, String)>,
        calls: std::rc::Rc<std::cell::RefCell<Vec<CommandSpec>>>,
    }

    impl crate::dispatch::Host for ProbeHost {
        fn run(&mut self, _: &CommandSpec) -> std::io::Result<i32> {
            panic!("unexpected attached process");
        }
        fn read_line(&mut self, _: &str) -> Option<String> {
            None
        }
        fn output(
            &mut self,
            spec: &CommandSpec,
            _: Option<Duration>,
        ) -> std::io::Result<crate::dispatch::CommandOutput> {
            self.calls.borrow_mut().push(spec.clone());
            let (code, text) = self.responses.pop_front().expect("unexpected Docker call");
            Ok(crate::dispatch::CommandOutput {
                code,
                stdout: text.into_bytes(),
                ..Default::default()
            })
        }
    }

    fn probe(responses: Vec<(i32, String)>) -> (Result<(), String>, Vec<CommandSpec>) {
        let calls = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut output = Vec::new();
        let mut rt = Runtime::new(crate::types::DateContext::system(), 80, &mut output, |_| {
            None
        });
        rt.host = Box::new(ProbeHost {
            responses: responses.into(),
            calls: calls.clone(),
        });
        let result = stack_up_preflight(
            &mut rt,
            "scratch",
            Path::new("/scratch/stack/docker-compose.yaml"),
            &[],
        );
        let calls = calls.borrow().clone();
        (result, calls)
    }

    fn config() -> String {
        serde_json::json!({"services": {"db": {"image": "postgres:18", "volumes": [{"type": "volume", "source": "data"}]}}, "volumes": {"data": {"name": "scratch_data"}}}).to_string()
    }

    #[test]
    fn mismatch_refuses_with_a_read_only_probe_and_no_pull() {
        let (result, calls) = probe(vec![
            (0, config()),
            (0, String::new()),
            (0, "scratch_data".into()),
            (0, "17\n".into()),
        ]);
        let error = result.unwrap_err();
        assert!(error.contains("PG_VERSION") && error.contains("upgrade postgres 18"));
        let args = &calls.last().unwrap().args;
        assert!(args.windows(2).any(|w| w == ["--pull", "never"]));
        assert!(
            args.iter()
                .any(|a| a == "type=volume,source=scratch_data,target=/mrsm-data,readonly")
        );
    }

    #[test]
    fn missing_volume_is_not_created_or_mounted() {
        let (result, calls) = probe(vec![(0, config()), (0, String::new()), (1, String::new())]);
        assert!(result.is_ok());
        assert!(
            !calls
                .iter()
                .any(|c| c.args.first().map(String::as_str) == Some("run"))
        );
    }

    #[test]
    fn matching_running_image_skips_volume_inspection() {
        let container = serde_json::json!({"Config": {"Image": "postgres:18", "Labels": {"com.docker.compose.project.config_files": "/scratch/stack/docker-compose.yaml"}}, "State": {"Running": true}});
        let (result, calls) = probe(vec![
            (0, config()),
            (0, "scratch-db-1".into()),
            (0, container.to_string()),
        ]);
        assert!(result.is_ok());
        assert_eq!(calls.len(), 3);
    }

    #[test]
    fn another_root_cannot_probe_or_start_the_project_database() {
        let container = serde_json::json!({"Config": {"Image": "postgres:17", "Labels": {"com.docker.compose.project.config_files": "/another/stack/docker-compose.yaml"}}, "State": {"Running": false}});
        let (result, calls) = probe(vec![
            (0, config()),
            (0, "scratch-db-1".into()),
            (0, container.to_string()),
        ]);
        assert!(result.unwrap_err().contains("not created from this root"));
        assert_eq!(calls.len(), 3);
    }
}
