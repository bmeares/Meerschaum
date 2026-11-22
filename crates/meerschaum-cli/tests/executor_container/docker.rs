//! The fixture's owned Docker project: image digests, labelled resources and their cleanup.

use super::job_checks::file_tail;
use super::*;

pub(super) fn digest_variable(name: &str) -> String {
    let value = std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must identify the exact tested artifact"));
    assert!(value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()));
    value
}
pub(super) struct Docker {
    pub(super) project: String,
    pub(super) compose: PathBuf,
    pub(super) main: PathBuf,
}

impl Docker {
    fn log_tail(&self, id: &str) -> String {
        use std::io::Read;
        use std::process::{Command, Stdio};
        // The caller checked this exact immutable ID's unpredictable owner/project labels.
        // Pipe readers stop at the cap (including a single hostile overlong log line), and
        // the child is killed at the deadline rather than blocking failure cleanup.
        let mut command = Command::new("docker");
        command
            .env_clear()
            .args(["--context", "default", "logs", "--tail", "80", id])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for key in ["PATH", "HOME"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let Ok(mut child) = command.spawn() else {
            return "unable to start owned log diagnostic".into();
        };
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let read = |pipe: Box<dyn Read + Send>| {
            std::thread::spawn(move || {
                let mut bytes = Vec::new();
                let _ = pipe.take(16 * 1024).read_to_end(&mut bytes);
                bytes
            })
        };
        let out = read(Box::new(stdout));
        let err = read(Box::new(stderr));
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = child.kill();
        let _ = child.wait();
        let mut bytes = out.join().unwrap_or_default();
        bytes.extend(err.join().unwrap_or_default());
        String::from_utf8_lossy(&bytes).into_owned()
    }

    fn main_log_tails(&self) -> String {
        let mut text = String::new();
        let logs = self.main.join("logs");
        if !std::fs::symlink_metadata(&logs)
            .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
        {
            return text;
        }
        let Ok(entries) = std::fs::read_dir(logs) else {
            return text;
        };
        let mut files: Vec<_> = entries
            .take(512)
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains(".log"))
            })
            .map(|entry| entry.path())
            .collect();
        files.sort();
        for path in files.into_iter().rev().take(8) {
            text.push_str(&format!(
                "\nOwned Main log {}:\n{}",
                path.file_name().unwrap().to_string_lossy(),
                file_tail(&path)
            ));
        }
        text
    }

    fn output(&self, args: Vec<String>, seconds: u64) -> CommandOutput {
        let mut command = CommandSpec {
            program: "docker".into(),
            args: vec!["--context".into(), "default".into()],
            env_clear: true,
            quiet: true,
            scrub_secrets: true,
            ..Default::default()
        };
        command.args.extend(args);
        command.env = ["PATH", "HOME"]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
            .collect();
        SystemHost
            .output(&command, Some(Duration::from_secs(seconds)))
            .unwrap()
    }
    pub(super) fn openssl(&self, args: &[&str]) {
        let mut command = CommandSpec {
            program: "openssl".into(),
            args: args.iter().map(|value| value.to_string()).collect(),
            env_clear: true,
            quiet: true,
            scrub_secrets: true,
            ..Default::default()
        };
        command.env = ["PATH", "HOME"]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
            .collect();
        let output = SystemHost
            .output(&command, Some(Duration::from_secs(30)))
            .unwrap();
        assert!(
            output.code == 0 && !output.timed_out,
            "fixture certificate setup failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    pub(super) fn checked(&self, args: Vec<String>, seconds: u64) -> String {
        let output = self.output(args, seconds);
        assert!(
            !output.timed_out && output.code == 0,
            "owned Docker operation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }
    pub(super) fn command(&self, args: &[&str]) -> String {
        self.checked(args.iter().map(|value| value.to_string()).collect(), 40)
    }
    pub(super) fn compose(&self, args: &[&str]) -> String {
        let mut command = vec![
            "compose".into(),
            "--project-name".into(),
            self.project.clone(),
            "--file".into(),
            self.compose.display().to_string(),
        ];
        command.extend(args.iter().map(|value| value.to_string()));
        self.checked(command, 120)
    }
    pub(super) fn id(&self, service: &str) -> String {
        self.compose(&["ps", "--all", "--quiet", service])
    }
    pub(super) fn main_origin(&self) -> String {
        // Docker assigns a fresh loopback port when the container starts again.
        let published: std::net::SocketAddr =
            self.compose(&["port", "api", "8989"]).parse().unwrap();
        assert!(published.ip().is_loopback() && published.port() != 0);
        format!("http://{published}")
    }
    pub(super) fn inspect(&self, service: &str) -> Json {
        serde_json::from_str::<Json>(&self.command(&["container", "inspect", &self.id(service)]))
            .unwrap()[0]
            .clone()
    }
    pub(super) fn exec(&self, service: &str, code: &str) -> String {
        self.command(&["exec", &self.id(service), "python3", "-P", "-c", code])
    }
    pub(super) fn wait_executor(&self, service: &str) {
        self.exec(service, "import socket,time; deadline=time.monotonic()+25\nwhile True:\n try:\n  s=socket.create_connection(('127.0.0.1',8989),1);s.close();break\n except OSError:\n  assert time.monotonic()<deadline,'executor did not listen';time.sleep(.05)\nprint('ready')");
    }
    fn resources(&self, kind: &str) -> Vec<String> {
        let mut args = vec![kind.into(), "ls".into(), "--quiet".into()];
        if kind == "container" {
            args.push("--all".into());
        }
        if kind != "volume" {
            args.push("--no-trunc".into());
        }
        args.extend([
            "--filter".into(),
            format!("label=mrsm.test.owner={}", self.project),
            "--filter".into(),
            format!("label=com.docker.compose.project={}", self.project),
        ]);
        let output = self.output(args, 30);
        if output.code != 0 || output.timed_out {
            return Vec::new();
        }
        String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    }
}
impl Drop for Docker {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "Owned container fixture {} Main diagnostics:{}",
                self.project,
                self.main_log_tails()
            );
        }
        // Query only this unpredictable tpr owner/project, then recheck each immutable ID.
        // Never compose-down/prune or select another project's resources by a generic prefix.
        for kind in ["container", "network", "volume"] {
            for id in self.resources(kind) {
                let output = self.output(vec![kind.into(), "inspect".into(), id.clone()], 20);
                let Ok(values) = serde_json::from_slice::<Json>(&output.stdout) else {
                    continue;
                };
                let value = &values[0];
                let labels = if kind == "container" {
                    &value["Config"]["Labels"]
                } else {
                    &value["Labels"]
                };
                if labels["mrsm.test.owner"] != self.project
                    || labels["com.docker.compose.project"] != self.project
                {
                    continue;
                }
                if kind == "container" && std::thread::panicking() {
                    eprintln!(
                        "Owned container {} ({}) diagnostics:\n{}",
                        id,
                        value["Config"]["Labels"]["com.docker.compose.service"],
                        self.log_tail(&id)
                    );
                }
                let mut args = vec![kind.into(), "rm".into()];
                if kind == "container" {
                    args.push("--force".into());
                }
                args.push(id);
                let _ = self.output(args, 30);
            }
        }
    }
}
pub(super) struct SelfCommand(pub(super) Vec<String>);
impl Drop for SelfCommand {
    fn drop(&mut self) {
        meerschaum_core::self_exe::set_self_command(self.0.clone());
    }
}
