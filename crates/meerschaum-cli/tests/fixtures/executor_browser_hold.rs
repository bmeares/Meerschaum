//! Unregistered test-only bridge for an actual isolated Firefox acceptance run.
//!
//! The owning fixture supplies a newly created console session and keeps its exact servers
//! alive while this bounded file protocol runs. There is no test HTTP route or auth bypass.
//! Select a fresh tpr-<UUID>.localhost origin so the cookie cannot replace a user's cookies
//! for 127.0.0.1 or another local service. Run in a new isolated browser context/tab only.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use uuid::Uuid;

const LIMIT: usize = 4096;
const MAX_CONTROLS: u16 = 16;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    RevokeSource,
    RestoreSource,
    CloseSession,
    Release { passed: bool },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireControl {
    nonce: String,
    sequence: u16,
    command: Action,
}

pub struct Control {
    pub sequence: u16,
    pub action: Action,
}

pub struct BrowserGate {
    root: PathBuf,
    nonce: String,
    next: u16,
    deadline: Instant,
    owned: BTreeSet<PathBuf>,
}

#[derive(Serialize)]
struct Ready<'a> {
    schema: u8,
    origin: &'a str,
    cookie_name: &'static str,
    cookie_value: &'a str,
    pages: &'a [&'a str],
    nonce: &'a str,
    absolute_budget_seconds: u16,
}

fn private_file(root: &Path, name: &str, bytes: &[u8]) -> Result<PathBuf, String> {
    if bytes.len() > LIMIT {
        return Err("Browser control material exceeds its byte limit.".into());
    }
    let path = root.join(name);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
    }
    let mut file = options
        .open(&path)
        .map_err(|_| "Browser control file already exists or cannot be created.")?;
    file.write_all(bytes)
        .map_err(|_| "Could not write the private browser control file.")?;
    Ok(path)
}

impl BrowserGate {
    /// `root` must be an explicit fresh path below the owning test's scratch directory.
    pub fn open(root: &Path, origin: &str, cookie: &str, pages: &[&str]) -> Result<Self, String> {
        let url = reqwest::Url::parse(origin).map_err(|_| "Invalid browser fixture origin.")?;
        let hostname = url.host_str().ok_or("Missing browser fixture hostname.")?;
        let label = hostname
            .strip_prefix("tpr-")
            .and_then(|name| name.strip_suffix(".localhost"))
            .ok_or("Browser fixture requires its own tpr-UUID.localhost hostname.")?;
        if Uuid::parse_str(label).is_err()
            || !matches!(url.scheme(), "http" | "https")
            || url.port().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || !matches!(url.path(), "" | "/")
            || url.query().is_some()
            || url.fragment().is_some()
            || Uuid::parse_str(cookie).is_err()
            || pages.is_empty()
            || pages.len() > 16
            || pages.iter().any(|path| {
                !path.starts_with("/_pages/")
                    || path.len() > 1024
                    || path.contains(['\\', '?', '#'])
                    || path.chars().any(char::is_control)
            })
        {
            return Err("Invalid isolated browser fixture material.".into());
        }
        let parent = root
            .parent()
            .ok_or("The browser scratch path needs a parent.")?;
        if !root.is_absolute()
            || parent.canonicalize().ok().as_deref() != Some(parent)
            || root.file_name().is_none()
            || std::fs::symlink_metadata(root).is_ok()
        {
            return Err(
                "Browser scratch must be a fresh absolute path below a canonical parent.".into(),
            );
        }
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(root)
            .map_err(|_| "Could not create the private browser scratch directory.")?;
        let mut gate = Self {
            root: root.to_path_buf(),
            nonce: Uuid::new_v4().simple().to_string(),
            next: 1,
            deadline: Instant::now() + Duration::from_secs(300),
            owned: BTreeSet::new(),
        };
        let ready = serde_json::to_vec(&Ready {
            schema: 1,
            origin,
            cookie_name: "mrsm-session-id",
            cookie_value: cookie,
            pages,
            nonce: &gate.nonce,
            absolute_budget_seconds: 300,
        })
        .map_err(|_| "Invalid browser ready material.")?;
        let ready_path = private_file(root, "ready.json", &ready)?;
        gate.owned.insert(ready_path);
        Ok(gate)
    }

    /// Return one exact sequenced request. The owner performs the actual source/session
    /// mutation, awaits persistence, then acknowledges; this helper cannot fake the result.
    pub async fn next(&mut self) -> Result<Control, String> {
        if self.next > MAX_CONTROLS {
            return Err("The browser control count exceeds its bound.".into());
        }
        loop {
            if Instant::now() >= self.deadline {
                return Err("The absolute five-minute browser proof budget expired.".into());
            }
            let path = self.root.join("control.json");
            let file = rustix::fs::open(
                &path,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            );
            let fd = match file {
                Ok(fd) => fd,
                Err(rustix::io::Errno::NOENT) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(_) => return Err("The browser control file cannot be opened safely.".into()),
            };
            let file = std::fs::File::from(fd);
            let metadata = file
                .metadata()
                .map_err(|_| "Missing browser control metadata.")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.uid() != rustix::process::geteuid().as_raw()
                    || metadata.mode() & 0o077 != 0
                {
                    return Err("Browser controls must be private to the fixture owner.".into());
                }
            }
            if !metadata.is_file() || metadata.len() > LIMIT as u64 {
                return Err("The browser control file exceeds its type or size bound.".into());
            }
            let mut bytes = Vec::new();
            file.take(LIMIT as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| "Could not read bounded browser control material.")?;
            if bytes.len() > LIMIT {
                return Err("The browser control file grew beyond its byte limit.".into());
            }
            let request: WireControl =
                serde_json::from_slice(&bytes).map_err(|_| "Invalid typed browser control.")?;
            if request.nonce != self.nonce || request.sequence != self.next {
                return Err("The browser control belongs to another run or sequence.".into());
            }
            std::fs::remove_file(&path)
                .map_err(|_| "Could not consume the exact browser control.")?;
            self.next += 1;
            return Ok(Control {
                sequence: request.sequence,
                action: request.command,
            });
        }
    }

    pub fn acknowledge(&mut self, sequence: u16, persisted: bool) -> Result<(), String> {
        if sequence.checked_add(1) != Some(self.next) {
            return Err(
                "Only the most recently consumed browser control can be acknowledged.".into(),
            );
        }
        let bytes =
            serde_json::to_vec(&serde_json::json!({"sequence":sequence,"persisted":persisted}))
                .map_err(|_| "Invalid browser acknowledgement.")?;
        let path = private_file(&self.root, &format!("ack-{sequence}.json"), &bytes)?;
        self.owned.insert(path);
        Ok(())
    }
}

impl Drop for BrowserGate {
    fn drop(&mut self) {
        // Only files created by this object are removed; leave unexpected entries alone.
        for path in &self.owned {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_dir(&self.root);
    }
}
