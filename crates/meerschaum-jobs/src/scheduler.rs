//! Per-root schedule and debounce state (§5.5 "Continuous mode", §6.4).
//!
//! Everything here lives **under one `MRSM_ROOT_DIR`** (`{root}/.internal/schedules` and
//! `{root}/.internal/debounce`), and nothing is process-global: two roots on one host (or two
//! [`RootScheduler`]s in one process) never share a next-fire time, a last-fire time or a
//! debounce window, while two processes on the *same* root do (a restarted supervisor resumes
//! its schedule instead of firing twice, and two triggers for the same key are debounced
//! together).
//!
//! - [`RootScheduler::next_fire`] resolves the next fire time of a job's `-s` schedule. It
//!   resumes the persisted next-fire time when it is still in the future and the schedule text
//!   is unchanged, so a restart inside a long interval does not re-anchor the cadence; a
//!   changed schedule starts over.
//! - [`RootScheduler::record_fire`] persists a fire.
//! - [`RootScheduler::debounce`] is an atomic "may this key fire now?" with a window, shared by
//!   every process on the root (the engine's `system.engine.debounce_seconds` uses the same
//!   semantics in memory; this is the persisted form for separate job processes).

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::daemon::JobsContext;
use crate::schedule::{Fire, JitterSpec, ScheduleError, isoformat, parse_schedule_jittered};

/// Per-root schedule and debounce state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootScheduler {
    root: PathBuf,
}

/// A job's persisted schedule state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleState {
    /// The schedule text the state belongs to.
    pub schedule: String,
    /// The last fire time.
    pub last_fire: Option<Fire>,
    /// The next fire time recorded after it.
    pub next_fire: Option<Fire>,
}

fn encode_key(key: &str) -> String {
    // A reversible, filesystem-safe name (names may contain `/`, `:` and spaces).
    key.bytes().map(|b| format!("{b:02x}")).collect()
}

fn parse_fire(text: &str) -> Option<Fire> {
    DateTime::parse_from_rfc3339(text.trim()).ok()
}

impl RootScheduler {
    /// The scheduler of a context's root.
    pub fn for_context(ctx: &JobsContext) -> RootScheduler {
        RootScheduler::for_root(&ctx.paths.root)
    }

    /// The scheduler of a root directory.
    pub fn for_root(root: impl AsRef<Path>) -> RootScheduler {
        RootScheduler {
            root: root.as_ref().to_path_buf(),
        }
    }

    /// The root this scheduler belongs to.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn schedules_dir(&self) -> PathBuf {
        self.root.join(".internal").join("schedules")
    }

    fn debounce_dir(&self) -> PathBuf {
        self.root.join(".internal").join("debounce")
    }

    fn schedule_path(&self, name: &str) -> PathBuf {
        self.schedules_dir()
            .join(format!("{}.json", encode_key(name)))
    }

    /// The persisted state of a job's schedule, if any.
    pub fn state(&self, name: &str) -> Option<ScheduleState> {
        let text = fs::read_to_string(self.schedule_path(name)).ok()?;
        let value: serde_json::Value = serde_json::from_str(&text).ok()?;
        Some(ScheduleState {
            schedule: value.get("schedule")?.as_str()?.to_string(),
            last_fire: value
                .get("last_fire")
                .and_then(|v| v.as_str())
                .and_then(parse_fire),
            next_fire: value
                .get("next_fire")
                .and_then(|v| v.as_str())
                .and_then(parse_fire),
        })
    }

    /// Persist a fire of `name`'s `schedule` and the next fire time after it.
    pub fn record_fire(
        &self,
        name: &str,
        schedule: &str,
        fired: Fire,
        next: Option<Fire>,
    ) -> std::io::Result<()> {
        fs::create_dir_all(self.schedules_dir())?;
        let value = serde_json::json!({
            "schedule": schedule,
            "last_fire": isoformat(&fired),
            "next_fire": next.map(|n| isoformat(&n)),
        });
        write_atomic(&self.schedule_path(name), value.to_string().as_bytes())
    }

    /// Forget `name`'s schedule state (when the job is deleted or its schedule is changed).
    pub fn clear(&self, name: &str) {
        let _ = fs::remove_file(self.schedule_path(name));
    }

    /// The next fire time of `schedule` after `now`. A persisted next-fire time of the same
    /// schedule text that is still in the future is resumed as is.
    pub fn next_fire(
        &self,
        name: &str,
        schedule: &str,
        now: Fire,
    ) -> Result<Option<Fire>, ScheduleError> {
        self.next_fire_jittered(name, schedule, now, 0.0)
    }

    /// [`RootScheduler::next_fire`] for a schedule with a default jitter: a fresh schedule is
    /// parsed with `default_jitter_seconds` and the job's name as the seed of its fixed offset
    /// (a `jitter` suffix in the text wins). A resumed time already carries its offset.
    pub fn next_fire_jittered(
        &self,
        name: &str,
        schedule: &str,
        now: Fire,
        default_jitter_seconds: f64,
    ) -> Result<Option<Fire>, ScheduleError> {
        if let Some(state) = self.state(name)
            && state.schedule == schedule
            && let Some(next) = state.next_fire.filter(|n| *n > now)
        {
            return Ok(Some(next));
        }
        let spec = JitterSpec {
            default_seconds: default_jitter_seconds,
            seed: name,
        };
        let mut trigger = parse_schedule_jittered(schedule, now, Some(spec))?;
        Ok(trigger.next_after(now))
    }

    /// Whether `key` may fire at `now`: `true` (and the window restarts) when no fire was
    /// recorded for it on this root within `window`; `false` inside the window. Atomic across
    /// processes on this root.
    pub fn debounce(&self, key: &str, window: Duration, now: DateTime<Utc>) -> bool {
        let dir = self.debounce_dir();
        if fs::create_dir_all(&dir).is_err() {
            // Without state we cannot debounce; never block a fire on a read-only root.
            return true;
        }
        let path = dir.join(encode_key(key));
        let Ok(mut file) = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
        else {
            return true;
        };
        #[cfg(unix)]
        let _guard = {
            use nix::fcntl::{Flock, FlockArg};
            let Ok(clone) = file.try_clone() else {
                return true;
            };
            Flock::lock(clone, FlockArg::LockExclusive).ok()
        };
        let mut text = String::new();
        let _ = file.read_to_string(&mut text);
        let last = text
            .trim()
            .parse::<i64>()
            .ok()
            .and_then(DateTime::from_timestamp_micros);
        if let Some(last) = last {
            let elapsed = now.signed_duration_since(last);
            if elapsed >= chrono::TimeDelta::zero()
                && elapsed < chrono::TimeDelta::from_std(window).unwrap_or(chrono::TimeDelta::MAX)
            {
                return false;
            }
        }
        let _ = file.set_len(0);
        let _ = file.seek(SeekFrom::Start(0));
        let _ = write!(file, "{}", now.timestamp_micros());
        true
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fire(s: &str) -> Fire {
        DateTime::parse_from_rfc3339(s).unwrap()
    }

    #[test]
    fn debounce_window_and_keys() {
        let dir = tempfile::tempdir().unwrap();
        let s = RootScheduler::for_root(dir.path());
        let t0 = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let w = Duration::from_secs(10);
        assert!(s.debounce("a", w, t0));
        assert!(!s.debounce("a", w, t0 + chrono::TimeDelta::seconds(9)));
        assert!(s.debounce("b/c d", w, t0 + chrono::TimeDelta::seconds(9)));
        assert!(s.debounce("a", w, t0 + chrono::TimeDelta::seconds(10)));
    }

    #[test]
    fn schedule_resumes_until_changed() {
        let dir = tempfile::tempdir().unwrap();
        let s = RootScheduler::for_root(dir.path());
        let now = fire("2026-01-01T00:00:00+00:00");
        let first = s.next_fire("job", "every 1 hour", now).unwrap().unwrap();
        s.record_fire("job", "every 1 hour", now, Some(first))
            .unwrap();
        // A restart half an hour later resumes the recorded time.
        let later = fire("2026-01-01T00:30:00+00:00");
        assert_eq!(
            s.next_fire("job", "every 1 hour", later).unwrap(),
            Some(first)
        );
        // A changed schedule re-anchors.
        let other = s.next_fire("job", "every 2 hours", later).unwrap().unwrap();
        assert_ne!(other, first);
        s.clear("job");
        assert!(s.state("job").is_none());
    }
}
