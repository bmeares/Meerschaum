//! A saved, single-registration job retains the input words separately from each admitted epoch.
use super::{ApiError, DirectAction, PipeOperation};
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};

fn refused() -> ApiError {
    ApiError::unavailable(
        "This stored graph needs one exact registered destination and a supported native argument set for every launch.",
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredIntent {
    original: Vec<String>,
}
/// Repeating work is dispatched by Main. These options never enter a one-shot worker's argv.
#[derive(Clone, Debug, PartialEq)]
pub enum Cadence {
    Schedule {
        expression: String,
        jitter: Option<f64>,
    },
    Loop {
        min_seconds: f64,
    },
    Restart,
}
impl StoredIntent {
    pub fn capture(words: &[String]) -> Result<Self, ApiError> {
        let intent = Self {
            original: words.to_vec(),
        };
        intent.action()?;
        Ok(intent)
    }
    pub fn original(&self) -> &[String] {
        &self.original
    }

    pub fn request(&self) -> Result<PipeOperation, ApiError> {
        self.action()?.request()
    }
    pub fn action(&self) -> Result<DirectAction, ApiError> {
        self.parse().map(|(action, _)| action)
    }

    pub fn cadence(&self) -> Result<Option<Cadence>, ApiError> {
        self.parse().map(|(_, cadence)| cadence)
    }

    /// The dedicated Team worker uses its configured data service and an approved plugin.
    /// Keep the ordinary single-pipe argv intact, including optional location and bounds.
    pub fn team_sync_target(&self) -> Result<(String, String), ApiError> {
        let (action, cadence) = self.parse()?;
        if cadence.is_some() || self.original.first().map(String::as_str) != Some("sync") {
            return Err(refused());
        }
        let instance = action.selector("mrsm_instance")?.ok_or_else(refused)?;
        let plugin = action
            .selector("connector_keys")?
            .and_then(|keys| keys.strip_prefix("plugin:"))
            .filter(|name| crate::executor::plugins::valid_name(name))
            .ok_or_else(refused)?;
        Ok((instance.to_owned(), plugin.to_owned()))
    }

    fn parse(&self) -> Result<(DirectAction, Option<Cadence>), ApiError> {
        let [action, pipes, options @ ..] = self.original.as_slice() else {
            return Err(refused());
        };
        if !matches!(action.as_str(), "sync" | "verify")
            || pipes != "pipes"
            || options.len() > 20
            || self
                .original
                .iter()
                .any(|word| word.len() > 512 || word.chars().any(char::is_control))
        {
            return Err(refused());
        }
        let mut keywords = Map::new();
        keywords.insert(
            "action".into(),
            Value::List(vec![Value::Str("pipes".into())]),
        );
        let mut iter = options.iter();
        let mut schedule = None;
        let mut repeat = false;
        let mut restart = false;
        let mut jitter = None;
        let mut min_seconds = None;
        while let Some(word) = iter.next() {
            let (flag, inline) = word
                .split_once('=')
                .map_or((word.as_str(), None), |(k, v)| (k, Some(v)));
            if matches!(flag, "--loop" | "--restart") {
                let value = if flag == "--loop" {
                    &mut repeat
                } else {
                    &mut restart
                };
                if inline.is_some() || std::mem::replace(value, true) {
                    return Err(refused());
                }
                continue;
            }
            let value = match inline {
                Some(text) => text,
                None => iter.next().map(String::as_str).ok_or_else(refused)?,
            };
            if value.is_empty()
                || (inline.is_none() && value.starts_with('-') && value.parse::<i64>().is_err())
            {
                return Err(refused());
            }
            match flag {
                "-s" | "--schedule" => {
                    if schedule.replace(value.to_owned()).is_some() {
                        return Err(refused());
                    }
                    continue;
                }
                "--min-seconds" | "--jitter" => {
                    let slot = if flag == "--jitter" {
                        &mut jitter
                    } else {
                        &mut min_seconds
                    };
                    let seconds = value
                        .parse::<f64>()
                        .ok()
                        .filter(|n| n.is_finite() && *n >= 0.0)
                        .ok_or_else(refused)?;
                    if slot.replace(seconds).is_some() {
                        return Err(refused());
                    }
                    continue;
                }
                _ => {}
            }
            let (key, list) = match flag {
                "--connector-keys" | "-c" => ("connector_keys", true),
                "--metric-keys" | "-m" => ("metric_keys", true),
                "--location-keys" | "-l" => ("location_keys", true),
                "--mrsm-instance" | "-i" => ("mrsm_instance", false),
                "--begin" => ("begin", false),
                "--end" => ("end", false),
                _ => return Err(refused()),
            };
            let value = Value::Str(value.into());
            if keywords
                .insert(
                    key.into(),
                    if list {
                        Value::List(vec![value])
                    } else {
                        value
                    },
                )
                .is_some()
            {
                return Err(refused());
            }
        }
        let cadence = match (schedule, repeat, min_seconds, jitter, restart) {
            (Some(expression), false, None, jitter, _) => {
                Some(Cadence::Schedule { expression, jitter })
            }
            (None, true, seconds, None, _) => Some(Cadence::Loop {
                min_seconds: seconds.unwrap_or(1.0),
            }),
            (None, false, None, None, true) => Some(Cadence::Restart),
            (None, false, None, None, false) => None,
            _ => return Err(refused()),
        };
        let action = DirectAction::capture(action, &keywords)?.ok_or_else(refused)?;
        Ok((action, cadence))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredPipe {
    pub intent: StoredIntent,
    pub normalized_at: i64,
    pub pipe: PipeOperation,
}
impl StoredPipe {
    /// Host and remote supervisors keep their original physical job definition. Their command
    /// selects only this node. The fresh Ready/Verify instructions own the actual data bounds.
    pub fn worker_args(&self) -> Result<Vec<String>, ApiError> {
        let mut selector = self.intent.action()?.request_for(&self.pipe.pipe_ref)?;
        if selector.pipe_ref != self.pipe.pipe_ref
            || selector.verify != self.pipe.verify
            || self.pipe.parents.is_some()
            || self.pipe.children.is_some()
            || self.pipe.registration.is_null()
        {
            return Err(refused());
        }
        selector.registration = self.pipe.registration.clone();
        selector.begin = None;
        selector.end = None;
        selector.sysargs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stored_argv_is_immutable_while_worker_definition_is_stable_across_epochs() {
        let original = [
            "verify",
            "pipes",
            "-c",
            "sql:source",
            "-m",
            "events",
            "-l",
            "None",
            "-i",
            "sql:main",
            "--begin=1 day ago",
            "--end=now",
        ]
        .map(str::to_string);
        let intent = StoredIntent::capture(&original).unwrap();
        let mut pipe = intent.request().unwrap();
        pipe.registration = Value::Int(1);
        pipe.begin = Some("2026-11-15T00:00:00".into());
        pipe.end = Some("2026-11-16T00:00:00".into());
        let first = StoredPipe {
            intent,
            normalized_at: 1,
            pipe,
        };
        let mut next = first.clone();
        next.normalized_at = 2;
        next.pipe.begin = first.pipe.end.clone();
        next.pipe.end = Some("2026-11-17T00:00:00".into());
        assert_eq!(first.worker_args().unwrap(), next.worker_args().unwrap());
        assert!(
            next.worker_args()
                .unwrap()
                .iter()
                .all(|word| !word.starts_with("--begin") && !word.starts_with("--end"))
        );
        assert_eq!(next.intent.original(), original.as_slice());
        assert_ne!(first.pipe.sysargs().unwrap(), next.pipe.sysargs().unwrap());
        next.pipe.pipe_ref = "sql:main|sql:source|sibling|None".into();
        assert!(next.worker_args().is_err());
    }
    #[test]
    fn scheduling_arguments_are_separate_from_the_one_shot_command() {
        let raw = [
            "sync",
            "pipes",
            "--connector-keys=x",
            "--metric-keys=y",
            "--location-keys=None",
            "--mrsm-instance=sql:main",
        ]
        .map(str::to_string);
        assert!(StoredIntent::capture(&raw).is_ok());
        for tail in [
            vec!["--schedule=hourly"],
            vec!["--loop", "--min-seconds=3"],
            vec!["--restart"],
        ] {
            let mut words = raw.to_vec();
            words.extend(tail.into_iter().map(str::to_string));
            let intent = StoredIntent::capture(&words).unwrap();
            assert!(intent.cadence().unwrap().is_some());
            assert_eq!(
                intent.request().unwrap(),
                StoredIntent::capture(&raw).unwrap().request().unwrap()
            );
        }
        for suffix in [
            vec!["--schedule=hourly", "--loop"],
            vec!["--min-seconds=4"],
            vec!["+", "show", "version"],
            vec!["--end=5", "--end=8"],
            vec!["--params={}"],
        ] {
            let mut words = raw.to_vec();
            words.extend(suffix.into_iter().map(str::to_string));
            assert!(StoredIntent::capture(&words).is_err());
        }
    }
}
