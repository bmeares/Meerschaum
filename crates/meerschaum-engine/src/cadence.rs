//! Per-pipe schedules for continuous mode (§5.5): `parameters['schedule']`.
//!
//! The engine does not parse schedules itself (the grammar lives in `meerschaum-jobs`, which
//! sits beside the engine in the crate layering). A host that wants `parameters['schedule']`
//! hands the engine a [`Cadence`] with [`crate::Engine::with_cadence`]; without one the key is
//! ignored and every pipe runs on the loop's `--min-seconds` cadence.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use meerschaum_core::Value;

use crate::node::PipeRef;

/// One pipe's schedule: a stateful sequence of fire times.
pub trait Timer: Send {
    /// The first fire time, from `now`. `None` when the schedule never fires.
    fn first(&mut self, now: DateTime<Utc>) -> Option<DateTime<Utc>>;
    /// The first fire time strictly after `after`. `None` when the schedule is exhausted.
    fn after(&mut self, after: DateTime<Utc>) -> Option<DateTime<Utc>>;
}

/// Parses schedule text (`every 6 hours`, `daily starting 03:00`, ...).
pub trait Cadence: Send + Sync {
    /// A timer for `schedule`, or the parse error.
    fn timer(&self, schedule: &str, now: DateTime<Utc>) -> Result<Box<dyn Timer>, String>;
}

struct Entry {
    text: String,
    timer: Box<dyn Timer>,
    next: Option<DateTime<Utc>>,
    exhausted: bool,
}

/// The schedules of the root nodes of a selection and when each is next due.
#[derive(Default)]
pub struct PipeSchedules {
    entries: HashMap<PipeRef, Entry>,
}

impl std::fmt::Debug for PipeSchedules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.entries
                    .iter()
                    .map(|(r, e)| (r.label(), &e.text, e.next)),
            )
            .finish()
    }
}

/// The schedule text of a pipe's parameters (`parameters['schedule']`), if it is a string.
pub fn schedule_text(parameters: &meerschaum_core::Map) -> Option<String> {
    match parameters.get("schedule") {
        Some(Value::Str(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        _ => None,
    }
}

/// Why each scheduled pipe of a graph run that has parents in it does not follow its
/// `parameters['schedule']`: in a graph run only the pipes without parents in the run follow
/// their schedule; a pipe with parents syncs when they have new data. (Every pipe of a flat
/// selection is a root and follows its schedule.) One message per such pipe, in the run's order.
pub fn ignored_schedules(graph: &crate::Graph, sub: &crate::Subgraph) -> Vec<String> {
    if sub.flat {
        return Vec::new();
    }
    sub.order
        .iter()
        .filter(|r| sub.parent_edges(r).next().is_some())
        .filter_map(|r| {
            let text = schedule_text(&graph.node(r)?.record.parameters)?;
            Some(format!(
                "{}: its schedule '{text}' is ignored because it has parents in this run; it \
                 syncs when its parents have new data.",
                r.label()
            ))
        })
        .collect()
}

impl PipeSchedules {
    /// Parse the schedules of `nodes` (`(node, parameters)` pairs). A schedule that does not
    /// parse is reported in the returned warnings and the pipe keeps the loop's cadence.
    pub fn parse<'a>(
        cadence: &dyn Cadence,
        nodes: impl IntoIterator<Item = (&'a PipeRef, &'a meerschaum_core::Map)>,
        now: DateTime<Utc>,
    ) -> (PipeSchedules, Vec<String>) {
        let mut out = PipeSchedules::default();
        let mut warnings = Vec::new();
        for (r, params) in nodes {
            let Some(text) = schedule_text(params) else {
                continue;
            };
            match cadence.timer(&text, now) {
                Ok(mut timer) => {
                    let next = timer.first(now);
                    out.entries.insert(
                        r.clone(),
                        Entry {
                            text,
                            timer,
                            exhausted: next.is_none(),
                            next,
                        },
                    );
                }
                Err(e) => warnings.push(format!(
                    "{}: ignoring the invalid schedule '{text}': {e}",
                    r.label()
                )),
            }
        }
        (out, warnings)
    }

    /// Every scheduled node.
    pub fn nodes(&self) -> Vec<PipeRef> {
        self.entries.keys().cloned().collect()
    }

    /// Whether no node has a schedule.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The scheduled nodes whose next fire time is still ahead of `now` (held out of the lap).
    pub fn not_due(&self, now: DateTime<Utc>) -> Vec<PipeRef> {
        self.entries
            .iter()
            .filter(|(_, e)| e.exhausted || e.next.is_some_and(|n| n > now))
            .map(|(r, _)| r.clone())
            .collect()
    }

    /// A node ran in a lap at `now`: move its next fire time past `now`.
    pub fn ran(&mut self, node: &PipeRef, now: DateTime<Utc>) {
        if let Some(e) = self.entries.get_mut(node)
            && e.next.is_some_and(|n| n <= now)
        {
            e.next = e.timer.after(now);
            e.exhausted = e.next.is_none();
        }
    }

    /// The earliest upcoming fire time of any node that is not exhausted.
    pub fn next_due(&self) -> Option<DateTime<Utc>> {
        self.entries
            .values()
            .filter(|e| !e.exhausted)
            .filter_map(|e| e.next)
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    struct Every(i64);
    struct EveryTimer {
        secs: i64,
        last: Option<DateTime<Utc>>,
    }
    impl Timer for EveryTimer {
        fn first(&mut self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
            let t = now + TimeDelta::seconds(self.secs);
            self.last = Some(t);
            Some(t)
        }
        fn after(&mut self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
            let mut t = self.last?;
            while t <= after {
                t += TimeDelta::seconds(self.secs);
            }
            self.last = Some(t);
            Some(t)
        }
    }
    impl Cadence for Every {
        fn timer(&self, schedule: &str, _now: DateTime<Utc>) -> Result<Box<dyn Timer>, String> {
            if schedule == "bad" {
                return Err("no".into());
            }
            Ok(Box::new(EveryTimer {
                secs: self.0,
                last: None,
            }))
        }
    }

    #[test]
    fn due_and_advance() {
        let now = Utc::now();
        let r = PipeRef::new("sql:i", "sql:i", "m", None);
        let bad = PipeRef::new("sql:i", "sql:i", "b", None);
        let mut p1 = meerschaum_core::Map::new();
        p1.insert("schedule".into(), Value::Str("every".into()));
        let mut p2 = meerschaum_core::Map::new();
        p2.insert("schedule".into(), Value::Str("bad".into()));
        let (mut s, warnings) = PipeSchedules::parse(&Every(60), [(&r, &p1), (&bad, &p2)], now);
        assert_eq!(warnings.len(), 1);
        assert_eq!(s.not_due(now), vec![r.clone()]);
        let later = now + TimeDelta::seconds(61);
        assert!(s.not_due(later).is_empty());
        s.ran(&r, later);
        assert_eq!(s.not_due(later), vec![r.clone()]);
        assert!(s.next_due().unwrap() > later);
    }
}
