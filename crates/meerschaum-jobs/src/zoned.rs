//! Zone-aware stepping for schedule triggers (v5).
//!
//! A trigger with a [`ScheduleZone`] steps on the zone's wall clock: each trigger produces an
//! ascending sequence of wall times (`anchor + k * interval`, the start's time of day on each
//! calendar step, the wall times a cron expression matches) and a stream turns them into strictly
//! increasing instants, reported with the zone's real offset at each instant.
//!
//! What a wall time becomes (`Policy`):
//!
//! - a wall time that exists once is that instant;
//! - a wall time that occurs twice (the clock falls back) is its first occurrence, or both
//!   occurrences for an interval no longer than the repeated period and a cron hour field that is
//!   a wildcard or a step, so the real cadence through the repeated period is unbroken;
//! - a wall time that does not exist (the clock springs forward) is skipped for an interval no
//!   longer than the gap and a cron hour field that is a wildcard or a step (the next wall time
//!   keeps the cadence), and otherwise fired once at the first instant after the gap (`daily`,
//!   `monthly`, `every 12 hours`, a specific cron hour).
//!
//! The wall-time sequences are ascending but their instants need not be (inside a repeated
//! period the second occurrences follow the first ones), so the stream keeps a small heap and
//! emits an instant only once no later wall time can resolve to an earlier one.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use chrono::{DateTime, Duration, LocalResult, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;

use crate::schedule::Fire;

/// The zone a schedule steps in: `tz local` (or a host-zone abbreviation after `starting`), or a
/// named IANA zone (`tz America/New_York`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleZone {
    tz: Tz,
    host: bool,
}

/// What a wall time is in a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WallTime {
    /// The wall time exists once.
    Single(Fire),
    /// The wall time occurs twice (the clock falls back): both instants, earliest first.
    Repeated(Fire, Fire),
    /// The wall time does not exist (the clock springs forward): the first instant after the
    /// gap and the length of the gap in microseconds.
    Missing(Fire, i64),
}

impl ScheduleZone {
    /// A named zone.
    pub fn named(tz: Tz) -> ScheduleZone {
        ScheduleZone { tz, host: false }
    }

    /// The host's zone (`tz local`).
    pub fn host(tz: Tz) -> ScheduleZone {
        ScheduleZone { tz, host: true }
    }

    /// The IANA zone.
    pub fn tz(&self) -> Tz {
        self.tz
    }

    /// The canonical IANA name (`America/New_York`).
    pub fn name(&self) -> &'static str {
        self.tz.name()
    }

    /// Whether this is the host's zone (`tz local`).
    pub fn is_host(&self) -> bool {
        self.host
    }

    /// `instant` with this zone's offset at that instant.
    pub fn fire(&self, instant: DateTime<Utc>) -> Fire {
        instant.with_timezone(&self.tz).fixed_offset()
    }

    /// The wall time of `instant` in this zone.
    pub fn wall(&self, instant: &Fire) -> NaiveDateTime {
        instant.with_timezone(&self.tz).naive_local()
    }

    fn offset_at_utc(&self, utc: NaiveDateTime) -> i64 {
        i64::from(
            self.tz
                .offset_from_utc_datetime(&utc)
                .fix()
                .local_minus_utc(),
        )
    }

    /// What `wall` is in this zone.
    pub fn resolve(&self, wall: NaiveDateTime) -> WallTime {
        match self.tz.from_local_datetime(&wall) {
            LocalResult::Single(dt) => WallTime::Single(dt.fixed_offset()),
            LocalResult::Ambiguous(a, b) => {
                let (a, b) = (a.fixed_offset(), b.fixed_offset());
                if a <= b {
                    WallTime::Repeated(a, b)
                } else {
                    WallTime::Repeated(b, a)
                }
            }
            LocalResult::None => self.after_gap(wall),
        }
    }

    /// The transition that skips `wall`: the first instant whose wall time is later than `wall`,
    /// found by bisection over whole seconds (transitions fall on whole seconds).
    fn after_gap(&self, wall: NaiveDateTime) -> WallTime {
        let local_of = |secs: i64| -> Option<NaiveDateTime> {
            let utc = DateTime::from_timestamp(secs, 0)?.naive_utc();
            Some(utc + Duration::seconds(self.offset_at_utc(utc)))
        };
        let center = wall.and_utc().timestamp();
        let (mut lo, mut hi) = (center - 30 * 3600, center + 30 * 3600);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            match local_of(mid) {
                Some(local) if local > wall => hi = mid,
                _ => lo = mid,
            }
        }
        let instant = DateTime::from_timestamp(hi, 0).unwrap_or_default();
        let before = self.offset_at_utc(instant.naive_utc() - Duration::seconds(1));
        let after = self.offset_at_utc(instant.naive_utc());
        WallTime::Missing(self.fire(instant), (after - before).max(0) * 1_000_000)
    }

    /// The instant a start written as `wall` names: its only or first occurrence, or the first
    /// instant after the gap.
    pub fn start_instant(&self, wall: NaiveDateTime) -> Fire {
        match self.resolve(wall) {
            WallTime::Single(f) | WallTime::Repeated(f, _) | WallTime::Missing(f, _) => f,
        }
    }

    /// The largest offset (seconds east of UTC) this zone has within a day of `wall`.
    fn max_offset_near(&self, wall: NaiveDateTime) -> i64 {
        [-26, 0, 26]
            .into_iter()
            .map(|h| self.offset_at_utc(wall + Duration::hours(h)))
            .max()
            .unwrap_or(0)
    }
}

/// What a repeated wall time fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fold {
    /// The first occurrence only.
    First,
    /// Both occurrences.
    Both,
    /// Both occurrences when the interval (microseconds) is no longer than the repeated period.
    BothWithin(i64),
}

/// What a missing wall time fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gap {
    /// Nothing.
    Skip,
    /// The first instant after the gap.
    Shift,
    /// Nothing when the interval (microseconds) is no longer than the gap (the next wall time
    /// after the gap keeps the real cadence), else the first instant after the gap.
    SkipWithin(i64),
}

/// How a trigger's wall times become instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Policy {
    /// What a missing wall time fires.
    pub gap: Gap,
    /// What a repeated wall time fires.
    pub fold: Fold,
}

impl Policy {
    /// An interval of `step_us` microseconds (`every 5 minutes`, `daily`, `weekly`).
    pub fn interval(step_us: i64) -> Policy {
        Policy {
            gap: Gap::SkipWithin(step_us),
            fold: Fold::BothWithin(step_us),
        }
    }

    /// Calendar steps (`monthly`, `yearly`).
    pub fn calendar() -> Policy {
        Policy {
            gap: Gap::Shift,
            fold: Fold::First,
        }
    }

    /// A cron expression: a specific hour fires once (first occurrence, or just after the gap);
    /// a wildcard or stepped hour fires through a repeated hour twice and not inside a gap.
    pub fn cron(wildcard_hour: bool) -> Policy {
        Policy {
            gap: if wildcard_hour { Gap::Skip } else { Gap::Shift },
            fold: if wildcard_hour {
                Fold::Both
            } else {
                Fold::First
            },
        }
    }

    /// The instants `wall` fires at in `zone`, earliest first (none, one or two).
    pub fn instants(&self, zone: &ScheduleZone, wall: NaiveDateTime) -> Vec<Fire> {
        match zone.resolve(wall) {
            WallTime::Single(f) => vec![f],
            // PARITY: v4's `hourly` skips the second occurrence (wall-clock `+ timedelta` keeps
            // `fold=0`) and its cron fires a specific hour in both (absolute stepping); v5 fires
            // both only for a short interval or a wildcard hour (`PARITY.md` 51-SCHEDULE-TZNAME).
            WallTime::Repeated(a, b) => {
                let both = match self.fold {
                    Fold::First => false,
                    Fold::Both => true,
                    Fold::BothWithin(step) => step <= (b - a).num_microseconds().unwrap_or(0),
                };
                if both { vec![a, b] } else { vec![a] }
            }
            // PARITY: v4 reads a missing wall time with the offset before the gap (`02:30-04:00`,
            // an hour before the clock springs forward) or skips it (cron, `monthly`); v5 fires
            // after the gap or skips it by this policy (`PARITY.md` 51-SCHEDULE-TZNAME).
            WallTime::Missing(after, gap_us) => {
                let shift = match self.gap {
                    Gap::Skip => false,
                    Gap::Shift => true,
                    Gap::SkipWithin(step) => step > gap_us,
                };
                if shift { vec![after] } else { Vec::new() }
            }
        }
    }
}

/// A source of ascending wall times.
pub(crate) trait WallSource {
    /// The next wall time, or `None` when the schedule is exhausted.
    fn next_wall(&mut self) -> Option<NaiveDateTime>;
}

/// Turns a trigger's ascending wall times into strictly increasing instants.
#[derive(Debug, Clone)]
pub(crate) struct Stream {
    pub zone: ScheduleZone,
    pub policy: Policy,
    /// Never emit an instant earlier than this (the start).
    floor: Option<Fire>,
    /// Emit only instants later than this (the last fire, or `after`).
    last: Option<Fire>,
    /// Resolved instants not emitted yet, earliest first.
    buffer: BinaryHeap<Reverse<Fire>>,
    /// The latest wall time pulled from the source.
    last_wall: Option<NaiveDateTime>,
    exhausted: bool,
}

impl Stream {
    pub fn new(zone: ScheduleZone, policy: Policy) -> Stream {
        Stream {
            zone,
            policy,
            floor: None,
            last: None,
            buffer: BinaryHeap::new(),
            last_wall: None,
            exhausted: false,
        }
    }

    /// Start over: emit nothing earlier than `floor` or not later than `last`; `seed` is an
    /// extra instant to consider (the start, when it is an occurrence of its own wall time).
    pub fn reset(&mut self, floor: Option<Fire>, last: Option<Fire>, seed: Option<Fire>) {
        self.floor = floor;
        self.last = last;
        self.buffer = seed.into_iter().map(Reverse).collect();
        self.last_wall = None;
        self.exhausted = false;
    }

    /// The next instant.
    pub fn next(&mut self, source: &mut impl WallSource) -> Option<Fire> {
        loop {
            match self.buffer.peek().map(|Reverse(f)| *f) {
                Some(fire) if self.exhausted || self.settled(&fire) => {
                    self.buffer.pop();
                    if self.last.is_some_and(|l| fire <= l) || self.floor.is_some_and(|f| fire < f)
                    {
                        continue;
                    }
                    self.last = Some(fire);
                    return Some(fire);
                }
                None if self.exhausted => return None,
                _ => {}
            }
            match source.next_wall() {
                None => self.exhausted = true,
                Some(wall) => {
                    self.last_wall = Some(wall);
                    self.buffer.extend(
                        self.policy
                            .instants(&self.zone, wall)
                            .into_iter()
                            .map(Reverse),
                    );
                }
            }
        }
    }

    /// Whether no wall time at or after the latest one pulled can resolve to an instant earlier
    /// than `fire`.
    fn settled(&self, fire: &Fire) -> bool {
        let Some(wall) = self.last_wall else {
            return false;
        };
        let bound = wall - Duration::seconds(self.zone.max_offset_near(wall));
        fire.naive_utc() <= bound
    }
}

/// `fire`, when it is an occurrence of `wall` in `zone` (a start written as an abbreviation of
/// the zone names one; a start read from an instant always is).
pub(crate) fn occurrence_of(zone: &ScheduleZone, wall: NaiveDateTime, fire: Fire) -> Option<Fire> {
    match zone.resolve(wall) {
        WallTime::Single(f) => (f == fire).then_some(f),
        WallTime::Repeated(a, b) => [a, b].into_iter().find(|f| *f == fire),
        WallTime::Missing(..) => None,
    }
}
