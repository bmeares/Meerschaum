//! Port of `meerschaum/utils/schedule.py`: the `-s` / `--schedule` grammar and its triggers.
//!
//! v4.1.4 ships its own triggers (APScheduler is no longer used): `_IntervalTrigger`,
//! `_CalendarIntervalTrigger`, `_CronTrigger`, `_OrTrigger` and `_AndTrigger`. Times are aware
//! datetimes with a fixed UTC offset; all arithmetic is done in whole microseconds, which is what
//! Python's `datetime.fromtimestamp(ts + step, tz)` round-trips to.
//!
//! A schedule may carry a zone (v5's `tz <zone>` clause, or an abbreviation of the host's zone
//! after `starting`, which v4 reads as `tzlocal()`): its triggers then step on that zone's wall
//! clock (see [`crate::zoned`]) and report each fire time with the zone's offset at that instant.
//! Without one they keep the fixed offset of the start.

use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone,
    Timelike,
};
use chrono_tz::Tz;
use meerschaum_core::pyfmt::{PyDateTime, parse_py_float, parse_py_int};
use regex::Regex;
use std::sync::Arc;

use crate::local_zone::LocalZone;
use crate::zoned::{Policy, ScheduleZone, Stream, WallSource, occurrence_of};

/// An aware datetime with a fixed offset (`datetime` with `tzinfo`).
pub type Fire = DateTime<FixedOffset>;

/// `STARTING_KEYWORD`.
pub const STARTING_KEYWORD: &str = "starting";
/// `INTERVAL_UNITS`.
pub const INTERVAL_UNITS: [&str; 7] = [
    "months", "weeks", "days", "hours", "minutes", "seconds", "years",
];
/// `CRON_DAYS_OF_WEEK`.
pub const CRON_DAYS_OF_WEEK: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
/// `CRON_MONTHS`.
pub const CRON_MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];

/// `SCHEDULE_ALIASES`, in dict order (`FREQUENCY_ALIASES`, `LOGIC_ALIASES`,
/// `CRON_DAYS_OF_WEEK_ALIASES`, `CRON_MONTHS_ALIASES`).
pub const SCHEDULE_ALIASES: [(&str, &str); 38] = [
    ("daily", "every 1 day"),
    ("hourly", "every 1 hour"),
    ("minutely", "every 1 minute"),
    ("weekly", "every 1 week"),
    ("monthly", "every 1 month"),
    ("secondly", "every 1 second"),
    ("yearly", "every 1 year"),
    ("and", "&"),
    ("or", "|"),
    (" through ", "-"),
    (" thru ", "-"),
    (" - ", "-"),
    ("monday", "mon"),
    ("tuesday", "tue"),
    ("tues", "tue"),
    ("wednesday", "wed"),
    ("thursday", "thu"),
    ("thurs", "thu"),
    ("friday", "fri"),
    ("saturday", "sat"),
    ("sunday", "sun"),
    ("january", "jan"),
    ("february", "feb"),
    ("march", "mar"),
    ("april", "apr"),
    ("may", "may"),
    ("june", "jun"),
    ("july", "jul"),
    ("august", "aug"),
    ("september", "sep"),
    ("october", "oct"),
    ("november", "nov"),
    ("december", "dec"),
    // Padding entries keep the array length fixed; replacing a string by itself is a no-op.
    ("", ""),
    ("", ""),
    ("", ""),
    ("", ""),
    ("", ""),
];

/// A schedule error (Python raises `ValueError`, or `TypeError` from `timedelta`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleError {
    /// A frequency error: the message as Python's `str(e)`.
    #[error("{0}")]
    Frequency(String),
    /// A starting-time error, raised through `meerschaum.utils.warnings.error`.
    #[error("{0}")]
    StartTime(String),
}

impl ScheduleError {
    /// `str(e)` as Python reports it: starting-time errors pass through `error()`, which formats
    /// them as `\n\x1b[1;31m 🛑 <message>\x1b[0m`.
    pub fn py_message(&self) -> String {
        match self {
            ScheduleError::Frequency(m) => m.clone(),
            ScheduleError::StartTime(m) => format!("\n\x1b[1;31m 🛑 {m}\x1b[0m"),
        }
    }
}

fn freq(msg: impl Into<String>) -> ScheduleError {
    ScheduleError::Frequency(msg.into())
}

fn invalid_literal(s: &str) -> String {
    format!(
        "invalid literal for int() with base 10: {}",
        meerschaum_core::pyfmt::str_repr(s)
    )
}

fn add_us(dt: Fire, us: i64) -> Fire {
    dt + Duration::microseconds(us)
}

/// `fromtimestamp(datetime.combine(date, timetz).timestamp(), tz)` for a fixed offset.
fn combine(date: NaiveDate, time: NaiveTime, tz: FixedOffset) -> Option<Fire> {
    tz.from_local_datetime(&NaiveDateTime::new(date, time))
        .single()
}

/// `_IntervalTrigger`.
#[derive(Debug, Clone)]
pub struct IntervalTrigger {
    start_time: Fire,
    interval_us: i64,
    last: Option<Fire>,
    zoned: Option<Box<Zoned<IntervalWalls>>>,
}

impl IntervalTrigger {
    fn new(start_time: Fire, interval_us: i64) -> Result<Self, ScheduleError> {
        if interval_us <= 0 {
            return Err(freq("The time interval must be positive"));
        }
        Ok(IntervalTrigger {
            start_time,
            interval_us,
            last: None,
            zoned: None,
        })
    }

    /// Step on `zone`'s wall clock from `start` (see [`crate::zoned`]).
    fn new_zoned(
        start: &Start,
        zone: ScheduleZone,
        interval_us: i64,
    ) -> Result<Self, ScheduleError> {
        let mut trigger = IntervalTrigger::new(start.fire, interval_us)?;
        let walls = IntervalWalls {
            anchor: start.wall,
            step_us: interval_us,
            k: 0,
        };
        trigger.zoned = Some(Box::new(Zoned::new(
            zone,
            Policy::interval(interval_us),
            start,
            walls,
            true,
        )));
        Ok(trigger)
    }

    fn next(&mut self) -> Option<Fire> {
        if let Some(z) = &mut self.zoned {
            return z.next();
        }
        let next = match self.last {
            None => self.start_time,
            Some(last) => add_us(last, self.interval_us),
        };
        self.last = Some(next);
        Some(next)
    }

    fn next_after(&mut self, after: Fire) -> Option<Fire> {
        if let Some(z) = &mut self.zoned {
            // PARITY: v4 returns `start + timedelta(0)`, which drops `fold`: for a start written
            // as the second occurrence of a repeated wall time that is an hour before the start.
            if after < z.start {
                return z.restart();
            }
            // Resume the wall-clock steps a few intervals before `after` (a repeated period can
            // put a later step's instant before an earlier one's); the stream drops the rest.
            let step = self.interval_us;
            let wall_after = z.stream.zone.wall(&after);
            let delta = (wall_after - z.walls.anchor)
                .num_microseconds()
                .unwrap_or(i64::MAX);
            let margin = 3 * 3_600_000_000 / step + 1;
            z.walls.k = (delta.div_euclid(step) - margin).max(0);
            let seed = if z.walls.k == 0 { z.seed } else { None };
            z.stream.reset(Some(z.start), Some(after), seed);
            return z.next();
        }
        let diff = (after - self.start_time)
            .num_microseconds()
            .unwrap_or(i64::MAX);
        let intervals = (diff.div_euclid(self.interval_us) + 1).max(0);
        let next = add_us(self.start_time, self.interval_us.saturating_mul(intervals));
        self.last = Some(next);
        Some(next)
    }
}

/// The wall times of a zoned interval: `anchor + k * step`.
#[derive(Debug, Clone)]
struct IntervalWalls {
    anchor: NaiveDateTime,
    step_us: i64,
    k: i64,
}

impl WallSource for IntervalWalls {
    fn next_wall(&mut self) -> Option<NaiveDateTime> {
        let offset = Duration::microseconds(self.step_us.checked_mul(self.k)?);
        let wall = self.anchor.checked_add_signed(offset)?;
        self.k += 1;
        (wall.year() <= 9999).then_some(wall)
    }
}

/// A zoned trigger's state: its wall-time source, the stream that resolves it, and its start.
#[derive(Debug, Clone)]
struct Zoned<W: WallSource + Clone> {
    stream: Stream,
    walls: W,
    /// The first source state, for a restart.
    initial: W,
    /// The start instant: nothing fires before it.
    start: Fire,
    /// The start itself, when it is an occurrence of its wall time (it fires first).
    seed: Option<Fire>,
}

impl<W: WallSource + Clone> Zoned<W> {
    /// `seeded`: the start is the first fire (intervals and calendar steps; a cron start fires
    /// only when it matches).
    fn new(zone: ScheduleZone, policy: Policy, start: &Start, walls: W, seeded: bool) -> Self {
        let seed = seeded
            .then(|| occurrence_of(&zone, start.wall, start.fire))
            .flatten();
        let mut stream = Stream::new(zone, policy);
        stream.reset(Some(start.fire), None, seed);
        Zoned {
            stream,
            initial: walls.clone(),
            walls,
            start: start.fire,
            seed,
        }
    }

    fn next(&mut self) -> Option<Fire> {
        self.stream.next(&mut self.walls)
    }

    /// Start over from the first fire time and return it.
    fn restart(&mut self) -> Option<Fire> {
        self.walls = self.initial.clone();
        self.stream.reset(Some(self.start), None, self.seed);
        self.next()
    }
}

/// A schedule's start: the instant it names and its wall time in the schedule's zone (the
/// written time when it was read in the zone, else the instant's wall time there).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Start {
    fire: Fire,
    wall: NaiveDateTime,
}

/// `_CalendarIntervalTrigger`: month / year steps which keep the start's day and skip invalid
/// dates.
#[derive(Debug, Clone)]
pub struct CalendarIntervalTrigger {
    start_date: NaiveDate,
    tz: FixedOffset,
    time: NaiveTime,
    years: i32,
    months: i32,
    last_date: Option<NaiveDate>,
    zoned: Option<Box<Zoned<CalendarWalls>>>,
}

/// The date `years` and `months` after `prev`, keeping its day and skipping months without it.
fn calendar_step(prev: NaiveDate, years: i32, months: i32) -> Option<NaiveDate> {
    let (mut year, mut month) = (prev.year(), prev.month() as i32);
    loop {
        month += months;
        year += years + (month - 1).div_euclid(12);
        month = (month - 1).rem_euclid(12) + 1;
        if year > 9999 {
            return None;
        }
        if let Some(d) = NaiveDate::from_ymd_opt(year, month as u32, prev.day()) {
            return Some(d);
        }
    }
}

impl CalendarIntervalTrigger {
    fn new(start_time: Fire, years: i64, months: i64) -> Result<Self, ScheduleError> {
        if years < 0 || months < 0 || (years == 0 && months == 0) {
            return Err(freq("The calendar interval must be positive"));
        }
        Ok(CalendarIntervalTrigger {
            start_date: start_time.date_naive(),
            tz: *start_time.offset(),
            time: start_time.time(),
            years: years as i32,
            months: months as i32,
            last_date: None,
            zoned: None,
        })
    }

    /// Step on `zone`'s wall clock: the start's wall time on each calendar step.
    fn new_zoned(
        start: &Start,
        zone: ScheduleZone,
        years: i64,
        months: i64,
    ) -> Result<Self, ScheduleError> {
        let mut trigger = CalendarIntervalTrigger::new(start.fire, years, months)?;
        let walls = CalendarWalls {
            start_date: start.wall.date(),
            time: start.wall.time(),
            years: years as i32,
            months: months as i32,
            last_date: None,
        };
        trigger.zoned = Some(Box::new(Zoned::new(
            zone,
            Policy::calendar(),
            start,
            walls,
            true,
        )));
        Ok(trigger)
    }

    fn next(&mut self) -> Option<Fire> {
        if let Some(z) = &mut self.zoned {
            return z.next();
        }
        let next_date = match self.last_date {
            None => self.start_date,
            Some(prev) => calendar_step(prev, self.years, self.months)?,
        };
        // PARITY: the `candidate.timetz() != self._time` DST check never fires for fixed offsets.
        let candidate = combine(next_date, self.time, self.tz)?;
        self.last_date = Some(next_date);
        Some(candidate)
    }

    fn next_after(&mut self, after: Fire) -> Option<Fire> {
        if let Some(z) = &mut self.zoned {
            // `next_after` walks from the start, as v4 does.
            z.walls = z.initial.clone();
            z.stream.reset(Some(z.start), Some(after), z.seed);
            return z.next();
        }
        self.last_date = None;
        let mut candidate = self.next()?;
        while candidate <= after {
            candidate = self.next()?;
        }
        Some(candidate)
    }
}

/// The wall times of a zoned calendar interval.
#[derive(Debug, Clone)]
struct CalendarWalls {
    start_date: NaiveDate,
    time: NaiveTime,
    years: i32,
    months: i32,
    last_date: Option<NaiveDate>,
}

impl WallSource for CalendarWalls {
    fn next_wall(&mut self) -> Option<NaiveDateTime> {
        let date = match self.last_date {
            None => self.start_date,
            Some(prev) => calendar_step(prev, self.years, self.months)?,
        };
        self.last_date = Some(date);
        Some(NaiveDateTime::new(date, self.time))
    }
}

/// `_expand_cron_field(expression, minimum, maximum, names)`.
fn expand_cron_field(
    expression: &str,
    minimum: i64,
    maximum: i64,
    names: &[&str],
) -> Result<Vec<i64>, ScheduleError> {
    let invalid = || freq(format!("Invalid cron field '{expression}'."));
    let as_int = |value: &str| -> Result<i64, ScheduleError> {
        let lower = value.to_lowercase();
        // PARITY: the default `int(value)` is evaluated eagerly.
        let default = {
            let stripped = value.trim_start_matches('-');
            if !stripped.is_empty() && stripped.chars().all(|c| c.is_ascii_digit()) {
                parse_py_int(value).ok_or_else(|| freq(invalid_literal(value)))?
            } else {
                -1
            }
        };
        Ok(names
            .iter()
            .position(|n| *n == lower)
            .map(|i| i as i64 + minimum)
            .unwrap_or(default))
    };
    let mut values = Vec::new();
    for part in expression.to_lowercase().split(',') {
        let (range_part, separator, step_str) = match part.split_once('/') {
            Some((r, s)) => (r, true, s),
            None => (part, false, ""),
        };
        let step = if !step_str.is_empty() {
            parse_py_int(step_str).ok_or_else(|| freq(invalid_literal(step_str)))?
        } else {
            1
        };
        if step <= 0 {
            return Err(freq(format!("Invalid cron step '{step}'.")));
        }
        let (start, stop) = if range_part == "*" {
            (minimum, maximum)
        } else if let Some((a, b)) = range_part.split_once('-') {
            (as_int(a)?, as_int(b)?)
        } else {
            let start = as_int(range_part)?;
            (start, if separator { maximum } else { start })
        };
        if start < minimum || stop > maximum || start > stop {
            return Err(invalid());
        }
        let mut v = start;
        while v <= stop {
            values.push(v);
            v += step;
        }
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

/// `_expand_cron_weekdays(expression)`: Python weekdays (Monday = 0), with cron's 0 / 7 = Sunday.
fn expand_cron_weekdays(expression: &str) -> Result<Vec<u32>, ScheduleError> {
    let invalid = || freq(format!("Invalid cron field '{expression}'."));
    let as_cron_int = |value: &str| -> Result<i64, ScheduleError> {
        let lower = value.to_lowercase();
        if lower == "sun" {
            return Ok(0);
        }
        if let Some(i) = CRON_DAYS_OF_WEEK[..6].iter().position(|n| *n == lower) {
            return Ok(i as i64 + 1);
        }
        if !value.is_empty() && value.chars().all(|c| c.is_ascii_digit()) {
            return parse_py_int(value).ok_or_else(|| freq(invalid_literal(value)));
        }
        Ok(-1)
    };
    let mut values: Vec<u32> = Vec::new();
    for part in expression.to_lowercase().split(',') {
        let (range_part, separator, step_str) = match part.split_once('/') {
            Some((r, s)) => (r, true, s),
            None => (part, false, ""),
        };
        let step = if separator {
            parse_py_int(step_str).ok_or_else(|| freq(invalid_literal(step_str)))?
        } else {
            1
        };
        let cron_values: Vec<i64> = if range_part == "*" {
            if step == 0 {
                return Err(freq("range() arg 3 must not be zero"));
            }
            if step < 0 {
                Vec::new()
            } else {
                (0..7).step_by(step as usize).collect()
            }
        } else {
            let endpoints = range_part
                .splitn(2, '-')
                .map(as_cron_int)
                .collect::<Result<Vec<_>, _>>()?;
            if endpoints.iter().any(|v| *v < 0 || *v > 7)
                || step <= 0
                || (endpoints.len() == 2 && endpoints[0] > endpoints[1])
            {
                return Err(invalid());
            }
            if endpoints.len() == 2 {
                (endpoints[0]..=endpoints[1])
                    .step_by(step as usize)
                    .collect()
            } else if separator {
                (endpoints[0]..8).step_by(step as usize).collect()
            } else {
                endpoints
            }
        };
        if step <= 0 {
            return Err(invalid());
        }
        values.extend(
            cron_values
                .into_iter()
                .map(|v| if v == 0 || v == 7 { 6 } else { (v - 1) as u32 }),
        );
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

/// The fields of a `_CronTrigger`.
#[derive(Debug, Clone)]
pub struct CronFields {
    /// `minute`.
    pub minute: String,
    /// `hour`.
    pub hour: String,
    /// `day`.
    pub day: String,
    /// `month`.
    pub month: String,
    /// `day_of_week`.
    pub day_of_week: String,
    /// `year`.
    pub year: String,
    /// `second`.
    pub second: String,
    /// `microsecond`.
    pub microsecond: u32,
}

impl Default for CronFields {
    fn default() -> Self {
        CronFields {
            minute: "*".into(),
            hour: "*".into(),
            day: "*".into(),
            month: "*".into(),
            day_of_week: "*".into(),
            year: "*".into(),
            second: "0".into(),
            microsecond: 0,
        }
    }
}

/// The fields a `_CronTrigger` matches, expanded.
#[derive(Debug, Clone)]
struct CronMatcher {
    minutes: Vec<i64>,
    hours: Vec<i64>,
    days: Vec<i64>,
    days_have_wildcard: bool,
    months: Vec<i64>,
    weekdays: Vec<u32>,
    weekdays_have_wildcard: bool,
    years: Option<Vec<i64>>,
    seconds: Vec<i64>,
}

impl CronMatcher {
    fn date_matches<T: Datelike>(&self, c: &T) -> bool {
        let mut day_matches = self.days.contains(&(c.day() as i64));
        let weekday_matches = self.weekdays.contains(&c.weekday().num_days_from_monday());
        if self.days_have_wildcard || self.weekdays_have_wildcard {
            day_matches = day_matches && weekday_matches;
        } else {
            day_matches = day_matches || weekday_matches;
        }
        self.years
            .as_ref()
            .is_none_or(|y| y.contains(&(c.year() as i64)))
            && self.months.contains(&(c.month() as i64))
            && day_matches
    }

    fn matches<T: Datelike + Timelike>(&self, c: &T) -> bool {
        self.date_matches(c)
            && self.hours.contains(&(c.hour() as i64))
            && self.minutes.contains(&(c.minute() as i64))
            && self.seconds.contains(&(c.second() as i64))
    }

    fn min_second(&self) -> u32 {
        self.seconds.first().copied().unwrap_or(0) as u32
    }
}

/// `_CronTrigger`: the five-field cron subset (plus year and second).
#[derive(Debug, Clone)]
pub struct CronTrigger {
    start_time: Fire,
    tz: FixedOffset,
    m: CronMatcher,
    microsecond: u32,
    step_seconds: i64,
    last: Option<Fire>,
    zoned: Option<Box<Zoned<CronWalls>>>,
}

/// v4's `max_iterations` of one `_CronTrigger.next()` call.
fn cron_max_iterations(step_seconds: i64) -> u64 {
    11 * 366 * 24 * 60 * if step_seconds == 1 { 60 } else { 1 }
}

impl CronTrigger {
    /// `_CronTrigger(start_time, **fields)`.
    pub fn new(start_time: Fire, f: CronFields) -> Result<Self, ScheduleError> {
        let minutes = expand_cron_field(&f.minute, 0, 59, &[])?;
        let hours = expand_cron_field(&f.hour, 0, 23, &[])?;
        let days = expand_cron_field(&f.day, 1, 31, &[])?;
        let months = expand_cron_field(&f.month, 1, 12, &CRON_MONTHS)?;
        let weekdays = expand_cron_weekdays(&f.day_of_week)?;
        let years = if f.year == "*" {
            None
        } else {
            Some(expand_cron_field(&f.year, 1, 9999, &[])?)
        };
        let seconds = expand_cron_field(&f.second, 0, 59, &[])?;
        let step_seconds = if seconds.len() > 1 { 1 } else { 60 };
        Ok(CronTrigger {
            tz: *start_time.offset(),
            start_time,
            m: CronMatcher {
                minutes,
                hours,
                days,
                days_have_wildcard: f.day.contains('*'),
                months,
                weekdays,
                weekdays_have_wildcard: f.day_of_week.contains('*'),
                years,
                seconds,
            },
            microsecond: f.microsecond,
            step_seconds,
            last: None,
            zoned: None,
        })
    }

    /// Match the fields against `zone`'s wall clock. An hour field that is a wildcard or a step
    /// fires through a repeated hour twice and not inside a gap; a specific hour fires once.
    fn new_zoned(start: &Start, zone: ScheduleZone, f: CronFields) -> Result<Self, ScheduleError> {
        let wildcard_hour = f.hour.contains('*') || f.hour.contains('/');
        let mut trigger = CronTrigger::new(start.fire, f)?;
        let walls = CronWalls {
            m: trigger.m.clone(),
            step_seconds: trigger.step_seconds,
            candidate: CronWalls::first_candidate(
                start.wall,
                trigger.microsecond,
                trigger.step_seconds,
                trigger.m.min_second(),
            ),
        };
        trigger.zoned = Some(Box::new(Zoned::new(
            zone,
            Policy::cron(wildcard_hour),
            start,
            walls,
            false,
        )));
        Ok(trigger)
    }

    fn with_micro(dt: Fire, micro: u32) -> Fire {
        dt.with_nanosecond(micro * 1000).unwrap_or(dt)
    }

    fn step(&self, dt: Fire) -> Fire {
        dt + Duration::seconds(self.step_seconds)
    }

    fn next(&mut self) -> Option<Fire> {
        if let Some(z) = &mut self.zoned {
            return z.next();
        }
        let mut candidate = match self.last {
            None => {
                let mut c = Self::with_micro(self.start_time, self.microsecond);
                if self.step_seconds == 60 {
                    c = c.with_second(self.m.min_second()).unwrap_or(c);
                }
                if c < self.start_time {
                    c = self.step(c);
                }
                c
            }
            Some(last) => self.step(last),
        };
        // PARITY: a rejected date jumps to the next day at the *same time of day*, so e.g. `mon`
        // first fires on Monday at the start's wall-clock time, not at midnight.
        for _ in 0..cron_max_iterations(self.step_seconds) {
            if self.m.matches(&candidate) {
                self.last = Some(candidate);
                return Some(candidate);
            }
            if !self.m.date_matches(&candidate) {
                let date = candidate.date_naive().succ_opt()?;
                candidate = combine(date, candidate.time(), self.tz)?;
                continue;
            }
            candidate = self.step(candidate);
        }
        None
    }

    fn next_after(&mut self, after: Fire) -> Option<Fire> {
        if let Some(z) = &mut self.zoned {
            if after < z.start {
                return z.restart();
            }
            let wall_after = z.stream.zone.wall(&after);
            let mut candidate = wall_after
                .with_nanosecond(self.microsecond * 1000)
                .unwrap_or(wall_after);
            if self.step_seconds == 60 {
                candidate = candidate
                    .with_second(self.m.min_second())
                    .unwrap_or(candidate);
            }
            if candidate <= wall_after {
                candidate += Duration::seconds(self.step_seconds);
            }
            // A wildcard hour fires a repeated hour's second occurrences after its first ones:
            // walk the hours before `after` on its (matching) date again so they are not missed.
            if z.stream.policy == Policy::cron(true) && self.m.date_matches(&candidate) {
                let midnight = candidate.date().and_time(NaiveTime::MIN);
                candidate = (candidate - Duration::hours(3)).max(midnight);
            }
            z.walls.candidate = Some(candidate);
            z.stream.reset(Some(z.start), Some(after), None);
            return z.next();
        }
        if after < self.start_time {
            self.last = None;
            return self.next();
        }
        let after = after.with_timezone(&self.tz);
        let mut candidate = Self::with_micro(after, self.microsecond);
        if self.step_seconds == 60 {
            candidate = candidate
                .with_second(self.m.min_second())
                .unwrap_or(candidate);
        }
        if candidate <= after {
            candidate = self.step(candidate);
        }
        self.last = Some(candidate - Duration::seconds(self.step_seconds));
        self.next()
    }
}

/// The wall times a zoned cron trigger matches, walked the way v4 walks its candidates.
#[derive(Debug, Clone)]
struct CronWalls {
    m: CronMatcher,
    step_seconds: i64,
    /// The next wall time to test.
    candidate: Option<NaiveDateTime>,
}

impl CronWalls {
    /// v4's first candidate: the start with the trigger's microsecond (and smallest second),
    /// stepped once when that is before the start.
    fn first_candidate(
        start: NaiveDateTime,
        microsecond: u32,
        step_seconds: i64,
        min_second: u32,
    ) -> Option<NaiveDateTime> {
        let mut c = start.with_nanosecond(microsecond * 1000).unwrap_or(start);
        if step_seconds == 60 {
            c = c.with_second(min_second).unwrap_or(c);
        }
        if c < start {
            c += Duration::seconds(step_seconds);
        }
        Some(c)
    }
}

impl WallSource for CronWalls {
    fn next_wall(&mut self) -> Option<NaiveDateTime> {
        let mut candidate = self.candidate?;
        let step = Duration::seconds(self.step_seconds);
        // PARITY: a rejected date jumps to the next day at the same time of day (see
        // `CronTrigger::next`).
        for _ in 0..cron_max_iterations(self.step_seconds) {
            if self.m.matches(&candidate) {
                self.candidate = candidate.checked_add_signed(step);
                return Some(candidate);
            }
            if !self.m.date_matches(&candidate) {
                candidate = NaiveDateTime::new(candidate.date().succ_opt()?, candidate.time());
                continue;
            }
            candidate = candidate.checked_add_signed(step)?;
        }
        self.candidate = None;
        None
    }
}

/// A parsed schedule: a stateful object with `next()` and `next_after()`.
#[derive(Debug, Clone)]
pub enum Trigger {
    /// `_IntervalTrigger`.
    Interval(IntervalTrigger),
    /// `_CalendarIntervalTrigger`.
    Calendar(CalendarIntervalTrigger),
    /// `_CronTrigger`.
    Cron(CronTrigger),
    /// `_OrTrigger`.
    Or(Vec<Trigger>, Vec<Option<Fire>>),
    /// `_AndTrigger`.
    And(Vec<Trigger>, Vec<Option<Fire>>),
    /// A trigger whose every fire time is moved by a fixed offset (`... jitter 30 seconds`,
    /// `--jitter`, `jobs.schedule.jitter_seconds`; v5 only). The offset is the job's own, seeded
    /// by its name: it is the same on every fire and every restart.
    Jittered(Box<Trigger>, i64),
}

impl Trigger {
    /// The Python class name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Trigger::Interval(_) => "_IntervalTrigger",
            Trigger::Calendar(_) => "_CalendarIntervalTrigger",
            Trigger::Cron(_) => "_CronTrigger",
            Trigger::Or(..) => "_OrTrigger",
            Trigger::And(..) => "_AndTrigger",
            Trigger::Jittered(..) => "_JitteredTrigger",
        }
    }

    /// The zone this trigger steps in (`tz <zone>`, or the host's zone named by an abbreviation
    /// after `starting`), or `None` for fixed-offset stepping.
    pub fn zone(&self) -> Option<ScheduleZone> {
        match self {
            Trigger::Interval(t) => t.zoned.as_ref().map(|z| z.stream.zone),
            Trigger::Calendar(t) => t.zoned.as_ref().map(|z| z.stream.zone),
            Trigger::Cron(t) => t.zoned.as_ref().map(|z| z.stream.zone),
            Trigger::Or(triggers, _) | Trigger::And(triggers, _) => {
                triggers.iter().find_map(Trigger::zone)
            }
            Trigger::Jittered(inner, _) => inner.zone(),
        }
    }

    /// `trigger.next()`: the next fire time, or `None` when the schedule is exhausted.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Fire> {
        match self {
            Trigger::Interval(t) => t.next(),
            Trigger::Calendar(t) => t.next(),
            Trigger::Cron(t) => t.next(),
            Trigger::Jittered(inner, offset_us) => inner.next().map(|t| add_us(t, *offset_us)),
            Trigger::Or(triggers, fires) => {
                if fires.is_empty() {
                    *fires = triggers.iter_mut().map(Trigger::next).collect();
                }
                let earliest = fires.iter().flatten().min().copied();
                if let Some(e) = earliest {
                    for (i, f) in fires.iter_mut().enumerate() {
                        if *f == Some(e) {
                            *f = triggers[i].next();
                        }
                    }
                }
                earliest
            }
            Trigger::And(triggers, fires) => {
                if fires.is_empty() {
                    *fires = triggers.iter_mut().map(Trigger::next).collect();
                }
                for _ in 0..1_000_000 {
                    if fires.iter().any(Option::is_none) {
                        return None;
                    }
                    let earliest = fires.iter().flatten().min().copied()?;
                    let latest = fires.iter().flatten().max().copied()?;
                    for (i, f) in fires.iter_mut().enumerate() {
                        if *f == Some(earliest) {
                            *f = triggers[i].next();
                        }
                    }
                    if latest == earliest {
                        return Some(earliest);
                    }
                }
                // PARITY: Python raises `RuntimeError("Maximum iterations reached while combining
                // schedules.")`; the Rust trigger reports exhaustion instead.
                None
            }
        }
    }

    /// `trigger.next_after(after)`: the first fire time strictly after `after` (for interval
    /// triggers, the first start-aligned time after it).
    pub fn next_after(&mut self, after: Fire) -> Option<Fire> {
        match self {
            Trigger::Interval(t) => t.next_after(after),
            Trigger::Calendar(t) => t.next_after(after),
            Trigger::Cron(t) => t.next_after(after),
            Trigger::Jittered(inner, offset_us) => inner
                .next_after(add_us(after, -*offset_us))
                .map(|t| add_us(t, *offset_us)),
            Trigger::Or(triggers, fires) | Trigger::And(triggers, fires) => {
                *fires = triggers.iter_mut().map(|t| t.next_after(after)).collect();
                self.next()
            }
        }
    }
}

fn starting_regex() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)\b(?:starting|beginning)\b").expect("valid regex"))
}

/// `_canonicalize_starting_keyword`: `beginning` (any case) becomes `starting`.
pub fn canonicalize_starting_keyword(schedule: &str) -> String {
    starting_regex()
        .replace_all(schedule, STARTING_KEYWORD)
        .to_string()
}

/// `items_str(items)`: `'a', 'b', and 'c'`.
fn items_str(items: &[&str]) -> String {
    let quoted: Vec<String> = items.iter().map(|i| format!("'{i}'")).collect();
    match quoted.len() {
        0 => String::new(),
        1 => quoted[0].clone(),
        2 => format!("{} and {}", quoted[0], quoted[1]),
        n => format!("{}, and {}", quoted[..n - 1].join(", "), quoted[n - 1]),
    }
}

fn unit_us(unit: &str) -> Option<f64> {
    Some(match unit {
        "weeks" => 604_800_000_000.0,
        "days" => 86_400_000_000.0,
        "hours" => 3_600_000_000.0,
        "minutes" => 60_000_000.0,
        "seconds" => 1_000_000.0,
        "milliseconds" => 1_000.0,
        "microseconds" => 1.0,
        _ => return None,
    })
}

/// `timedelta(**{unit: num})` in microseconds (round-half-even like CPython).
fn timedelta_us(unit: &str, num: f64) -> Result<i64, String> {
    let per = unit_us(unit)
        .ok_or_else(|| format!("__new__() got an unexpected keyword argument '{unit}'"))?;
    Ok((num * per).round_ties_even() as i64)
}

/// A duration in seconds from `30`, `30s`, `1.5 seconds`, `5m`, `1 hour`, `250ms` (the
/// `--jitter` value and the `jitter` suffix of `-s`). `None` when it is not one.
pub fn parse_jitter_seconds(text: &str) -> Option<f64> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"(?i)^\s*(\d+(?:\.\d+)?|\.\d+)\s*([a-z]*)\s*$").expect("valid regex")
    });
    let caps = re.captures(text)?;
    let number: f64 = caps[1].parse().ok()?;
    let per = match caps[2].to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
        "ms" | "millisecond" | "milliseconds" => 0.001,
        "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3600.0,
        _ => return None,
    };
    let seconds = number * per;
    seconds.is_finite().then_some(seconds)
}

/// Split a trailing `jitter <n> [unit]` off a schedule (`every 5 minutes jitter 30 seconds`).
/// The jitter is `None` when the schedule has no such suffix; an unreadable one is an error.
pub fn split_jitter(schedule: &str) -> Result<(String, Option<f64>), ScheduleError> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"(?i)^(.*?)\s+jitter\s+(\S+)(?:\s+(\S+))?\s*$").expect("valid regex")
    });
    let Some(caps) = re.captures(schedule) else {
        return Ok((schedule.to_string(), None));
    };
    let text = match caps.get(3) {
        Some(unit) => format!("{} {}", &caps[2], unit.as_str()),
        None => caps[2].to_string(),
    };
    let seconds = parse_jitter_seconds(&text).ok_or_else(|| {
        freq(format!(
            "Invalid jitter '{text}'.\n    Use a number and an optional unit, e.g. 'jitter 30 seconds'."
        ))
    })?;
    Ok((caps[1].to_string(), Some(seconds)))
}

/// Where the jitter of a schedule comes from besides its own `jitter` suffix.
#[derive(Debug, Clone, Copy)]
pub struct JitterSpec<'a> {
    /// The bound used when the schedule has no suffix (`--jitter`, `jobs.schedule.jitter_seconds`).
    pub default_seconds: f64,
    /// Seeds the fixed offset (the job's name).
    pub seed: &'a str,
}

/// What `dateutil.parser.parse` reads from the process: the zone behind `tzlocal()` and
/// `time.tzname`, and the default date ("today" on the local wall clock). Tests pin both.
#[derive(Debug, Clone)]
pub struct ParseEnv {
    /// The local zone, or `None` when it is unknown (no abbreviation then means "local"). Always
    /// `None` on Windows (see [`LocalZone::detect`]).
    pub local: Option<Arc<LocalZone>>,
    /// The date a bare time (`21:00`) takes.
    pub today: NaiveDate,
    /// The zone `tz local` names (on every platform), or `None` when the host's zone cannot be
    /// determined (`tz local` then steps in UTC, with a warning).
    pub host: Option<Tz>,
}

impl ParseEnv {
    /// This process's zone and today's date in it.
    pub fn process() -> ParseEnv {
        let local = LocalZone::detect();
        let today = match &local {
            Some(zone) => zone.today(chrono::Utc::now()),
            None => chrono::Local::now().date_naive(),
        };
        ParseEnv {
            local,
            today,
            host: crate::local_zone::host_tz(),
        }
    }

    /// A process whose zone is the IANA zone `name`, as Python sees it at `as_of`, on `today`
    /// (for tests and for callers that pin the zone).
    pub fn in_zone(name: &str, as_of: chrono::DateTime<chrono::Utc>, today: NaiveDate) -> ParseEnv {
        let local = LocalZone::from_name(name, as_of);
        ParseEnv {
            host: local.as_ref().map(LocalZone::tz),
            local: local.map(Arc::new),
            today,
        }
    }
}

/// Split the trailing `tz <zone>` and `jitter <n> [unit]` clauses off a schedule, in either order
/// (`daily starting 21:00 tz local jitter 5 minutes`). `tz` anywhere else is an error.
pub fn split_clauses(
    schedule: &str,
) -> Result<(String, Option<f64>, Option<String>), ScheduleError> {
    static TZ_SUFFIX: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    static TZ_WORD: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let tz_suffix =
        TZ_SUFFIX.get_or_init(|| Regex::new(r"(?i)^(.*?)\s+tz\s+(\S+)\s*$").expect("valid regex"));
    let tz_word = TZ_WORD.get_or_init(|| Regex::new(r"(?i)(?:^|\s)tz\s+\S").expect("valid regex"));
    let mut base = schedule.to_string();
    let (mut jitter, mut zone) = (None, None);
    for _ in 0..2 {
        if zone.is_none()
            && let Some(caps) = tz_suffix.captures(&base)
        {
            zone = Some(caps[2].to_string());
            base = caps[1].to_string();
            continue;
        }
        if jitter.is_none() {
            let (rest, seconds) = split_jitter(&base)?;
            if seconds.is_some() {
                jitter = seconds;
                base = rest;
                continue;
            }
        }
        break;
    }
    if tz_word.is_match(&base) {
        return Err(freq(
            "'tz <zone>' must come at the end of the schedule (after 'starting', before or after 'jitter').",
        ));
    }
    Ok((base, jitter, zone))
}

/// The zone a `tz <zone>` clause names: `None` for `utc` (the default), the host's zone for
/// `local` (UTC with a warning when it cannot be determined), or an IANA zone matched without
/// regard to case.
fn parse_zone_clause(
    text: &str,
    env: &ParseEnv,
    warnings: &mut Vec<String>,
) -> Result<Option<ScheduleZone>, ScheduleError> {
    if text.eq_ignore_ascii_case("utc") {
        return Ok(None);
    }
    if text.eq_ignore_ascii_case("local") {
        return Ok(match env.host {
            Some(tz) => Some(ScheduleZone::host(tz)),
            None => {
                warnings.push(
                    "The time zone of this host could not be determined; 'tz local' steps in UTC.  Set TZ or name the zone, e.g. 'tz America/New_York'."
                        .to_string(),
                );
                None
            }
        });
    }
    chrono_tz::TZ_VARIANTS
        .iter()
        .find(|tz| tz.name().eq_ignore_ascii_case(text))
        .map(|tz| Some(ScheduleZone::named(*tz)))
        .ok_or_else(|| {
            freq(format!(
                "Unknown time zone '{text}'.\n    Use 'local' (this host's zone), 'utc', or an IANA name such as 'America/New_York'."
            ))
        })
}

/// `parse_schedule(schedule, now)`. A trailing `jitter <n> [unit]` moves every fire time by a
/// fixed offset in `[0, n]` seconds (seeded by the schedule text; see [`parse_schedule_jittered`]).
pub fn parse_schedule(schedule: &str, now: Fire) -> Result<Trigger, ScheduleError> {
    parse_schedule_jittered(schedule, now, None)
}

/// [`parse_schedule`] with a default jitter and the seed of the fixed offset: the offset is
/// `fnv1a(seed) mod (jitter + 1ms)` (see [`crate::startup::fixed_offset`]), so a job keeps the
/// same offset on every fire and every restart, and different jobs spread. The schedule's own
/// `jitter` suffix wins over `spec.default_seconds`.
pub fn parse_schedule_jittered(
    schedule: &str,
    now: Fire,
    spec: Option<JitterSpec<'_>>,
) -> Result<Trigger, ScheduleError> {
    parse_schedule_warned(schedule, now, spec).map(|(trigger, _)| trigger)
}

/// [`parse_schedule_jittered`] with the warnings the parse raised (dateutil's
/// `UnknownTimezoneWarning`, see [`parse_datetime_in`]): one entry per warning, for the caller to
/// show through its own warning path. Call it once per parse that should warn.
pub fn parse_schedule_warned(
    schedule: &str,
    now: Fire,
    spec: Option<JitterSpec<'_>>,
) -> Result<(Trigger, Vec<String>), ScheduleError> {
    parse_schedule_in(schedule, now, spec, &ParseEnv::process())
}

/// [`parse_schedule_warned`] in an explicit [`ParseEnv`].
pub fn parse_schedule_in(
    schedule: &str,
    now: Fire,
    spec: Option<JitterSpec<'_>>,
    env: &ParseEnv,
) -> Result<(Trigger, Vec<String>), ScheduleError> {
    let (base, suffix, zone) = split_clauses(schedule)?;
    let offset_us = jitter_us(&base, suffix, spec);
    let mut warnings = Vec::new();
    let clause = zone
        .map(|text| parse_zone_clause(&text, env, &mut warnings))
        .transpose()?;
    let trigger = parse_schedule_unjittered(&base, now, env, &mut warnings, clause)?;
    if offset_us == 0 {
        return Ok((trigger, warnings));
    }
    Ok((Trigger::Jittered(Box::new(trigger), offset_us), warnings))
}

/// The fixed offset of a schedule's fire times in microseconds (see [`parse_schedule_jittered`]):
/// its own `jitter` suffix, else the default, seeded by the job (or the schedule without its
/// clauses).
fn jitter_us(base: &str, suffix: Option<f64>, spec: Option<JitterSpec<'_>>) -> i64 {
    let seconds = suffix.or(spec.map(|s| s.default_seconds)).unwrap_or(0.0);
    crate::startup::fixed_offset(spec.map_or(base, |s| s.seed), seconds).as_micros() as i64
}

/// [`parse_schedule_jittered`] for a job that resumes `fire`, a fire time an earlier process
/// drew from this schedule: the trigger is anchored there, so `next_after(fire)` keeps the
/// cadence of that fire (`daily` stays 24 hours after it, `daily tz local` stays at its wall-clock
/// time across a daylight-saving change) instead of starting over from the restart. Cron fields
/// and an explicit `starting` time do not depend on the anchor.
pub fn parse_schedule_resumed(
    schedule: &str,
    fire: Fire,
    spec: Option<JitterSpec<'_>>,
) -> Result<Trigger, ScheduleError> {
    parse_schedule_resumed_in(schedule, fire, spec, &ParseEnv::process())
}

/// [`parse_schedule_resumed`] in an explicit [`ParseEnv`].
pub fn parse_schedule_resumed_in(
    schedule: &str,
    fire: Fire,
    spec: Option<JitterSpec<'_>>,
    env: &ParseEnv,
) -> Result<Trigger, ScheduleError> {
    let (base, suffix, _) = split_clauses(schedule)?;
    let offset_us = jitter_us(&base, suffix, spec);
    parse_schedule_in(schedule, add_us(fire, -offset_us), spec, env).map(|(t, _)| t)
}

/// The longest gap between a schedule's next few fire times, in seconds (`every 10 minutes`:
/// 600; `weekly`: a week). `None` for a schedule that does not parse, fires fewer than twice, or
/// combines schedules with `and` (finding its fire times can take long). Job health expects a
/// scheduled job's laps this often.
pub fn period_seconds(schedule: &str) -> Option<f64> {
    period_seconds_at(schedule, chrono::Utc::now().fixed_offset())
}

fn period_seconds_at(schedule: &str, now: Fire) -> Option<f64> {
    let mut trigger = parse_schedule(schedule, now).ok()?;
    let combined = |t: &Trigger| match t {
        Trigger::And(..) => true,
        Trigger::Jittered(inner, _) => matches!(inner.as_ref(), Trigger::And(..)),
        _ => false,
    };
    if combined(&trigger) {
        return None;
    }
    let mut previous = trigger.next()?;
    let mut longest: Option<f64> = None;
    for _ in 0..4 {
        let Some(next) = trigger.next() else { break };
        let gap = (next - previous).num_milliseconds() as f64 / 1000.0;
        longest = Some(longest.map_or(gap, |l: f64| l.max(gap)));
        previous = next;
    }
    longest.filter(|s| *s > 0.0)
}

/// `clause` is the `tz <zone>` clause: `None` without one, `Some(None)` for `tz utc`.
fn parse_schedule_unjittered(
    schedule: &str,
    now: Fire,
    env: &ParseEnv,
    warnings: &mut Vec<String>,
    clause: Option<Option<ScheduleZone>>,
) -> Result<Trigger, ScheduleError> {
    let schedule = canonicalize_starting_keyword(schedule);
    let info = parse_start(
        &schedule,
        Some(now),
        env,
        warnings,
        clause.flatten().as_ref(),
    )?;
    let starting_ts = info.fire;
    // The zone the triggers step in: the clause's, else the host's when the start was written
    // with one of its abbreviations (v4's `tzlocal()`), else none (a fixed offset).
    let zone = match clause {
        Some(zone) => zone,
        None if info.local => env.local.as_ref().map(|l| ScheduleZone::host(l.tz())),
        None => None,
    };
    let start = zone.map(|z| Start {
        fire: starting_ts,
        wall: info.wall.unwrap_or_else(|| z.wall(&starting_ts)),
    });
    // The start's wall-clock fields (a named weekday or month keeps the start's minute).
    let anchor = start.map_or(starting_ts.naive_local(), |s| s.wall);
    let mut s = schedule
        .split(STARTING_KEYWORD)
        .next()
        .unwrap_or("")
        .trim()
        .to_lowercase();
    for (alias, true_keyword) in SCHEDULE_ALIASES {
        if !alias.is_empty() {
            // PARITY: plain substring replacement (`'fortnights'` contains `'or'`).
            s = s.replace(alias, true_keyword);
        }
    }
    if s.contains('&') && s.contains('|') {
        return Err(freq(
            "Cannot accept both 'and' + 'or' logic in the schedule frequency.",
        ));
    }
    let join = if s.contains('|') { '|' } else { '&' };
    let has_seconds = s.contains("second");
    let has_minutes = s.contains("minute");
    let mut triggers = Vec::new();
    for part in s.split(join).map(str::trim) {
        let trigger = if let Some(rest) = part.strip_prefix("every ") {
            let Some((num_str, unit)) = rest.split_once(' ') else {
                return Err(freq("not enough values to unpack (expected 2, got 1)"));
            };
            let unit = format!("{}s", unit.trim_end_matches('s'));
            if !INTERVAL_UNITS.contains(&unit.as_str()) {
                return Err(freq(format!(
                    "Invalid interval '{unit}'.\n    Accepted values are {}.",
                    items_str(&INTERVAL_UNITS)
                )));
            }
            // PARITY: `int(s) if is_int(s) else float(s)`: `'1.0'` passes `is_int` and then
            // fails `int()`.
            let (num, num_repr): (f64, String) = if meerschaum_core::pyfmt::is_int(num_str) {
                let i = parse_py_int(num_str).ok_or_else(|| freq(invalid_literal(num_str)))?;
                (i as f64, i.to_string())
            } else {
                let f = parse_py_float(num_str).ok_or_else(|| {
                    freq(format!(
                        "could not convert string to float: {}",
                        meerschaum_core::pyfmt::str_repr(num_str)
                    ))
                })?;
                (f, meerschaum_core::pyfmt::float_repr(f))
            };
            if unit == "months" || unit == "years" {
                if num.fract() != 0.0 || !num.is_finite() {
                    return Err(freq(format!(
                        "Calendar interval '{num_repr}' must be an integer."
                    )));
                }
                let n = num as i64;
                let (years, months) = if unit == "years" { (n, 0) } else { (0, n) };
                Trigger::Calendar(match (&start, zone) {
                    (Some(start), Some(zone)) => {
                        CalendarIntervalTrigger::new_zoned(start, zone, years, months)?
                    }
                    _ => CalendarIntervalTrigger::new(starting_ts, years, months)?,
                })
            } else {
                let us = timedelta_us(&unit, num).map_err(freq)?;
                Trigger::Interval(match (&start, zone) {
                    (Some(start), Some(zone)) => IntervalTrigger::new_zoned(start, zone, us)?,
                    _ => IntervalTrigger::new(starting_ts, us)?,
                })
            }
        } else {
            let prefix3: String = part.chars().take(3).collect::<String>().to_lowercase();
            let prefix4: String = part.chars().take(4).collect::<String>().to_lowercase();
            let mut fields = CronFields::default();
            let mut is_named = true;
            if CRON_DAYS_OF_WEEK.contains(&prefix3.as_str()) {
                fields.day_of_week = part.to_string();
            } else if CRON_MONTHS.contains(&prefix3.as_str()) {
                fields.month = part.to_string();
            } else if meerschaum_core::pyfmt::is_int(&prefix4) && prefix4.chars().count() == 4 {
                fields.year = part.to_string();
            } else {
                is_named = false;
            }
            if is_named {
                fields.hour = "*".into();
                fields.minute = if has_minutes || has_seconds {
                    "*".into()
                } else {
                    anchor.minute().to_string()
                };
                fields.second = if has_seconds {
                    "*".into()
                } else {
                    anchor.second().to_string()
                };
                fields.microsecond = anchor.nanosecond() / 1000;
                Trigger::Cron(new_cron(starting_ts, start.as_ref(), zone, fields)?)
            } else {
                let cron_parts: Vec<&str> = part.split_whitespace().collect();
                if cron_parts.len() != 5 {
                    return Err(freq(format!("Invalid cron schedule '{part}'.")));
                }
                fields.minute = cron_parts[0].into();
                fields.hour = cron_parts[1].into();
                fields.day = cron_parts[2].into();
                fields.month = cron_parts[3].into();
                fields.day_of_week = cron_parts[4].into();
                Trigger::Cron(new_cron(starting_ts, start.as_ref(), zone, fields)?)
            }
        };
        triggers.push(trigger);
    }
    if triggers.len() == 1 {
        return Ok(triggers.pop().expect("one trigger"));
    }
    Ok(if join == '|' {
        Trigger::Or(triggers, Vec::new())
    } else {
        Trigger::And(triggers, Vec::new())
    })
}

/// `parse_start_time(schedule, now)`: the explicit starting datetime, or `now`.
pub fn parse_start_time(schedule: &str, now: Option<Fire>) -> Result<Fire, ScheduleError> {
    parse_start_time_in(schedule, now, &ParseEnv::process(), &mut Vec::new())
}

/// [`parse_start_time`] in an explicit [`ParseEnv`], collecting dateutil's warnings.
pub fn parse_start_time_in(
    schedule: &str,
    now: Option<Fire>,
    env: &ParseEnv,
    warnings: &mut Vec<String>,
) -> Result<Fire, ScheduleError> {
    parse_start(schedule, now, env, warnings, None).map(|info| info.fire)
}

/// The starting time of a schedule that may carry the v5 `tz <zone>` and `jitter` clauses
/// (without them this is [`parse_start_time_in`]): under `tz`, a time without a zone of its own is
/// read on the zone's wall clock. Returns the warnings the parse raised.
pub fn parse_start_time_clauses(
    schedule: &str,
    now: Option<Fire>,
    env: &ParseEnv,
) -> Result<(Fire, Vec<String>), ScheduleError> {
    let (base, _, zone) = split_clauses(schedule)?;
    let mut warnings = Vec::new();
    let zone = zone
        .map(|text| parse_zone_clause(&text, env, &mut warnings))
        .transpose()?
        .flatten();
    let info = parse_start(&base, now, env, &mut warnings, zone.as_ref())?;
    Ok((info.fire, warnings))
}

/// A `_CronTrigger`, matched on `zone`'s wall clock when there is one.
fn new_cron(
    starting_ts: Fire,
    start: Option<&Start>,
    zone: Option<ScheduleZone>,
    fields: CronFields,
) -> Result<CronTrigger, ScheduleError> {
    match (start, zone) {
        (Some(start), Some(zone)) => CronTrigger::new_zoned(start, zone, fields),
        _ => CronTrigger::new(starting_ts, fields),
    }
}

/// What a `starting` clause names.
struct StartInfo {
    /// The instant.
    fire: Fire,
    /// The wall time as written, when it was read in the schedule's zone (or in the host's zone
    /// through one of its abbreviations).
    wall: Option<NaiveDateTime>,
    /// Whether the time carried an abbreviation of the host's zone (dateutil's `tzlocal()`).
    local: bool,
}

/// The starting time of `schedule`. Without `zone` this is v4's `parse_start_time`; with a zone
/// (`tz <zone>`), relative forms and times without a zone of their own are read on its wall
/// clock, and a time with its own offset or abbreviation keeps the instant it names.
fn parse_start(
    schedule: &str,
    now: Option<Fire>,
    env: &ParseEnv,
    warnings: &mut Vec<String>,
    zone: Option<&ScheduleZone>,
) -> Result<StartInfo, ScheduleError> {
    let schedule = canonicalize_starting_keyword(schedule);
    let parts: Vec<&str> = schedule.split(STARTING_KEYWORD).collect();
    let starting_str = if parts.len() == 1 {
        "now"
    } else {
        parts[parts.len() - 1]
    }
    .trim();
    let now = now.unwrap_or_else(|| chrono::Utc::now().fixed_offset());
    let err = ScheduleError::StartTime;
    let instant = |fire: Fire| StartInfo {
        fire,
        wall: None,
        local: false,
    };
    // A wall time on the zone's clock.
    let in_zone = |zone: &ScheduleZone, wall: NaiveDateTime| StartInfo {
        fire: zone.start_instant(wall),
        wall: Some(wall),
        local: false,
    };
    // dateutil fills a bare time with today's date: the zone's today under `tz`.
    let zone_env;
    let env = match zone {
        Some(zone) => {
            zone_env = ParseEnv {
                today: zone.wall(&now).date(),
                ..env.clone()
            };
            &zone_env
        }
        None => env,
    };
    let starting = if starting_str == "now" {
        instant(now)
    } else if starting_str.starts_with("in ") {
        let replaced = starting_str.replace("in ", "");
        let delta_vals: Vec<&str> = replaced.splitn(2, ' ').collect();
        let unit = format!(
            "{}s",
            delta_vals[delta_vals.len() - 1].trim_end_matches('s')
        );
        let num = parse_py_float(delta_vals[0]).ok_or_else(|| {
            err(format!(
                "could not convert string to float: {}",
                meerschaum_core::pyfmt::str_repr(delta_vals[0])
            ))
        })?;
        let us = timedelta_us(&unit, num).map_err(err)?;
        instant(add_us(now, us))
    } else if starting_str.contains("tomorrow") || starting_str.contains("today") {
        let is_tomorrow = starting_str.contains("tomorrow");
        let time_str = starting_str
            .replace("tomorrow", "")
            .replace("today", "")
            .trim()
            .to_string();
        let (hour, minute) = if time_str.is_empty() {
            (0, 0)
        } else {
            let parsed = parse_datetime_in(&time_str, env, warnings).map_err(err)?;
            (parsed.hour(), parsed.minute())
        };
        let clock = Duration::hours(hour as i64) + Duration::minutes(minute as i64);
        match zone {
            // Midnight of today (or tomorrow) on the zone's calendar, plus the time.
            Some(zone) => {
                let date = zone.wall(&now).date();
                let date = if is_tomorrow {
                    date.succ_opt()
                        .ok_or_else(|| err("Invalid date.".to_string()))?
                } else {
                    date
                };
                in_zone(zone, date.and_time(NaiveTime::MIN) + clock)
            }
            None => {
                // `round_time(now, timedelta(days=1))`: midnight of `now`'s wall-clock date.
                let today = combine(now.date_naive(), NaiveTime::MIN, *now.offset())
                    .ok_or_else(|| err("Invalid date.".to_string()))?;
                let tomorrow = today + Duration::days(1);
                instant((if is_tomorrow { tomorrow } else { today }) + clock)
            }
        }
    } else {
        let (parsed, local) = parse_datetime_flagged(starting_str, env, warnings).map_err(err)?;
        match (parsed, zone) {
            (Parsed::Naive(n), Some(zone)) => in_zone(zone, n),
            (Parsed::Naive(n), None) => instant(
                FixedOffset::east_opt(0)
                    .and_then(|utc| utc.from_local_datetime(&n).single())
                    .ok_or_else(|| err("Invalid datetime.".to_string()))?,
            ),
            (Parsed::Aware(a), Some(_)) => instant(a),
            // The written wall time on the host's clock (its offset is the zone's on that date).
            (Parsed::Aware(a), None) => StartInfo {
                fire: a,
                wall: local.then(|| a.naive_local()),
                local,
            },
        }
    };
    Ok(starting)
}

/// A `dateutil.parser.parse()` result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parsed {
    /// No timezone in the string.
    Naive(NaiveDateTime),
    /// With a timezone (`Z`, `UTC`, `GMT`, or a numeric offset).
    Aware(DateTime<FixedOffset>),
}

impl Parsed {
    fn hour(&self) -> u32 {
        match self {
            Parsed::Naive(n) => n.hour(),
            Parsed::Aware(a) => a.hour(),
        }
    }

    fn minute(&self) -> u32 {
        match self {
            Parsed::Naive(n) => n.minute(),
            Parsed::Aware(a) => a.minute(),
        }
    }
}

/// [`parse_datetime_in`] in this process's zone, discarding warnings.
pub fn parse_datetime(s: &str) -> Result<Parsed, String> {
    parse_datetime_in(s, &ParseEnv::process(), &mut Vec::new())
}

/// The subset of `dateutil.parser.parse()` the corpus uses: `YYYY-MM-DD` / `YYYY/MM/DD`, an
/// optional `[T ]H:MM[:SS[.ffffff]]` with `AM` / `PM`, then an optional zone read the way
/// dateutil reads it (see `parse_tz_tail` and `build_tzaware`), or a bare time (`10:00`,
/// `7pm`) on `env.today`. Errors carry dateutil's messages; dateutil's `UnknownTimezoneWarning` is
/// pushed on `warnings`.
pub fn parse_datetime_in(
    s: &str,
    env: &ParseEnv,
    warnings: &mut Vec<String>,
) -> Result<Parsed, String> {
    parse_datetime_flagged(s, env, warnings).map(|(parsed, _)| parsed)
}

/// [`parse_datetime_in`], and whether the zone was an abbreviation of the host's zone
/// (dateutil's `tzlocal()`).
fn parse_datetime_flagged(
    s: &str,
    env: &ParseEnv,
    warnings: &mut Vec<String>,
) -> Result<(Parsed, bool), String> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r"(?isx)^\s*
            (?:(?P<y>\d{4})[-/](?P<mo>\d{1,2})[-/](?P<d>\d{1,2}))?
            (?:(?:^|[T\s])\s*
                (?P<h>\d{1,2})(?::(?P<mi>\d{1,2})(?::(?P<s>\d{1,2})(?:\.(?P<f>\d{1,6}))?)?)?
                (?:\s*(?P<ampm>(?:am|pm)\b))?
            )?
            (?P<rest>.*)$",
        )
        .expect("valid regex")
    });
    let unknown = || format!("Unknown string format: {s}");
    if s.trim().is_empty() {
        return Err(format!("String does not contain a date: {s}"));
    }
    let caps = re.captures(s).ok_or_else(unknown)?;
    let num = |name: &str| {
        caps.name(name)
            .map(|m| m.as_str().parse::<i64>().unwrap_or(0))
    };
    let has_date = caps.name("y").is_some();
    let has_time = caps.name("h").is_some();
    if !has_date && !has_time {
        return Err(unknown());
    }
    // A bare number is not a time unless it has minutes or AM / PM.
    if !has_date && caps.name("mi").is_none() && caps.name("ampm").is_none() {
        return Err(unknown());
    }
    let mut hour = num("h").unwrap_or(0);
    if let Some(ampm) = caps.name("ampm") {
        let pm = ampm.as_str().eq_ignore_ascii_case("pm");
        if hour > 12 {
            return Err(unknown());
        }
        if pm && hour != 12 {
            hour += 12;
        } else if !pm && hour == 12 {
            hour = 0;
        }
    }
    let minute = num("mi").unwrap_or(0);
    let second = num("s").unwrap_or(0);
    let micro = caps
        .name("f")
        .map(|m| format!("{:0<6}", m.as_str()).parse::<i64>().unwrap_or(0))
        .unwrap_or(0);
    let (year, month, day) = if has_date {
        (
            num("y").unwrap_or(1),
            num("mo").unwrap_or(1),
            num("d").unwrap_or(1),
        )
    } else {
        // dateutil's default: today's local date.
        (
            env.today.year() as i64,
            env.today.month() as i64,
            env.today.day() as i64,
        )
    };
    let suffix = |m: String| format!("{m}: {s}");
    if !(1..=12).contains(&month) {
        return Err(suffix(format!("month must be in 1..12, not {month}")));
    }
    let Some(date) =
        NaiveDate::from_ymd_opt(year as i32, month as u32, day.max(1) as u32).filter(|_| day >= 1)
    else {
        let last = (28..=31)
            .rev()
            .find(|d| NaiveDate::from_ymd_opt(year as i32, month as u32, *d).is_some())
            .unwrap_or(28);
        return Err(suffix(format!(
            "day {day} must be in range 1..{last} for month {month} in year {year}"
        )));
    };
    if hour > 23 {
        return Err(suffix(format!("hour must be in 0..23, not {hour}")));
    }
    if minute > 59 {
        return Err(suffix(format!("minute must be in 0..59, not {minute}")));
    }
    if second > 59 {
        return Err(suffix(format!("second must be in 0..59, not {second}")));
    }
    let time =
        NaiveTime::from_hms_micro_opt(hour as u32, minute as u32, second as u32, micro as u32)
            .ok_or_else(unknown)?;
    let naive = NaiveDateTime::new(date, time);
    let rest = caps.name("rest").map_or("", |m| m.as_str());
    let tz = parse_tz_tail(rest, has_time).ok_or_else(unknown)?;
    build_tzaware(naive, tz, env, warnings).ok_or_else(unknown)
}

/// `res.tzname` / `res.tzoffset` after dateutil read the text behind a time.
#[derive(Debug, Default, PartialEq, Eq)]
struct TzFields {
    name: Option<String>,
    /// Seconds east of UTC.
    offset: Option<i64>,
}

/// `parserinfo.UTCZONE`: `parserinfo.utczone(name)` is case-insensitive.
fn is_utczone(name: &str) -> bool {
    matches!(name.to_ascii_lowercase().as_str(), "utc" | "gmt" | "z")
}

/// `_could_be_tzname`: up to five capitals, or exactly one of `UTC`, `GMT`, `Z`, `z`.
fn could_be_tzname(token: &str) -> bool {
    token.chars().count() <= 5
        && (token.chars().all(|c| c.is_ascii_uppercase())
            || matches!(token, "UTC" | "GMT" | "Z" | "z"))
}

/// `_timelex.split` for what can follow a time: one token per whitespace character, runs of
/// letters, runs of digits, every other character alone.
fn lex_tail(rest: &str) -> Vec<String> {
    let chars: Vec<char> = rest.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let mut j = i + 1;
        if c.is_whitespace() {
            tokens.push(" ".to_string());
            i = j;
            continue;
        }
        if c.is_alphabetic() {
            while j < chars.len() && chars[j].is_alphabetic() {
                j += 1;
            }
        } else if c.is_ascii_digit() {
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            if j + 1 < chars.len() && chars[j] == '.' && chars[j + 1].is_ascii_digit() {
                j += 1;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
            }
        }
        tokens.push(chars[i..j].iter().collect());
        i = j;
    }
    tokens
}

/// `parserinfo.jump`: tokens dateutil skips.
fn is_jump(token: &str) -> bool {
    matches!(
        token.to_lowercase().as_str(),
        " " | "."
            | ","
            | ";"
            | "-"
            | "/"
            | "'"
            | "at"
            | "on"
            | "and"
            | "ad"
            | "m"
            | "t"
            | "of"
            | "st"
            | "nd"
            | "rd"
            | "th"
    )
}

/// The timezone branches of `parser._parse` for the text after the time, then
/// `parserinfo.validate`. `hour_set` is whether a time was read (`res.hour is not None`: without
/// it dateutil reads neither a name nor an offset). `None` is dateutil's `ValueError`.
///
/// Not ported: dateutil's other readings of what follows a time (a date, a weekday or month name,
/// `AM` / `PM` after a zone); they are an error here.
fn parse_tz_tail(rest: &str, hour_set: bool) -> Option<TzFields> {
    let mut l = lex_tail(rest);
    let len = l.len();
    let mut res = TzFields::default();
    let mut i = 0;
    while i < len {
        let token = l[i].clone();
        if hour_set && res.name.is_none() && res.offset.is_none() && could_be_tzname(&token) {
            // `info.tzoffset(name)`: its table holds only the lowercase `z`.
            res.offset = (token == "z").then_some(0);
            res.name = Some(token);
            // PARITY: dateutil reads `GMT+3` / `EDT+3` as "my time +3 is GMT": the sign flips,
            // and `UTC` / `GMT` / `Z` followed by an offset is not the UTC zone itself.
            if i + 1 < len && (l[i + 1] == "+" || l[i + 1] == "-") {
                l[i + 1] = if l[i + 1] == "+" { "-" } else { "+" }.to_string();
                res.offset = None;
                if res.name.as_deref().is_some_and(is_utczone) {
                    res.name = None;
                }
            }
        } else if hour_set && (token == "+" || token == "-") {
            let sign: i64 = if token == "+" { 1 } else { -1 };
            let digits = l.get(i + 1)?.clone();
            let int = |text: &str| -> Option<i64> {
                (!text.is_empty() && text.chars().all(|c| c.is_ascii_digit()))
                    .then(|| text.parse().ok())
                    .flatten()
            };
            let head = |text: &str, n: usize| text.chars().take(n).collect::<String>();
            let (hours, minutes);
            if digits.chars().count() == 4 {
                hours = int(&head(&digits, 2))?;
                minutes = int(&digits.chars().skip(2).collect::<String>())?;
            } else if i + 2 < len && l[i + 2] == ":" {
                hours = int(&digits)?;
                minutes = int(l.get(i + 3)?)?;
                i += 2;
            } else if digits.chars().count() <= 2 {
                hours = int(&head(&digits, 2))?;
                minutes = 0;
            } else {
                return None;
            }
            res.offset = Some(sign * (hours * 3600 + minutes * 60));
            // `-0300 (BRST)`: a name in parentheses behind the offset.
            if i + 5 < len
                && is_jump(&l[i + 2])
                && l[i + 3] == "("
                && l[i + 5] == ")"
                && l[i + 4].chars().count() >= 3
                && res.name.is_none()
                && could_be_tzname(&l[i + 4])
            {
                res.name = Some(l[i + 4].clone());
                i += 4;
            }
            i += 1;
        } else if !is_jump(&token) {
            return None;
        }
        i += 1;
    }
    // `parserinfo.validate`.
    let nameless = res.name.as_deref().is_none_or(str::is_empty);
    if (res.offset == Some(0) && nameless) || matches!(res.name.as_deref(), Some("Z" | "z")) {
        res.name = Some("UTC".to_string());
        res.offset = Some(0);
    } else if res.offset != Some(0) && !nameless && res.name.as_deref().is_some_and(is_utczone) {
        res.offset = Some(0);
    }
    Some(res)
}

/// `parser._build_tzaware`, minus `tzinfos`: the zone of a parsed time.
fn build_tzaware(
    naive: NaiveDateTime,
    tz: TzFields,
    env: &ParseEnv,
    warnings: &mut Vec<String>,
) -> Option<(Parsed, bool)> {
    let utc = FixedOffset::east_opt(0)?;
    let aware = |offset: FixedOffset| {
        offset
            .from_local_datetime(&naive)
            .single()
            .map(|a| (Parsed::Aware(a), false))
    };
    let name = tz.name.as_deref().filter(|n| !n.is_empty());
    if let (Some(name), Some(local)) = (name, &env.local)
        && local.has_name(name)
    {
        // PARITY: a name equal to either of `time.tzname` (`EST` and `EDT` alike) is the local
        // zone, `tzlocal()`: the offset is the zone's own on that date, not the abbreviation's.
        // `_assign_tzname` picks the second occurrence of a repeated wall time when only that one
        // carries the name.
        let fold =
            local.abbreviation(naive, false) != name && local.abbreviation(naive, true) == name;
        // PARITY: "mostly relevant for winter GMT zones parsed in the UK": a `UTC` / `GMT` name
        // the local zone does not carry on that date is UTC.
        if local.abbreviation(naive, fold) != name && matches!(name, "UTC" | "GMT" | "Z" | "z") {
            return aware(utc);
        }
        return aware(local.offset(naive, fold)).map(|(parsed, _)| (parsed, true));
    }
    match tz.offset {
        Some(0) => aware(utc),
        Some(seconds) => aware(FixedOffset::east_opt(i32::try_from(seconds).ok()?)?),
        None => {
            if let Some(name) = name {
                // PARITY: dateutil's `UnknownTimezoneWarning`: the abbreviation is neither a
                // UTC name nor one of `time.tzname`, so the time stays naive (Meerschaum reads
                // a naive starting time as UTC).
                warnings.push(format!(
                    "tzname {name} identified but not understood.  The starting time is read as UTC; use a UTC offset such as -07:00 or an abbreviation of this host's time zone."
                ));
            }
            Some((Parsed::Naive(naive), false))
        }
    }
}

/// `dt.isoformat()` for a fire time.
pub fn isoformat(dt: &Fire) -> String {
    PyDateTime::Aware(*dt).isoformat()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Fire {
        chrono::DateTime::parse_from_rfc3339("2025-03-14T15:26:53.589793+00:00").unwrap()
    }

    #[test]
    fn daily_and_weekday() {
        let mut t = parse_schedule("daily", now()).unwrap();
        assert_eq!(
            isoformat(&t.next().unwrap()),
            "2025-03-14T15:26:53.589793+00:00"
        );
        assert_eq!(
            isoformat(&t.next().unwrap()),
            "2025-03-15T15:26:53.589793+00:00"
        );
        let mut t = parse_schedule("mon", now()).unwrap();
        assert_eq!(
            isoformat(&t.next().unwrap()),
            "2025-03-17T15:26:53.589793+00:00"
        );
        assert_eq!(
            isoformat(&t.next().unwrap()),
            "2025-03-17T16:26:53.589793+00:00"
        );
    }

    #[test]
    fn periods_are_the_longest_gap_between_fires() {
        assert_eq!(period_seconds_at("every 10 minutes", now()), Some(600.0));
        assert_eq!(period_seconds_at("daily", now()), Some(86400.0));
        assert_eq!(period_seconds_at("hourly", now()), Some(3600.0));
        assert_eq!(period_seconds_at("weekly", now()), Some(7.0 * 86400.0));
        assert_eq!(period_seconds_at("not a schedule", now()), None);
        // Combined schedules are not searched (their fire times can take long to find).
        let begin = std::time::Instant::now();
        assert_eq!(period_seconds_at("daily and mon-fri", now()), None);
        assert!(begin.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn periods_of_zoned_schedules_include_the_clock_change() {
        let march = chrono::DateTime::parse_from_rfc3339("2027-03-01T00:00:00Z").unwrap();
        // The day the clock springs forward is 23 hours; the longest gap is still a day.
        assert_eq!(
            period_seconds_at("daily starting 2027-03-12 21:00 tz America/New_York", march),
            Some(86400.0)
        );
        // The day it falls back is 25 hours.
        assert_eq!(
            period_seconds_at("daily starting 2027-11-05 21:00 tz America/New_York", march),
            Some(90000.0)
        );
        assert_eq!(
            period_seconds_at("every 10 minutes tz Asia/Tokyo jitter 30", march),
            Some(600.0)
        );
    }

    #[test]
    fn quirky_errors() {
        let e = parse_schedule("every 5 fortnights", now()).unwrap_err();
        assert!(e.py_message().starts_with("Invalid interval 'fs'."));
        let e = parse_schedule("daily starting Tomorrow", now()).unwrap_err();
        assert_eq!(
            e.py_message(),
            "\n\x1b[1;31m 🛑 Unknown string format: Tomorrow\x1b[0m"
        );
    }

    fn new_york(as_of: &str) -> ParseEnv {
        ParseEnv::in_zone(
            "America/New_York",
            chrono::DateTime::parse_from_rfc3339(as_of)
                .unwrap()
                .to_utc(),
            NaiveDate::from_ymd_opt(2026, 11, 14).unwrap(),
        )
    }

    fn start(schedule: &str, env: &ParseEnv) -> (Result<String, String>, Vec<String>) {
        let mut warnings = Vec::new();
        let start = parse_start_time_in(schedule, Some(now()), env, &mut warnings)
            .map(|f| isoformat(&f))
            .map_err(|e| e.to_string());
        (start, warnings)
    }

    #[test]
    fn either_abbreviation_of_the_local_zone_is_the_local_zone() {
        let env = new_york("2026-11-14T12:00:00Z");
        for name in ["EDT", "EST"] {
            // PARITY: dateutil maps both `time.tzname` entries to `tzlocal()`.
            assert_eq!(
                start(&format!("daily starting 21:00 {name}"), &env).0,
                Ok("2026-11-14T21:00:00-05:00".to_string())
            );
            assert_eq!(
                start(&format!("daily starting 2026-07-01 21:00 {name}"), &env).0,
                Ok("2026-07-01T21:00:00-04:00".to_string())
            );
        }
        // A repeated wall time takes the occurrence that carries the name.
        assert_eq!(
            start("daily starting 2026-11-01 01:30 EST", &env).0,
            Ok("2026-11-01T01:30:00-05:00".to_string())
        );
        assert_eq!(
            start("daily starting 2026-11-01 01:30 EDT", &env).0,
            Ok("2026-11-01T01:30:00-04:00".to_string())
        );
    }

    #[test]
    fn utc_names_and_offsets_do_not_need_a_local_zone() {
        let env = ParseEnv {
            local: None,
            today: NaiveDate::from_ymd_opt(2026, 11, 14).unwrap(),
            host: None,
        };
        for (text, want) in [
            ("21:00 UTC", "2026-11-14T21:00:00+00:00"),
            ("21:00 GMT", "2026-11-14T21:00:00+00:00"),
            ("21:00 Z", "2026-11-14T21:00:00+00:00"),
            ("21:00Z", "2026-11-14T21:00:00+00:00"),
            ("21:00 -0400", "2026-11-14T21:00:00-04:00"),
            ("21:00 UTC+3", "2026-11-14T21:00:00-03:00"),
            ("21:00 GMT-3", "2026-11-14T21:00:00+03:00"),
            ("21:00 EDT+3", "2026-11-14T21:00:00-03:00"),
        ] {
            let (got, warnings) = start(&format!("daily starting {text}"), &env);
            assert_eq!(got.as_deref(), Ok(want), "{text}");
            assert!(warnings.is_empty(), "{text}");
        }
        // PARITY: dateutil reads zone names only in capitals.
        for text in [
            "21:00 utc",
            "21:00 gmt",
            "21:00 edt",
            "2026-07-01 EDT",
            "21:00 ABCDEF",
        ] {
            assert!(
                start(&format!("daily starting {text}"), &env).0.is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn an_unknown_abbreviation_warns_once_and_reads_utc() {
        let env = new_york("2026-11-14T12:00:00Z");
        let (got, warnings) = start("daily starting 21:00 PDT", &env);
        assert_eq!(got, Ok("2026-11-14T21:00:00+00:00".to_string()));
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].starts_with("tzname PDT identified but not understood."));
        // Without a local zone even EDT is unknown (a host whose zone cannot be named).
        let none = ParseEnv {
            local: None,
            ..new_york("2026-11-14T12:00:00Z")
        };
        let (got, warnings) = start("daily starting 21:00 EDT", &none);
        assert_eq!(got, Ok("2026-11-14T21:00:00+00:00".to_string()));
        assert_eq!(warnings.len(), 1);
        // A schedule parse carries the same warning to its caller.
        let (_, warnings) =
            parse_schedule_in("daily starting 21:00 PDT", now(), None, &env).unwrap();
        assert_eq!(warnings.len(), 1);
    }

    #[test]
    fn a_resumed_fire_keeps_its_cadence() {
        let spec = JitterSpec {
            default_seconds: 30.0,
            seed: "prune",
        };
        // A `daily` job drew 08:46 in an earlier process and restarts at 05:21 the next day.
        let mut before = parse_schedule_jittered("daily", now(), Some(spec)).unwrap();
        let resumed = before.next_after(now()).unwrap();
        let restart = resumed - Duration::hours(3);
        let mut restarted = parse_schedule_jittered("daily", restart, Some(spec)).unwrap();
        assert_ne!(
            restarted.next_after(resumed),
            Some(resumed + Duration::days(1))
        );
        let mut t = parse_schedule_resumed("daily", resumed, Some(spec)).unwrap();
        assert_eq!(t.next_after(resumed), Some(resumed + Duration::days(1)));
        // Cron fields do not depend on the anchor.
        let mut cron = parse_schedule_resumed("30 3 * * *", resumed, Some(spec)).unwrap();
        let mut fresh = parse_schedule_jittered("30 3 * * *", restart, Some(spec)).unwrap();
        assert_eq!(cron.next_after(resumed), fresh.next_after(resumed));
    }
}
