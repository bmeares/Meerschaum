//! Port of `meerschaum/utils/schedule.py`: the `-s` / `--schedule` grammar and its triggers.
//!
//! v4.1.4 ships its own triggers (APScheduler is no longer used): `_IntervalTrigger`,
//! `_CalendarIntervalTrigger`, `_CronTrigger`, `_OrTrigger` and `_AndTrigger`. Times are aware
//! datetimes with a fixed UTC offset; all arithmetic is done in whole microseconds, which is what
//! Python's `datetime.fromtimestamp(ts + step, tz)` round-trips to.

use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone,
    Timelike,
};
use meerschaum_core::pyfmt::{PyDateTime, parse_py_float, parse_py_int};
use regex::Regex;

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
        })
    }

    fn next(&mut self) -> Option<Fire> {
        let next = match self.last {
            None => self.start_time,
            Some(last) => add_us(last, self.interval_us),
        };
        self.last = Some(next);
        Some(next)
    }

    fn next_after(&mut self, after: Fire) -> Option<Fire> {
        let diff = (after - self.start_time)
            .num_microseconds()
            .unwrap_or(i64::MAX);
        let intervals = (diff.div_euclid(self.interval_us) + 1).max(0);
        let next = add_us(self.start_time, self.interval_us.saturating_mul(intervals));
        self.last = Some(next);
        Some(next)
    }
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
        })
    }

    fn next(&mut self) -> Option<Fire> {
        let next_date = match self.last_date {
            None => self.start_date,
            Some(prev) => {
                let (mut year, mut month) = (prev.year(), prev.month() as i32);
                loop {
                    month += self.months;
                    year += self.years + (month - 1).div_euclid(12);
                    month = (month - 1).rem_euclid(12) + 1;
                    if year > 9999 {
                        return None;
                    }
                    if let Some(d) = NaiveDate::from_ymd_opt(year, month as u32, prev.day()) {
                        break d;
                    }
                }
            }
        };
        // PARITY: the `candidate.timetz() != self._time` DST check never fires for fixed offsets.
        let candidate = combine(next_date, self.time, self.tz)?;
        self.last_date = Some(next_date);
        Some(candidate)
    }

    fn next_after(&mut self, after: Fire) -> Option<Fire> {
        self.last_date = None;
        let mut candidate = self.next()?;
        while candidate <= after {
            candidate = self.next()?;
        }
        Some(candidate)
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

/// `_CronTrigger`: the five-field cron subset (plus year and second).
#[derive(Debug, Clone)]
pub struct CronTrigger {
    start_time: Fire,
    tz: FixedOffset,
    minutes: Vec<i64>,
    hours: Vec<i64>,
    days: Vec<i64>,
    days_have_wildcard: bool,
    months: Vec<i64>,
    weekdays: Vec<u32>,
    weekdays_have_wildcard: bool,
    years: Option<Vec<i64>>,
    seconds: Vec<i64>,
    microsecond: u32,
    step_seconds: i64,
    last: Option<Fire>,
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
            minutes,
            hours,
            days,
            days_have_wildcard: f.day.contains('*'),
            months,
            weekdays,
            weekdays_have_wildcard: f.day_of_week.contains('*'),
            years,
            seconds,
            microsecond: f.microsecond,
            step_seconds,
            last: None,
        })
    }

    fn date_matches(&self, c: &Fire) -> bool {
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

    fn matches(&self, c: &Fire) -> bool {
        self.date_matches(c)
            && self.hours.contains(&(c.hour() as i64))
            && self.minutes.contains(&(c.minute() as i64))
            && self.seconds.contains(&(c.second() as i64))
    }

    fn with_micro(dt: Fire, micro: u32) -> Fire {
        dt.with_nanosecond(micro * 1000).unwrap_or(dt)
    }

    fn min_second(&self) -> u32 {
        self.seconds.first().copied().unwrap_or(0) as u32
    }

    fn step(&self, dt: Fire) -> Fire {
        dt + Duration::seconds(self.step_seconds)
    }

    fn next(&mut self) -> Option<Fire> {
        let mut candidate = match self.last {
            None => {
                let mut c = Self::with_micro(self.start_time, self.microsecond);
                if self.step_seconds == 60 {
                    c = c.with_second(self.min_second()).unwrap_or(c);
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
        let max_iterations: u64 = 11 * 366 * 24 * 60 * if self.step_seconds == 1 { 60 } else { 1 };
        for _ in 0..max_iterations {
            if self.matches(&candidate) {
                self.last = Some(candidate);
                return Some(candidate);
            }
            if !self.date_matches(&candidate) {
                let date = candidate.date_naive().succ_opt()?;
                candidate = combine(date, candidate.time(), self.tz)?;
                continue;
            }
            candidate = self.step(candidate);
        }
        None
    }

    fn next_after(&mut self, after: Fire) -> Option<Fire> {
        if after < self.start_time {
            self.last = None;
            return self.next();
        }
        let after = after.with_timezone(&self.tz);
        let mut candidate = Self::with_micro(after, self.microsecond);
        if self.step_seconds == 60 {
            candidate = candidate
                .with_second(self.min_second())
                .unwrap_or(candidate);
        }
        if candidate <= after {
            candidate = self.step(candidate);
        }
        self.last = Some(candidate - Duration::seconds(self.step_seconds));
        self.next()
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
    let (base, suffix) = split_jitter(schedule)?;
    let trigger = parse_schedule_unjittered(&base, now)?;
    let seconds = suffix.or(spec.map(|s| s.default_seconds)).unwrap_or(0.0);
    let offset = crate::startup::fixed_offset(spec.map_or(base.as_str(), |s| s.seed), seconds);
    if offset.is_zero() {
        return Ok(trigger);
    }
    Ok(Trigger::Jittered(
        Box::new(trigger),
        offset.as_micros() as i64,
    ))
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

fn parse_schedule_unjittered(schedule: &str, now: Fire) -> Result<Trigger, ScheduleError> {
    let schedule = canonicalize_starting_keyword(schedule);
    let starting_ts = parse_start_time(&schedule, Some(now))?;
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
                Trigger::Calendar(CalendarIntervalTrigger::new(starting_ts, years, months)?)
            } else {
                let us = timedelta_us(&unit, num).map_err(freq)?;
                Trigger::Interval(IntervalTrigger::new(starting_ts, us)?)
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
                    starting_ts.minute().to_string()
                };
                fields.second = if has_seconds {
                    "*".into()
                } else {
                    starting_ts.second().to_string()
                };
                fields.microsecond = starting_ts.nanosecond() / 1000;
                Trigger::Cron(CronTrigger::new(starting_ts, fields)?)
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
                Trigger::Cron(CronTrigger::new(starting_ts, fields)?)
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
    let starting = if starting_str == "now" {
        now
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
        add_us(now, us)
    } else if starting_str.contains("tomorrow") || starting_str.contains("today") {
        // `round_time(now, timedelta(days=1))`: midnight of `now`'s wall-clock date.
        let today = combine(now.date_naive(), NaiveTime::MIN, *now.offset())
            .ok_or_else(|| err("Invalid date.".to_string()))?;
        let tomorrow = today + Duration::days(1);
        let is_tomorrow = starting_str.contains("tomorrow");
        let time_str = starting_str
            .replace("tomorrow", "")
            .replace("today", "")
            .trim()
            .to_string();
        let (hour, minute) = if time_str.is_empty() {
            (0, 0)
        } else {
            let parsed = parse_datetime(&time_str).map_err(err)?;
            (parsed.hour(), parsed.minute())
        };
        (if is_tomorrow { tomorrow } else { today })
            + Duration::hours(hour as i64)
            + Duration::minutes(minute as i64)
    } else {
        match parse_datetime(starting_str).map_err(err)? {
            Parsed::Naive(n) => FixedOffset::east_opt(0)
                .and_then(|utc| utc.from_local_datetime(&n).single())
                .ok_or_else(|| err("Invalid datetime.".to_string()))?,
            Parsed::Aware(a) => a,
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

/// The subset of `dateutil.parser.parse()` the corpus uses: `YYYY-MM-DD` / `YYYY/MM/DD`, an
/// optional `[T ]H:MM[:SS[.ffffff]]` with `AM` / `PM`, an optional zone (`Z`, `UTC`, `GMT`,
/// `±HH[:MM]`), or a bare time (`10:00`, `7pm`) on today's local date. Errors carry dateutil's
/// messages.
pub fn parse_datetime(s: &str) -> Result<Parsed, String> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r"(?ix)^\s*
            (?:(?P<y>\d{4})[-/](?P<mo>\d{1,2})[-/](?P<d>\d{1,2}))?
            (?:(?:^|[T\s])\s*
                (?P<h>\d{1,2})(?::(?P<mi>\d{1,2})(?::(?P<s>\d{1,2})(?:\.(?P<f>\d{1,6}))?)?)?
                \s*(?P<ampm>am|pm)?
            )?
            \s*(?P<tz>z|utc|gmt|[+-]\d{2}(?::?\d{2})?)?
            \s*$",
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
        let today = chrono::Local::now().date_naive();
        (
            today.year() as i64,
            today.month() as i64,
            today.day() as i64,
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
    let Some(tz) = caps.name("tz") else {
        return Ok(Parsed::Naive(naive));
    };
    let tz = tz.as_str().to_ascii_lowercase();
    let offset_seconds = if tz == "z" || tz == "utc" || tz == "gmt" {
        0
    } else {
        let sign = if tz.starts_with('-') { -1 } else { 1 };
        let digits: String = tz[1..].chars().filter(|c| c.is_ascii_digit()).collect();
        let hh: i32 = digits[..2].parse().unwrap_or(0);
        let mm: i32 = digits.get(2..4).and_then(|m| m.parse().ok()).unwrap_or(0);
        sign * (hh * 3600 + mm * 60)
    };
    let offset = FixedOffset::east_opt(offset_seconds).ok_or_else(unknown)?;
    let aware = offset
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(unknown)?;
    Ok(Parsed::Aware(aware))
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
    fn quirky_errors() {
        let e = parse_schedule("every 5 fortnights", now()).unwrap_err();
        assert!(e.py_message().starts_with("Invalid interval 'fs'."));
        let e = parse_schedule("daily starting Tomorrow", now()).unwrap_err();
        assert_eq!(
            e.py_message(),
            "\n\x1b[1;31m 🛑 Unknown string format: Tomorrow\x1b[0m"
        );
    }
}
