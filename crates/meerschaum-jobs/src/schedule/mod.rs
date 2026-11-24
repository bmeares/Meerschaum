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

mod datetime;
mod triggers;

use datetime::parse_datetime_flagged;
pub use datetime::{Parsed, parse_datetime, parse_datetime_in};
pub use triggers::{CalendarIntervalTrigger, CronFields, CronTrigger, IntervalTrigger, Trigger};

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

/// A schedule's start: the instant it names and its wall time in the schedule's zone (the
/// written time when it was read in the zone, else the instant's wall time there).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Start {
    fire: Fire,
    wall: NaiveDateTime,
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
            }
            Trigger::Cron(CronTrigger::for_schedule(
                starting_ts,
                start.as_ref(),
                zone,
                fields,
                !is_named,
            )?)
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

/// `dt.isoformat()` for a fire time.
pub fn isoformat(dt: &Fire) -> String {
    PyDateTime::Aware(*dt).isoformat()
}

#[cfg(test)]
mod tests;
