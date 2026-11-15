//! The process's local time zone as Python sees it through `dateutil`.
//!
//! v4 reads a schedule's `starting` time with `dateutil.parser.parse`. When the string carries a
//! zone abbreviation equal to one of `time.tzname` (`EST` and `EDT` in `America/New_York`),
//! dateutil returns the time in `tz.tzlocal()`; this module reproduces the two inputs that
//! decision and the resulting offset need:
//!
//! - `time.tzname`, derived the way CPython derives it from the zone's own rules (the
//!   abbreviation and offset in effect on 1 January and about 2 July of the current year, the one
//!   with the smaller UTC offset being standard time), so no table of abbreviations is kept;
//! - `tzlocal`'s two-offset model (`-time.timezone` when standard, `-time.altzone` when daylight
//!   saving time is in force), including its choice for a wall time that does not exist or that
//!   occurs twice.
//!
//! The zone comes from `chrono-tz` (the IANA database), named by `TZ` or, without it, by the
//! system's own zone name (`iana-time-zone`): safe code, no `tzset` / `tzname` calls. Windows has
//! no IANA abbreviations in `time.tzname` (it holds the zone's long Windows names, which can
//! never equal an abbreviation token), so v4 never matches a local abbreviation there; neither do
//! we ([`LocalZone::detect`] is `None` on Windows).

use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::{OffsetComponents, OffsetName, Tz};

/// CPython's `YEAR` in `init_timezone`: `(365 * 24 + 6) * 3600` seconds.
const CPYTHON_YEAR_SECONDS: i64 = (365 * 24 + 6) * 3600;

/// A zone in the terms of `time.tzname` / `time.timezone` / `time.altzone` and `tzlocal`.
#[derive(Debug, Clone)]
pub struct LocalZone {
    tz: Tz,
    /// `time.tzname`: standard and daylight abbreviations.
    names: [String; 2],
    /// `-time.timezone` in seconds east of UTC.
    std_offset: i32,
    /// `-time.altzone` (`std_offset` when the zone has no daylight saving time).
    dst_offset: i32,
}

impl LocalZone {
    /// `tz` as Python initializes `time.tzname` at `as_of` (the current instant in production).
    pub fn new(tz: Tz, as_of: DateTime<Utc>) -> LocalZone {
        // PARITY: CPython (`time.tzset()` / import) samples `localtime` at the start of the
        // current 365.25-day year since the epoch and half a year later.
        let jan = (as_of.timestamp().div_euclid(CPYTHON_YEAR_SECONDS)) * CPYTHON_YEAR_SECONDS;
        let july = jan + CPYTHON_YEAR_SECONDS / 2;
        let sample = |secs: i64| {
            let at = DateTime::from_timestamp(secs, 0)
                .unwrap_or(as_of)
                .naive_utc();
            let offset = tz.offset_from_utc_datetime(&at);
            let gmtoff = offset.fix().local_minus_utc();
            // The database spells a zone without letters as its offset (`-03`, `+0530`).
            let name = match offset.abbreviation() {
                Some(name) if !name.is_empty() => name.to_string(),
                _ => numeric_abbreviation(gmtoff),
            };
            (name, -gmtoff)
        };
        let (jan_name, jan_zone) = sample(jan);
        let (july_name, july_zone) = sample(july);
        // DST is reversed in the southern hemisphere.
        let (names, timezone, altzone) = if jan_zone < july_zone {
            ([july_name, jan_name], july_zone, jan_zone)
        } else {
            ([jan_name, july_name], jan_zone, july_zone)
        };
        LocalZone {
            tz,
            names,
            std_offset: -timezone,
            dst_offset: -altzone,
        }
    }

    /// [`LocalZone::new`] from an IANA zone name (`America/New_York`).
    pub fn from_name(name: &str, as_of: DateTime<Utc>) -> Option<LocalZone> {
        name.parse::<Tz>().ok().map(|tz| LocalZone::new(tz, as_of))
    }

    /// The zone of this process (`TZ`, else the system zone), or `None` when it cannot be named
    /// (and always on Windows: see the module docs). Computed once.
    pub fn detect() -> Option<Arc<LocalZone>> {
        static LOCAL: OnceLock<Option<Arc<LocalZone>>> = OnceLock::new();
        LOCAL
            .get_or_init(|| {
                detect_name().and_then(|name| LocalZone::from_name(&name, Utc::now()).map(Arc::new))
            })
            .clone()
    }

    /// The IANA zone.
    pub fn tz(&self) -> Tz {
        self.tz
    }

    /// `time.tzname`.
    pub fn names(&self) -> [&str; 2] {
        [&self.names[0], &self.names[1]]
    }

    /// Whether `name` is one of `time.tzname` (`name in time.tzname`).
    pub fn has_name(&self, name: &str) -> bool {
        self.names.iter().any(|n| n == name)
    }

    /// The date of `instant` on this zone's wall clock (dateutil's default date is "today").
    pub fn today(&self, instant: DateTime<Utc>) -> NaiveDate {
        instant.with_timezone(&self.tz).date_naive()
    }

    fn has_dst(&self) -> bool {
        self.dst_offset != self.std_offset
    }

    /// `tzlocal._naive_is_dst`: `localtime(wall read as standard time).tm_isdst`.
    fn naive_is_dst(&self, wall: NaiveDateTime) -> bool {
        let utc = wall - Duration::seconds(i64::from(self.std_offset));
        self.tz.offset_from_utc_datetime(&utc).dst_offset() != chrono::TimeDelta::zero()
    }

    /// `tzlocal._isdst(dt)` for `dt` with `fold`.
    fn is_dst(&self, wall: NaiveDateTime, fold: bool) -> bool {
        if !self.has_dst() {
            return false;
        }
        let saved = Duration::seconds(i64::from(self.dst_offset - self.std_offset));
        let ambiguous = !self.naive_is_dst(wall) && self.naive_is_dst(wall - saved);
        if ambiguous {
            return !fold;
        }
        self.naive_is_dst(wall)
    }

    /// `tzlocal.tzname(dt)`.
    pub fn abbreviation(&self, wall: NaiveDateTime, fold: bool) -> &str {
        &self.names[usize::from(self.is_dst(wall, fold))]
    }

    /// `tzlocal.utcoffset(dt)`: the offset of the wall time `wall` (`fold` selects the second
    /// occurrence of a repeated wall time).
    pub fn offset(&self, wall: NaiveDateTime, fold: bool) -> FixedOffset {
        let seconds = if self.is_dst(wall, fold) {
            self.dst_offset
        } else {
            self.std_offset
        };
        FixedOffset::east_opt(seconds).unwrap_or_else(|| Utc.fix())
    }
}

/// tzdata's abbreviation for a zone that has none: `-03`, `+0530`.
fn numeric_abbreviation(gmtoff: i32) -> String {
    let sign = if gmtoff < 0 { '-' } else { '+' };
    let (hours, minutes) = (gmtoff.abs() / 3600, gmtoff.abs() % 3600 / 60);
    if minutes == 0 {
        format!("{sign}{hours:02}")
    } else {
        format!("{sign}{hours:02}{minutes:02}")
    }
}

/// The IANA name of the process's zone (see [`LocalZone::detect`]).
#[cfg(unix)]
fn detect_name() -> Option<String> {
    match std::env::var("TZ") {
        Ok(tz) => {
            // glibc: a leading `:` is ignored, an empty value is UTC, a path names a zoneinfo file.
            let tz = tz.strip_prefix(':').unwrap_or(&tz);
            let tz = tz.split_once("zoneinfo/").map_or(tz, |(_, name)| name);
            Some(if tz.is_empty() { "UTC" } else { tz }.to_string())
        }
        Err(_) => iana_time_zone::get_timezone().ok(),
    }
}

#[cfg(not(unix))]
fn detect_name() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zone(name: &str) -> LocalZone {
        let as_of = DateTime::parse_from_rfc3339("2026-11-14T12:00:00Z")
            .unwrap()
            .to_utc();
        LocalZone::from_name(name, as_of).unwrap()
    }

    fn wall(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M").unwrap()
    }

    #[test]
    fn tzname_matches_cpython() {
        // Recorded from CPython 3.14 `time.tzname` under each `TZ` (glibc).
        for (name, expected) in [
            ("America/New_York", ["EST", "EDT"]),
            ("UTC", ["UTC", "UTC"]),
            ("Europe/London", ["GMT", "BST"]),
            ("Australia/Sydney", ["AEST", "AEDT"]),
            ("Asia/Kolkata", ["IST", "IST"]),
            ("America/Sao_Paulo", ["-03", "-03"]),
        ] {
            assert_eq!(zone(name).names(), expected, "{name}");
        }
    }

    #[test]
    fn offsets_follow_the_dates_rules() {
        let ny = zone("America/New_York");
        let off = |s: &str, fold| ny.offset(wall(s), fold).local_minus_utc() / 3600;
        assert_eq!(off("2026-01-01 21:00", false), -5);
        assert_eq!(off("2026-07-01 21:00", false), -4);
        // A wall time that does not exist is read as standard time, which is daylight time.
        assert_eq!(off("2026-03-08 02:30", false), -4);
        // A repeated wall time: first occurrence is daylight time, `fold` picks standard time.
        assert_eq!(off("2026-11-01 01:30", false), -4);
        assert_eq!(off("2026-11-01 01:30", true), -5);
        assert_eq!(ny.abbreviation(wall("2026-11-01 01:30"), true), "EST");
    }
}
