//! Port of `dateutil.parser.parse()` for the `starting` clause: naive and aware datetimes and
//! the time zone names and offsets that may follow them.

use super::*;

/// A `dateutil.parser.parse()` result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parsed {
    /// No timezone in the string.
    Naive(NaiveDateTime),
    /// With a timezone (`Z`, `UTC`, `GMT`, or a numeric offset).
    Aware(DateTime<FixedOffset>),
}

impl Parsed {
    pub(super) fn hour(&self) -> u32 {
        match self {
            Parsed::Naive(n) => n.hour(),
            Parsed::Aware(a) => a.hour(),
        }
    }

    pub(super) fn minute(&self) -> u32 {
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
pub(super) fn parse_datetime_flagged(
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
