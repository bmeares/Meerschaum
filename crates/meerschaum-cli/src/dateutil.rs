//! A port of `dateutil.parser.parse` (dateutil 2.9, default `parserinfo`, `fuzzy=False`,
//! `dayfirst=False`, `yearfirst=False`, no `tzinfos`) as `parse_datetime` calls it.
//!
//! Follows `dateutil/parser/_parser.py` (`_timelex`, `_ymd`, `parser._parse`,
//! `_parse_numeric_token`, `_build_naive`, `_build_tzaware`) branch for branch. The only
//! environment inputs are in [`DateContext`]: the `default` datetime (today, local midnight) and
//! the local year used to pivot two-digit years.
//!
//! Known gap: a timezone name equal to one of the local `time.tzname` abbreviations (e.g. `EST`
//! on a US/Eastern host) makes dateutil return a `tzlocal()` datetime; this port does not know
//! the local abbreviations and returns the naive time (treated as UTC), as dateutil does for any
//! other unknown abbreviation.

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::str::FromStr;

use crate::types::{DateContext, add_micros, days_in_month, py_float};
use crate::value::NotParsed;

/// Parse a date/time string. Returns the naive datetime and, if the string carried a UTC
/// offset (or a UTC zone name), that offset in seconds.
pub fn parse(timestr: &str, ctx: &DateContext) -> Result<(NaiveDateTime, Option<i64>), NotParsed> {
    parse_inner(timestr, ctx).map_err(|()| NotParsed)
}

fn parse_inner(timestr: &str, ctx: &DateContext) -> Result<(NaiveDateTime, Option<i64>), ()> {
    let res = parse_tokens(timestr, ctx.local_year)?;
    if res.len() == 0 {
        return Err(());
    }
    let naive = build_naive(&res, &ctx.dateutil_default)?;
    // `_build_tzaware` (no tzinfos; local tz names are not known, see the module docs).
    let offset = match (&res.tzname, res.tzoffset) {
        (_, Some(0)) => Some(0),
        (_, Some(off)) => Some(off),
        _ => None,
    };
    Ok((naive, offset))
}

// ---------------------------------------------------------------------------------------------
// _timelex
// ---------------------------------------------------------------------------------------------

fn isword(c: char) -> bool {
    c.is_alphabetic()
}

/// `str.isdigit` for ASCII. PARITY gap: Python also accepts non-ASCII digits here.
fn isnum(c: char) -> bool {
    c.is_ascii_digit()
}

fn isspace(c: char) -> bool {
    c.is_whitespace()
}

/// `_timelex.split`.
fn timelex(s: &str) -> Vec<String> {
    let chars: Vec<char> = s.chars().filter(|c| *c != '\0').collect();
    let mut pos = 0usize;
    let mut charstack: Vec<char> = Vec::new();
    let mut tokenstack: std::collections::VecDeque<String> = Default::default();
    let mut out = Vec::new();
    let mut eof = false;

    loop {
        if let Some(tok) = tokenstack.pop_front() {
            out.push(tok);
            continue;
        }
        if eof {
            break;
        }
        let mut seenletters = false;
        let mut token: Option<String> = None;
        let mut state: Option<&str> = None;
        loop {
            let nextchar = if !charstack.is_empty() {
                Some(charstack.remove(0))
            } else if pos < chars.len() {
                pos += 1;
                Some(chars[pos - 1])
            } else {
                None
            };
            let Some(nextchar) = nextchar else {
                eof = true;
                break;
            };
            match state {
                None => {
                    token = Some(nextchar.to_string());
                    if isword(nextchar) {
                        state = Some("a");
                    } else if isnum(nextchar) {
                        state = Some("0");
                    } else if isspace(nextchar) {
                        token = Some(" ".into());
                        break;
                    } else {
                        break;
                    }
                }
                Some("a") => {
                    seenletters = true;
                    let t = token.as_mut().expect("token started");
                    if isword(nextchar) {
                        t.push(nextchar);
                    } else if nextchar == '.' {
                        t.push(nextchar);
                        state = Some("a.");
                    } else {
                        charstack.push(nextchar);
                        break;
                    }
                }
                Some("0") => {
                    let t = token.as_mut().expect("token started");
                    if isnum(nextchar) {
                        t.push(nextchar);
                    } else if nextchar == '.' || (nextchar == ',' && t.chars().count() >= 2) {
                        t.push(nextchar);
                        state = Some("0.");
                    } else {
                        charstack.push(nextchar);
                        break;
                    }
                }
                Some("a.") => {
                    seenletters = true;
                    let t = token.as_mut().expect("token started");
                    if nextchar == '.' || isword(nextchar) {
                        t.push(nextchar);
                    } else if isnum(nextchar) && t.ends_with('.') {
                        t.push(nextchar);
                        state = Some("0.");
                    } else {
                        charstack.push(nextchar);
                        break;
                    }
                }
                Some("0.") => {
                    let t = token.as_mut().expect("token started");
                    if nextchar == '.' || isnum(nextchar) {
                        t.push(nextchar);
                    } else if isword(nextchar) && t.ends_with('.') {
                        t.push(nextchar);
                        state = Some("a.");
                    } else {
                        charstack.push(nextchar);
                        break;
                    }
                }
                Some(_) => unreachable!(),
            }
        }
        let Some(mut tok) = token else {
            break;
        };
        if matches!(state, Some("a.") | Some("0."))
            && (seenletters
                || tok.matches('.').count() > 1
                || tok.ends_with('.')
                || tok.ends_with(','))
        {
            // `_split_decimal.split(token)` with the capture group `([.,])`.
            let mut pieces: Vec<String> = Vec::new();
            let mut cur = String::new();
            for c in tok.chars() {
                if c == '.' || c == ',' {
                    pieces.push(std::mem::take(&mut cur));
                    pieces.push(c.to_string());
                } else {
                    cur.push(c);
                }
            }
            pieces.push(cur);
            tok = pieces[0].clone();
            for p in pieces.into_iter().skip(1) {
                if !p.is_empty() {
                    tokenstack.push_back(p);
                }
            }
        }
        if state == Some("0.") && !tok.contains('.') {
            tok = tok.replace(',', ".");
        }
        out.push(tok);
    }
    out
}

// ---------------------------------------------------------------------------------------------
// parserinfo
// ---------------------------------------------------------------------------------------------

const JUMP: &[&str] = &[
    " ", ".", ",", ";", "-", "/", "'", "at", "on", "and", "ad", "m", "t", "of", "st", "nd", "rd",
    "th",
];
const WEEKDAYS: &[&[&str]] = &[
    &["mon", "monday"],
    &["tue", "tuesday"],
    &["wed", "wednesday"],
    &["thu", "thursday"],
    &["fri", "friday"],
    &["sat", "saturday"],
    &["sun", "sunday"],
];
const MONTHS: &[&[&str]] = &[
    &["jan", "january"],
    &["feb", "february"],
    &["mar", "march"],
    &["apr", "april"],
    &["may", "may"],
    &["jun", "june"],
    &["jul", "july"],
    &["aug", "august"],
    &["sep", "sept", "september"],
    &["oct", "october"],
    &["nov", "november"],
    &["dec", "december"],
];
const HMS: &[&[&str]] = &[
    &["h", "hour", "hours"],
    &["m", "minute", "minutes"],
    &["s", "second", "seconds"],
];
const AMPM: &[&[&str]] = &[&["am", "a"], &["pm", "p"]];
const UTCZONE: &[&str] = &["UTC", "GMT", "Z", "z"];

fn lookup(table: &[&[&str]], name: &str) -> Option<i64> {
    let lower = name.to_lowercase();
    table
        .iter()
        .position(|names| names.contains(&lower.as_str()))
        .map(|i| i as i64)
}

fn jump(name: &str) -> bool {
    JUMP.contains(&name.to_lowercase().as_str())
}
fn weekday(name: &str) -> Option<i64> {
    lookup(WEEKDAYS, name)
}
fn month(name: &str) -> Option<i64> {
    lookup(MONTHS, name).map(|m| m + 1)
}
fn hms(name: &str) -> Option<i64> {
    lookup(HMS, name)
}
fn ampm(name: &str) -> Option<i64> {
    lookup(AMPM, name)
}
fn pertain(name: &str) -> bool {
    name.to_lowercase() == "of"
}
fn utczone(name: &str) -> bool {
    UTCZONE
        .iter()
        .any(|z| z.to_lowercase() == name.to_lowercase())
}
fn tzoffset_of(name: &str) -> Option<i64> {
    // PARITY: `parserinfo.tzoffset` checks `name in self._utczone`, whose keys are lowercased,
    // so only "z" matches here; "UTC"/"GMT" get their zero offset later in `validate`.
    if ["utc", "gmt", "z"].contains(&name) {
        return Some(0);
    }
    None
}

fn convertyear(year: i64, century_specified: bool, local_year: i64) -> Result<i64, ()> {
    if year < 0 {
        return Err(());
    }
    let mut year = year;
    if year < 100 && !century_specified {
        let century = local_year / 100 * 100;
        year += century;
        if year >= local_year + 50 {
            year -= 100;
        } else if year < local_year - 50 {
            year += 100;
        }
    }
    Ok(year)
}

// ---------------------------------------------------------------------------------------------
// Values: tokens can be `str`, `Decimal` or `int` when appended to `_ymd`.
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum YmdArg {
    Str(String),
    Dec(Decimal),
    Int(i64),
}

/// Python `int(str)` for timelex tokens (digits only; `None` → `ValueError`).
fn int_of(s: &str) -> Result<i64, ()> {
    let t = s.trim();
    let (neg, body) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    if body.is_empty() || !body.chars().all(|c| c.is_ascii_digit() || c == '_') {
        return Err(());
    }
    if body.starts_with('_') || body.ends_with('_') || body.contains("__") {
        return Err(());
    }
    let v: i64 = body.replace('_', "").parse().map_err(|_| ())?;
    Ok(if neg { -v } else { v })
}

/// `parser._to_decimal`.
fn to_decimal(s: &str) -> Result<Decimal, ()> {
    if !s.chars().all(|c| c.is_ascii_digit() || c == '.')
        || s.is_empty()
        || s.matches('.').count() > 1
        || s == "."
    {
        return Err(());
    }
    Decimal::from_str(s).map_err(|_| ())
}

fn dec_trunc(d: &Decimal) -> i64 {
    d.trunc().to_i64().unwrap_or(i64::MAX)
}

fn dec_frac(d: &Decimal) -> Decimal {
    d.fract()
}

#[derive(Debug, Default)]
struct Ymd {
    values: Vec<i64>,
    century_specified: bool,
    dstridx: Option<usize>,
    mstridx: Option<usize>,
    ystridx: Option<usize>,
}

impl Ymd {
    fn len(&self) -> usize {
        self.values.len()
    }

    /// Python list indexing, including negative indices.
    fn at(&self, ix: i64) -> Result<i64, ()> {
        let n = self.values.len() as i64;
        let real = if ix < 0 { n + ix } else { ix };
        if real < 0 || real >= n {
            return Err(());
        }
        Ok(self.values[real as usize])
    }

    fn could_be_day(&self, value: i64) -> bool {
        if self.dstridx.is_some() {
            false
        } else if self.mstridx.is_none() {
            (1..=31).contains(&value)
        } else if self.ystridx.is_none() {
            let m = self.values[self.mstridx.unwrap_or(0)];
            match days_in_month(2000, m) {
                Some(d) => 1 <= value && value <= d,
                None => false,
            }
        } else {
            let m = self.values[self.mstridx.unwrap_or(0)];
            let y = self.values[self.ystridx.unwrap_or(0)];
            match days_in_month(y, m) {
                Some(d) => 1 <= value && value <= d,
                None => false,
            }
        }
    }

    fn could_be_day_dec(&self, value: &Decimal) -> bool {
        // `1 <= value <= 31` with a Decimal value.
        if value.fract() != Decimal::ZERO {
            if self.dstridx.is_some() {
                return false;
            }
            let upper = if self.mstridx.is_none() {
                31
            } else {
                let m = self.values[self.mstridx.unwrap_or(0)];
                let y = self.ystridx.map(|i| self.values[i]).unwrap_or(2000);
                match days_in_month(y, m) {
                    Some(d) => d,
                    None => return false,
                }
            };
            return *value >= Decimal::ONE && *value <= Decimal::from(upper);
        }
        match value.to_i64() {
            Some(v) => self.could_be_day(v),
            None => false,
        }
    }

    fn append(&mut self, val: YmdArg, label: Option<char>) -> Result<(), ()> {
        let mut label = label;
        let int_val = match &val {
            YmdArg::Str(s) => {
                if s.chars().all(|c| c.is_ascii_digit()) && !s.is_empty() && s.chars().count() > 2 {
                    self.century_specified = true;
                    if !matches!(label, None | Some('Y')) {
                        return Err(());
                    }
                    label = Some('Y');
                }
                int_of(s)?
            }
            YmdArg::Dec(d) => {
                if *d > Decimal::from(100) {
                    self.century_specified = true;
                    if !matches!(label, None | Some('Y')) {
                        return Err(());
                    }
                    label = Some('Y');
                }
                dec_trunc(d)
            }
            YmdArg::Int(i) => {
                if *i > 100 {
                    self.century_specified = true;
                    if !matches!(label, None | Some('Y')) {
                        return Err(());
                    }
                    label = Some('Y');
                }
                *i
            }
        };
        self.values.push(int_val);
        let ix = self.values.len() - 1;
        match label {
            Some('M') => {
                if self.mstridx.is_some() {
                    return Err(());
                }
                self.mstridx = Some(ix);
            }
            Some('D') => {
                if self.dstridx.is_some() {
                    return Err(());
                }
                self.dstridx = Some(ix);
            }
            Some('Y') => {
                if self.ystridx.is_some() {
                    return Err(());
                }
                self.ystridx = Some(ix);
            }
            _ => {}
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn resolve_from_stridxs(
        &self,
        strids: &mut Vec<(char, usize)>,
    ) -> (Option<i64>, Option<i64>, Option<i64>) {
        if self.len() == 3 && strids.len() == 2 {
            let used: Vec<usize> = strids.iter().map(|(_, v)| *v).collect();
            let missing = (0..3).find(|x| !used.contains(x)).unwrap_or(0);
            let key = ['y', 'm', 'd']
                .into_iter()
                .find(|k| !strids.iter().any(|(s, _)| s == k))
                .unwrap_or('y');
            strids.push((key, missing));
        }
        let get = |k: char| {
            strids
                .iter()
                .find(|(s, _)| *s == k)
                .map(|(_, ix)| self.values[*ix])
        };
        (get('y'), get('m'), get('d'))
    }

    #[allow(clippy::type_complexity)]
    fn resolve_ymd(&self) -> Result<(Option<i64>, Option<i64>, Option<i64>), ()> {
        // dayfirst = yearfirst = False.
        let len_ymd = self.len();
        let mut strids: Vec<(char, usize)> = Vec::new();
        for (k, v) in [
            ('y', self.ystridx),
            ('m', self.mstridx),
            ('d', self.dstridx),
        ] {
            if let Some(v) = v {
                strids.push((k, v));
            }
        }
        if (len_ymd == strids.len() && !strids.is_empty()) || (len_ymd == 3 && strids.len() == 2) {
            return Ok(self.resolve_from_stridxs(&mut strids));
        }
        let (mut year, mut month, mut day) = (None, None, None);
        let mstridx = self.mstridx;
        if len_ymd > 3 {
            return Err(());
        } else if len_ymd == 1 || (mstridx.is_some() && len_ymd == 2) {
            let other;
            if let Some(mi) = mstridx {
                month = Some(self.values[mi]);
                other = self.at(mi as i64 - 1)?;
            } else {
                other = self.values[0];
            }
            if len_ymd > 1 || mstridx.is_none() {
                if other > 31 {
                    year = Some(other);
                } else {
                    day = Some(other);
                }
            }
        } else if len_ymd == 2 {
            let (a, b) = (self.values[0], self.values[1]);
            if a > 31 {
                year = Some(a);
                month = Some(b);
            } else if b > 31 {
                month = Some(a);
                year = Some(b);
            } else {
                month = Some(a);
                day = Some(b);
            }
        } else if len_ymd == 3 {
            let (a, b, c) = (self.values[0], self.values[1], self.values[2]);
            match mstridx {
                Some(0) => {
                    if b > 31 {
                        month = Some(a);
                        year = Some(b);
                        day = Some(c);
                    } else {
                        month = Some(a);
                        day = Some(b);
                        year = Some(c);
                    }
                }
                Some(1) => {
                    if a > 31 {
                        year = Some(a);
                        month = Some(b);
                        day = Some(c);
                    } else {
                        day = Some(a);
                        month = Some(b);
                        year = Some(c);
                    }
                }
                Some(2) => {
                    if b > 31 {
                        day = Some(a);
                        year = Some(b);
                        month = Some(c);
                    } else {
                        year = Some(a);
                        day = Some(b);
                        month = Some(c);
                    }
                }
                _ => {
                    if a > 31 || self.ystridx == Some(0) {
                        year = Some(a);
                        month = Some(b);
                        day = Some(c);
                    } else if a > 12 {
                        day = Some(a);
                        month = Some(b);
                        year = Some(c);
                    } else {
                        month = Some(a);
                        day = Some(b);
                        year = Some(c);
                    }
                }
            }
        }
        Ok((year, month, day))
    }
}

#[derive(Debug, Default)]
struct ParseResult {
    year: Option<i64>,
    month: Option<i64>,
    day: Option<i64>,
    weekday: Option<i64>,
    hour: Option<i64>,
    minute: Option<i64>,
    second: Option<i64>,
    microsecond: Option<i64>,
    tzname: Option<String>,
    tzoffset: Option<i64>,
    ampm: Option<i64>,
    century_specified: bool,
}

impl ParseResult {
    /// `len(res)`: the number of non-`None` slots.
    fn len(&self) -> usize {
        [
            self.year.is_some(),
            self.month.is_some(),
            self.day.is_some(),
            self.weekday.is_some(),
            self.hour.is_some(),
            self.minute.is_some(),
            self.second.is_some(),
            self.microsecond.is_some(),
            self.tzname.is_some(),
            self.tzoffset.is_some(),
            self.ampm.is_some(),
        ]
        .iter()
        .filter(|x| **x)
        .count()
    }
}

/// Bounds-checked token access (Python `IndexError` → `Err`).
fn tok(l: &[String], i: usize) -> Result<&str, ()> {
    l.get(i).map(String::as_str).ok_or(())
}

fn could_be_tzname(res: &ParseResult, token: &str) -> bool {
    res.hour.is_some()
        && res.tzname.is_none()
        && res.tzoffset.is_none()
        && token.chars().count() <= 5
        && (token.chars().all(|c| c.is_ascii_uppercase()) || UTCZONE.contains(&token))
}

fn could_be_tzname_with(hour: Option<i64>, tzname: &Option<String>, token: &str) -> bool {
    hour.is_some()
        && tzname.is_none()
        && token.chars().count() <= 5
        && (token.chars().all(|c| c.is_ascii_uppercase()) || UTCZONE.contains(&token))
}

fn adjust_ampm(hour: i64, ampm: i64) -> i64 {
    if hour < 12 && ampm == 1 {
        hour + 12
    } else if hour == 12 && ampm == 0 {
        0
    } else {
        hour
    }
}

fn parse_min_sec(value: &Decimal) -> (i64, Option<i64>) {
    let minute = dec_trunc(value);
    let rem = dec_frac(value);
    let second = if rem != Decimal::ZERO {
        Some(dec_trunc(&(Decimal::from(60) * rem)))
    } else {
        None
    };
    (minute, second)
}

fn parsems(value: &str) -> Result<(i64, i64), ()> {
    if !value.contains('.') {
        return Ok((int_of(value)?, 0));
    }
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() != 2 {
        return Err(());
    }
    let f: String = format!("{:0<6}", parts[1]).chars().take(6).collect();
    Ok((int_of(parts[0])?, int_of(&f)?))
}

fn find_hms_idx(idx: usize, tokens: &[String]) -> Option<usize> {
    let len_l = tokens.len();
    if idx + 1 < len_l && hms(&tokens[idx + 1]).is_some() {
        Some(idx + 1)
    } else if idx + 2 < len_l && tokens[idx + 1] == " " && hms(&tokens[idx + 2]).is_some() {
        Some(idx + 2)
    } else if idx > 0 && hms(&tokens[idx - 1]).is_some() {
        Some(idx - 1)
    } else if 1 < idx
        && idx == len_l - 1
        && tokens[idx - 1] == " "
        && hms(&tokens[idx - 2]).is_some()
    {
        Some(idx - 2)
    } else {
        None
    }
}

fn assign_hms(res: &mut ParseResult, value_repr: &str, hms_kind: i64) -> Result<(), ()> {
    let value = to_decimal(value_repr)?;
    match hms_kind {
        0 => {
            res.hour = Some(dec_trunc(&value));
            let frac = dec_frac(&value);
            if frac != Decimal::ZERO {
                res.minute = Some(dec_trunc(&(Decimal::from(60) * frac)));
            }
        }
        1 => {
            let (m, s) = parse_min_sec(&value);
            res.minute = Some(m);
            res.second = s;
        }
        2 => {
            let (s, us) = parsems(value_repr)?;
            res.second = Some(s);
            res.microsecond = Some(us);
        }
        _ => {}
    }
    Ok(())
}

fn parse_numeric_token(
    tokens: &[String],
    mut idx: usize,
    ymd: &mut Ymd,
    res: &mut ParseResult,
) -> Result<usize, ()> {
    let value_repr = tokens[idx].clone();
    let value = to_decimal(&value_repr)?;
    let len_li = value_repr.chars().count();
    let len_l = tokens.len();

    if ymd.len() == 3
        && (len_li == 2 || len_li == 4)
        && res.hour.is_none()
        && (idx + 1 >= len_l || (tokens[idx + 1] != ":" && hms(&tokens[idx + 1]).is_none()))
    {
        // 19990101T23[59]
        let s = &value_repr;
        res.hour = Some(int_of(&s[..2])?);
        if len_li == 4 {
            res.minute = Some(int_of(&s[2..])?);
        }
    } else if len_li == 6 || (len_li > 6 && value_repr.find('.') == Some(6)) {
        // YYMMDD or HHMMSS[.ss]
        let s = &value_repr;
        if ymd.len() == 0 && !s.contains('.') {
            ymd.append(YmdArg::Str(s[..2].into()), None)?;
            ymd.append(YmdArg::Str(s[2..4].into()), None)?;
            ymd.append(YmdArg::Str(s[4..].into()), None)?;
        } else {
            res.hour = Some(int_of(&s[..2])?);
            res.minute = Some(int_of(&s[2..4])?);
            let (sec, us) = parsems(&s[4..])?;
            res.second = Some(sec);
            res.microsecond = Some(us);
        }
    } else if len_li == 8 || len_li == 12 || len_li == 14 {
        // YYYYMMDD
        let s = &value_repr;
        ymd.append(YmdArg::Str(s[..4].into()), Some('Y'))?;
        ymd.append(YmdArg::Str(s[4..6].into()), None)?;
        ymd.append(YmdArg::Str(s[6..8].into()), None)?;
        if len_li > 8 {
            res.hour = Some(int_of(&s[8..10])?);
            res.minute = Some(int_of(&s[10..12])?);
            if len_li > 12 {
                res.second = Some(int_of(&s[12..])?);
            }
        }
    } else if let Some(hms_idx) = find_hms_idx(idx, tokens) {
        // HH[ ]h or MM[ ]m or SS[.ss][ ]s
        let (new_idx, kind) = if hms_idx > idx {
            (hms_idx, hms(&tokens[hms_idx]))
        } else {
            (idx, hms(&tokens[hms_idx]).map(|h| h + 1))
        };
        idx = new_idx;
        if let Some(kind) = kind {
            assign_hms(res, &value_repr, kind)?;
        }
    } else if idx + 2 < len_l && tokens[idx + 1] == ":" {
        // HH:MM[:SS[.ss]]
        res.hour = Some(dec_trunc(&value));
        let v2 = to_decimal(&tokens[idx + 2])?;
        let (m, s) = parse_min_sec(&v2);
        res.minute = Some(m);
        res.second = s;
        if idx + 4 < len_l && tokens[idx + 3] == ":" {
            let (sec, us) = parsems(&tokens[idx + 4])?;
            res.second = Some(sec);
            res.microsecond = Some(us);
            idx += 2;
        }
        idx += 2;
    } else if idx + 1 < len_l && ["-", "/", "."].contains(&tokens[idx + 1].as_str()) {
        let sep = tokens[idx + 1].clone();
        ymd.append(YmdArg::Str(value_repr.clone()), None)?;
        if idx + 2 < len_l && !jump(&tokens[idx + 2]) {
            let t2 = &tokens[idx + 2];
            if !t2.is_empty() && t2.chars().all(isnum) {
                // 01-01[-01]
                ymd.append(YmdArg::Str(t2.clone()), None)?;
            } else {
                // 01-Jan[-01]
                match month(t2) {
                    Some(m) => ymd.append(YmdArg::Int(m), Some('M'))?,
                    None => return Err(()),
                }
            }
            if idx + 3 < len_l && tokens[idx + 3] == sep {
                // We have three members.
                let t4 = tok(tokens, idx + 4)?;
                match month(t4) {
                    Some(m) => ymd.append(YmdArg::Int(m), Some('M'))?,
                    None => ymd.append(YmdArg::Str(t4.to_string()), None)?,
                }
                idx += 2;
            }
            idx += 1;
        }
        idx += 1;
    } else if idx + 1 >= len_l || jump(&tokens[idx + 1]) {
        if idx + 2 < len_l && ampm(&tokens[idx + 2]).is_some() {
            // 12 am
            let hour = dec_trunc(&value);
            res.hour = Some(adjust_ampm(hour, ampm(&tokens[idx + 2]).unwrap_or(0)));
            idx += 1;
        } else {
            // Year, month or day.
            ymd.append(YmdArg::Dec(value), None)?;
        }
        idx += 1;
    } else if ampm(&tokens[idx + 1]).is_some()
        && value >= Decimal::ZERO
        && value < Decimal::from(24)
    {
        // 12am
        let hour = dec_trunc(&value);
        res.hour = Some(adjust_ampm(hour, ampm(&tokens[idx + 1]).unwrap_or(0)));
        idx += 1;
    } else if ymd.could_be_day_dec(&value) {
        ymd.append(YmdArg::Dec(value), None)?;
    } else {
        return Err(());
    }
    Ok(idx)
}

/// `parser._parse` followed by `parserinfo.validate`.
fn parse_tokens(timestr: &str, local_year: i32) -> Result<ParseResult, ()> {
    let mut res = ParseResult::default();
    let mut l = timelex(timestr);
    let mut ymd = Ymd::default();
    let len_l = l.len();
    let mut i = 0usize;
    while i < len_l {
        let value_repr = l[i].clone();
        if py_float(&value_repr).is_some() {
            i = parse_numeric_token(&l, i, &mut ymd, &mut res)?;
        } else if let Some(wd) = weekday(&l[i]) {
            res.weekday = Some(wd);
        } else if let Some(m) = month(&l[i]) {
            ymd.append(YmdArg::Int(m), Some('M'))?;
            if i + 1 < len_l {
                if l[i + 1] == "-" || l[i + 1] == "/" {
                    // Jan-01[-99]
                    let sep = l[i + 1].clone();
                    let t2 = tok(&l, i + 2)?.to_string();
                    ymd.append(YmdArg::Str(t2), None)?;
                    if i + 3 < len_l && l[i + 3] == sep {
                        // Jan-01-99
                        let t4 = tok(&l, i + 4)?.to_string();
                        ymd.append(YmdArg::Str(t4), None)?;
                        i += 2;
                    }
                    i += 2;
                } else if i + 4 < len_l && l[i + 1] == " " && l[i + 3] == " " && pertain(&l[i + 2])
                {
                    // Jan of 01
                    if !l[i + 4].is_empty() && l[i + 4].chars().all(isnum) {
                        let value = int_of(&l[i + 4])?;
                        let year = convertyear(value, false, local_year as i64)?;
                        ymd.append(YmdArg::Str(year.to_string()), Some('Y'))?;
                    }
                    i += 4;
                }
            }
        } else if let Some(ap) = ampm(&l[i]) {
            // `_ampm_valid` with fuzzy=False raises instead of rejecting.
            match res.hour {
                None => return Err(()),
                Some(h) if !(0..=12).contains(&h) => return Err(()),
                Some(h) => {
                    res.hour = Some(adjust_ampm(h, ap));
                    res.ampm = Some(ap);
                }
            }
        } else if could_be_tzname(&res, &l[i]) {
            res.tzname = Some(l[i].clone());
            res.tzoffset = tzoffset_of(&l[i]);
            // Check for something like GMT+3, or BRST+3.
            if i + 1 < len_l && (l[i + 1] == "+" || l[i + 1] == "-") {
                l[i + 1] = if l[i + 1] == "+" {
                    "-".into()
                } else {
                    "+".into()
                };
                res.tzoffset = None;
                if utczone(res.tzname.as_deref().unwrap_or_default()) {
                    res.tzname = None;
                }
            }
        } else if res.hour.is_some() && (l[i] == "+" || l[i] == "-") {
            let signal: i64 = if l[i] == "+" { 1 } else { -1 };
            let next = tok(&l, i + 1)?.to_string();
            let len_li = next.chars().count();
            let (hour_offset, min_offset);
            if len_li == 4 {
                // -0300
                let head: String = next.chars().take(2).collect();
                let tail: String = next.chars().skip(2).collect();
                hour_offset = int_of(&head)?;
                min_offset = int_of(&tail)?;
            } else if i + 2 < len_l && l[i + 2] == ":" {
                // -03:00
                hour_offset = int_of(&next)?;
                min_offset = int_of(tok(&l, i + 3)?)?;
                i += 2;
            } else if len_li <= 2 {
                // -[0]3
                let head: String = next.chars().take(2).collect();
                hour_offset = int_of(&head)?;
                min_offset = 0;
            } else {
                return Err(());
            }
            res.tzoffset = Some(signal * (hour_offset * 3600 + min_offset * 60));
            // Look for a timezone name between parenthesis.
            if i + 5 < len_l
                && jump(&l[i + 2])
                && l[i + 3] == "("
                && l[i + 5] == ")"
                && 3 <= l[i + 4].chars().count()
                && could_be_tzname_with(res.hour, &res.tzname, &l[i + 4])
            {
                // -0300 (BRST)
                res.tzname = Some(l[i + 4].clone());
                i += 4;
            }
            i += 1;
        } else if !jump(&l[i]) {
            return Err(());
        }
        i += 1;
    }

    let (year, mon, day) = ymd.resolve_ymd()?;
    res.century_specified = ymd.century_specified;
    res.year = year;
    res.month = mon;
    res.day = day;

    // `parserinfo.validate`.
    if let Some(y) = res.year {
        res.year = Some(convertyear(y, res.century_specified, local_year as i64)?);
    }
    let tzname_empty = res.tzname.as_deref().map(str::is_empty).unwrap_or(true);
    if (res.tzoffset == Some(0) && tzname_empty)
        || matches!(res.tzname.as_deref(), Some("Z") | Some("z"))
    {
        res.tzname = Some("UTC".into());
        res.tzoffset = Some(0);
    } else if res.tzoffset != Some(0)
        && !tzname_empty
        && utczone(res.tzname.as_deref().unwrap_or_default())
    {
        res.tzoffset = Some(0);
    }
    Ok(res)
}

/// `parser._build_naive`.
fn build_naive(res: &ParseResult, default: &NaiveDateTime) -> Result<NaiveDateTime, ()> {
    let year = res.year.unwrap_or(default.year() as i64);
    let month = res.month.unwrap_or(default.month() as i64);
    let mut day = res.day.unwrap_or(default.day() as i64);
    if res.day.is_none() {
        let dim = days_in_month(year, month).ok_or(())?;
        if day > dim {
            day = dim;
        }
    }
    let hour = res.hour.unwrap_or(default.hour() as i64);
    let minute = res.minute.unwrap_or(default.minute() as i64);
    let second = res.second.unwrap_or(default.second() as i64);
    let micro = res
        .microsecond
        .unwrap_or((default.nanosecond() / 1000) as i64);
    if !(1..=9999).contains(&year) {
        return Err(());
    }
    let date = NaiveDate::from_ymd_opt(
        year as i32,
        u32::try_from(month).map_err(|_| ())?,
        u32::try_from(day).map_err(|_| ())?,
    )
    .ok_or(())?;
    let naive = date
        .and_hms_micro_opt(
            u32::try_from(hour).map_err(|_| ())?,
            u32::try_from(minute).map_err(|_| ())?,
            u32::try_from(second).map_err(|_| ())?,
            u32::try_from(micro).map_err(|_| ())?,
        )
        .ok_or(())?;
    if second > 59 || micro > 999_999 {
        return Err(());
    }
    // `relativedelta(weekday=...)` when no day was given: jump forward to that weekday.
    if let Some(wd) = res.weekday
        && res.day.unwrap_or(0) == 0
    {
        let current = naive.weekday().num_days_from_monday() as i64;
        let jumpdays = (7 - current + wd).rem_euclid(7);
        return add_micros(&naive, jumpdays as i128 * 86_400_000_000).ok_or(());
    }
    Ok(naive)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timelex_splits_like_dateutil() {
        assert_eq!(
            timelex("2024-01-01T12:34:56.789Z"),
            vec![
                "2024", "-", "01", "-", "01", "T", "12", ":", "34", ":", "56.789", "Z"
            ]
        );
        assert_eq!(timelex("2024.01.02"), vec!["2024", ".", "01", ".", "02"]);
        assert_eq!(timelex("Sep.20.2009"), vec!["Sep", ".", "20", ".", "2009"]);
    }
}
