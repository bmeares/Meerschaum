//! The triggers a schedule parses into: `_IntervalTrigger`, `_CalendarIntervalTrigger`,
//! `_CronTrigger` and their `_OrTrigger` / `_AndTrigger` combinations, each stepping on a fixed
//! offset or on a zone's wall clock.

use super::*;

/// `_IntervalTrigger`.
#[derive(Debug, Clone)]
pub struct IntervalTrigger {
    start_time: Fire,
    interval_us: i64,
    last: Option<Fire>,
    zoned: Option<Box<Zoned<IntervalWalls>>>,
}

impl IntervalTrigger {
    pub(super) fn new(start_time: Fire, interval_us: i64) -> Result<Self, ScheduleError> {
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
    pub(super) fn new_zoned(
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
    pub(super) fn new(start_time: Fire, years: i64, months: i64) -> Result<Self, ScheduleError> {
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
    pub(super) fn new_zoned(
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
    restart_at_midnight: bool,
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

    fn next_date(&self, wall: NaiveDateTime) -> Option<NaiveDateTime> {
        // A five-field expression supplies its own clock. A named frequency such as
        // `mon` inherits the starting clock when advancing over a rejected date.
        let time = if self.restart_at_midnight {
            NaiveTime::from_hms_nano_opt(0, 0, self.min_second(), wall.nanosecond())?
        } else {
            wall.time()
        };
        Some(wall.date().succ_opt()?.and_time(time))
    }

    fn min_second(&self) -> u32 {
        self.seconds.first().copied().unwrap_or(0) as u32
    }

    /// First wall candidate to enumerate at an inclusive instant. A fold may repeat earlier
    /// wall minutes, and at a forward transition missing wall minutes may fire right now.
    fn zoned_candidate(
        &self,
        start: Start,
        zone: ScheduleZone,
        policy: Policy,
        microsecond: u32,
        step_seconds: i64,
    ) -> Option<NaiveDateTime> {
        let mut wall = start.wall;
        if policy == Policy::cron(true)
            && (self.restart_at_midnight || self.date_matches(&wall))
            && let crate::zoned::WallTime::Repeated(first, second) = zone.resolve(wall)
            && first == start.fire
        {
            let earlier = wall.checked_sub_signed(second - first)?;
            // A named frequency inherits its clock when jumping over a rejected date.
            wall = if self.restart_at_midnight {
                earlier
            } else {
                earlier.max(wall.date().and_time(NaiveTime::MIN))
            };
        } else if policy == Policy::cron(false)
            && start.fire.nanosecond() == 0
            && wall == zone.wall(&start.fire)
            && let Some(previous) = start.fire.checked_sub_signed(Duration::seconds(1))
            && let Some(next_wall) = zone
                .wall(&previous)
                .checked_add_signed(Duration::seconds(1))
        {
            // At the transition itself, include the skipped minutes that shift to this instant.
            wall = wall.min(next_wall);
        }
        let mut candidate = wall.with_nanosecond(microsecond * 1000)?;
        if step_seconds == 60 {
            candidate = candidate.with_second(self.min_second())?;
        }
        if candidate < wall {
            candidate = candidate.checked_add_signed(Duration::seconds(step_seconds))?;
        }
        Some(candidate)
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
                restart_at_midnight: true,
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
    pub(super) fn for_schedule(
        starting_ts: Fire,
        start: Option<&Start>,
        zone: Option<ScheduleZone>,
        f: CronFields,
        restart_at_midnight: bool,
    ) -> Result<Self, ScheduleError> {
        let wildcard_hour = f.hour.contains('*') || f.hour.contains('/');
        let mut trigger = CronTrigger::new(starting_ts, f)?;
        trigger.m.restart_at_midnight = restart_at_midnight;
        let (Some(start), Some(zone)) = (start, zone) else {
            return Ok(trigger);
        };
        let policy = Policy::cron(wildcard_hour);
        let walls = CronWalls {
            m: trigger.m.clone(),
            step_seconds: trigger.step_seconds,
            candidate: trigger.m.zoned_candidate(
                *start,
                zone,
                policy,
                trigger.microsecond,
                trigger.step_seconds,
            ),
        };
        trigger.zoned = Some(Box::new(Zoned::new(zone, policy, start, walls, false)));
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
        for _ in 0..cron_max_iterations(self.step_seconds) {
            if self.m.matches(&candidate) {
                self.last = Some(candidate);
                return Some(candidate);
            }
            if !self.m.date_matches(&candidate) {
                candidate = self
                    .tz
                    .from_local_datetime(&self.m.next_date(candidate.naive_local())?)
                    .single()?;
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
            let fire = after.checked_add_signed(Duration::nanoseconds(1))?;
            let start = Start {
                fire,
                wall: z.stream.zone.wall(&fire),
            };
            z.walls.candidate = self.m.zoned_candidate(
                start,
                z.stream.zone,
                z.stream.policy,
                self.microsecond,
                self.step_seconds,
            );
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

impl WallSource for CronWalls {
    fn next_wall(&mut self) -> Option<NaiveDateTime> {
        let mut candidate = self.candidate?;
        let step = Duration::seconds(self.step_seconds);
        for _ in 0..cron_max_iterations(self.step_seconds) {
            if self.m.matches(&candidate) {
                self.candidate = candidate.checked_add_signed(step);
                return Some(candidate);
            }
            if !self.m.date_matches(&candidate) {
                candidate = self.m.next_date(candidate)?;
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
