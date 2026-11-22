//! `-s` schedules for Python: `_core.ScheduleTrigger` and the schedule and start-time parsers.

use super::*;

/// A parsed `-s` schedule (`meerschaum.utils.schedule` uses it for the v5 `tz` / `jitter`
/// clauses). Fire times cross the boundary as ISO-8601 text with the schedule's offset.
#[pyclass(name = "ScheduleTrigger", module = "meerschaum._core")]
pub struct PyScheduleTrigger {
    inner: Mutex<meerschaum_jobs::schedule::Trigger>,
}

fn parse_fire_text(text: &str) -> PyResult<meerschaum_jobs::schedule::Fire> {
    chrono::DateTime::parse_from_rfc3339(text)
        .map_err(|e| PyValueError::new_err(format!("Invalid timestamp '{text}': {e}")))
}

fn schedule_err(e: meerschaum_jobs::schedule::ScheduleError) -> PyErr {
    PyValueError::new_err(e.py_message())
}

#[pymethods]
impl PyScheduleTrigger {
    /// The next fire time, or `None` when the schedule is exhausted.
    fn next(&self) -> Option<String> {
        let mut t = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        t.next().map(|f| meerschaum_jobs::schedule::isoformat(&f))
    }

    /// The first fire time after `after` (ISO-8601 with an offset).
    fn next_after(&self, after: &str) -> PyResult<Option<String>> {
        let after = parse_fire_text(after)?;
        let mut t = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(t.next_after(after)
            .map(|f| meerschaum_jobs::schedule::isoformat(&f)))
    }

    /// The IANA zone the schedule steps in, or `None` for a fixed offset.
    #[getter]
    fn zone(&self) -> Option<String> {
        let t = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        t.zone().map(|z| z.name().to_string())
    }

    /// The trigger's class name (`_IntervalTrigger`, ...).
    #[getter]
    fn type_name(&self) -> &'static str {
        let t = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        t.type_name()
    }
}

/// `parse_schedule(schedule, now)` with the full v5 grammar: the trigger and the warnings the
/// parse raised. `now` is ISO-8601 with an offset.
#[pyfunction]
pub fn jobs_parse_schedule(
    schedule: &str,
    now: &str,
) -> PyResult<(PyScheduleTrigger, Vec<String>)> {
    let now = parse_fire_text(now)?;
    let (trigger, warnings) =
        meerschaum_jobs::parse_schedule_warned(schedule, now, None).map_err(schedule_err)?;
    Ok((
        PyScheduleTrigger {
            inner: Mutex::new(trigger),
        },
        warnings,
    ))
}

/// `parse_start_time(schedule, now)` with the full v5 grammar: the start (ISO-8601) and the
/// warnings the parse raised.
#[pyfunction]
pub fn jobs_parse_start_time(schedule: &str, now: &str) -> PyResult<(String, Vec<String>)> {
    let now = parse_fire_text(now)?;
    let env = meerschaum_jobs::schedule::ParseEnv::process();
    let (start, warnings) =
        meerschaum_jobs::schedule::parse_start_time_clauses(schedule, Some(now), &env)
            .map_err(schedule_err)?;
    Ok((meerschaum_jobs::schedule::isoformat(&start), warnings))
}
