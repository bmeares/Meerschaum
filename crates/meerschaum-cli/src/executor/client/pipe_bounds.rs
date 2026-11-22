//! Main-only once-captured CLI datetime semantics before immutable
//! typed-operation argv/catalog capture. No Python/imports or worker-local clock.

use crate::types::{DateContext, parse_datetime};
use crate::value::{PyValue, datetime_isoformat};
use meerschaum_server::executor::pipe_operation::PipeOperation;

fn normalize(value: Option<&str>, clock: &DateContext) -> Result<Option<String>, String> {
    let Some(value) = value else { return Ok(None) };
    if value.len() > 256 || value.chars().any(char::is_control) {
        return Err("Invalid accepted pipe bound.".into());
    }
    match parse_datetime(value, clock).map_err(|_| "Invalid accepted pipe bound.".to_string())? {
        PyValue::DateTime(value) => Ok(Some(datetime_isoformat(&value))),
        PyValue::Int(value) => Ok(Some(
            i64::try_from(value)
                .map_err(|_| "The accepted integer bound exceeds the native axis range.")?
                .to_string(),
        )),
        // Retain the native CLI's explicit-null spelling; do not turn it into
        // a new bound or derive whole-relation authority from this input alone.
        PyValue::Str(value) if value == "None" => Ok(Some(value)),
        _ => Err("The accepted bound has no native datetime/integer representation.".into()),
    }
}
pub(super) fn capture(operation: &mut PipeOperation, clock: &DateContext) -> Result<(), String> {
    // Validate original selectors/reserved flags before normalizing a legitimate
    // bound. Do not scan or rewrite arbitrary user argv/plugin argument strings.
    operation.sysargs().map_err(|error| error.detail_text())?;
    let begin = normalize(operation.begin.as_deref(), clock)?;
    let end = normalize(operation.end.as_deref(), clock)?;
    operation.begin = begin;
    operation.end = end;
    operation.sysargs().map_err(|error| error.detail_text())?;
    Ok(())
}

// Controller::run_pipe_operation exact seam, before its first await:
//   let operation_clock = crate::types::DateContext::system();
// After existing original registration capture, BEFORE immutable argv is made:
//   pipe_bounds::capture(&mut operation,&operation_clock)?;
// Then existing fresh object/source authorization + graph capture and sysargs()
// create the immutable Record. Renewal/restart use its already-normalized bounds;
// they never reparse the original relative words with a new clock.
// Generic action/pipeline graph origins need analogous TYPED capture, not blind
// argv rewriting. They remain pending; this helper covers typed endpoint ops.

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, NaiveDate};
    fn clock(day: u32) -> DateContext {
        let date = NaiveDate::from_ymd_opt(2026, 11, day).unwrap();
        DateContext {
            now_utc: date.and_hms_opt(12, 30, 0).unwrap(),
            dateutil_default: date.and_hms_opt(0, 0, 0).unwrap(),
            local_year: date.year(),
        }
    }
    #[test]
    fn relative_bounds_normalize_once_and_reparse_identically_under_later_worker_clock() {
        let main = clock(17);
        let worker = clock(18);
        for input in ["now", "1 day ago", "November 1"] {
            let captured = normalize(Some(input), &main).unwrap().unwrap();
            assert_eq!(
                parse_datetime(&captured, &worker).unwrap(),
                parse_datetime(input, &main).unwrap()
            );
        }
        assert_ne!(
            normalize(Some("now"), &main).unwrap(),
            normalize(Some("now"), &worker).unwrap()
        );
        assert_eq!(normalize(Some("42"), &main).unwrap(), Some("42".into()));
        assert_eq!(normalize(Some("None"), &main).unwrap(), Some("None".into()));
    }
}
