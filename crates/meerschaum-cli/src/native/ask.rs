//! The interactive prompts of `meerschaum/utils/prompt.py` beyond `prompt` / `yes_no` (which live
//! in `native/mod.rs`): `prompt(default=, default_editable=, is_password=)`, `choose`,
//! `get_password`. Every action that asks the user goes through here (or `mod.rs`), never
//! through a Python prompt.

use crate::builtin::ActionError;
use crate::dispatch::Runtime;
use crate::format::OptionsArgs;

use super::{check_noask, question_text, warn};

/// `prompt(question, ...)` options.
#[derive(Debug, Clone, Default)]
pub struct Ask {
    /// Hide the icon when `false` (`icon=False`).
    pub no_icon: bool,
    /// `default`: appended to the question as ` (default: ...)` and returned for `''`.
    pub default: Option<String>,
    /// The `[x]` of a `default=(answer, label)` tuple (`choose` uses it for numbered defaults).
    pub default_label: Option<String>,
    /// `default_editable`: typed into the editable buffer.
    pub editable: Option<String>,
    /// `is_password=True`.
    pub password: bool,
    /// `detect_password=False` turns off the `password`-in-the-question detection.
    pub no_detect_password: bool,
    /// `noask=True`.
    pub noask: bool,
}

impl Ask {
    /// Defaults (icon, no default).
    pub fn new() -> Ask {
        <Ask as Default>::default()
    }

    /// `default=...`.
    pub fn default(mut self, value: impl Into<String>) -> Ask {
        self.default = Some(value.into());
        self
    }

    /// `default_editable=...`.
    pub fn editable(mut self, value: impl Into<String>) -> Ask {
        self.editable = Some(value.into());
        self
    }

    /// `icon=False`.
    pub fn no_icon(mut self) -> Ask {
        self.no_icon = true;
        self
    }

    /// `noask=...`.
    pub fn noask(mut self, noask: bool) -> Ask {
        self.noask = noask;
        self
    }

    /// `is_password=True`.
    pub fn password(mut self) -> Ask {
        self.password = true;
        self
    }
}

/// The error an interactive prompt raises when input ends (`EOFError`).
pub fn eof() -> ActionError {
    ActionError::new("EOFError", "")
}

/// The error a prompt raises on Ctrl-C (`KeyboardInterrupt`); wizards catch it to skip a question.
pub fn interrupt() -> ActionError {
    ActionError::new("KeyboardInterrupt", "")
}

/// Whether `error` is a [`interrupt`] (v4's `except KeyboardInterrupt`).
pub fn is_interrupt(error: &ActionError) -> bool {
    error.class == "KeyboardInterrupt"
}

/// `except KeyboardInterrupt:` around a prompt: `Ok(None)` after Ctrl-C, other errors pass.
pub fn interrupted<T>(result: Result<T, ActionError>) -> Result<Option<T>, ActionError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(e) if is_interrupt(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// `prompt(question, **options)`: the answer, `EOFError` at the end of input or
/// `KeyboardInterrupt` on Ctrl-C.
pub fn ask(rt: &mut Runtime<'_>, question: &str, options: &Ask) -> Result<String, ActionError> {
    let noask = check_noask(rt, options.noask);
    let mut question = question.to_string();
    if let Some(default) = &options.default {
        question.push_str(" (default: ");
        match &options.default_label {
            Some(label) => question.push_str(&format!("{default} [{label}]")),
            None => question.push_str(default),
        }
        question.push(')');
    }
    let is_password = options.password
        || (!options.no_detect_password && question.to_lowercase().contains("password"));
    let answer = if noask {
        String::new()
    } else {
        let text = question_text(rt, &question, !options.no_icon);
        crate::format::progress::suspend();
        let read = if is_password {
            rt.host.read_secret(&text)
        } else if let Some(prefill) = &options.editable {
            rt.host.read_line_prefilled(&text, prefill)
        } else {
            rt.host.read_line_interruptible(&text)
        };
        crate::format::progress::resume(true);
        match read {
            Some(line) => line,
            None if super::terminal::take_interrupt() => return Err(interrupt()),
            None => return Err(eof()),
        }
    };
    if answer.is_empty()
        && let Some(default) = &options.default
    {
        return Ok(default.clone());
    }
    Ok(answer)
}

/// `choose(question, choices, default=None, numeric=True)` (a single answer).
pub fn choose(
    rt: &mut Runtime<'_>,
    question: &str,
    choices: &[String],
    default: Option<&str>,
    numeric: bool,
) -> Result<Option<String>, ActionError> {
    let noask = check_noask(rt, false);
    if choices.is_empty() {
        warn(
            rt,
            &format!(
                "No available choices. Returning default value '{}'.",
                default.unwrap_or("None")
            ),
        );
        return Ok(default.map(str::to_string));
    }
    let mut numeric = numeric;
    if let Some(d) = default
        && !choices.iter().any(|c| c == d)
    {
        warn(
            rt,
            &format!(
                "Default choice '{d}' is not contained in the choices. Setting numeric = False."
            ),
        );
        numeric = false;
    }
    let mut options: Vec<String> = Vec::new();
    let mut default_tuple: Option<(String, Option<String>)> = None;
    if numeric {
        let default_prompt = default
            .and_then(|d| choices.iter().position(|c| c == d))
            .map(|i| (i + 1).to_string())
            .unwrap_or_default();
        let digits = choices.len().to_string().len();
        for (i, c) in choices.iter().enumerate() {
            let ix = (i + 1).to_string();
            options.push(format!("  {ix}. {}{c}\n", " ".repeat(digits - ix.len())));
        }
        if let Some(d) = default {
            default_tuple = Some((default_prompt, Some(d.to_string())));
        }
    } else {
        default_tuple = default.map(|d| (d.to_string(), None));
        for c in choices {
            options.push(format!("  \u{2022} {c}\n"));
        }
    }
    loop {
        let text = rt.with_formatter(|f| {
            f.print_options(
                &options,
                &OptionsArgs {
                    header: Some(String::new()),
                    ..OptionsArgs::default()
                },
            )
        });
        rt.print(&text);
        let mut options_ask = Ask::new().noask(noask);
        if let Some((shown, label)) = &default_tuple {
            options_ask = options_ask.default(shown.clone());
            options_ask.default_label = label.clone();
        }
        let mut answer = ask(rt, question, &options_ask)?;
        // `default_answer` of a tuple default is its first item; the loop then replaces an empty
        // answer with the real default.
        if (answer.is_empty() || Some(&answer) == default_tuple.as_ref().map(|t| &t.0))
            && let Some(d) = default
        {
            answer = d.to_string();
        }
        if answer.is_empty() {
            warn(rt, "Please pick a valid choice.");
            // PARITY: v4 loops forever when `--noask` leaves nothing to choose; end instead.
            if noask {
                return Err(eof());
            }
            continue;
        }
        let selected = if numeric {
            if let Some(i) = choices.iter().position(|c| *c == answer) {
                Some(i)
            } else {
                answer
                    .parse::<usize>()
                    .ok()
                    .filter(|n| (1..=choices.len()).contains(n) && answer == n.to_string())
                    .map(|n| n - 1)
            }
        } else {
            choices.iter().position(|c| *c == answer)
        };
        match selected {
            Some(i) => return Ok(Some(choices[i].clone())),
            None => warn(rt, "Please pick a valid choice."),
        }
    }
}

/// `get_password(username, minimum_length, confirm)`.
pub fn get_password(
    rt: &mut Runtime<'_>,
    username: Option<&str>,
    minimum_length: Option<usize>,
    confirm: bool,
) -> Result<String, ActionError> {
    let suffix = match username {
        Some(u) => format!(" for user '{u}':"),
        None => ":".to_string(),
    };
    loop {
        let password = ask(rt, &format!("Password{suffix}"), &Ask::new().password())?;
        if let Some(min) = minimum_length
            && password.chars().count() < min
        {
            warn(
                rt,
                &format!(
                    "Password is too short. Please enter a password that is at least {min} characters."
                ),
            );
            continue;
        }
        if !confirm {
            return Ok(password);
        }
        let again = ask(
            rt,
            &format!("Confirm password{suffix}"),
            &Ask::new().password(),
        )?;
        if password != again {
            warn(rt, "Passwords do not match! Please try again.");
            continue;
        }
        return Ok(password);
    }
}
