//! The bottom toolbar (status bar), ported from v4's `input_with_sigint.bottom_toolbar()`
//! (`meerschaum/_internal/shell/Shell.py`) and rendered the way prompt_toolkit draws it: the
//! `bottom-toolbar` class is `reverse` plus the shell's `Style.from_dict({'bottom-toolbar':
//! 'black'})`, so every cell is reverse video with a black foreground, and the background of
//! each segment comes from the ANSI `on <color>` styles v4 applies with `colored()`.

use unicode_width::UnicodeWidthChar;

use super::truncate_text_for_display;
use crate::format::rich::style::Style;

/// The values the toolbar shows, captured before each prompt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolbarModel {
    /// `shell.bottom_toolbar.enabled`.
    pub enabled: bool,
    /// `formatting:ansi`.
    pub ansi: bool,
    /// `formatting:unicode` (the truncation suffix).
    pub unicode: bool,
    /// The instance keys.
    pub instance: String,
    /// The repository keys.
    pub repo: String,
    /// The executor keys.
    pub executor: String,
    /// The Compose project name.
    pub compose: Option<String>,
    /// Whether the instance connector has been built in this session.
    pub connected: bool,
    /// `shell.ansi.{instance,repo,executor,compose}.rich.style`.
    pub instance_style: String,
    /// See `instance_style`.
    pub repo_style: String,
    /// See `instance_style`.
    pub executor_style: String,
    /// See `instance_style`.
    pub compose_style: String,
    /// `formatting.{connected,disconnected}.ansi.rich.style`.
    pub connected_style: String,
    /// See `connected_style`.
    pub disconnected_style: String,
    /// `formatting.{connected,disconnected}.<charset>.icon`.
    pub connected_icon: String,
    /// See `connected_icon`.
    pub disconnected_icon: String,
}

/// The SGR every toolbar cell starts from: `reverse` with the `black` foreground (rendered by
/// prompt_toolkit as the 256-color `#000000`).
const BASE: &str = "0;38;5;16;7";

/// One styled run: text plus the SGR parameters of its `colored(text, style)`.
#[derive(Debug, Clone)]
struct Run {
    text: String,
    sgr: String,
}

fn plain(text: &str) -> Run {
    Run {
        text: text.to_string(),
        sgr: String::new(),
    }
}

/// `colored(text, style)`: the style's SGR (an unparsable style colors nothing).
fn colored(text: &str, style: &str) -> Run {
    let sgr = Style::parse(style.trim())
        .map(|s| s.ansi_codes())
        .unwrap_or_default();
    Run {
        text: text.to_string(),
        sgr,
    }
}

fn width(s: &str) -> usize {
    s.chars().map(|c| c.width().unwrap_or(0)).sum()
}

fn len(runs: &[Run]) -> i64 {
    runs.iter().map(|r| r.text.chars().count() as i64).sum()
}

/// Render runs to one terminal row of exactly `cols` cells (padded with the toolbar style,
/// truncated like prompt_toolkit when a wide character would not fit).
fn render_row(runs: &[Run], cols: usize) -> String {
    let mut out = String::new();
    let mut used = 0usize;
    'outer: for run in runs {
        out.push_str(&format!("\x1b[{BASE}"));
        if !run.sgr.is_empty() {
            out.push(';');
            out.push_str(&run.sgr);
        }
        out.push('m');
        for c in run.text.chars() {
            let w = c.width().unwrap_or(0);
            if used + w > cols {
                break 'outer;
            }
            out.push(c);
            used += w;
        }
    }
    if used < cols {
        out.push_str(&format!("\x1b[{BASE}m{}", " ".repeat(cols - used)));
    }
    out.push_str("\x1b[0m");
    out
}

impl ToolbarModel {
    /// The toolbar rows for a terminal `cols` wide. A toolbar too wide
    /// for the terminal wraps onto a second row (v4 joins the halves with `'\n '`).
    pub fn render(&self, cols: usize) -> Option<Vec<String>> {
        if !self.enabled {
            // PARITY: `bottom_toolbar()` returns `None`, but the toolbar window is still shown
            // (the session was given a callable), so prompt_toolkit draws one empty row in the
            // `bottom-toolbar` style.
            return Some(vec![render_row(&[], cols)]);
        }
        let suffix = if self.unicode { "…" } else { "..." };
        let max_length = cols / 4;
        let trunc = |s: &str| truncate_text_for_display(s, max_length, suffix);
        let on = |style: &str| format!("on {style}");
        let white = "on white";

        let instance_text = trunc(&self.instance);
        let instance = if self.ansi {
            colored(&instance_text, &on(&self.instance_style))
        } else {
            colored(&instance_text, white)
        };
        let repo = if self.ansi {
            colored(&trunc(&self.repo), &on(&self.repo_style))
        } else {
            // PARITY: the no-ANSI branch colors the untruncated repository keys.
            colored(&self.repo, white)
        };
        let executor_text = trunc(&self.executor);
        let executor = if self.ansi {
            colored(&executor_text, &on(&self.executor_style))
        } else {
            colored(&executor_text, white)
        };

        let connected_str = trunc(&self.instance);
        let (status_style, icon) = if self.connected {
            (&self.connected_style, &self.connected_icon)
        } else {
            (&self.disconnected_style, &self.disconnected_icon)
        };
        let mut right: Vec<Run> = if self.ansi {
            // `'on ' + style + '  '`: the trailing spaces are part of the style string.
            vec![colored(&connected_str, &format!("on {status_style}  "))]
        } else {
            vec![colored(&connected_str, white), plain(" ")]
        };
        right.push(plain(&format!(" {icon}")));

        let mut left: Vec<Run> = vec![plain(" ")];
        if let Some(compose) = self.compose.as_deref().filter(|c| !c.is_empty()) {
            if self.ansi {
                left.push(colored(&trunc(compose), &on(&self.compose_style)));
            } else {
                // PARITY: the no-ANSI branch does not truncate the project name.
                left.push(colored(compose, white));
            }
            left.push(colored(" | ", white));
        }
        left.push(instance);
        left.push(colored(" | ", white));
        left.push(executor);
        left.push(colored(" | ", white));
        left.push(repo);

        let buffer_size = cols as i64 - (len(&left) + len(&right) + if self.ansi { 2 } else { 0 });
        if buffer_size > 0 {
            let mut runs = left;
            runs.push(plain(&" ".repeat(buffer_size as usize)));
            runs.extend(right);
            return Some(vec![render_row(&runs, cols)]);
        }
        let mut second = vec![plain(" ")];
        second.extend(right);
        Some(vec![render_row(&left, cols), render_row(&second, cols)])
    }
}

/// The display width of a rendered toolbar row (for tests).
pub fn visible_width(row: &str) -> usize {
    width(&crate::format::remove_ansi(row))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(ansi: bool) -> ToolbarModel {
        ToolbarModel {
            enabled: true,
            ansi,
            unicode: true,
            instance: "sql:local".into(),
            repo: "api:mrsm".into(),
            executor: "local".into(),
            compose: None,
            connected: false,
            instance_style: "cyan".into(),
            repo_style: "magenta".into(),
            executor_style: "yellow".into(),
            compose_style: "bright_magenta".into(),
            connected_style: "green".into(),
            disconnected_style: "red".into(),
            connected_icon: "🟢".into(),
            disconnected_icon: "🔴".into(),
        }
    }

    #[test]
    fn one_row_at_full_width() {
        let rows = model(true).render(100).unwrap();
        assert_eq!(rows.len(), 1);
        let text = crate::format::remove_ansi(&rows[0]);
        assert!(text.starts_with(" sql:local | local | api:mrsm "), "{text}");
        assert!(text.trim_end().ends_with("sql:local 🔴"), "{text}");
        assert_eq!(visible_width(&rows[0]), 100);
        assert!(rows[0].contains("\x1b[0;38;5;16;7;46m"), "{:?}", rows[0]);
    }

    #[test]
    fn narrow_terminals_wrap_onto_two_rows() {
        let rows = model(true).render(40).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            crate::format::remove_ansi(&rows[1]).trim_end(),
            " sql:local 🔴"
        );
    }

    #[test]
    fn disabled_is_one_empty_row() {
        let mut m = model(false);
        m.enabled = false;
        let rows = m.render(80).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(crate::format::remove_ansi(&rows[0]), " ".repeat(80));
    }
}
