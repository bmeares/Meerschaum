//! The jobs monitor: the jobs of an executor (status, began, ended, schedule, executor) and a
//! pane following the selected job's log, with keys to start, stop and pause it.
//!
//! The listing, the log follower and the job commands run on background threads (a remote
//! executor or a slow `systemctl` never freezes the screen) and reach the view through
//! channels: [`JobsMonitor::tick`] drains them. The listing is what `show jobs` reads
//! (`get_filtered_jobs`), the follower is `show logs`' (`Job::monitor_logs`), and the commands
//! are `start|stop|pause job`'s (`Job::start`, `Job::stop`, `Job::pause`).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent};
use meerschaum_core::SuccessTuple;
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_jobs::executor::get_filtered_jobs_for;
use meerschaum_jobs::{Job, JobOptions, JobsContext, MonitorControl, MonitorOptions, Status};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use super::app::{Step, View};
use super::run_view::scroll_offset;
use super::theme::{Theme, Tone, truncate};

/// The most log lines the pane keeps.
const LOG_LINES: usize = 2000;
/// How often the job list is re-read.
const REFRESH: Duration = Duration::from_secs(2);

/// One job as the monitor shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct JobRow {
    /// The job's name.
    pub name: String,
    /// `local`, `systemd`, `api:<label>`.
    pub executor: String,
    /// `running`, `paused` or `stopped`.
    pub status: String,
    /// The command (`job.label`).
    pub command: String,
    /// When the process began (UTC).
    pub began: Option<String>,
    /// When it ended (UTC).
    pub ended: Option<String>,
    /// The `-s` / `--schedule` / `--cron` value.
    pub schedule: Option<String>,
    /// The process ID.
    pub pid: Option<i32>,
    /// Whether it restarts.
    pub restarts: bool,
    /// The result of a stopped job.
    pub result: Option<SuccessTuple>,
    /// Waiting for input.
    pub blocking: bool,
}

impl JobRow {
    /// The status column (with `waiting for input`, `restarts`).
    pub fn status_text(&self) -> String {
        let mut text = if self.blocking {
            "waiting for input".to_string()
        } else {
            self.status.clone()
        };
        if self.restarts {
            text.push_str(" (restarts)");
        }
        text
    }

    /// How the status is colored.
    pub fn tone(&self) -> Tone {
        match self.status.as_str() {
            "running" if self.blocking => Tone::Warn,
            "running" => Tone::Good,
            "paused" => Tone::Warn,
            _ if self.result.as_ref().is_some_and(|r| !r.success) => Tone::Bad,
            _ => Tone::Dim,
        }
    }
}

fn utc_text(dt: Option<PyDateTime>) -> Option<String> {
    Some(match dt? {
        PyDateTime::Naive(n) => n.format("%Y-%m-%d %H:%M").to_string(),
        PyDateTime::Aware(a) => a
            .with_timezone(&chrono::Utc)
            .format("%Y-%m-%d %H:%M")
            .to_string(),
    })
}

/// The schedule of a job's command line (`-s`, `--schedule`, `--cron`, also `--schedule=...`).
pub fn schedule_of(sysargs: &[String]) -> Option<String> {
    let mut words = sysargs.iter();
    while let Some(word) = words.next() {
        if matches!(word.as_str(), "-s" | "--schedule" | "--cron") {
            return words.next().cloned();
        }
        for prefix in ["--schedule=", "--cron="] {
            if let Some(value) = word.strip_prefix(prefix) {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Read one job into a row.
pub fn job_row(job: &mut Job) -> JobRow {
    let status = job.status();
    let sysargs = job.sysargs();
    JobRow {
        name: job.name.clone(),
        executor: job.executor_keys.replace("systemd:main", "systemd"),
        status: status.as_str().to_string(),
        command: job.label(),
        began: utc_text(job.began()),
        ended: utc_text(job.ended()),
        schedule: schedule_of(&sysargs),
        pid: (status == Status::Running).then(|| job.pid()).flatten(),
        restarts: job.restarts(),
        result: (status == Status::Stopped).then(|| job.result()),
        blocking: status == Status::Running && job.is_blocking_on_stdin(),
    }
}

/// A job command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobCommand {
    /// `start job` (also resumes a paused job).
    Start,
    /// `stop job`.
    Stop,
    /// `pause job`.
    Pause,
}

impl JobCommand {
    /// The verb.
    pub fn verb(self) -> &'static str {
        match self {
            JobCommand::Start => "start",
            JobCommand::Stop => "stop",
            JobCommand::Pause => "pause",
        }
    }

    fn id(self) -> u32 {
        self as u32
    }

    fn from_id(id: u32) -> Option<JobCommand> {
        [JobCommand::Start, JobCommand::Stop, JobCommand::Pause]
            .into_iter()
            .find(|c| c.id() == id)
    }
}

/// Where the monitor reads jobs from.
#[derive(Clone)]
pub struct JobsSource {
    /// The jobs context (paths, config, remote executors).
    pub ctx: JobsContext,
    /// `--executor-keys`.
    pub executor_keys: Option<String>,
    /// Job names to show (all when empty).
    pub filter: Vec<String>,
}

impl JobsSource {
    fn open(&self, name: &str, executor_keys: &str) -> Option<Job> {
        Job::new(
            name,
            None,
            JobOptions {
                executor_keys: Some(executor_keys.to_string()),
                ..Default::default()
            },
            &self.ctx,
        )
        .ok()
    }

    /// One listing (what `show jobs` reads).
    pub fn list(&self) -> Result<Vec<JobRow>, String> {
        let filtered = get_filtered_jobs_for(
            &self.ctx,
            self.executor_keys.as_deref(),
            &self.filter,
            false,
            true,
        )?;
        let mut rows: Vec<JobRow> = filtered
            .jobs
            .into_values()
            .map(|mut j| job_row(&mut j))
            .collect();
        // Running first, then paused, then stopped; by name within each.
        let rank = |s: &str| match s {
            "running" => 0,
            "paused" => 1,
            _ => 2,
        };
        rows.sort_by(|a, b| {
            rank(&a.status)
                .cmp(&rank(&b.status))
                .then_with(|| a.name.cmp(&b.name))
        });
        Ok(rows)
    }
}

enum Msg {
    Rows(Result<Vec<JobRow>, String>),
    Log {
        name: String,
        line: String,
    },
    Done {
        verb: &'static str,
        name: String,
        result: SuccessTuple,
    },
}

/// The jobs monitor view.
pub struct JobsMonitor {
    /// The jobs.
    pub rows: Vec<JobRow>,
    /// The selected job's name.
    pub selected: Option<String>,
    /// The followed job's log lines.
    pub log: VecDeque<String>,
    /// Lines scrolled up from the newest (0 follows).
    pub log_scroll: usize,
    /// The executor (title).
    pub executor: String,
    /// The last command's result or a read error.
    pub notice: Option<(String, Tone, Instant)>,
    /// Commands confirmed while there is no source (tests).
    pub issued: Vec<(JobCommand, String)>,
    /// The listing has arrived once.
    pub loaded: bool,
    source: Option<JobsSource>,
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
    stop: Arc<AtomicBool>,
    follower: Option<(String, Arc<AtomicBool>)>,
    refresh_now: Arc<AtomicBool>,
}

impl Drop for JobsMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some((_, flag)) = self.follower.take() {
            flag.store(true, Ordering::SeqCst);
        }
    }
}

impl JobsMonitor {
    /// A monitor over `source` (`None`: fed by hand, for tests).
    pub fn new(source: Option<JobsSource>, executor: &str) -> JobsMonitor {
        let (tx, rx) = channel();
        let monitor = JobsMonitor {
            rows: Vec::new(),
            selected: None,
            log: VecDeque::new(),
            log_scroll: 0,
            executor: executor.to_string(),
            notice: None,
            issued: Vec::new(),
            loaded: false,
            source,
            tx,
            rx,
            stop: Arc::new(AtomicBool::new(false)),
            follower: None,
            refresh_now: Arc::new(AtomicBool::new(false)),
        };
        if let Some(source) = monitor.source.clone() {
            let (tx, stop, now) = (
                monitor.tx.clone(),
                monitor.stop.clone(),
                monitor.refresh_now.clone(),
            );
            std::thread::spawn(move || {
                let mut last: Option<Instant> = None;
                while !stop.load(Ordering::SeqCst) {
                    if last.is_none_or(|l| l.elapsed() >= REFRESH)
                        || now.swap(false, Ordering::SeqCst)
                    {
                        last = Some(Instant::now());
                        let rows = super::app::guarded(|| source.list()).and_then(|r| r);
                        if tx.send(Msg::Rows(rows)).is_err() {
                            return;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            });
        }
        monitor
    }

    /// Replace the listing (keeps the selection by name).
    pub fn apply_rows(&mut self, rows: Vec<JobRow>) {
        self.rows = rows;
        self.loaded = true;
        let keep = self
            .selected
            .as_ref()
            .is_some_and(|s| self.rows.iter().any(|r| &r.name == s));
        if !keep {
            self.selected = self.rows.first().map(|r| r.name.clone());
        }
        self.follow_selected();
    }

    /// Add a line of the followed job's log.
    pub fn apply_log(&mut self, name: &str, line: &str) {
        if self.selected.as_deref() != Some(name) {
            return;
        }
        let clean = crate::format::remove_ansi(line).replace('\t', "    ");
        for part in clean.trim_end_matches('\n').split('\n') {
            self.log.push_back(part.trim_end_matches('\r').to_string());
            if self.log_scroll > 0 {
                self.log_scroll += 1;
            }
        }
        while self.log.len() > LOG_LINES {
            self.log.pop_front();
        }
    }

    /// The selected row's index.
    pub fn selected_index(&self) -> Option<usize> {
        let name = self.selected.as_ref()?;
        self.rows.iter().position(|r| &r.name == name)
    }

    fn select(&mut self, index: usize) {
        if let Some(row) = self.rows.get(index) {
            if self.selected.as_ref() != Some(&row.name) {
                self.selected = Some(row.name.clone());
                self.log.clear();
                self.log_scroll = 0;
            }
            self.follow_selected();
        }
    }

    /// Follow the selected job's log (stop following the previous one).
    fn follow_selected(&mut self) {
        let Some(name) = self.selected.clone() else {
            return;
        };
        if self.follower.as_ref().is_some_and(|(n, _)| *n == name) {
            return;
        }
        if let Some((_, flag)) = self.follower.take() {
            flag.store(true, Ordering::SeqCst);
        }
        self.log.clear();
        self.log_scroll = 0;
        let flag = Arc::new(AtomicBool::new(false));
        self.follower = Some((name.clone(), flag.clone()));
        let (Some(source), Some(row)) = (self.source.clone(), self.selected_row().cloned()) else {
            return;
        };
        let tx = self.tx.clone();
        std::thread::spawn(move || {
            let (tx_end, name_end) = (tx.clone(), name.clone());
            let followed = super::app::guarded(move || {
                let Some(mut job) = source.open(&name, &row.executor_keys_full()) else {
                    return;
                };
                let _ = job.monitor_logs(
                    |line: &str| {
                        let msg = Msg::Log {
                            name: name.clone(),
                            line: line.to_string(),
                        };
                        if flag.load(Ordering::SeqCst) || tx.send(msg).is_err() {
                            return MonitorControl::Stop;
                        }
                        MonitorControl::Continue
                    },
                    MonitorOptions {
                        accept_input: false,
                        stop_on_exit: false,
                        stop: Some(flag.clone()),
                        ..Default::default()
                    },
                );
            });
            if let Err(error) = followed {
                let _ = tx_end.send(Msg::Log {
                    name: name_end,
                    line: format!("(Stopped following the log: {error})"),
                });
            }
        });
    }

    /// The selected row.
    pub fn selected_row(&self) -> Option<&JobRow> {
        self.selected_index().map(|i| &self.rows[i])
    }

    fn command(&mut self, command: JobCommand, name: String) {
        let Some(source) = self.source.clone() else {
            self.issued.push((command, name));
            return;
        };
        let Some(row) = self.rows.iter().find(|r| r.name == name).cloned() else {
            return;
        };
        let doing = match command {
            JobCommand::Start => "Starting",
            JobCommand::Stop => "Stopping",
            JobCommand::Pause => "Pausing",
        };
        self.notice = Some((
            format!("{doing} job '{name}'..."),
            Tone::Active,
            Instant::now(),
        ));
        let (tx, now) = (self.tx.clone(), self.refresh_now.clone());
        std::thread::spawn(move || {
            let result =
                super::app::guarded(|| match source.open(&name, &row.executor_keys_full()) {
                    Some(mut job) => match command {
                        JobCommand::Start => job.start(),
                        JobCommand::Stop => job.stop(None),
                        JobCommand::Pause => job.pause(None),
                    },
                    None => SuccessTuple::fail(format!("Cannot open job '{name}'.")),
                })
                .unwrap_or_else(SuccessTuple::fail);
            now.store(true, Ordering::SeqCst);
            let _ = tx.send(Msg::Done {
                verb: command.verb(),
                name,
                result,
            });
        });
    }
}

/// The display width of the widest text, within `[min, max]`.
fn widest<'a>(texts: impl Iterator<Item = &'a str>, min: u16, max: u16) -> u16 {
    texts
        .map(|t| unicode_width::UnicodeWidthStr::width(t) as u16)
        .max()
        .unwrap_or(0)
        .clamp(min, max)
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

impl JobRow {
    /// The executor keys to open the job with (`systemd` is `systemd:main`).
    fn executor_keys_full(&self) -> String {
        if self.executor == "systemd" {
            "systemd:main".into()
        } else {
            self.executor.clone()
        }
    }
}

/// How long a command's result stays in the footer.
const NOTICE_FOR: Duration = Duration::from_secs(8);

impl View for JobsMonitor {
    fn title(&self) -> String {
        format!("jobs on {}", self.executor)
    }

    fn help(&self) -> Vec<(&'static str, &'static str)> {
        vec![
            ("Up/Down, j/k", "select a job"),
            ("s", "start (or resume) the selected job"),
            ("x", "stop the selected job"),
            ("p", "pause the selected job"),
            ("r", "refresh the list now"),
            ("PgUp/PgDn", "scroll the log"),
            ("End, G", "follow the newest log lines"),
        ]
    }

    fn hints(&self) -> String {
        "Up/Down select  s start  x stop  p pause  r refresh  PgUp/PgDn log".into()
    }

    fn status(&self) -> Option<(String, Tone)> {
        self.notice
            .as_ref()
            .filter(|(_, _, at)| at.elapsed() < NOTICE_FOR)
            .map(|(text, tone, _)| (text.clone(), *tone))
    }

    fn tick(&mut self) -> bool {
        let mut changed = false;
        while let Ok(msg) = self.rx.try_recv() {
            changed = true;
            match msg {
                Msg::Rows(Ok(rows)) => self.apply_rows(rows),
                Msg::Rows(Err(error)) => {
                    self.notice = Some((
                        format!("Could not get the jobs: {error}"),
                        Tone::Bad,
                        Instant::now(),
                    ))
                }
                Msg::Log { name, line } => self.apply_log(&name, &line),
                Msg::Done { verb, name, result } => {
                    let tone = if result.success {
                        Tone::Good
                    } else {
                        Tone::Bad
                    };
                    let message = result.message.lines().next().unwrap_or("").to_string();
                    self.notice =
                        Some((format!("{verb} '{name}': {message}"), tone, Instant::now()));
                }
            }
        }
        changed
    }

    fn draw(&mut self, frame: &mut Frame<'_>, area: Rect, theme: &Theme) {
        let n = self.rows.len() as u16;
        let list_h = (n.max(1) + 3)
            .min(area.height * 2 / 5)
            .max(4.min(area.height));
        let [list, log] =
            Layout::vertical([Constraint::Length(list_h), Constraint::Min(0)]).areas(area);
        let header = Row::new(
            [
                "name",
                "status",
                "began (UTC)",
                "ended (UTC)",
                "schedule",
                "executor",
                "command",
            ]
            .map(|h| Cell::from(h).style(theme.style(Tone::Accent))),
        );
        let visible = list.height.saturating_sub(3) as usize;
        let selected = self.selected_index();
        let offset = scroll_offset(selected.unwrap_or(0), visible, self.rows.len());
        let rows: Vec<Row> = self
            .rows
            .iter()
            .enumerate()
            .skip(offset)
            .take(visible)
            .map(|(i, r)| {
                let row = Row::new(vec![
                    Cell::from(r.name.clone()),
                    Cell::from(r.status_text()).style(theme.style(r.tone())),
                    Cell::from(r.began.clone().unwrap_or_default()),
                    Cell::from(r.ended.clone().unwrap_or_default()),
                    Cell::from(r.schedule.clone().unwrap_or_default()),
                    Cell::from(r.executor.clone()),
                    Cell::from(r.command.clone()),
                ]);
                if Some(i) == selected {
                    row.style(theme.selected())
                } else {
                    row
                }
            })
            .collect();
        let empty = rows.is_empty();
        let table = Table::new(
            rows,
            [
                Constraint::Length(widest(self.rows.iter().map(|r| r.name.as_str()), 4, 30)),
                Constraint::Length(widest(
                    self.rows
                        .iter()
                        .map(|r| r.status_text())
                        .collect::<Vec<_>>()
                        .iter()
                        .map(String::as_str),
                    6,
                    30,
                )),
                Constraint::Length(16),
                Constraint::Length(16),
                Constraint::Length(widest(
                    self.rows.iter().filter_map(|r| r.schedule.as_deref()),
                    8,
                    30,
                )),
                Constraint::Length(widest(self.rows.iter().map(|r| r.executor.as_str()), 8, 20)),
                Constraint::Fill(1),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(theme.border())
                .title(format!(" Jobs ({}) ", self.rows.len())),
        );
        frame.render_widget(table, list);
        if empty && list.height > 3 {
            let text = if self.loaded {
                "No jobs. Start one with `start job sync pipes --loop` (or `-d`)."
            } else {
                "Reading jobs..."
            };
            let inner = Rect {
                x: list.x + 2,
                y: list.y + 2,
                width: list.width.saturating_sub(4),
                height: 1,
            };
            frame.render_widget(
                Paragraph::new(Span::styled(text, theme.style(Tone::Dim))),
                inner,
            );
        }
        if log.height < 3 {
            return;
        }
        let title = match (self.selected_row(), self.log_scroll) {
            (Some(r), 0) => {
                let mut title = format!(" Log: {} ", r.name);
                if let Some(result) = &r.result
                    && !result.message.is_empty()
                {
                    title.push_str(&format!(
                        "({}) ",
                        truncate(
                            result.message.lines().next().unwrap_or(""),
                            40,
                            theme.ellipsis()
                        )
                    ));
                }
                title
            }
            (Some(r), up) => format!(" Log: {} (scrolled {up} up, End follows) ", r.name),
            (None, _) => " Log ".into(),
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_set(theme.border())
            .title(title);
        let inner = block.inner(log);
        frame.render_widget(block, log);
        let rows = inner.height as usize;
        let total = self.log.len();
        let end = total.saturating_sub(self.log_scroll.min(total.saturating_sub(rows.min(total))));
        let start = end.saturating_sub(rows);
        let lines: Vec<Line> = self
            .log
            .range(start..end)
            .map(|l| Line::from(l.clone()))
            .collect();
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn on_key(&mut self, key: KeyEvent) -> Step {
        let index = self.selected_index().unwrap_or(0);
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.select(index.saturating_sub(1)),
            KeyCode::Down | KeyCode::Char('j') => {
                self.select((index + 1).min(self.rows.len().saturating_sub(1)))
            }
            KeyCode::Home | KeyCode::Char('g') => self.select(0),
            KeyCode::PageUp => self.log_scroll = (self.log_scroll + 10).min(self.log.len()),
            KeyCode::PageDown => self.log_scroll = self.log_scroll.saturating_sub(10),
            KeyCode::End | KeyCode::Char('G') => self.log_scroll = 0,
            KeyCode::Char('r') => {
                self.refresh_now.store(true, Ordering::SeqCst);
                self.notice = Some(("Refreshing...".into(), Tone::Dim, Instant::now()));
            }
            KeyCode::Char(c @ ('s' | 'x' | 'p')) => {
                let Some(row) = self.selected_row() else {
                    return Step::Continue;
                };
                let command = match c {
                    's' => JobCommand::Start,
                    'x' => JobCommand::Stop,
                    _ => JobCommand::Pause,
                };
                let refused = match (command, row.status.as_str()) {
                    (JobCommand::Start, "running") => Some("is already running"),
                    (JobCommand::Stop, "stopped") => Some("is already stopped"),
                    (JobCommand::Pause, "paused") => Some("is already paused"),
                    (JobCommand::Pause, "stopped") => Some("is not running"),
                    _ => None,
                };
                if let Some(why) = refused {
                    self.notice = Some((
                        format!("Job '{}' {why}.", row.name),
                        Tone::Warn,
                        Instant::now(),
                    ));
                    return Step::Continue;
                }
                return Step::Confirm {
                    id: command.id(),
                    prompt: format!("{} job '{}'?", capitalize(command.verb()), row.name),
                };
            }
            _ => return Step::Ignored,
        }
        Step::Continue
    }

    fn on_confirm(&mut self, id: u32) {
        if let (Some(command), Some(name)) = (JobCommand::from_id(id), self.selected.clone()) {
            self.command(command, name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::app::tests::{key, screen};
    use crate::tui::app::{KeyOutcome, Overlay, handle_key};

    fn row(name: &str, status: &str) -> JobRow {
        JobRow {
            name: name.into(),
            executor: "local".into(),
            status: status.into(),
            command: format!("sync pipes -c {name} --loop"),
            began: Some("2026-01-02 03:04".into()),
            ended: (status == "stopped").then(|| "2026-01-02 04:05".into()),
            schedule: (name == "b").then(|| "every 5 minutes".into()),
            pid: None,
            restarts: name == "a",
            result: (status == "stopped").then(|| SuccessTuple::fail("It broke.")),
            blocking: false,
        }
    }

    fn monitor() -> JobsMonitor {
        let mut m = JobsMonitor::new(None, "local");
        m.apply_rows(vec![
            row("a", "running"),
            row("b", "paused"),
            row("c", "stopped"),
        ]);
        m
    }

    #[test]
    fn schedules_are_read_from_sysargs() {
        let words = |s: &str| s.split(' ').map(str::to_string).collect::<Vec<_>>();
        assert_eq!(
            schedule_of(&words("sync pipes -s daily")),
            Some("daily".into())
        );
        assert_eq!(
            schedule_of(&words("sync pipes --schedule=hourly")),
            Some("hourly".into())
        );
        assert_eq!(schedule_of(&words("sync pipes --loop")), None);
    }

    #[test]
    fn selection_follows_names_across_refreshes() {
        let mut m = monitor();
        assert_eq!(m.selected.as_deref(), Some("a"));
        m.on_key(key(KeyCode::Down));
        assert_eq!(m.selected.as_deref(), Some("b"));
        m.apply_log("b", "2026-01-02 03:04 | hello\n");
        m.apply_log("a", "not shown\n");
        assert_eq!(m.log, ["2026-01-02 03:04 | hello"]);
        // A refresh in a different order keeps `b`.
        m.apply_rows(vec![row("c", "stopped"), row("b", "running")]);
        assert_eq!(m.selected_index(), Some(1));
        assert_eq!(m.log.len(), 1, "the follower is kept");
        // `b` disappears: the first job is selected and its log starts over.
        m.apply_rows(vec![row("c", "stopped")]);
        assert_eq!(m.selected.as_deref(), Some("c"));
        assert!(m.log.is_empty());
        m.apply_log("c", "\u{1b}[31mred\u{1b}[0m\tline\r\n");
        assert_eq!(m.log, ["red    line"]);
    }

    #[test]
    fn commands_ask_first_and_refuse_no_ops() {
        let mut m = monitor();
        let mut overlay = Overlay::None;
        // `a` runs: starting it is refused without a question.
        assert_eq!(
            handle_key(&mut m, &mut overlay, key(KeyCode::Char('s'))),
            KeyOutcome::Continue
        );
        assert_eq!(overlay, Overlay::None);
        assert!(m.status().unwrap().0.contains("already running"));
        handle_key(&mut m, &mut overlay, key(KeyCode::Char('x')));
        assert_eq!(
            overlay,
            Overlay::Confirm {
                id: JobCommand::Stop.id(),
                prompt: "Stop job 'a'?".into()
            }
        );
        handle_key(&mut m, &mut overlay, key(KeyCode::Char('n')));
        assert!(m.issued.is_empty());
        handle_key(&mut m, &mut overlay, key(KeyCode::Char('p')));
        handle_key(&mut m, &mut overlay, key(KeyCode::Char('y')));
        assert_eq!(m.issued, vec![(JobCommand::Pause, "a".to_string())]);
        m.on_key(key(KeyCode::Down));
        handle_key(&mut m, &mut overlay, key(KeyCode::Char('s')));
        handle_key(&mut m, &mut overlay, key(KeyCode::Char('y')));
        assert_eq!(m.issued[1], (JobCommand::Start, "b".to_string()));
        assert_eq!(
            handle_key(&mut m, &mut overlay, key(KeyCode::Char('q'))),
            KeyOutcome::Exit(crate::tui::app::Exit::Quit)
        );
    }

    #[test]
    fn renders_the_list_and_the_log() {
        let mut m = monitor();
        for i in 0..50 {
            m.apply_log("a", &format!("line {i}\n"));
        }
        let rows = screen(&mut m, &Overlay::None, &Theme::plain(), 120, 24);
        let text = rows.join("\n");
        assert!(rows[0].contains("jobs on local"), "{text}");
        assert!(text.contains("Jobs (3)"), "{text}");
        assert!(text.contains("running (restarts)"), "{text}");
        assert!(text.contains("every 5 minutes"), "{text}");
        assert!(text.contains("2026-01-02 04:05"), "{text}");
        assert!(text.contains("Log: a"), "{text}");
        assert!(text.contains("line 49"), "follows the newest line: {text}");
        m.on_key(key(KeyCode::PageUp));
        let text = screen(&mut m, &Overlay::None, &Theme::plain(), 120, 24).join("\n");
        assert!(text.contains("scrolled 10 up"), "{text}");
        assert!(!text.contains("line 49"), "{text}");
        // Empty and tiny.
        let mut empty = JobsMonitor::new(None, "local");
        let text = screen(&mut empty, &Overlay::None, &Theme::plain(), 80, 10).join("\n");
        assert!(text.contains("Reading jobs..."), "{text}");
        empty.apply_rows(Vec::new());
        let text = screen(&mut empty, &Overlay::None, &Theme::plain(), 80, 10).join("\n");
        assert!(text.contains("No jobs."), "{text}");
        for (w, h) in [(1, 1), (10, 3), (30, 8)] {
            let _ = screen(&mut m, &Overlay::None, &Theme::plain(), w, h);
        }
    }
}
