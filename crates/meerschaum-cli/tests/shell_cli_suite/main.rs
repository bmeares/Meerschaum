#[allow(dead_code)]
#[path = "../python_support/mod.rs"]
mod python_support;

mod access_cli;
mod actions;
mod actions_parity;
#[cfg(unix)]
mod child_env;
#[cfg(unix)]
mod cli_ux;
mod debug_output;
#[cfg(unix)]
mod doctor;
mod drivers;
mod format;
#[cfg(unix)]
mod graceful_shutdown;
#[cfg(unix)]
mod interrupt_shell;
mod invalid_root;
#[cfg(unix)]
mod live_sync_pty;
#[cfg(unix)]
mod native_actions;
mod non_admin_commands;
#[cfg(unix)]
mod non_admin_shell;
#[cfg(unix)]
mod non_utf8_env;
mod parity;
#[cfg(unix)]
mod progress_interrupt;
#[cfg(unix)]
#[allow(dead_code)]
mod prompts_pty;
mod shell;
mod shell_completion_scripts;
mod shell_completions;
mod shell_screens;
#[cfg(unix)]
mod show_views;
#[cfg(unix)]
mod tui_pty;
#[cfg(unix)]
mod version_help_no_worker;
mod wiring_parity;
