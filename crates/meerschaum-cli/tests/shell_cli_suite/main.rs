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
#[cfg(unix)]
mod live_sync_pty;
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
#[cfg(unix)]
mod residual_actions_a;
mod secret_config_writers;
mod shell;
mod shell_completion_scripts;
mod shell_completions;
mod shell_screens;
#[cfg(unix)]
mod tui_pty;
#[cfg(unix)]
mod version_help_no_worker;
#[cfg(unix)]
mod w18_show;
mod w9_invalid_root;
mod wiring_parity;
