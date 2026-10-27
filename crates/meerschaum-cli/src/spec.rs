//! The declarative argument table, ported from `meerschaum/_internal/arguments/_parser.py`.
//!
//! Every entry mirrors one `add_argument` call: option strings in their declared order, `dest`,
//! `action`, `nargs`, `type`, `choices` and the help text. The table drives the clap
//! [`Command`](clap::Command) (`crate::command`), the `argparse`-compatible parser
//! (`crate::argparse`), the help formatter and the JSON schema (`crate::schema`).
//!
//! The option strings, arities and types of every v4 argument are unchanged; the help text is
//! written for today's behavior. Arguments added since ([`V5_ONLY_DESTS`]) come last in their
//! groups, and the Python mirror (`meerschaum/_internal/arguments/_parser.py`) declares the same
//! ones so plugins parse the same command lines.

/// `prog` passed to `ArgumentParser`.
pub const PROG: &str = "mrsm";
/// `description` passed to `ArgumentParser`.
pub const DESCRIPTION: &str = "Create and Build Pipes with Meerschaum.";
/// `usage` passed to `ArgumentParser`.
pub const USAGE: &str = "mrsm [actions] {options}";

/// The `argparse` argument groups, in creation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Group {
    /// The default `positional arguments` group (empty: `action` lives in `Actions options`).
    Positional,
    /// The default `options` group (empty: `add_help=False`).
    Options,
    /// `Actions options`
    Actions,
    /// `Jobs options`
    Jobs,
    /// `Pipes options`
    Pipes,
    /// `Sync options`
    Sync,
    /// `API options`
    Api,
    /// `Plugins options`
    Plugins,
    /// `Tokens options`
    Tokens,
    /// `Packages options`
    Packages,
    /// `Compose options`
    Compose,
    /// `Debugging options`
    Debug,
    /// `Miscellaneous options`
    Misc,
}

impl Group {
    /// Every group in `parser._action_groups` order.
    pub const ALL: [Group; 13] = [
        Group::Positional,
        Group::Options,
        Group::Actions,
        Group::Jobs,
        Group::Pipes,
        Group::Sync,
        Group::Api,
        Group::Plugins,
        Group::Tokens,
        Group::Packages,
        Group::Compose,
        Group::Debug,
        Group::Misc,
    ];

    /// The group title.
    pub fn title(self) -> &'static str {
        match self {
            Group::Positional => "positional arguments",
            Group::Options => "options",
            Group::Actions => "Actions options",
            Group::Jobs => "Jobs options",
            Group::Pipes => "Pipes options",
            Group::Sync => "Sync options",
            Group::Api => "API options",
            Group::Plugins => "Plugins options",
            Group::Tokens => "Tokens options",
            Group::Packages => "Packages options",
            Group::Compose => "Compose options",
            Group::Debug => "Debugging options",
            Group::Misc => "Miscellaneous options",
        }
    }
}

/// The `argparse` action class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// `_StoreAction` (the default).
    Store,
    /// `_StoreTrueAction` (`action='store_true'`).
    StoreTrue,
}

impl Action {
    /// The Python class name.
    pub fn class_name(self) -> &'static str {
        match self {
            Action::Store => "_StoreAction",
            Action::StoreTrue => "_StoreTrueAction",
        }
    }
}

/// `nargs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nargs {
    /// `nargs=None`: exactly one value, stored as a scalar.
    Single,
    /// `nargs='*'`
    ZeroOrMore,
    /// `nargs='+'`
    OneOrMore,
    /// `nargs=argparse.REMAINDER` (`'...'`)
    Remainder,
    /// An integer `nargs` (`store_true` uses `0`).
    Exactly(usize),
}

/// The `type=` callable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgType {
    /// No `type=` (the raw string).
    None,
    /// `type=str`
    Str,
    /// `type=int`
    Int,
    /// `type=float`
    Float,
    /// `type=pathlib.Path`
    Path,
    /// `type=parse_datetime`
    ParseDatetime,
    /// `type=string_to_dict`
    StringToDict,
    /// `type=parse_name`
    ParseName,
    /// `type=parse_executor_keys`
    ParseExecutorKeys,
}

impl ArgType {
    /// `type.__name__`, as `argparse` uses it in `invalid <type> value` messages.
    pub fn name(self) -> Option<&'static str> {
        Some(match self {
            ArgType::None => return None,
            ArgType::Str => "str",
            ArgType::Int => "int",
            ArgType::Float => "float",
            ArgType::Path => "Path",
            ArgType::ParseDatetime => "parse_datetime",
            ArgType::StringToDict => "string_to_dict",
            ArgType::ParseName => "parse_name",
            ArgType::ParseExecutorKeys => "parse_executor_keys",
        })
    }
}

/// One `add_argument` call.
#[derive(Debug, Clone, Copy)]
pub struct ArgSpec {
    /// `dest`
    pub dest: &'static str,
    /// Option strings in declaration order; empty for a positional.
    pub option_strings: &'static [&'static str],
    /// The action class.
    pub action: Action,
    /// `nargs`
    pub nargs: Nargs,
    /// `type`
    pub arg_type: ArgType,
    /// `choices`
    pub choices: Option<&'static [&'static str]>,
    /// `help`
    pub help: &'static str,
    /// The argument group.
    pub group: Group,
}

impl ArgSpec {
    /// Whether this is a positional argument.
    pub fn is_positional(&self) -> bool {
        self.option_strings.is_empty()
    }

    /// `argparse._get_action_name`.
    pub fn display_name(&self) -> String {
        if self.is_positional() {
            self.dest.to_string()
        } else {
            self.option_strings.join("/")
        }
    }
}

const fn store(
    dest: &'static str,
    option_strings: &'static [&'static str],
    arg_type: ArgType,
    help: &'static str,
    group: Group,
) -> ArgSpec {
    ArgSpec {
        dest,
        option_strings,
        action: Action::Store,
        nargs: Nargs::Single,
        arg_type,
        choices: None,
        help,
        group,
    }
}

const fn store_list(
    dest: &'static str,
    option_strings: &'static [&'static str],
    help: &'static str,
    group: Group,
) -> ArgSpec {
    ArgSpec {
        dest,
        option_strings,
        action: Action::Store,
        nargs: Nargs::OneOrMore,
        arg_type: ArgType::None,
        choices: None,
        help,
        group,
    }
}

const fn flag(
    dest: &'static str,
    option_strings: &'static [&'static str],
    help: &'static str,
    group: Group,
) -> ArgSpec {
    ArgSpec {
        dest,
        option_strings,
        action: Action::StoreTrue,
        nargs: Nargs::Exactly(0),
        arg_type: ArgType::None,
        choices: None,
        help,
        group,
    }
}

use ArgType as T;
use Group as G;

/// Every argument, in `parser._actions` order.
pub static ARGUMENTS: &[ArgSpec] = &[
    // ---------------------------------------------------------------- Actions options
    ArgSpec {
        dest: "action",
        option_strings: &[],
        action: Action::Store,
        nargs: Nargs::ZeroOrMore,
        arg_type: ArgType::None,
        choices: None,
        help: "Actions list to execute. E.g. `start api`",
        group: G::Actions,
    },
    flag(
        "help",
        &["-h", "--help"],
        "Print a help message for an action.",
        G::Actions,
    ),
    flag(
        "loop",
        &["--loop"],
        "Repeat the action until it is stopped, waiting `--min-seconds` between laps.",
        G::Actions,
    ),
    flag(
        "yes",
        &["-y", "--yes"],
        "Agree to the default choices for prompts",
        G::Actions,
    ),
    flag(
        "force",
        &["-f", "--force"],
        "Override safety checks",
        G::Actions,
    ),
    flag(
        "noask",
        &["--noask", "--no-ask"],
        "Automatically choose the default answers to questions. Does not result in data loss.",
        G::Actions,
    ),
    ArgSpec {
        dest: "sub_args",
        option_strings: &["-A", "--sub-args"],
        action: Action::Store,
        nargs: Nargs::Remainder,
        arg_type: ArgType::None,
        choices: None,
        help: concat!(
            "Provide a list of arguments for subprocesses. ",
            "You can also type sub-arguments in [] instead.",
            " E.g. `stack -A='--version'`, `ls [-lh]`, `echo -A these are sub-arguments`"
        ),
        group: G::Actions,
    },
    // ---------------------------------------------------------------- Jobs options
    flag(
        "daemon",
        &["-d", "--daemon"],
        "Run an action as a background daemon to create a job.",
        G::Jobs,
    ),
    store(
        "name",
        &["--name", "--job-name"],
        T::ParseName,
        "Assign a name to a job. If no name is provided, a random name will be assigned.",
        G::Jobs,
    ),
    store(
        "schedule",
        &["-s", "--schedule", "--cron"],
        T::Str,
        concat!(
            "Run the action on a schedule, e.g. 'every 10 minutes', 'daily starting 00:00' or a ",
            "cron expression ('0 * * * *'). Often used with `-d`; check a schedule with ",
            "`mrsm show schedule '<schedule>'`. Syntax: ",
            "https://meerschaum.io/reference/background-jobs/#schedules"
        ),
        G::Jobs,
    ),
    flag(
        "restart",
        &["--restart"],
        "Restart a job if not stopped manually.",
        G::Jobs,
    ),
    store(
        "executor_keys",
        &["-e", "--executor-keys"],
        T::ParseExecutorKeys,
        "Execute jobs locally or remotely. Supported values are 'local', 'systemd', and 'api:{label}'.",
        G::Jobs,
    ),
    flag(
        "rm",
        &["--rm"],
        "Delete a job once it has finished executing.",
        G::Jobs,
    ),
    // ---------------------------------------------------------------- Pipes options
    store_list(
        "connector_keys",
        &["-c", "-C", "--connector-keys"],
        "List of connector keys to search for. e.g. -c sql:main api:main",
        G::Pipes,
    ),
    store_list(
        "metric_keys",
        &["-m", "-M", "--metric-keys"],
        "List of metric keys to include in the search",
        G::Pipes,
    ),
    store_list(
        "location_keys",
        &["-l", "-L", "--location-keys"],
        "List of location keys to include in the search",
        G::Pipes,
    ),
    store(
        "mrsm_instance",
        &["-i", "-I", "--mrsm-instance", "--instance"],
        T::None,
        concat!(
            "The instance (connector keys `type:label`) where pipes are registered, e.g. ",
            "`sql:local` or `api:main`. A bare type uses its `main` label. Defaults to the ",
            "configured instance (`sql:main` unless changed with `mrsm edit config`)."
        ),
        G::Pipes,
    ),
    store_list(
        "tags",
        &["-t", "--tags"],
        "Only include pipes with these tags.",
        G::Pipes,
    ),
    store_list(
        "targets",
        &["--targets", "--target"],
        "Filter pipes by targets. May be negated with `_`.",
        G::Pipes,
    ),
    ArgSpec {
        dest: "datetime_dtypes",
        option_strings: &[
            "--datetime-dtypes",
            "--datetime-dtype",
            "--dtype",
            "--dtypes",
        ],
        action: Action::Store,
        nargs: Nargs::OneOrMore,
        arg_type: ArgType::None,
        choices: Some(&["datetime", "int", "None", "_datetime", "_int", "_None"]),
        help: concat!(
            "Only select pipes with the corresponding datetime dtypes (`datetime`, `int`, or `None`).",
            "\nMay be negated with `_`."
        ),
        group: G::Pipes,
    },
    flag(
        "no_policy",
        &["--no-policy", "--nopolicy"],
        concat!(
            "Leave the compression (columnstore) policy untouched. With `compress pipes`, compress ",
            "existing chunks now without installing a policy; with `decompress pipes`, decompress ",
            "existing chunks now without removing the policy (they will be recompressed on schedule). ",
            "TimescaleDB only."
        ),
        G::Pipes,
    ),
    flag(
        "full",
        &["--full"],
        concat!(
            "When running `vacuum pipes`, run `VACUUM FULL` to return freed space to the OS. ",
            "Takes an exclusive lock. PostgreSQL family only."
        ),
        G::Pipes,
    ),
    // ---------------------------------------------------------------- Sync options
    store(
        "min_seconds",
        &["--min-seconds", "--cooldown"],
        T::Float,
        "The minimum number of seconds between laps of `--loop`. Defaults to 1.",
        G::Sync,
    ),
    store(
        "timeout_seconds",
        &["--timeout-seconds", "--timeout"],
        T::Float,
        concat!(
            "How many seconds to wait before giving up: on a job to stop (`stop jobs`, ",
            "`pause jobs`), or on each step of an action chain (`... : --timeout 60`)."
        ),
        G::Sync,
    ),
    flag(
        "listen",
        &["--listen"],
        concat!(
            "Keep the process alive after the initial sync pass so subscription-style ",
            "connectors (e.g. MQTT) continue syncing via callbacks. Stops on `stop jobs` or CTRL-C."
        ),
        G::Sync,
    ),
    flag(
        "unblock",
        &["--unblock"],
        "Run the action asynchronously, if possible.",
        G::Sync,
    ),
    flag(
        "async",
        &["--async"],
        "Run the action asynchronously, if possible. Alias for --unblock",
        G::Sync,
    ),
    store(
        "begin",
        &["--begin"],
        T::ParseDatetime,
        "The begin datetime when syncing, fetching data.",
        G::Sync,
    ),
    store(
        "end",
        &["--end"],
        T::ParseDatetime,
        "The end datetime (exclusive) when syncing, fetching data.",
        G::Sync,
    ),
    store(
        "chunksize",
        &["--chunksize"],
        T::Int,
        "How many rows per chunk. Defaults to 100,000.",
        G::Sync,
    ),
    store(
        "batchsize",
        &["--batchsize"],
        T::Int,
        "How many chunks to process in parallel. Defaults to number of CPUs.",
        G::Sync,
    ),
    store(
        "chunk_minutes",
        &["--chunk-minutes"],
        T::Int,
        concat!(
            "Specify the chunk interval in minutes for verification syncs (and `partition pipes`). ",
            "Defaults to 43200 (30 days)."
        ),
        G::Sync,
    ),
    store(
        "chunk_hours",
        &["--chunk-hours"],
        T::Int,
        "Specify the chunk interval in hours for verification syncs. See `--chunk-minutes`.",
        G::Sync,
    ),
    store(
        "chunk_days",
        &["--chunk-days"],
        T::Int,
        "Specify the chunk interval in days for verification syncs. See `--chunk-minutes`.",
        G::Sync,
    ),
    flag(
        "verify",
        &["--verify"],
        "Completely resync a pipe's entire interval.",
        G::Sync,
    ),
    flag(
        "deduplicate",
        &["--deduplicate", "--dedup"],
        "Remove duplicate rows from a pipe's table.",
        G::Sync,
    ),
    flag(
        "bounded",
        &["--bounded", "--bound"],
        "When verifying, do not sync outside the existing oldest and newest datetime bounds.",
        G::Sync,
    ),
    flag(
        "skip_check_existing",
        &["--skip-check-existing", "--allow-duplicates"],
        concat!(
            "Skip checking for duplicate rows when syncing. ",
            "This improves performance when all rows to be synced are unique. "
        ),
        G::Sync,
    ),
    flag(
        "skip_enforce_dtypes",
        &["--skip-enforce-dtypes"],
        concat!(
            "Skip enforcing incoming data to a pipe's dtypes. ",
            "This improves performance when all rows are expected to already be of the correct type."
        ),
        G::Sync,
    ),
    flag(
        "skip_chunks_with_greater_rowcounts",
        &["--skip-chunks-with-greater-rowcounts"],
        "When verifying, skip chunks with rowcounts greater than the remote's.",
        G::Sync,
    ),
    flag(
        "check_rowcounts_only",
        &["--check-rowcounts-only"],
        "Only compare row-counts when verifying pipes.",
        G::Sync,
    ),
    flag(
        "skip_hooks",
        &["--skip-hooks"],
        "Skip calling the sync hooks.",
        G::Sync,
    ),
    ArgSpec {
        dest: "sync_data",
        option_strings: &["--sync-data"],
        action: Action::Store,
        nargs: Nargs::Single,
        arg_type: ArgType::None,
        choices: Some(&["nothing", "backtrack", "incremental", "all", "filter"]),
        help: concat!(
            "When copying pipes, specify what data to sync into destination pipes. ",
            "'backtrack' uses the source pipe's sync time; 'incremental' uses the ",
            "destination pipe's sync time (both apply the backtrack interval). ",
            "'filter' uses --begin, --end, and --params to restrict the range. ",
            "Defaults to 'nothing' (copy pipe definitions only)."
        ),
        group: G::Sync,
    },
    flag(
        "cache",
        &["--cache"],
        "No effect; accepted so that older commands keep working.",
        G::Sync,
    ),
    // v5 (C4): consumers read the `replace` key (`SyncOptions.replace`).
    flag(
        "replace",
        &["--replace"],
        "Replace the rows between --begin and --end with the fetched rows in one transaction.",
        G::Sync,
    ),
    // ---------------------------------------------------------------- API options
    store(
        "port",
        &["-p", "--port"],
        T::Int,
        "The port on which to run the Web API server",
        G::Api,
    ),
    store(
        "host",
        &["--host"],
        T::Str,
        "The host address to bind to for the API server. Defaults to '0.0.0.0'.",
        G::Api,
    ),
    store(
        "webterm_port",
        &["--webterm-port"],
        T::Int,
        concat!(
            "No effect: the API serves the web terminal on its own port ",
            "(`start webterm -p` for a standalone one). Accepted so that older commands keep working."
        ),
        G::Api,
    ),
    store(
        "workers",
        &["-w", "--workers"],
        T::Int,
        "How many workers to run a concurrent action (e.g. running the API or syncing pipes)",
        G::Api,
    ),
    flag(
        "no_dash",
        &["--no-dash", "--nodash"],
        "When starting the API, do not serve the web console.",
        G::Api,
    ),
    flag(
        "no_webterm",
        &["--no-webterm", "--nowebterm"],
        "When starting the API, do not start the Webterm.",
        G::Api,
    ),
    flag(
        "private",
        &["--private", "--private-mode"],
        "Require authentication for all endpoints.",
        G::Api,
    ),
    flag(
        "secure",
        &["--secure"],
        "Start the API in secure mode, only allowing `admin` users to execute actions.",
        G::Api,
    ),
    flag(
        "no_auth",
        &["--no-auth", "--noauth", "--insecure"],
        "When starting the API, do not require authentication. WARNING: This is dangerous!",
        G::Api,
    ),
    flag(
        "production",
        &["--production", "--gunicorn"],
        concat!(
            "Start the API in production mode: the interactive API docs (`/docs`, `/redoc`) ",
            "are not served unless `api:endpoints:docs_in_production` is set."
        ),
        G::Api,
    ),
    // v5: the server does not terminate TLS; `start api` refuses these (`server::tls_refusal`).
    store(
        "keyfile",
        &["--keyfile"],
        T::Str,
        concat!(
            "Not supported: the API server does not serve HTTPS itself, and `start api` stops ",
            "with this flag. Put a reverse proxy that terminates TLS in front of it."
        ),
        G::Api,
    ),
    store(
        "certfile",
        &["--certfile"],
        T::Str,
        concat!(
            "Not supported: the API server does not serve HTTPS itself, and `start api` stops ",
            "with this flag. Put a reverse proxy that terminates TLS in front of it."
        ),
        G::Api,
    ),
    // v5 (C4, Q34): consumers read the `public` key (`api:permissions:preset: public`).
    flag(
        "public",
        &["--public"],
        "Serve only public routes: login, health, version and plugin routes and pages.",
        G::Api,
    ),
    // ---------------------------------------------------------------- Plugins options
    store(
        "repository",
        &["-r", "--repository", "--repo"],
        T::Str,
        "Meerschaum plugins repository to connect to. Specify an API label (default: 'mrsm')",
        G::Plugins,
    ),
    // ---------------------------------------------------------------- Tokens options
    store(
        "ttl_days",
        &["--ttl-days"],
        T::Int,
        "For how many days should a token be valid.",
        G::Tokens,
    ),
    store_list(
        "scopes",
        &["--scopes", "--scope"],
        "Which scope permissions to grant to a token.",
        G::Tokens,
    ),
    // ---------------------------------------------------------------- Packages options
    store(
        "venv",
        &["--venv"],
        T::Str,
        "Choose which virtual environment to target when installing or removing packages.",
        G::Packages,
    ),
    // ---------------------------------------------------------------- Compose options
    store(
        "file",
        &["--file", "--compose-file"],
        T::Path,
        "Specify an alternate compose file (default: mrsm-compose.yaml).",
        G::Compose,
    ),
    store(
        "env_file",
        &["--env-file"],
        T::Path,
        "Specify an alternate environment file (default: .env).",
        G::Compose,
    ),
    flag(
        "dry",
        &["--dry"],
        "Update project pipes without syncing.",
        G::Compose,
    ),
    flag(
        "drop",
        &["--drop", "-v", "--volumes"],
        "Drop named pipes when running `mrsm compose down`.",
        G::Compose,
    ),
    flag(
        "presync",
        &["--presync"],
        "Run syncs before bringing up jobs.",
        G::Compose,
    ),
    flag(
        "no_jobs",
        &["--no-jobs"],
        "Exit before starting background jobs.",
        G::Compose,
    ),
    flag(
        "isolated",
        &["--isolated"],
        "Execute Compose commands in subprocesses.",
        G::Compose,
    ),
    // ---------------------------------------------------------------- Debugging options
    flag(
        "debug",
        &["--debug"],
        "Print debug statements (max verbosity)",
        G::Debug,
    ),
    flag(
        "trace",
        &["--trace"],
        "No effect; accepted so that older commands keep working. Use `--debug` instead.",
        G::Debug,
    ),
    // ---------------------------------------------------------------- Miscellaneous options
    flag(
        "version",
        &["-V", "--version"],
        "Print the Meerschaum version and exit. Has no effect from within the shell.",
        G::Misc,
    ),
    flag(
        "patch",
        &["--patch"],
        "Patch parameters instead of overwriting.",
        G::Misc,
    ),
    flag(
        "nopretty",
        &["--nopretty"],
        "Print elements without 'pretty' formatting",
        G::Misc,
    ),
    flag(
        "rollback",
        &["--rollback", "--restore"],
        "When editing config, restore a key from its most recent backup instead of editing.",
        G::Misc,
    ),
    flag(
        "skip_deps",
        &["--skip-deps"],
        "Skip dependencies when installing plugins.",
        G::Misc,
    ),
    flag(
        "dry_run",
        &["--dry-run"],
        concat!(
            "Show what the action would change without changing anything ",
            "(e.g. `install packages`, `verify schema`)."
        ),
        G::Misc,
    ),
    store(
        "params",
        &["-P", "--params"],
        T::StringToDict,
        concat!(
            "Parameters dictionary in JSON format or simple format. ",
            "Simple format is for one-depth dictionaries and does not need braces or quotes.",
            "\nJSON Example:\n",
            "--params '{\"columns\": {\"datetime\": \"timestamp\", \"id\": \"station\"}}'",
            "\n\nSimple example:\n",
            "--params key1:value1,key2:value2"
        ),
        G::Misc,
    ),
    flag(
        "temporary",
        &["--temporary", "--temp"],
        concat!(
            "Skip creating or modifying instance tables when working with pipes ",
            "(plugins and users still trigger table creation)."
        ),
        G::Misc,
    ),
    flag(
        "gui",
        &["--gui"],
        "No effect; accepted so that older commands keep working. See `mrsm start gui`.",
        G::Misc,
    ),
    flag(
        "shell",
        &["--shell"],
        "Open the Meerschaum shell and execute the provided action.",
        G::Misc,
    ),
    // v4's `sh` defaulted to `use_bash=True` and its CLI never checked `allow_shell_job`.
    flag(
        "use_bash",
        &["--use-bash"],
        concat!(
            "No effect (commands that are not actions always run with `bash -c`); ",
            "accepted so that older commands keep working."
        ),
        G::Misc,
    ),
    flag(
        "allow_shell_job",
        &["--allow-shell-job"],
        concat!(
            "No effect (a job may run any command, e.g. `mrsm echo hi -d`); ",
            "accepted so that older commands keep working."
        ),
        G::Misc,
    ),
    flag(
        "no_daemon",
        &["--no-daemon"],
        "No effect (actions always run in this process); accepted so that older commands keep working.",
        G::Misc,
    ),
    // v5 (C4, Q20): consumers read the `allow_protected` key; the flag is forwarded to jobs and
    // child processes like `--debug`. Listed with the actions options; kept last in the table so
    // the v4 arguments keep their order.
    flag(
        "allow_protected",
        &["--allow-protected"],
        "Allow writes to connectors marked `protected: true`.",
        G::Actions,
    ),
];

/// The arguments added since v4.1 (absent from the v4 parser fixture `tests/fixtures/parser.json`).
pub const V5_ONLY_DESTS: &[&str] = &["replace", "public", "allow_protected"];

/// Find an argument by `dest`.
pub fn by_dest(dest: &str) -> Option<&'static ArgSpec> {
    ARGUMENTS.iter().find(|a| a.dest == dest)
}

/// `STATIC_CONFIG['system']['arguments']` (`meerschaum/_internal/static.py`).
pub mod keys {
    /// `sub_decorators[0]`
    pub const SUB_BEGIN: &str = "[";
    /// `sub_decorators[1]`
    pub const SUB_END: &str = "]";
    /// `underscore_standin`
    pub const UNDERSCORE_STANDIN: &str = "<UNDERSCORE>";
    /// `failure_key`
    pub const FAILURE_KEY: &str = "_argparse_exception";
    /// `and_key`
    pub const AND: &str = "+";
    /// `escaped_and_key`
    pub const ESCAPED_AND: &str = "++";
    /// `pipeline_key`
    pub const PIPELINE: &str = ":";
    /// `escaped_pipeline_key`
    pub const ESCAPED_PIPELINE: &str = "::";
}
