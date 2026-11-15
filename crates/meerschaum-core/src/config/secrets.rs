//! Secret references (5.1): `MRSM{file:<path>}` and `MRSM{env:<NAME>}` inside the `MRSM{...}`
//! syntax, wherever a symlink works (config files, `MRSM_CONFIG` / `MRSM_PATCH`, the JSON or URI
//! of an `MRSM_<TYPE>_<LABEL>` connector, a compose project's `config:` block).
//!
//! - `file:<path>`: the file's text, minus exactly one trailing newline (`\n` or `\r\n`). The path
//!   is everything after `file:` up to the closing brace (it may hold `:`, a Windows drive), `~`
//!   is the home directory and a relative path is relative to the root directory.
//! - `env:<NAME>`: the environment variable's value.
//!
//! **When.** A reference is substituted with the other symlinks, when its key is loaded, and its
//! original stays under `_symlinks` (so every writer keeps the reference). Each `get_config` of a
//! path holding a reference re-reads it ([`refresh`](super::ConfigState) in `get_config`): a file
//! is read again only when its length, modification time or inode changed, so a
//! rotated file reaches the next connector built, and the cost of an unchanged one is one
//! `stat`. A process that never met a reference pays one atomic load ([`seen`]).
//!
//! **Failures.** A reference that cannot be resolved stays as its own text (`MRSM{file:...}`,
//! which names the source and never a secret): reading other keys, `show config`, `edit config`
//! and the shell are unaffected. Building a connector whose attributes still hold one fails
//! ([`check_connector_attributes`]) with the reference, the config key and the reason; any other
//! setting read through `get_config` warns once with the same text.
//!
//! **Masking.** Every value a reference produced is recorded ([`known_values`]); the masking
//! code (`show connectors`, `show config`, the console, job metadata on the API and MCP) masks
//! those values whatever the key they sit under.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, RwLock};

use crate::value::{Map, Value};

/// The first segment of a file reference (`MRSM{file:...}`).
pub const FILE_SOURCE: &str = "file";
/// The first segment of an environment reference (`MRSM{env:...}`).
pub const ENV_SOURCE: &str = "env";
/// The first segments that name a secret source, never a top-level config key.
pub const RESERVED_KEYS: [&str; 2] = [FILE_SOURCE, ENV_SOURCE];
/// What a masked secret shows (the CLI's `connector_mask::MASK`).
pub const MASK: &str = "******";
/// A known secret shorter than this is masked only where it is a whole value, never inside
/// other text (masking `abc` everywhere would garble output).
pub const MIN_SUBSTRING_MASK: usize = 4;

/// Where a reference reads from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    /// `MRSM{file:<path>}`.
    File,
    /// `MRSM{env:<NAME>}`.
    Env,
}

/// One secret reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretRef {
    /// The source.
    pub source: Source,
    /// The path or the variable name.
    pub target: String,
}

impl SecretRef {
    /// The reference the keys of a symlink name (`["file", "/run", "x"]` is the file `/run:x`),
    /// with the literal prefix (`!`) already removed. `None` for an ordinary symlink.
    pub fn from_keys(keys: &[String]) -> Option<SecretRef> {
        let source = match keys.first()?.as_str() {
            FILE_SOURCE => Source::File,
            ENV_SOURCE => Source::Env,
            _ => return None,
        };
        if keys.len() < 2 {
            return None;
        }
        Some(SecretRef {
            source,
            target: keys[1..].join(":"),
        })
    }

    /// The reference as written (`MRSM{file:/run/secrets/pg}`).
    pub fn text(&self) -> String {
        let source = match self.source {
            Source::File => FILE_SOURCE,
            Source::Env => ENV_SOURCE,
        };
        format!("MRSM{{{source}:{}}}", self.target)
    }
}

/// Why a reference could not be resolved. Never holds the secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretError {
    /// The reference as written.
    pub reference: String,
    /// What went wrong (`the file does not exist`).
    pub reason: String,
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The secret reference `{}` cannot be resolved: {}.",
            self.reference, self.reason
        )
    }
}

impl std::error::Error for SecretError {}

static SEEN: AtomicBool = AtomicBool::new(false);

/// Whether this process has met a secret reference (resolved or not). Until it has, nothing in
/// `get_config` looks for one.
pub fn seen() -> bool {
    SEEN.load(Ordering::Relaxed)
}

/// The cache of secret files: the text of each, valid while its length, modification time and
/// inode are those it was read with.
#[derive(Debug, Default)]
struct FileCache {
    entries: HashMap<PathBuf, (FileStamp, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<std::time::SystemTime>,
    inode: u64,
}

impl FileStamp {
    fn of(meta: &std::fs::Metadata) -> FileStamp {
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(meta);
        #[cfg(not(unix))]
        let inode = 0;
        FileStamp {
            len: meta.len(),
            modified: meta.modified().ok(),
            inode,
        }
    }
}

static FILES: LazyLock<Mutex<FileCache>> = LazyLock::new(|| Mutex::new(FileCache::default()));
static FILE_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many times a secret file was actually read (not served from the cache) in this process.
pub fn file_reads() -> u64 {
    FILE_READS.load(Ordering::Relaxed)
}

/// The path a file reference names: `~` is the home directory, a relative path is relative to
/// `root_dir`.
pub fn resolve_path(target: &str, root_dir: &Path) -> PathBuf {
    let home = || {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
    };
    if target == "~" {
        if let Some(home) = home() {
            return home;
        }
    } else if let Some(rest) = target
        .strip_prefix("~/")
        .or_else(|| target.strip_prefix("~\\"))
        && let Some(home) = home()
    {
        return home.join(rest);
    }
    let path = PathBuf::from(target);
    if path.is_absolute() || looks_like_windows_absolute(target) {
        path
    } else {
        root_dir.join(path)
    }
}

/// `C:\x` / `C:/x` / `\\server\share` are absolute on Windows even when this build is not.
fn looks_like_windows_absolute(target: &str) -> bool {
    let b = target.as_bytes();
    (b.len() >= 3 && b[0].is_ascii_alphabetic() && b[1] == b':' && (b[2] == b'\\' || b[2] == b'/'))
        || target.starts_with("\\\\")
}

/// Remove exactly one trailing newline (`\n` or `\r\n`).
pub fn strip_one_newline(mut text: String) -> String {
    if text.ends_with("\r\n") {
        text.truncate(text.len() - 2);
    } else if text.ends_with('\n') {
        text.truncate(text.len() - 1);
    }
    text
}

fn io_reason(error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::NotFound => "the file does not exist".into(),
        std::io::ErrorKind::PermissionDenied => {
            "the file cannot be read (permission denied)".into()
        }
        _ => format!("the file cannot be read ({error})"),
    }
}

fn read_file(path: &Path) -> Result<String, String> {
    let meta = std::fs::metadata(path).map_err(|e| io_reason(&e))?;
    if meta.is_dir() {
        return Err("the path is a directory".into());
    }
    let stamp = FileStamp::of(&meta);
    {
        let cache = FILES.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((cached, text)) = cache.entries.get(path)
            && *cached == stamp
        {
            return Ok(text.clone());
        }
    }
    let bytes = std::fs::read(path).map_err(|e| io_reason(&e))?;
    FILE_READS.fetch_add(1, Ordering::Relaxed);
    let text = String::from_utf8(bytes).map_err(|_| "the file is not UTF-8 text".to_string())?;
    let text = strip_one_newline(text);
    FILES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entries
        .insert(path.to_path_buf(), (stamp, text.clone()));
    Ok(text)
}

fn read_env(name: &str) -> Result<String, String> {
    match std::env::var_os(name) {
        None => Err("the environment variable is not set".into()),
        Some(value) => value
            .into_string()
            .map_err(|_| "the environment variable is not UTF-8 text".into()),
    }
}

static FAILURES: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Resolve a reference, recording the value for masking (or the reason it failed).
pub fn resolve(reference: &SecretRef, root_dir: &Path) -> Result<String, SecretError> {
    SEEN.store(true, Ordering::Relaxed);
    let result = match reference.source {
        Source::File => read_file(&resolve_path(&reference.target, root_dir)),
        Source::Env => read_env(&reference.target),
    };
    let text = reference.text();
    match result {
        Ok(value) => {
            register_value(&value);
            FAILURES
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&text);
            Ok(value)
        }
        Err(reason) => {
            FAILURES
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(text.clone(), reason.clone());
            Err(SecretError {
                reference: text,
                reason,
            })
        }
    }
}

/// Why the reference (as written) last failed to resolve in this process, if it did.
pub fn failure_reason(reference: &str) -> Option<String> {
    FAILURES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(reference)
        .cloned()
}

static VALUES: LazyLock<RwLock<Vec<String>>> = LazyLock::new(|| RwLock::new(Vec::new()));

/// Record a value a reference produced (masked from then on).
pub fn register_value(value: &str) {
    if value.is_empty() {
        return;
    }
    if VALUES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .any(|v| v == value)
    {
        return;
    }
    let mut values = VALUES.write().unwrap_or_else(|e| e.into_inner());
    if !values.iter().any(|v| v == value) {
        values.push(value.to_string());
        // Longest first, so a secret that contains another is masked whole.
        values.sort_by_key(|v| std::cmp::Reverse(v.len()));
    }
}

/// Every value a secret reference produced in this process (longest first).
pub fn known_values() -> Vec<String> {
    VALUES.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Whether `text` is a value a secret reference produced.
pub fn is_known_value(text: &str) -> bool {
    !text.is_empty()
        && VALUES
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|v| v == text)
}

/// `text` with every known secret masked ([`MASK`]): a whole value of any length, and inside
/// other text those of at least [`MIN_SUBSTRING_MASK`] characters.
pub fn mask_known(text: &str) -> String {
    let values = VALUES.read().unwrap_or_else(|e| e.into_inner());
    if values.is_empty() {
        return text.to_string();
    }
    if values.iter().any(|v| v == text) {
        return MASK.to_string();
    }
    let mut out = text.to_string();
    for value in values.iter() {
        if value.chars().count() >= MIN_SUBSTRING_MASK && out.contains(value.as_str()) {
            out = out.replace(value.as_str(), MASK);
        }
    }
    out
}

/// A copy of `value` with every known secret masked in its strings ([`mask_known`]).
pub fn mask_known_in(value: &Value) -> Value {
    if VALUES.read().unwrap_or_else(|e| e.into_inner()).is_empty() {
        return value.clone();
    }
    fn walk(value: &Value) -> Value {
        match value {
            Value::Str(s) => Value::Str(mask_known(s)),
            Value::List(items) => Value::List(items.iter().map(walk).collect()),
            Value::Map(map) => Value::Map(map.iter().map(|(k, v)| (k.clone(), walk(v))).collect()),
            other => other.clone(),
        }
    }
    walk(value)
}

static REFERENCE_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"MRSM\{!?(?:file|env):[^}\n]*\}").expect("valid regex"));

/// Whether `text` holds a secret reference.
pub fn contains_reference(text: &str) -> bool {
    text.contains("MRSM{") && REFERENCE_RE.is_match(text)
}

/// The secret references in `text`, as written.
pub fn references_in(text: &str) -> Vec<String> {
    if !text.contains("MRSM{") {
        return Vec::new();
    }
    REFERENCE_RE
        .find_iter(text)
        .map(|m| m.as_str().replacen("MRSM{!", "MRSM{", 1))
        .collect()
}

static ANY_SYMLINK_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"MRSM\{(?P<keys>[^}\n]*)\}").expect("valid regex"));

/// `text` with each secret reference replaced by its value (a reference that cannot be resolved
/// stays as its text), without going through the general substitution: the fast path of a
/// refresh. `None` when `text` also holds an ordinary symlink (`MRSM{meerschaum:...}`), which
/// only the registry can resolve.
pub fn substitute_references(text: &str, root_dir: &Path) -> Option<String> {
    substitute_references_with(text, &|| root_dir.to_path_buf())
}

/// [`substitute_references`] with the root directory computed only if a relative file path needs
/// it (the Python boundary asks its host for it).
pub fn substitute_references_with(text: &str, root_dir: &dyn Fn() -> PathBuf) -> Option<String> {
    if !text.contains("MRSM{") {
        return Some(text.to_string());
    }
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for caps in ANY_SYMLINK_RE.captures_iter(text) {
        let whole = caps.get(0).expect("match");
        let keys: Vec<String> = caps["keys"].split(':').map(String::from).collect();
        let mut keys = keys;
        if let Some(first) = keys.first_mut()
            && let Some(stripped) = first.strip_prefix('!')
        {
            *first = stripped.to_string();
        }
        let reference = SecretRef::from_keys(&keys)?;
        out.push_str(&text[last..whole.start()]);
        let needs_root = reference.source == Source::File
            && !Path::new(&reference.target).is_absolute()
            && !reference.target.starts_with('~')
            && !looks_like_windows_absolute(&reference.target);
        let root = if needs_root {
            root_dir()
        } else {
            PathBuf::new()
        };
        match resolve(&reference, &root) {
            Ok(value) => out.push_str(&value),
            Err(_) => out.push_str(whole.as_str()),
        }
        last = whole.end();
    }
    out.push_str(&text[last..]);
    Some(out)
}

/// The first attribute (a `:`-joined path) of `attributes` whose value still holds a secret
/// reference (one that could not be resolved), with that reference.
pub fn unresolved_attribute(attributes: &Map) -> Option<(String, String)> {
    fn walk(value: &Value, path: &str) -> Option<(String, String)> {
        match value {
            Value::Str(s) => references_in(s)
                .into_iter()
                .next()
                .map(|r| (path.to_string(), r)),
            Value::List(items) => items.iter().find_map(|item| walk(item, path)),
            Value::Map(map) => map.iter().find_map(|(k, v)| {
                let path = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}:{k}")
                };
                walk(v, &path)
            }),
            _ => None,
        }
    }
    attributes.iter().find_map(|(k, v)| walk(v, k))
}

/// `Err` naming the reference, the config key and the reason when a connector's attributes still
/// hold a secret reference that could not be resolved. Never includes a secret.
pub fn check_connector_attributes(typ: &str, label: &str, attributes: &Map) -> Result<(), String> {
    if !seen() && !attributes.values().any(holds_reference_text) {
        return Ok(());
    }
    match unresolved_attribute(attributes) {
        None => Ok(()),
        Some((attribute, reference)) => Err(unresolved_message(
            &format!("meerschaum:connectors:{typ}:{label}:{attribute}"),
            &reference,
        )),
    }
}

fn holds_reference_text(value: &Value) -> bool {
    match value {
        Value::Str(s) => contains_reference(s),
        Value::List(items) => items.iter().any(holds_reference_text),
        Value::Map(map) => map.values().any(holds_reference_text),
        _ => false,
    }
}

/// The message for a reference under `key` that could not be resolved.
pub fn unresolved_message(key: &str, reference: &str) -> String {
    let reason = failure_reason(reference)
        .unwrap_or_else(|| "it could not be resolved when the configuration was read".into());
    format!("The secret reference `{reference}` under `{key}` cannot be resolved: {reason}.")
}

static WARNED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Whether `token` is new to this process (for once-per-process warnings).
pub fn first_time(token: &str) -> bool {
    WARNED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(token.to_string())
}

/// The names whose values are secrets (the masking code's notion: `password`, `api_key`,
/// `client_secret`, `token`, ...), lowercase substrings.
pub const SECRET_WORDS: &[&str] = &[
    "password",
    "passwd",
    "pass",
    "pwd",
    "pw",
    "secret",
    "token",
    "key",
    "credential",
    "private",
    "auth",
    "phrase",
    "cookie",
    "session",
    "sig",
    "salt",
    "sas",
    "jwt",
    "bearer",
    "otp",
];

/// Whether a name looks like a secret's ([`SECRET_WORDS`]).
pub fn is_secret_name(name: &str) -> bool {
    let lowered = name.to_lowercase();
    SECRET_WORDS.iter().any(|word| lowered.contains(word))
}

/// The words that make a name a credential's when they end it ([`is_credential_name`]).
pub const CREDENTIAL_WORDS: &[&str] = &[
    "password",
    "passwd",
    "pass",
    "pwd",
    "pw",
    "passphrase",
    "secret",
    "token",
    "credential",
    "credentials",
    "apikey",
    "privatekey",
];

/// The two-word endings that make a name a credential's (`api_key`, `private_key`, ...).
pub const CREDENTIAL_PAIRS: &[(&str, &str)] = &[
    ("api", "key"),
    ("private", "key"),
    ("secret", "key"),
    ("access", "key"),
    ("signing", "key"),
];

/// The words of a name: split at `_`, `-`, `.`, spaces and lowercase-to-uppercase steps
/// (`apiKey`, `ADMIN_PASSWORD`), lowercase.
fn name_words(name: &str) -> Vec<String> {
    let mut words: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut previous_lower = false;
    for c in name.chars() {
        if !c.is_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            previous_lower = false;
            continue;
        }
        if c.is_uppercase() && previous_lower && !current.is_empty() {
            words.push(std::mem::take(&mut current));
        }
        previous_lower = c.is_lowercase() || c.is_ascii_digit();
        current.extend(c.to_lowercase());
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// Whether a config key's name means a credential (5.1, for the readable-file warning): its last
/// word is one of [`CREDENTIAL_WORDS`] (`password`, `db_pass`, `client_secret`, `access_token`,
/// `credentials`) or its last two are one of [`CREDENTIAL_PAIRS`] (`api_key`, `privateKey`).
/// Unlike [`is_secret_name`] (the masking code's broad net), a name that merely contains `key`,
/// `auth`, `session` or `policy` is not one (`allowed_instance_keys`, `VALKEY_RDB_POLICY_DISABLED`,
/// `session_timeout`), nor is a credential's file path (`GF_SECURITY_ADMIN_PASSWORD__FILE`).
pub fn is_credential_name(name: &str) -> bool {
    let words = name_words(name);
    let Some(last) = words.last() else {
        return false;
    };
    if CREDENTIAL_WORDS.contains(&last.as_str()) {
        return true;
    }
    words.len() >= 2 && {
        let pair = (words[words.len() - 2].as_str(), last.as_str());
        CREDENTIAL_PAIRS.contains(&pair)
    }
}

static INLINE_URI_PASSWORD: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"[A-Za-z][A-Za-z0-9+.\-]*://[^:/@\s?#]*:(?P<password>[^@\s]+)@")
        .expect("valid regex")
});

/// Whether a value is a secret written inline: not empty, not only references (`MRSM{...}`)
/// and not a `$VAR` / `<DOLLAR>VAR` placeholder for another program to fill in.
fn is_inline_literal(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.starts_with('$') || trimmed.starts_with("<DOLLAR>") {
        return false;
    }
    let without_refs = SYMLINK_RE.replace_all(trimmed, "");
    !without_refs.trim().is_empty()
}

static SYMLINK_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"MRSM\{[^}\n]*\}").expect("valid regex"));

/// The `:`-joined path of the first inline secret in a config file's value (5.1): a non-empty
/// literal under a credential's name ([`is_credential_name`]: a password, a token, an API key), or a
/// URI with a password. References (`MRSM{file:...}`, `MRSM{env:...}`, other symlinks) are not
/// inline secrets. The cosmetic `formatting` key (its icons are named `key`) is skipped.
pub fn first_inline_secret(top_key: &str, value: &super::ConfigValue) -> Option<String> {
    use super::ConfigValue;
    if top_key == "formatting" {
        return None;
    }
    fn walk(name: &str, value: &ConfigValue, path: &str) -> Option<String> {
        match value {
            ConfigValue::Str(s) => {
                let named = is_credential_name(name) && is_inline_literal(s);
                let in_uri = INLINE_URI_PASSWORD
                    .captures(s)
                    .is_some_and(|c| is_inline_literal(&c["password"]));
                (named || in_uri).then(|| path.to_string())
            }
            ConfigValue::Int(_) | ConfigValue::BigInt(_) | ConfigValue::Float(_)
                if is_credential_name(name) =>
            {
                // A numeric password (`password: 1234`, `pin`-like secrets); not `max_keys: 10`.
                Some(path.to_string())
            }
            ConfigValue::Map(m) => m
                .iter()
                .find_map(|(k, v)| walk(k, v, &format!("{path}:{k}"))),
            ConfigValue::List(items) | ConfigValue::Tuple(items) => {
                items.iter().find_map(|v| walk(name, v, path))
            }
            _ => None,
        }
    }
    walk(top_key, value, top_key)
}

/// On Unix, warn once per process (on stderr) when a config file that holds an inline secret is
/// readable by its group or by others, naming the file and the `chmod` that fixes it. Files a
/// reference reads (`MRSM{file:...}`) are never checked: a Docker secret is world-readable inside
/// its container by design. Silent on other platforms.
pub fn warn_if_exposed(path: &Path, top_key: &str, value: &super::ConfigValue) {
    #[cfg(unix)]
    {
        if let Some(message) = exposure_warning(path, top_key, value)
            && first_time(&format!("exposed\u{0}{}", path.display()))
        {
            eprintln!("{message}");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, top_key, value);
    }
}

/// The warning [`warn_if_exposed`] prints for a file, if any (every time it applies).
#[cfg(unix)]
pub fn exposure_warning(path: &Path, top_key: &str, value: &super::ConfigValue) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
    if mode & 0o077 == 0 {
        return None;
    }
    let at = first_inline_secret(top_key, value)?;
    let display = path.display();
    Some(format!(
        "Warning: the config file '{display}' holds a secret (at `{at}`) and is readable by \
         {}. Run `chmod 600 '{display}'`, or move the secret into a file and refer to it with \
         `MRSM{{file:<path>}}`.",
        match (mode & 0o070 != 0, mode & 0o007 != 0) {
            (true, true) => "its group and by others",
            (true, false) => "its group",
            _ => "others",
        }
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(text: &str) -> Vec<String> {
        text.split(':').map(String::from).collect()
    }

    #[test]
    fn references_parse_with_colons_in_the_target() {
        let r = SecretRef::from_keys(&keys("file:C:\\secrets\\pg")).unwrap();
        assert_eq!(r.source, Source::File);
        assert_eq!(r.target, "C:\\secrets\\pg");
        assert_eq!(r.text(), "MRSM{file:C:\\secrets\\pg}");
        assert_eq!(
            SecretRef::from_keys(&keys("env:PGPASSWORD"))
                .unwrap()
                .target,
            "PGPASSWORD"
        );
        assert!(SecretRef::from_keys(&keys("meerschaum:instance")).is_none());
        assert!(SecretRef::from_keys(&keys("file")).is_none());
    }

    #[test]
    fn exactly_one_trailing_newline_is_removed() {
        assert_eq!(strip_one_newline("a\n".into()), "a");
        assert_eq!(strip_one_newline("a\r\n".into()), "a");
        assert_eq!(strip_one_newline("a\n\n".into()), "a\n");
        assert_eq!(strip_one_newline(" a \t".into()), " a \t");
    }

    #[test]
    fn paths_resolve_against_home_and_root() {
        let root = Path::new("/root/dir");
        assert_eq!(resolve_path("/abs/x", root), PathBuf::from("/abs/x"));
        assert_eq!(
            resolve_path("rel/x", root),
            PathBuf::from("/root/dir/rel/x")
        );
        assert_eq!(resolve_path("C:\\x", root), PathBuf::from("C:\\x"));
        if let Some(home) = std::env::var_os("HOME") {
            assert_eq!(resolve_path("~/x", root), PathBuf::from(home).join("x"));
        }
    }

    #[test]
    fn a_rotated_file_is_read_again_and_an_unchanged_one_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pw");
        std::fs::write(&path, "first\n").unwrap();
        let r = SecretRef {
            source: Source::File,
            target: path.to_string_lossy().into_owned(),
        };
        assert_eq!(resolve(&r, dir.path()).unwrap(), "first");
        let reads = file_reads();
        assert_eq!(resolve(&r, dir.path()).unwrap(), "first");
        assert_eq!(file_reads(), reads, "served from the cache");
        // A rotation through a new file renamed over the old one (what secret mounts do).
        let next = dir.path().join("pw.new");
        std::fs::write(&next, "second-longer\n").unwrap();
        std::fs::rename(&next, &path).unwrap();
        assert_eq!(resolve(&r, dir.path()).unwrap(), "second-longer");
    }

    #[test]
    fn failures_name_the_reference_and_never_a_value() {
        let dir = tempfile::tempdir().unwrap();
        let r = SecretRef {
            source: Source::File,
            target: "missing-secret".into(),
        };
        let error = resolve(&r, dir.path()).unwrap_err();
        assert_eq!(error.reference, "MRSM{file:missing-secret}");
        assert_eq!(error.reason, "the file does not exist");
        let attrs: Map = [(
            "password".to_string(),
            Value::Str("MRSM{file:missing-secret}".into()),
        )]
        .into_iter()
        .collect();
        let message = check_connector_attributes("sql", "x", &attrs).unwrap_err();
        assert!(
            message.contains("MRSM{file:missing-secret}")
                && message.contains("meerschaum:connectors:sql:x:password")
                && message.contains("the file does not exist"),
            "{message}"
        );
    }

    #[test]
    fn known_values_are_masked_whole_or_inside_text() {
        register_value("unit-test-secret-value");
        register_value("ab");
        assert_eq!(mask_known("unit-test-secret-value"), MASK);
        assert_eq!(
            mask_known("postgresql://u:unit-test-secret-value@h/db"),
            format!("postgresql://u:{MASK}@h/db")
        );
        assert_eq!(mask_known("ab"), MASK);
        assert_eq!(mask_known("about"), "about", "short values only whole");
    }
}
