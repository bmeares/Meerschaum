//! A pipe's `parameters['locks']`: which operations each lock refuses, the refusal message, and
//! which parameter edits a lock allows.

use super::*;

/// A lock on a pipe (`parameters['locks']`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lock {
    /// No parameter edits (changing `locks` itself stays allowed).
    Parameters,
    /// No writes: syncs, replace windows, deduplication, verification syncs.
    Sync,
    /// No row deletes: clears, replace windows, rebuilds.
    Clear,
    /// No drops, rebuilds or table schema changes.
    Drop,
    /// No unregistering.
    Delete,
    /// All of the above.
    All,
}

impl Lock {
    /// The individual locks (everything but `All`), in their canonical order.
    pub const KINDS: [Lock; 5] = [
        Lock::Parameters,
        Lock::Sync,
        Lock::Clear,
        Lock::Drop,
        Lock::Delete,
    ];

    /// The stored name (`parameters`, `sync`, `clear`, `drop`, `delete`, `all`).
    pub fn name(&self) -> &'static str {
        match self {
            Lock::Parameters => "parameters",
            Lock::Sync => "sync",
            Lock::Clear => "clear",
            Lock::Drop => "drop",
            Lock::Delete => "delete",
            Lock::All => "all",
        }
    }

    /// Parse a lock name (any case; `params` and `parameter` for `parameters`, `*` for `all`).
    pub fn parse(text: &str) -> Option<Lock> {
        match text.trim().to_lowercase().as_str() {
            "parameters" | "parameter" | "params" => Some(Lock::Parameters),
            "sync" | "syncs" => Some(Lock::Sync),
            "clear" | "clears" => Some(Lock::Clear),
            "drop" | "drops" => Some(Lock::Drop),
            "delete" | "deletes" => Some(Lock::Delete),
            "all" | "*" => Some(Lock::All),
            _ => None,
        }
    }

    /// What the lock refuses, in words.
    pub fn describe(&self) -> &'static str {
        match self {
            Lock::Parameters => "parameter edits",
            Lock::Sync => "syncs (writes)",
            Lock::Clear => "row deletes",
            Lock::Drop => "drops, rebuilds and schema changes",
            Lock::Delete => "deletion",
            Lock::All => "all changes",
        }
    }
}

impl fmt::Display for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A pipe's locks: a set of [`Lock`]s (`All` covers every kind).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Locks {
    set: BTreeSet<Lock>,
    unknown: Vec<String>,
}

impl Locks {
    /// No locks.
    pub fn new() -> Locks {
        Locks::default()
    }

    /// `all`.
    pub fn all() -> Locks {
        Locks::of(&[Lock::All])
    }

    /// The given locks.
    pub fn of(locks: &[Lock]) -> Locks {
        Locks {
            set: locks.iter().copied().collect(),
            unknown: Vec::new(),
        }
    }

    /// The locks in a pipe's parameters (none without a `locks` key).
    pub fn from_parameters(parameters: &Map) -> Locks {
        Locks::from_value(parameters.get(LOCKS_KEY))
    }

    /// The locks of a pipe's in-memory parameters.
    pub fn of_pipe(pipe: &Pipe) -> Locks {
        Locks::from_parameters(pipe.raw_parameters())
    }

    /// Read a stored `locks` value leniently: a list of names, one name or comma-separated
    /// names, `true` (all) or `false` / null (none). Unknown names are kept in
    /// [`Locks::unknown`] and lock nothing.
    pub fn from_value(value: Option<&Value>) -> Locks {
        let mut out = Locks::new();
        let mut add = |text: &str| {
            for part in text.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                match Lock::parse(part) {
                    Some(lock) => {
                        out.set.insert(lock);
                    }
                    None => out.unknown.push(part.to_string()),
                }
            }
        };
        match value {
            None | Some(Value::Null) => {}
            Some(Value::Bool(true)) => add("all"),
            Some(Value::Bool(false)) => {}
            Some(Value::Str(text)) => match Value::from_json_str(text) {
                Ok(Value::List(items)) => {
                    for item in &items {
                        add(&crate::pyfmt::py_str(item));
                    }
                }
                _ => add(text),
            },
            Some(Value::List(items)) => {
                for item in items {
                    add(&crate::pyfmt::py_str(item));
                }
            }
            Some(other) => add(&crate::pyfmt::py_str(other)),
        }
        out
    }

    /// Parse locks a person typed (`drop,sync`, `all`): every name must be known. An empty text
    /// is `all` (the default of `lock pipes`).
    pub fn parse_list(text: &str) -> Result<Locks, String> {
        if text.trim().is_empty() {
            return Ok(Locks::all());
        }
        let locks = Locks::from_value(Some(&Value::Str(text.to_string())));
        if !locks.unknown.is_empty() {
            return Err(format!(
                "Unknown lock{} {}: use {}.",
                if locks.unknown.len() == 1 { "" } else { "s" },
                locks
                    .unknown
                    .iter()
                    .map(|u| format!("'{u}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                lock_names_hint()
            ));
        }
        Ok(locks)
    }

    /// Whether no lock is held.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Whether `all` is held.
    pub fn is_all(&self) -> bool {
        self.set.contains(&Lock::All)
    }

    /// Whether `lock` is held (directly or through `all`).
    pub fn contains(&self, lock: Lock) -> bool {
        self.is_all() || self.set.contains(&lock)
    }

    /// The held locks as written (`all` stays `all`).
    pub fn locks(&self) -> Vec<Lock> {
        self.set.iter().copied().collect()
    }

    /// The held kinds with `all` expanded.
    pub fn kinds(&self) -> Vec<Lock> {
        if self.is_all() {
            return Lock::KINDS.to_vec();
        }
        self.locks()
    }

    /// The names that were not locks (they lock nothing).
    pub fn unknown(&self) -> &[String] {
        &self.unknown
    }

    /// These locks and `other`'s.
    pub fn with(&self, other: &Locks) -> Locks {
        let mut set = self.set.clone();
        set.extend(other.set.iter().copied());
        if set.contains(&Lock::All) {
            set = [Lock::All].into_iter().collect();
        }
        Locks {
            set,
            unknown: Vec::new(),
        }
    }

    /// These locks without `other`'s: removing `all` removes every lock, and removing one kind
    /// from `all` keeps the other kinds.
    pub fn without(&self, other: &Locks) -> Locks {
        if other.is_all() {
            return Locks::new();
        }
        let set: BTreeSet<Lock> = self
            .kinds()
            .into_iter()
            .filter(|k| !other.set.contains(k))
            .collect();
        let set = if self.is_all() && set.len() == Lock::KINDS.len() {
            [Lock::All].into_iter().collect()
        } else {
            set
        };
        Locks {
            set,
            unknown: Vec::new(),
        }
    }

    /// The stored names (`["all"]`, `["sync", "drop"]`), in the canonical order.
    pub fn names(&self) -> Vec<String> {
        self.set.iter().map(|l| l.name().to_string()).collect()
    }

    /// The stored form: a list of names.
    pub fn to_value(&self) -> Value {
        Value::List(self.names().into_iter().map(Value::Str).collect())
    }

    /// The held locks that refuse `op` (`all` reported as `all`).
    pub fn blocking(&self, op: LockOp) -> Vec<Lock> {
        if self.is_all() {
            return vec![Lock::All];
        }
        op.locks()
            .iter()
            .copied()
            .filter(|l| self.set.contains(l))
            .collect()
    }
}

impl fmt::Display for Locks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.names().join(", "))
    }
}

/// `parameters` with `locks` replaced (no locks removes the key).
pub fn parameters_with_locks(parameters: &Map, locks: &Locks) -> Map {
    let mut out = parameters.clone();
    if locks.is_empty() {
        out.shift_remove(LOCKS_KEY);
    } else {
        out.insert(LOCKS_KEY.into(), locks.to_value());
    }
    out
}

/// `parameters`, `sync`, `clear`, `drop`, `delete` or `all`.
pub fn lock_names_hint() -> String {
    let mut names: Vec<String> = Lock::KINDS
        .iter()
        .map(|l| format!("`{}`", l.name()))
        .collect();
    names.push("`all`".into());
    let last = names.pop().unwrap_or_default();
    format!("{} or {last}", names.join(", "))
}

/// A write that locks may refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockOp {
    /// Edit the parameters (refused by `parameters`).
    EditParameters,
    /// Sync (write) rows (refused by `sync`).
    Sync,
    /// Replace the rows of a window (`sync --replace`; refused by `sync` and `clear`).
    Replace,
    /// Delete duplicate rows (refused by `sync`).
    Deduplicate,
    /// Delete rows (refused by `clear`).
    Clear,
    /// Drop the table (refused by `drop`).
    Drop,
    /// Clear and fully re-sync after a definition change (refused by `drop`, `clear` and
    /// `sync`).
    Rebuild,
    /// Change the table's schema: `verify schema`, repartitioning, dropping indices (refused by
    /// `drop`).
    Schema,
    /// Unregister the pipe (refused by `delete`).
    Delete,
    /// Drop the table and unregister the pipe (`pipe.delete()`, `delete pipes`; refused by
    /// `delete` and `drop`).
    DeleteAndDrop,
}

impl LockOp {
    /// The locks that refuse this write.
    pub fn locks(&self) -> &'static [Lock] {
        match self {
            LockOp::EditParameters => &[Lock::Parameters],
            LockOp::Sync | LockOp::Deduplicate => &[Lock::Sync],
            LockOp::Replace => &[Lock::Sync, Lock::Clear],
            LockOp::Clear => &[Lock::Clear],
            LockOp::Drop | LockOp::Schema => &[Lock::Drop],
            LockOp::Rebuild => &[Lock::Sync, Lock::Clear, Lock::Drop],
            LockOp::Delete => &[Lock::Delete],
            LockOp::DeleteAndDrop => &[Lock::Drop, Lock::Delete],
        }
    }

    /// The verb of the refusal (`Cannot {verb} Pipe(...)`).
    pub fn verb(&self) -> &'static str {
        match self {
            LockOp::EditParameters => "edit the parameters of",
            LockOp::Sync => "sync",
            LockOp::Replace => "replace rows of",
            LockOp::Deduplicate => "deduplicate",
            LockOp::Clear => "clear",
            LockOp::Drop => "drop",
            LockOp::Rebuild => "rebuild",
            LockOp::Schema => "change the table of",
            LockOp::Delete | LockOp::DeleteAndDrop => "delete",
        }
    }

    /// The name (`edit_parameters`, `sync`, ...).
    pub fn name(&self) -> &'static str {
        match self {
            LockOp::EditParameters => "edit_parameters",
            LockOp::Sync => "sync",
            LockOp::Replace => "replace",
            LockOp::Deduplicate => "deduplicate",
            LockOp::Clear => "clear",
            LockOp::Drop => "drop",
            LockOp::Rebuild => "rebuild",
            LockOp::Schema => "schema",
            LockOp::Delete => "delete",
            LockOp::DeleteAndDrop => "delete_and_drop",
        }
    }

    /// Parse an operation name ([`LockOp::name`], also `edit` / `parameters` and `verify` for
    /// a verification sync).
    pub fn parse(text: &str) -> Option<LockOp> {
        match text.trim().to_lowercase().as_str() {
            "edit_parameters" | "edit" | "parameters" | "register" => Some(LockOp::EditParameters),
            "sync" | "verify" | "write" => Some(LockOp::Sync),
            "replace" => Some(LockOp::Replace),
            "deduplicate" | "dedup" => Some(LockOp::Deduplicate),
            "clear" => Some(LockOp::Clear),
            "drop" => Some(LockOp::Drop),
            "rebuild" => Some(LockOp::Rebuild),
            "schema" | "verify_schema" | "repartition" | "drop_indices" => Some(LockOp::Schema),
            "delete" => Some(LockOp::Delete),
            "delete_and_drop" => Some(LockOp::DeleteAndDrop),
            _ => None,
        }
    }
}

impl From<Lock> for LockOp {
    /// The write a lock kind refuses on its own (`all` maps to the widest, a rebuild).
    fn from(lock: Lock) -> Self {
        match lock {
            Lock::Parameters => LockOp::EditParameters,
            Lock::Sync => LockOp::Sync,
            Lock::Clear => LockOp::Clear,
            Lock::Drop => LockOp::Drop,
            Lock::Delete => LockOp::Delete,
            Lock::All => LockOp::Rebuild,
        }
    }
}

impl fmt::Display for LockOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The text every lock refusal carries (see [`is_lock_refusal`]).
pub const LOCK_REFUSAL_MARKER: &str = "it is locked (";

/// `Ok(())` when `locks` allow `op`; else `Err` naming the locks that refuse it
/// (`it is locked (sync)`). [`lock_refusal`] builds the full message for a pipe.
pub fn check_lock(locks: &Locks, op: impl Into<LockOp>) -> Result<(), String> {
    let held = locks.blocking(op.into());
    if held.is_empty() {
        return Ok(());
    }
    Err(format!("{LOCK_REFUSAL_MARKER}{})", names_of(&held)))
}

fn names_of(locks: &[Lock]) -> String {
    locks
        .iter()
        .map(|l| l.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The command that removes `locks` from `pipe` (`mrsm unlock pipes -c ... -m ... [-l ...]
/// [-i ...] --locks sync`).
pub fn unlock_command(pipe: &Pipe, locks: &[Lock]) -> String {
    let mut cmd = format!(
        "mrsm unlock pipes -c {} -m {}",
        pipe.connector_keys(),
        pipe.metric_key()
    );
    if let Some(location) = pipe.location_key() {
        cmd.push_str(&format!(" -l {location}"));
    }
    if pipe.instance_keys() != pipe.default_instance_keys() {
        cmd.push_str(&format!(" -i {}", pipe.instance_keys()));
    }
    if !locks.is_empty() {
        cmd.push_str(&format!(" --locks {}", names_of(locks).replace(", ", ",")));
    }
    cmd
}

/// The refusal of `op` on `pipe` under `locks` (`None` when allowed):
/// `Cannot sync Pipe('sql:main', 'weather'): it is locked (sync). Unlock it with `mrsm unlock
/// pipes -c sql:main -m weather --locks sync` or in the web console (Manage > Locks).`
pub fn lock_refusal(pipe: &Pipe, locks: &Locks, op: impl Into<LockOp>) -> Option<String> {
    let op = op.into();
    let held = locks.blocking(op);
    if held.is_empty() {
        return None;
    }
    // What to unlock for this write: the kinds it needs that are held (`all` held: all of them).
    let to_unlock: Vec<Lock> = op
        .locks()
        .iter()
        .copied()
        .filter(|l| locks.contains(*l))
        .collect();
    Some(format!(
        "Cannot {} {pipe}: {LOCK_REFUSAL_MARKER}{}). Unlock it with `{}` or in the web console (Manage > Locks).",
        op.verb(),
        names_of(&held),
        unlock_command(pipe, &to_unlock)
    ))
}

/// Whether a message is a lock refusal ([`lock_refusal`]).
pub fn is_lock_refusal(message: &str) -> bool {
    message.contains(LOCK_REFUSAL_MARKER) && message.contains("mrsm unlock pipes")
}

/// The top-level parameter keys whose values differ between `old` and `new` (in `new`'s
/// order, then the keys only `old` has), ignoring `ignored`.
pub fn changed_parameter_keys(old: &Map, new: &Map, ignored: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (key, value) in new {
        if ignored.contains(&key.as_str()) {
            continue;
        }
        if old.get(key) != Some(value) {
            out.push(key.clone());
        }
    }
    for key in old.keys() {
        if !ignored.contains(&key.as_str()) && !new.contains_key(key) && !out.contains(key) {
            out.push(key.clone());
        }
    }
    out
}

/// Whether an edit from `old` to `new` is allowed under `locks`: without a `parameters` lock
/// every edit is; with one, only changes to `locks` itself (and, for `access_maintenance`, the
/// `access` changes the team and user stores make when a team is renamed or deleted or a user
/// is deleted).
pub fn parameters_edit_allowed(
    old: &Map,
    new: &Map,
    locks: &Locks,
    access_maintenance: bool,
) -> bool {
    if !locks.contains(Lock::Parameters) {
        return true;
    }
    let ignored: &[&str] = if access_maintenance {
        &[LOCKS_KEY, ACCESS_KEY]
    } else {
        &[LOCKS_KEY]
    };
    changed_parameter_keys(old, new, ignored).is_empty()
}
