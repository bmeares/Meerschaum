//! Schema verification (`verify schema`, `Pipe.verify_schema()`): how a pipe's table differs
//! from the pipe's declared dtypes, and the statements that bring it in line.
//!
//! An instance builds a [`SchemaPlan`] without writing anything
//! ([`crate::InstanceConnector::get_pipe_schema_plan`]) and applies it on request
//! ([`crate::InstanceConnector::apply_pipe_schema`]). A plan never drops a column: it adds the
//! declared columns the table lacks, widens types safely, and changes other types only when the
//! caller confirms them.

use serde::{Deserialize, Serialize};

/// What a [`SchemaChange`] does to a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeKind {
    /// A declared column the table does not have: `ADD COLUMN`.
    AddColumn,
    /// A widening that keeps every value: `int` → `bigint`, `int` → `float`, `float` →
    /// `numeric`, `varchar(n)` → `text`, `text` → `json`.
    Widen,
    /// Any other type change (`ALTER ... TYPE ... USING ...`): it may fail on existing values or
    /// change them, so it needs confirmation.
    Change,
}

impl SchemaChangeKind {
    /// The word the plan table shows.
    pub fn label(&self) -> &'static str {
        match self {
            SchemaChangeKind::AddColumn => "add column",
            SchemaChangeKind::Widen => "widen",
            SchemaChangeKind::Change => "change type",
        }
    }
}

/// One column of a [`SchemaPlan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaChange {
    /// The column.
    pub column: String,
    /// What happens to it.
    pub kind: SchemaChangeKind,
    /// The column's database type now (`None` for a column the table lacks).
    pub current: Option<String>,
    /// The database type it gets.
    pub target: String,
    /// The pipe's dtype for the column (what `dtypes` declares).
    pub dtype: String,
    /// Anything the user should know (`rebuilds the table`, the `USING` expression).
    pub note: Option<String>,
    /// Why the change cannot be applied as is (a compressed hypertable, an indexed column on
    /// MSSQL); the plan's statements leave it out.
    #[serde(default)]
    pub blocked: Option<String>,
}

impl SchemaChange {
    /// Whether the change keeps every existing value (applied without confirmation by
    /// `--noask`).
    pub fn is_safe(&self) -> bool {
        !matches!(self.kind, SchemaChangeKind::Change)
    }

    /// Whether the change can be applied (it is not [`blocked`](Self::blocked)).
    pub fn is_applicable(&self) -> bool {
        self.blocked.is_none()
    }
}

/// How a pipe's table differs from its declared dtypes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPlan {
    /// The table as the database names it (quoted, with its schema).
    pub table: String,
    /// Whether the table exists (a missing table has nothing to verify: the first sync creates
    /// it with the declared types).
    pub exists: bool,
    /// The changes, in column order.
    pub changes: Vec<SchemaChange>,
    /// The statements that apply every change, in order (one transaction where the database
    /// allows it).
    pub queries: Vec<String>,
    /// The statements that apply only the safe changes ([`SchemaChange::is_safe`]).
    pub safe_queries: Vec<String>,
    /// Declared columns the plan leaves alone, and why (no declared dtype for a missing column,
    /// an indexed column that keeps a bounded text type).
    pub notes: Vec<String>,
    /// Why some changes cannot be applied as is (each blocked change's reason, once), if any.
    /// The other changes may still be applied.
    pub blocked: Option<String>,
}

impl SchemaPlan {
    /// Whether the table already matches its declared dtypes.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// The safe changes.
    pub fn safe_changes(&self) -> Vec<&SchemaChange> {
        self.changes.iter().filter(|c| c.is_safe()).collect()
    }

    /// The changes that need confirmation.
    pub fn unsafe_changes(&self) -> Vec<&SchemaChange> {
        self.changes.iter().filter(|c| !c.is_safe()).collect()
    }

    /// The changes that cannot be applied as is.
    pub fn blocked_changes(&self) -> Vec<&SchemaChange> {
        self.changes.iter().filter(|c| !c.is_applicable()).collect()
    }

    /// Whether `other` (a fresh plan) has the same changes: applying refuses a plan that no
    /// longer describes the table.
    pub fn same_changes(&self, other: &SchemaPlan) -> bool {
        self.changes == other.changes
    }
}
