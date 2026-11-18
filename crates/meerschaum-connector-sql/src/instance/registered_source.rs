//! A single operator-approved, same-instance PostgreSQL table read.

use std::sync::atomic::AtomicBool;
use std::time::Instant;

use meerschaum_connector::DataQuery;
use meerschaum_core::Pipe;
use meerschaum_kernel::Frame;

use super::SqlConnector;
use crate::Flavor;

#[derive(Clone, Copy)]
pub struct RegisteredSource<'a> {
    pub pipe: &'a Pipe,
    pub registration: i64,
    pub schema: &'a str,
    pub table: &'a str,
}

#[derive(Clone, Copy)]
pub struct SourceReadBudget<'a> {
    pub deadline: Instant,
    pub cancelled: &'a AtomicBool,
    pub batch_rows: usize,
    pub batch_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum SourceReadError {
    Unsupported,
    Changed,
    TooLarge,
    Unavailable,
}

impl SqlConnector {
    /// Rechecks the exact SQL registration and relation on the connection used to read.
    /// Omit the query to recheck proof before returning an existing frozen page.
    pub async fn read_registered_source<F>(
        &self,
        source: RegisteredSource<'_>,
        query: Option<&DataQuery>,
        budget: SourceReadBudget<'_>,
        batch: F,
    ) -> Result<String, SourceReadError>
    where
        F: FnMut(Frame) -> Result<(), SourceReadError> + Send,
    {
        if self.flavor() != Flavor::Postgresql
            || source.pipe.connector_keys() != self.keys()
            || source.pipe.instance_keys() != self.keys()
            || source.registration <= 0
            || source.pipe.registration_id().as_deref() != Some(&source.registration.to_string())
        {
            return Err(SourceReadError::Unsupported);
        }
        let sql_pipe = self
            .sql_pipe(source.pipe)
            .map_err(|_| SourceReadError::Unsupported)?;
        if sql_pipe.target != source.table
            || self.get_pipe_schema(&sql_pipe).as_deref() != Some(source.schema)
        {
            return Err(SourceReadError::Changed);
        }
        crate::driver::registered_source::read(
            self.spec(),
            self.instance_schema().ok_or(SourceReadError::Unsupported)?,
            source,
            query,
            budget,
            batch,
        )
        .await
    }
}
