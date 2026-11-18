//! Bounded trusted authorization reads on the exact retained InnoDB writer.
use std::sync::Arc;
use std::time::Instant;

use futures_util::TryStreamExt;
use sqlx::{Column, MySql, Row, Transaction, ValueRef};

use super::{SqlDriver, SqlParam, backend, mysql::MySqlBackend};
use crate::Result;

#[derive(Clone)]
pub(super) struct Policy {
    pub(super) lineage: Arc<()>,
    pub(super) deadline: Instant,
}

pub(super) const TABLES: &[&str] = &["mrsm_users", "mrsm_teams", "mrsm_pipes"];
const MAX_ROWS: usize = 4096;
const MAX_BYTES: usize = 4 * 1024 * 1024;

pub(crate) fn refused() -> crate::SqlError {
    crate::SqlError::InvalidArgument(
        "The exact InnoDB authorization transaction could not be proved.".into(),
    )
}

pub(super) struct Held {
    tx: tokio::sync::Mutex<Option<Transaction<'static, MySql>>>,
    policy: Policy,
}

tokio::task_local! { static INNODB: Arc<Held>; }

impl Held {
    pub(super) fn new(tx: Transaction<'static, MySql>, policy: Policy) -> Arc<Self> {
        Arc::new(Self {
            tx: tokio::sync::Mutex::new(Some(tx)),
            policy,
        })
    }

    pub(super) async fn scope<F: std::future::Future>(
        self: &Arc<Self>,
        task: F,
    ) -> Result<F::Output> {
        tokio::time::timeout_at(
            self.policy.deadline.into(),
            INNODB.scope(self.clone(), task),
        )
        .await
        .map_err(|_| refused())
    }

    pub(super) async fn take(&self) -> Result<Transaction<'static, MySql>> {
        self.tx.lock().await.take().ok_or_else(refused)
    }
}

struct Query {
    sql: String,
    params: Vec<SqlParam>,
}

tokio::task_local! { static QUERY: Query; }

pub(crate) fn active() -> bool {
    INNODB.try_with(|_| ()).is_ok()
}

/// Only exact queries assembled inside the trusted registration, user and team readers
/// may borrow this transaction. All other driver paths remain unavailable in the scope.
pub(crate) async fn approve<F: std::future::Future>(
    sql: &str,
    params: &[SqlParam],
    task: F,
) -> F::Output {
    if !active() {
        return task.await;
    }
    let query = Query {
        sql: sql.into(),
        params: params.to_vec(),
    };
    QUERY.scope(query, task).await
}

pub(super) async fn read(
    driver: &SqlDriver,
    sql: &str,
    params: &[SqlParam],
    mode: super::authorization_reads::Mode,
) -> Result<Option<super::authorization_reads::Output>> {
    use super::authorization_reads::{Mode, Output};
    use backend::Backend;
    let Ok(held) = INNODB.try_with(Clone::clone) else {
        return Ok(None);
    };
    if !meerschaum_connector::mutation::rechecking()
        || !Arc::ptr_eq(&held.policy.lineage, &driver.lineage)
        || QUERY
            .try_with(|query| query.sql == sql && query.params == params)
            .ok()
            != Some(true)
        || sql.len() > 32 * 1024
        || params.len() > 256
        || !crate::readonly::is_read_statement(sql, Some(driver.flavor))
    {
        return Err(refused());
    }
    let mut locked = held.tx.lock().await;
    let transaction = locked.as_mut().ok_or_else(refused)?;
    let mut result = backend::bound::<MySqlBackend>(sql, params).fetch(&mut **transaction);
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    // Drain the actual statement even for a scalar; no half-consumed result enters DML.
    while let Some(row) = result.try_next().await? {
        for (index, col) in row.columns().iter().enumerate() {
            let value = row.try_get_raw(index)?;
            let size = if value.is_null() {
                0
            } else {
                row.try_get_unchecked::<&[u8], _>(index)?.len()
            };
            bytes = bytes.saturating_add(size + col.name().len() + 16);
        }
        if rows.len() == MAX_ROWS || bytes > MAX_BYTES {
            return Err(refused());
        }
        rows.push(row);
    }
    let output = match mode {
        Mode::Value => Output::Value(
            rows.first()
                .map(|row| {
                    let plans = MySqlBackend::plan(row.columns());
                    plans
                        .first()
                        .map(|plan| MySqlBackend::cell(row, 0, plan))
                        .transpose()
                })
                .transpose()?
                .flatten(),
        ),
        Mode::Batches | Mode::Docs => {
            let batch = backend::make_batch::<MySqlBackend>(&rows, &mut None)?;
            if mode == Mode::Docs {
                Output::Docs(meerschaum_kernel::Frame::from_record_batches(&[batch])?)
            } else {
                Output::Batches(vec![batch])
            }
        }
    };
    Ok(Some(output))
}
