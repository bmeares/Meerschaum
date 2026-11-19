//! Preflight for the bounded InnoDB append adapter.
use std::collections::BTreeSet;

use meerschaum_connector::{ConnectorResult, SyncOptions, mutation::DataWindow};
use meerschaum_core::{Pipe, Value, success::SyncResult};
use meerschaum_kernel::{Frame, value::Value as KValue};

use super::SqlConnector;
use crate::driver::{
    SqlParam,
    mysql_append::{Append, MAX_ROWS, name},
};

fn refused() -> meerschaum_connector::ConnectorError {
    super::be("A registered InnoDB batch requires an existing, explicitly typed append target.")
}

impl SqlConnector {
    pub(crate) fn innodb_append_plan(
        &self,
        pipe: &Pipe,
        frame: &Frame,
        options: &SyncOptions,
        window: &DataWindow,
    ) -> ConnectorResult<Append> {
        let db = self
            .spec()
            .database
            .as_deref()
            .filter(|value| name(value))
            .ok_or_else(refused)?;
        let target = pipe
            .raw_parameters()
            .get("target")
            .and_then(Value::as_str)
            .filter(|value| name(value))
            .ok_or_else(refused)?;
        let sp = self.sql_pipe(pipe)?;
        let id = sp
            .column("primary")
            .filter(|value| name(value))
            .ok_or_else(refused)?;
        let types = pipe.dtypes();
        let columns = frame.names();
        let registration = pipe
            .registration_id()
            .and_then(|id| id.parse::<i64>().ok())
            .filter(|id| *id > 0)
            .ok_or_else(refused)?;
        if pipe.temporary
            || pipe.connector_keys() == "mrsm"
            || sp.target != target
            || sp.autoincrement()
            || sp.upsert()
            || sp.autotime()
            || pipe.raw_parameters().contains_key("schema")
            || options.replace
            || options.check_existing
            || options.params.is_some()
            || frame.height() == 0
            || frame.height() > MAX_ROWS
            || columns.is_empty()
            || columns.len() > 32
            || columns
                .iter()
                .any(|name| !crate::driver::mysql_append::name(name))
        {
            return Err(refused());
        }
        window
            .validate_frame(pipe, frame, options)
            .map_err(super::be)?;
        let id_at = columns
            .iter()
            .position(|column| *column == id)
            .ok_or_else(refused)?;
        let mut bytes = 0usize;
        let mut ids = BTreeSet::new();
        let mut rows = Vec::with_capacity(frame.height());
        for index in 0..frame.height() {
            let row = frame.row(index);
            let mut params = Vec::with_capacity(columns.len());
            if !matches!(row[id_at], KValue::Int(i) if ids.insert(i)) {
                return Err(refused());
            }
            for (name, value) in columns.iter().zip(row) {
                let kind = types
                    .get(name)
                    .and_then(Value::as_str)
                    .ok_or_else(refused)?;
                let cell = match value {
                    KValue::Int(i) if meerschaum_core::dtypes::are_dtypes_equal(kind, "int") => {
                        bytes += 8;
                        SqlParam::Int(i)
                    }
                    KValue::Bytes(value) if kind == "bytes" => {
                        bytes = bytes.saturating_add(value.len());
                        SqlParam::Bytes(value)
                    }
                    _ => return Err(refused()),
                };
                if bytes > 4 * 1024 * 1024 {
                    return Err(refused());
                }
                params.push(cell);
            }
            rows.push(params);
        }
        Ok(Append {
            database: db.into(),
            target: target.into(),
            id,
            columns,
            rows,
            registration,
            pipe: pipe.clone(),
            window: window.clone(),
            options: options.clone(),
        })
    }

    pub(super) async fn execute_innodb_append(
        &self,
        append: Append,
    ) -> ConnectorResult<SyncResult> {
        let count = self
            .driver()
            .mysql_append(append)
            .await
            .map_err(super::be)?;
        let mut result = SyncResult::new(true, format!("Inserted {count}, updated 0 rows."));
        result.inserted = count;
        Ok(result)
    }
}
