//! Add nullable, ordinary value columns inside the same SQLite transaction as their rows.
use super::*;
use std::sync::OnceLock;

pub(in super::super) struct AddColumns {
    pub(super) statements: Vec<(String, Vec<SqlParam>)>,
    types: IndexMap<String, String>,
    table_sql: String,
    observed: OnceLock<SchemaSeal>,
}

impl AddColumns {
    pub(in super::super) fn new(
        conn: &SqlConnector,
        pipe: &SqlPipe,
        state: &PipeState,
        incoming: &IndexMap<String, String>,
    ) -> ConnectorResult<Self> {
        let additions = conn.columns_to_add(pipe, incoming, &state.cols_types);
        if conn.flavor != Flavor::Sqlite
            || !state.exists
            || additions.is_empty()
            || state.cols_types.len() + additions.len() > 512
        {
            return Err(be(
                "This native nullable-column transition exceeds the supported SQLite layout.",
            ));
        }
        let roles = columns_map(pipe);
        let mut types = state.cols_types.clone();
        for (column, dtype) in additions {
            if !native_type(&dtype)
                || roles
                    .values()
                    .any(|role| role.eq_ignore_ascii_case(&column))
                || types.keys().any(|name| name.eq_ignore_ascii_case(&column))
            {
                return Err(be(
                    "Only distinct ordinary nullable value columns may be added to this native target.",
                ));
            }
            types.insert(column, dtype);
        }
        let statements: Vec<_> = conn
            .add_columns_queries_for(pipe, incoming, &state.cols_types)
            .into_iter()
            .map(|sql| (sql, Vec::new()))
            .collect();
        crate::driver::write_guard::bounded_inputs(&statements).map_err(be)?;
        Ok(Self {
            table_sql: plain_table_sql(pipe, &types)?,
            types,
            statements,
            observed: OnceLock::new(),
        })
    }

    pub(super) fn projected_state(&self, pipe: &SqlPipe, state: &PipeState) -> PipeState {
        PipeState {
            exists: state.exists,
            cols_types: self.types.clone(),
            dtypes: super::super::super::data::pipe_dtypes(pipe, &self.types, true),
            explicit_dtypes: state.explicit_dtypes.clone(),
        }
    }

    /// A newly added BLOB has no existing values to convert. Use the exact post-ADD
    /// schema for the alter planner and retain conversions for all existing columns.
    pub(in super::super) fn project_alter(
        &self,
        input: &mut AlterColumnsInput,
        incoming: &IndexMap<String, String>,
    ) {
        retain_special_conversions(
            &mut input.new_special_cols,
            incoming,
            &input.db_cols_types,
            &self.types,
        );
        input.db_cols_types.clone_from(&self.types);
    }

    pub(super) fn after(&self) -> Result<&SchemaSeal, crate::SqlError> {
        self.observed.get().ok_or_else(|| {
            error("The native nullable column change has no verified SQLite schema.")
        })
    }

    pub(super) async fn apply(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
        before: &SchemaSeal,
    ) -> Result<(), crate::SqlError> {
        for (sql, parameters) in &self.statements {
            writer.execute_native(transaction, sql, parameters).await?;
        }
        let batch = transaction
            .raw_query(objects_query(), &[SqlParam::Text(before.target.clone())])
            .await?;
        let objects = decode_objects(&Frame::from_record_batches(&[batch])?)?;
        let mut tables = objects.iter().filter(|(kind, _, _)| kind == "table");
        let correct_table = tables.next().is_some_and(|(_, name, sql)| {
            name == &before.target && sql.as_deref().is_some_and(|sql| ddl(sql) == self.table_sql)
        });
        if !correct_table
            || tables.next().is_some()
            || before
                .objects
                .iter()
                .filter(|(kind, _, _)| kind != "table")
                .ne(objects.iter().filter(|(kind, _, _)| kind != "table"))
        {
            return Err(error(
                "The SQLite target differs from its exact nullable column addition plan.",
            ));
        }
        self.observed
            .set(SchemaSeal {
                target: before.target.clone(),
                objects,
                binding: before.binding.clone(),
            })
            .map_err(|_| error("The nullable column addition was already applied."))?;
        Ok(())
    }

    pub(super) fn effect(
        &self,
        before: &SchemaSeal,
    ) -> Result<meerschaum_connector::mutation::NativeSchemaEffect, crate::SqlError> {
        meerschaum_connector::mutation::NativeSchemaEffect::add_columns(
            create::digest(&before.objects)?,
            create::digest(&self.after()?.objects)?,
            create::digest(
                &self
                    .statements
                    .iter()
                    .map(|(sql, _)| sql)
                    .collect::<Vec<_>>(),
            )?,
        )
        .map_err(|reason| error(&reason))
    }
}
