//! Apply a frame with the ordinary update queries. Small patches are a CTE. A larger
//! patch lives on the merge transaction's connection and disappears on commit or rollback.

use super::*;

const PATCH_ROWS: usize = 1024;

impl SqlConnector {
    pub(super) async fn apply_postgres_patch(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        frame: &Frame,
        args: &mut UpdateQueriesArgs<'_>,
    ) -> ConnectorResult<u64> {
        let flavor = self.flavor;
        let markers = null_marker_columns(sp, state);
        let mut columns = Vec::new();
        let mut bindings = Vec::new();
        let mut casts = Vec::new();
        let mut names = Vec::new();
        for (i, (col, typ)) in args.patch_cols_types.iter().enumerate() {
            let name = sql_item_name(col, flavor, None);
            let encoding = ColumnEncoding {
                name: col.clone(),
                numeric: numeric_precision_scale(typ).map(|(p, s)| (Some(p), Some(s))),
                keep_null_markers: markers.contains(col),
            };
            columns.push((frame.data(col).map_err(be)?, encoding));
            bindings.push(format!("${}::text[]", i + 1));
            casts.push(format!("CAST({name} AS {typ}) AS {name}"));
            names.push(name);
        }
        let patch = sql_item_name(args.patch, flavor, None);
        let names = names.join(", ");
        let select = format!(
            "SELECT {} FROM unnest({}) AS cells({names})",
            casts.join(", "),
            bindings.join(", "),
        );
        let params = |start: usize, end: usize| {
            columns
                .iter()
                .map(|(data, enc)| {
                    SqlParam::TextArray(
                        (start..end)
                            .map(|row| encode_copy_field(&data.value_ref(row), enc))
                            .collect(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let mut tx = self.driver().begin_write().await?;
        let mut written = 0;
        if frame.height() <= PATCH_ROWS {
            args.patch_schema = Some("");
            let params = params(0, frame.height());
            for query in get_update_queries(args) {
                let statement = format!("WITH {patch} AS ({select})\n{query}");
                self.log(&statement);
                written += tx.exec_with(&statement, &params).await?;
            }
        } else {
            args.patch_schema = Some("pg_temp");
            let ddl = args
                .patch_cols_types
                .iter()
                .map(|(col, typ)| format!("{} {typ}", sql_item_name(col, flavor, None)))
                .collect::<Vec<_>>()
                .join(", ");
            let create = format!("CREATE TEMP TABLE {patch} ({ddl}) ON COMMIT DROP");
            self.log(&create);
            tx.exec(&create).await?;
            let insert = format!("INSERT INTO pg_temp.{patch} ({names}) {select}");
            for start in (0..frame.height()).step_by(PATCH_ROWS) {
                tx.exec_with(
                    &insert,
                    &params(start, (start + PATCH_ROWS).min(frame.height())),
                )
                .await?;
            }
            for query in get_update_queries(args) {
                written += tx.exec(&query).await?;
            }
        }
        tx.commit().await?;
        Ok(written)
    }
}
