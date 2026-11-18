//! Main data-sync inference uses an expected-old check under the native metadata write lock.

use super::{SqlConnector, frame_docs};
use crate::driver::{SqlDriver, SqlParam};
use meerschaum_connector::mutation::{self, MetadataInference, MutationCheck};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use std::sync::Arc;

struct Compare {
    parent: Arc<dyn MutationCheck>,
    driver: SqlDriver,
    query: String,
    id: i64,
    before: Map,
}
#[meerschaum_connector::async_trait]
impl MutationCheck for Compare {
    fn rollback_completed(&self, succeeded: bool) {
        self.parent.rollback_completed(succeeded);
    }
    fn lock_snapshot_reads(&self) -> bool {
        // This capability is created only for Main's validated inference-only metadata
        // UPDATE. Its authority intentionally compares the external old definition until
        // commit; its own changed metadata is accepted through the separate native receipt.
        // Keep exact raw reads under that write lock so a large parameter row spilling to
        // disk cannot deadlock a fresh reader on another pool connection.
        true
    }

    async fn check(&self) -> Result<(), String> {
        mutation::recheck(&self.parent).await?;
        let frame = self
            .driver
            .read_frame_with(&self.query, &[SqlParam::Int(self.id)])
            .await
            .map_err(|_| "Could not verify native inference metadata.")?;
        let docs = frame_docs(&frame);
        if docs.len() != 1
            || super::pipes::parse_parameters(docs[0].get("parameters")) != self.before
        {
            return Err(
                "Pipe metadata changed before native inference acquired its write lock.".into(),
            );
        }
        Ok(())
    }
}

impl SqlConnector {
    pub(super) async fn edit_inferred_metadata(
        &self,
        pipe: &Pipe,
        id: i64,
        parameters: Map,
        query: &str,
        params: &[SqlParam],
        parent: Arc<dyn MutationCheck>,
    ) -> SuccessTuple {
        let before = match self.get_pipe_parameters(pipe).await {
            Ok(Some(before)) => before,
            _ => return SuccessTuple::fail("Native inference lost its registered metadata."),
        };
        let change = MetadataInference {
            pipe: pipe.clone(),
            registration: Value::Int(id),
            before: before.clone(),
            after: parameters,
        };
        if let Err(reason) = change.validate() {
            return SuccessTuple::fail(reason);
        }
        let check = Arc::new(Compare {
            parent: parent.clone(),
            driver: self.driver().clone(),
            query: format!(
                "SELECT parameters FROM {} WHERE pipe_id = {}",
                self.instance_table_name("mrsm_pipes"),
                crate::driver::params::placeholder(self.flavor, 1)
            ),
            id,
            before,
        });
        let written = mutation::scope(check, self.exec_params(query, params)).await;
        if written.is_none() {
            return SuccessTuple::fail(
                "Native inference could not conditionally persist its metadata.",
            );
        }
        super::pipes::bump_metadata_epoch(SuccessTuple::success());
        match parent.metadata_inferred(change).await {
            Ok(()) => SuccessTuple::ok(format!("Successfully edited {pipe}.")),
            Err(reason) => SuccessTuple::fail(format!(
                "Native metadata inference committed, but its operation could not accept the transition: {reason}"
            )),
        }
    }
}
