//! Durable canonical commit transitions. Call only from owned blocking I/O tasks.
//!
//! Preparing requires the exact fresh live operation. Recording actual COMMIT/rollback
//! retains the owned non-deserializable Ticket even after source authority is revoked;
//! recording truth does not grant a new write or dependent execution.
//! Its `batch_journal` child holds the private batch Catalog transitions.

mod batch_journal;

use super::{Catalog, Record};
use meerschaum_connector::mutation::NativeDataEffect;
use meerschaum_server::executor::pipe_operation::{
    CallbackBinding,
    commits::{Binding, Ticket},
};
use std::collections::BTreeMap;

fn unavailable() -> String {
    "The canonical native commit identity changed or its catalog is busy. No commit intent was adopted; inspect the original operation before retrying uncertain work.".into()
}

fn matches_binding(record: &Record, binding: &CallbackBinding) -> bool {
    record.callback == Some(binding.grant)
        && record.remote_name == format!("mrsm-{}", binding.launch.simple())
        && record.context_revision == binding.context_revision
}

fn same_operation(current: &Record, expected: &Record) -> bool {
    let same_extension = match (&current.extension, &expected.extension) {
        (None, None) => true,
        (Some(current), Some(expected)) => {
            matches!((current.digest(), expected.digest()), (Ok(left), Ok(right)) if left == right)
        }
        _ => false,
    };
    current.remote_name == expected.remote_name
        && current.callback == expected.callback
        && current.context_revision == expected.context_revision
        && current.owner_binding == expected.owner_binding
        && current.team_id == expected.team_id
        && current.placement == expected.placement
        && current.profile == expected.profile
        && current.sysargs == expected.sysargs
        && current.properties == expected.properties
        && current.operation == expected.operation
        && current.source_selection == expected.source_selection
        && same_extension
        && current.discovery == expected.discovery
        && current.connector_registration == expected.connector_registration
        && current.action_request == expected.action_request
}

impl Catalog {
    pub(crate) fn install_prepared_children(
        &self,
        expected: &Record,
        proposed: meerschaum_server::executor::pipe_operation::GraphOperation,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let current = records.get_mut(&expected.name).ok_or_else(unavailable)?;
        if current.callback_suspended
            || !same_operation(current, expected)
            || current.commit_journal != expected.commit_journal
            || current.commit_journal.has_pending()
        {
            return Err(unavailable());
        }
        current.operation = Some(proposed);
        self.durable_commit_write(&records)
    }

    /// Only an actual native metadata receipt may advance the issuer's exact old
    /// member. A derived consumer retains its own narrower source and cannot replace
    /// a missing issuer or overwrite concurrent canonical definitions.
    pub(crate) fn accept_issued_inference(
        &self,
        consumer: &Record,
        issuer: &Record,
        change: &meerschaum_connector::mutation::MetadataInference,
    ) -> Result<bool, String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let live_consumer = records.get(&consumer.name).ok_or_else(unavailable)?;
        let live_issuer = records.get(&issuer.name).ok_or_else(unavailable)?;
        if !same_operation(live_consumer, consumer)
            || live_consumer.callback_suspended
            || !same_operation(live_issuer, issuer)
            || live_issuer.callback_suspended
        {
            return Err(unavailable());
        }
        let issuer = records.get_mut(&issuer.name).ok_or_else(unavailable)?;
        let Some(snapshot) = issuer
            .operation
            .as_mut()
            .and_then(|operation| operation.snapshot_mut())
        else {
            return Ok(false);
        };
        if !snapshot.accept_inference(change)? {
            return Ok(false);
        }
        self.durable_commit_write(&records)?;
        Ok(true)
    }
    fn commit_lock(&self) -> Result<std::fs::File, String> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
        }
        // A real canonical record has already created this private lock. Failure must
        // not create a second catalog or silently infer an empty operation.
        let file = options
            .open(self.directory.join("catalog.lock"))
            .map_err(|_| unavailable())?;
        Self::lock_with_deadline(&file, false)?;
        Ok(file)
    }

    fn durable_commit_write(&self, records: &BTreeMap<String, Record>) -> Result<(), String> {
        #[cfg(not(unix))]
        {
            let _ = records;
            Err("Durable native commit intent requires a supported filesystem host.".into())
        }
        #[cfg(unix)]
        {
            self.write(records)?;
            // write_private fsyncs its temporary file before atomic rename. Sync the
            // installed file as well (including its unchanged-content fast path), then
            // the directory entry and its established Main-root parent before COMMIT.
            std::fs::File::open(self.directory.join("catalog.json"))
                .and_then(|file| file.sync_all())
                .map_err(|_| "Could not sync the native commit catalog.")?;
            for directory in [Some(self.directory.as_path()), self.directory.parent()]
                .into_iter()
                .flatten()
            {
                let descriptor = rustix::fs::open(
                    directory,
                    rustix::fs::OFlags::RDONLY
                        | rustix::fs::OFlags::DIRECTORY
                        | rustix::fs::OFlags::NOFOLLOW
                        | rustix::fs::OFlags::CLOEXEC,
                    rustix::fs::Mode::empty(),
                )
                .map_err(|_| "Could not open the native commit directory for syncing.")?;
                std::fs::File::from(descriptor)
                    .sync_all()
                    .map_err(|_| "Could not durably sync the native commit directory.")?;
            }
            Ok(())
        }
    }

    /// The Ticket escapes only after its exact intent is durable before native COMMIT.
    pub(crate) fn prepare_data_commit(
        &self,
        consumer: &Record,
        issuer: &Record,
        binding: Binding,
        effect: &NativeDataEffect,
    ) -> Result<Ticket, String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let current_consumer = records.get(&consumer.name).ok_or_else(unavailable)?;
        let current_issuer = records.get(&issuer.name).ok_or_else(unavailable)?;
        if !same_operation(current_consumer, consumer)
            || current_consumer.callback_suspended
            || !same_operation(current_issuer, issuer)
            || current_issuer.callback_suspended
            || !matches_binding(current_consumer, binding.consumer())
            || !matches_binding(current_issuer, binding.issuer())
        {
            return Err(unavailable());
        }
        let current_issuer = records.get_mut(&issuer.name).ok_or_else(unavailable)?;
        let ticket = current_issuer.commit_journal.prepare(binding, effect)?;
        self.durable_commit_write(&records)?;
        Ok(ticket)
    }

    /// Actual outcome recording needs the same owned Ticket, not freshly granted data rights.
    pub(crate) fn confirm_data_commit(
        &self,
        consumer: &Record,
        issuer: &Record,
        ticket: &Ticket,
        fresh: &Binding,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let records = self.read()?;
        let current_consumer = records.get(&consumer.name).ok_or_else(unavailable)?;
        let current_issuer = records.get(&issuer.name).ok_or_else(unavailable)?;
        if !same_operation(current_consumer, consumer)
            || current_consumer.callback_suspended
            || !same_operation(current_issuer, issuer)
            || current_issuer.callback_suspended
            || !matches_binding(current_consumer, fresh.consumer())
            || !matches_binding(current_issuer, fresh.issuer())
        {
            return Err(unavailable());
        }
        current_issuer
            .commit_journal
            .confirm_prepared(ticket, fresh)
    }

    /// Actual outcome recording needs the same owned Ticket, not freshly granted data rights.
    pub(crate) fn observe_data_commit(
        &self,
        issuer: &Record,
        ticket: &Ticket,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let current = records
            .get_mut(&issuer.name)
            .filter(|record| record.remote_name == issuer.remote_name)
            .ok_or_else(unavailable)?;
        current.commit_journal.observed_commit(ticket)?;
        self.durable_commit_write(&records)
    }

    /// Fresh Main admission and native metadata publication precede this exact receipt CAS.
    pub(crate) fn acknowledge_data_commit(
        &self,
        consumer: &Record,
        issuer: &Record,
        ticket: &Ticket,
        fresh: &Binding,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let current_consumer = records.get(&consumer.name).ok_or_else(unavailable)?;
        let current_issuer = records.get(&issuer.name).ok_or_else(unavailable)?;
        if !same_operation(current_consumer, consumer)
            || current_consumer.callback_suspended
            || !same_operation(current_issuer, issuer)
            || current_issuer.callback_suspended
            || !matches_binding(current_consumer, fresh.consumer())
            || !matches_binding(current_issuer, fresh.issuer())
        {
            return Err(unavailable());
        }
        records
            .get_mut(&issuer.name)
            .ok_or_else(unavailable)?
            .commit_journal
            .acknowledge(ticket, fresh)?;
        self.durable_commit_write(&records)
    }

    /// Only the exact native driver's confirmed rollback may call this with its own Ticket.
    pub(crate) fn rollback_data_commit(
        &self,
        issuer: &Record,
        ticket: &Ticket,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let current = records
            .get_mut(&issuer.name)
            .filter(|record| record.remote_name == issuer.remote_name)
            .ok_or_else(unavailable)?;
        current.commit_journal.rolled_back(ticket, true)?;
        self.durable_commit_write(&records)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use meerschaum_connector::SyncOptions;
    use meerschaum_core::{Map, Pipe, Value};
    use meerschaum_server::{
        access::Access,
        executor::{
            environment::Profile,
            pipe_operation::{
                NodeWindow, PipeOperation, ReadAccess, Snapshot, SnapshotMember, WindowAxis,
                WindowCoverage, WindowRange,
            },
        },
    };
    use uuid::Uuid;

    fn fixture() -> (
        tempfile::TempDir,
        Catalog,
        Record,
        Binding,
        NativeDataEffect,
    ) {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(root.path());
        let pipe = Pipe::builder("sql:main", "durable-native")
            .instance("sql:main")
            .build()
            .unwrap();
        let registration = Value::Int(7);
        let window = NodeWindow::from_main_plan(
            &pipe,
            registration.clone(),
            WindowAxis::WholeRelation,
            vec![WindowRange {
                begin: None,
                end: None,
            }],
            WindowCoverage::WholeRelation,
            false,
        )
        .unwrap();
        let operation = PipeOperation {
            row_params: None,
            pipe_ref: pipe.pipe_ref(),
            registration: registration.clone(),
            verify: false,
            begin: None,
            end: None,
            parents: None,
            children: None,
            metadata: Some(
                Snapshot::new(BTreeMap::from([(
                    pipe.pipe_ref(),
                    SnapshotMember {
                        registration: registration.clone(),
                        parameters: pipe.raw_parameters().clone(),
                        writable: true,
                        node_window: Some(window),
                    },
                )]))
                .unwrap(),
            ),
        };
        let mut record = Record::new(
            "durable-native",
            Uuid::new_v4(),
            operation.sysargs().unwrap(),
            Map::new(),
        );
        record.profile = Profile::Api;
        record.operation = Some(operation.into());
        record.callback = Some(Uuid::new_v4());
        let callback = CallbackBinding {
            grant: record.callback.unwrap(),
            launch: Uuid::parse_str(record.remote_name.strip_prefix("mrsm-").unwrap()).unwrap(),
            context_revision: record.context_revision,
        };
        let admitted = ReadAccess {
            caller: Access::everything(),
            operation: record.operation.clone().unwrap(),
            binding: callback.clone(),
            issuer: callback,
            pending_nodes: Default::default(),
        };
        let window = admitted.write_window(&pipe).unwrap();
        let effect = NativeDataEffect::from_native(
            &pipe,
            registration,
            &window,
            &SyncOptions::default(),
            vec![2],
        )
        .unwrap();
        let binding = admitted.native_commit_binding(&effect).unwrap();
        assert!(catalog.reserve(record.clone()).unwrap());
        (root, catalog, record, binding, effect)
    }

    #[test]
    fn pending_native_commit_survives_new_catalog_and_blocks_delete_restart_and_replay() {
        let (root, catalog, record, binding, effect) = fixture();
        let ticket = catalog
            .prepare_data_commit(&record, &record, binding.clone(), &effect)
            .unwrap();
        catalog
            .confirm_data_commit(&record, &record, &ticket, &binding)
            .unwrap();
        let reopened = Catalog::new(root.path());
        let current = reopened.get(&record.name).unwrap().unwrap();
        assert!(
            current
                .commit_journal
                .has_pending_for(effect.logical_reference())
        );
        assert!(reopened.remove(&current).is_err());
        assert!(
            reopened
                .replace_callback(
                    &current,
                    current.callback.unwrap(),
                    Uuid::new_v4(),
                    current.operation.clone()
                )
                .is_err()
        );
        assert!(
            reopened
                .prepare_data_commit(&current, &current, binding.clone(), &effect)
                .is_err()
        );
        assert!(
            !current
                .metadata(None)
                .to_string()
                .contains("commit_journal")
        );
        reopened.rollback_data_commit(&current, &ticket).unwrap();
        assert!(
            !Catalog::new(root.path())
                .get(&record.name)
                .unwrap()
                .unwrap()
                .commit_journal
                .has_pending()
        );
        let next = reopened
            .prepare_data_commit(&record, &record, binding, &effect)
            .unwrap();
        assert_ne!(ticket.id(), next.id());
    }

    #[test]
    fn commit_truth_survives_source_suspension_and_never_accepts_stale_or_rollback_receipts() {
        let (root, catalog, record, binding, effect) = fixture();
        let ticket = catalog
            .prepare_data_commit(&record, &record, binding.clone(), &effect)
            .unwrap();
        catalog
            .update(&record, |current| current.callback_suspended = true)
            .unwrap();
        // A target already committed must remain recorded even after source authority dies.
        catalog.observe_data_commit(&record, &ticket).unwrap();
        assert!(
            catalog
                .confirm_data_commit(&record, &record, &ticket, &binding)
                .is_err()
        );
        assert!(
            catalog
                .acknowledge_data_commit(&record, &record, &ticket, &binding)
                .is_err()
        );
        assert!(catalog.rollback_data_commit(&record, &ticket).is_err());
        let persisted = Catalog::new(root.path())
            .get(&record.name)
            .unwrap()
            .unwrap();
        assert!(persisted.callback_suspended);
        assert!(persisted.commit_journal.has_pending());
        // Restoring source authority alone does not erase the commit. A fresh exact
        // Main receipt may acknowledge only this same current canonical binding.
        catalog
            .update(&record, |current| current.callback_suspended = false)
            .unwrap();
        let current = catalog.get(&record.name).unwrap().unwrap();
        catalog
            .acknowledge_data_commit(&current, &current, &ticket, &binding)
            .unwrap();
        assert!(
            !catalog
                .get(&record.name)
                .unwrap()
                .unwrap()
                .commit_journal
                .has_pending()
        );
        assert!(
            catalog
                .acknowledge_data_commit(&current, &current, &ticket, &binding)
                .is_err()
        );
    }

    #[test]
    fn busy_or_changed_catalog_cannot_prepare_or_adopt_a_native_commit() {
        let (_root, catalog, record, binding, effect) = fixture();
        let held = catalog.lock().unwrap();
        assert!(
            catalog
                .prepare_data_commit(&record, &record, binding.clone(), &effect)
                .is_err()
        );
        drop(held);
        assert!(
            !catalog
                .get(&record.name)
                .unwrap()
                .unwrap()
                .commit_journal
                .has_pending()
        );
        catalog
            .update(&record, |current| current.context_revision += 1)
            .unwrap();
        assert!(
            catalog
                .prepare_data_commit(&record, &record, binding, &effect)
                .is_err()
        );
        assert!(
            !catalog
                .get(&record.name)
                .unwrap()
                .unwrap()
                .commit_journal
                .has_pending()
        );
    }
}
