//! Real SQLite transaction ordering; the fake Main persists only its private journal.

use super::*;
use meerschaum_connector::{
    SyncOptions,
    mutation::{DataAxis, DataCoverage, DataSpan, DataWindow, MutationCheck},
};
use meerschaum_core::{Pipe, Value};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::Notify;

struct State {
    reader: SqlDriver,
    journal: PathBuf,
    window: DataWindow,
    allowed: AtomicBool,
    reject_prepare: AtomicBool,
    revoke_after_prepare: AtomicBool,
    lose_commit_observation: AtomicBool,
    hold_commit_observation: AtomicBool,
    commit_entered: Notify,
    release_commit: Notify,
    observed_rollbacks: AtomicUsize,
    guards: AtomicUsize,
}

impl State {
    fn phase(&self) -> String {
        std::fs::read_to_string(&self.journal).unwrap_or_else(|error| {
            assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
            "absent".into()
        })
    }
    fn persist(&self, phase: &str) -> std::result::Result<(), String> {
        // Same ordering required of Main: sync installed intent and containing directory
        // before returning to the real transaction's COMMIT call.
        let mut file = std::fs::File::create(&self.journal).map_err(|e| e.to_string())?;
        file.write_all(phase.as_bytes())
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        std::fs::File::open(self.journal.parent().unwrap())
            .and_then(|directory| directory.sync_all())
            .map_err(|e| e.to_string())
    }
    async fn count(&self) -> i64 {
        tokio::time::timeout(
            Duration::from_secs(3),
            self.reader.value("SELECT COUNT(*) FROM target"),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .as_i64()
        .unwrap()
    }
    fn permitted(&self) -> std::result::Result<(), String> {
        self.allowed
            .load(Ordering::SeqCst)
            .then_some(())
            .ok_or_else(|| "The original source permission was revoked.".into())
    }
}

struct Original(Arc<State>);
#[meerschaum_connector::async_trait]
impl MutationCheck for Original {
    async fn check(&self) -> std::result::Result<(), String> {
        self.0.permitted()
    }
    async fn write_window(&self) -> std::result::Result<Option<DataWindow>, String> {
        self.0.permitted()?;
        Ok(Some(self.0.window.clone()))
    }
    async fn prepare_data_commit(
        &self,
        effect: &NativeDataEffect,
    ) -> std::result::Result<Option<Arc<dyn PendingDataCommit>>, String> {
        self.0.permitted()?;
        if self.0.reject_prepare.load(Ordering::SeqCst)
            || matches!(self.0.phase().as_str(), "prepared" | "committed")
        {
            return Err("The original operation already has an unfinished native commit.".into());
        }
        assert_eq!(effect.affected(), &[1]);
        assert_eq!(effect.window(), &self.0.window);
        assert_eq!(
            self.0.count().await,
            0,
            "DML was visible before durable prepare"
        );
        self.0.persist("prepared")?;
        if self.0.revoke_after_prepare.load(Ordering::SeqCst) {
            self.0.allowed.store(false, Ordering::SeqCst);
        }
        Ok(Some(Arc::new(Pending(self.0.clone()))))
    }
    fn rollback_completed(&self, succeeded: bool) {
        assert!(
            succeeded,
            "Fixture unexpectedly failed its actual SQLite rollback"
        );
        self.0.observed_rollbacks.fetch_add(1, Ordering::SeqCst);
    }
}

struct Pending(Arc<State>);
#[meerschaum_connector::async_trait]
impl PendingDataCommit for Pending {
    async fn before_commit(&self, _: &NativeDataEffect) -> std::result::Result<(), String> {
        assert_eq!(self.0.phase(), "prepared");
        self.0.permitted()
    }
    async fn committed(&self, _: &NativeDataEffect) -> std::result::Result<(), String> {
        assert_eq!(
            self.0.count().await,
            1,
            "COMMIT observation preceded actual COMMIT"
        );
        assert_eq!(self.0.phase(), "prepared");
        self.0.commit_entered.notify_one();
        if self.0.hold_commit_observation.load(Ordering::SeqCst) {
            self.0.release_commit.notified().await;
        }
        if self.0.lose_commit_observation.load(Ordering::SeqCst) {
            return Err("Lost Main journal observation.".into());
        }
        self.0.persist("committed")
    }
    async fn acknowledge(&self, _: &NativeDataEffect) -> std::result::Result<(), String> {
        assert_eq!(self.0.phase(), "committed");
        self.0.permitted()?;
        self.0.persist("acknowledged")
    }
    async fn rolled_back(&self) -> std::result::Result<(), String> {
        assert_eq!(self.0.phase(), "prepared");
        assert_eq!(self.0.observed_rollbacks.load(Ordering::SeqCst), 1);
        assert_eq!(
            self.0.count().await,
            0,
            "Intent cleared before actual rollback"
        );
        self.0.persist("rolled_back")
    }
}

struct TargetGuard(Arc<State>);
#[meerschaum_connector::async_trait]
impl NativeCommitGuard for TargetGuard {
    async fn recheck(&self, transaction: &mut SqlTransaction) -> Result<()> {
        // Observe uncommitted target data on the original held transaction. The
        // independent Main reader in prepare sees none of this until actual COMMIT.
        let rows = transaction
            .read_frame("SELECT id, payload FROM target")
            .await?;
        if rows.height() != 1
            || rows.row(0)
                != vec![
                    meerschaum_kernel::Value::Int(7),
                    meerschaum_kernel::Value::Str("native".into()),
                ]
        {
            return Err(crate::SqlError::InvalidArgument(
                "The native locked target changed.".into(),
            ));
        }
        self.0.guards.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn driver(path: &Path) -> SqlDriver {
    let mut spec = ConnectionSpec::from_uri(
        &format!("sqlite:///{}", path.display()),
        Some("commit-boundary"),
        None,
    )
    .unwrap();
    spec.pool_size = 1;
    spec.max_overflow = 0;
    SqlDriver::connect(&spec).await.unwrap()
}

async fn fixture() -> (tempfile::TempDir, SqlDriver, Arc<State>, NativeDataEffect) {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("owned.db");
    let writer = driver(&database).await;
    writer
        .exec("CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT)")
        .await
        .unwrap();
    let reader = driver(&database).await;
    assert!(!Arc::ptr_eq(&writer.lineage, &reader.lineage));
    let pipe = Pipe::builder("sql:main", "commit-boundary")
        .instance("sql:main")
        .build()
        .unwrap();
    let window = DataWindow::new(
        pipe.pipe_ref(),
        "a".repeat(64),
        DataAxis::WholeRelation,
        DataCoverage::WholeRelation,
        vec![DataSpan {
            begin: None,
            end: None,
        }],
        None,
    )
    .unwrap();
    let effect = NativeDataEffect::from_native(
        &pipe,
        Value::Int(1),
        &window,
        &SyncOptions::default(),
        vec![1],
    )
    .unwrap();
    let state = Arc::new(State {
        reader,
        journal: root.path().join("main-intent"),
        window,
        allowed: AtomicBool::new(true),
        reject_prepare: AtomicBool::new(false),
        revoke_after_prepare: AtomicBool::new(false),
        lose_commit_observation: AtomicBool::new(false),
        hold_commit_observation: AtomicBool::new(false),
        commit_entered: Notify::new(),
        release_commit: Notify::new(),
        observed_rollbacks: AtomicUsize::new(0),
        guards: AtomicUsize::new(0),
    });
    (root, writer, state, effect)
}

async fn target(writer: &SqlDriver, state: &Arc<State>) -> SqlTransaction {
    let authority: Arc<dyn MutationCheck> = Arc::new(Original(state.clone()));
    let mut transaction = meerschaum_connector::mutation::scope(authority, writer.begin_write())
        .await
        .unwrap();
    assert_eq!(
        transaction
            .exec("INSERT INTO target VALUES(7,'native')")
            .await
            .unwrap(),
        1
    );
    transaction
}

#[tokio::test]
async fn durable_prepare_precedes_real_commit_and_ack_follows_actual_observation() {
    let (_root, writer, state, effect) = fixture().await;
    let transaction = target(&writer, &state).await;
    let receipt = transaction
        .commit_with_effect(effect, &TargetGuard(state.clone()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.guards.load(Ordering::SeqCst), 2);
    assert_eq!(state.phase(), "committed");
    assert_eq!(state.count().await, 1);
    assert_eq!(state.observed_rollbacks.load(Ordering::SeqCst), 0);
    receipt.acknowledge().await.unwrap();
    assert_eq!(state.phase(), "acknowledged");
}

#[tokio::test]
async fn refused_prepare_and_source_loss_after_prepare_roll_back_before_clearing_intent() {
    for after_prepare in [false, true] {
        let (_root, writer, state, effect) = fixture().await;
        let transaction = target(&writer, &state).await;
        state.reject_prepare.store(!after_prepare, Ordering::SeqCst);
        state
            .revoke_after_prepare
            .store(after_prepare, Ordering::SeqCst);
        assert!(
            transaction
                .commit_with_effect(effect, &TargetGuard(state.clone()))
                .await
                .is_err()
        );
        assert_eq!(state.count().await, 0);
        assert_eq!(state.observed_rollbacks.load(Ordering::SeqCst), 1);
        assert_eq!(
            state.phase(),
            if after_prepare {
                "rolled_back"
            } else {
                "absent"
            }
        );
    }
}

#[tokio::test]
async fn source_revoked_after_commit_keeps_truth_and_refuses_replay() {
    let (_root, writer, state, effect) = fixture().await;
    let transaction = target(&writer, &state).await;
    let receipt = transaction
        .commit_with_effect(effect, &TargetGuard(state.clone()))
        .await
        .unwrap()
        .unwrap();
    state.allowed.store(false, Ordering::SeqCst);
    assert!(
        receipt
            .acknowledge()
            .await
            .unwrap_err()
            .contains("already committed")
    );
    assert_eq!(state.phase(), "committed");
    assert_eq!(state.count().await, 1);
    assert_eq!(state.observed_rollbacks.load(Ordering::SeqCst), 0);
    // Restoring source authority does not remove an unacknowledged attempt.
    state.allowed.store(true, Ordering::SeqCst);
    let authority = Original(state.clone());
    assert!(
        authority
            .prepare_data_commit(
                &NativeDataEffect::from_native(
                    &Pipe::builder("sql:main", "commit-boundary")
                        .instance("sql:main")
                        .build()
                        .unwrap(),
                    Value::Int(1),
                    &state.window,
                    &SyncOptions::default(),
                    vec![1]
                )
                .unwrap()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lost_observation_or_cancellation_after_real_commit_retains_durable_unknown() {
    for canceled in [false, true] {
        let (_root, writer, state, effect) = fixture().await;
        let transaction = target(&writer, &state).await;
        state
            .hold_commit_observation
            .store(canceled, Ordering::SeqCst);
        state
            .lose_commit_observation
            .store(!canceled, Ordering::SeqCst);
        let own = state.clone();
        let task = tokio::spawn(async move {
            transaction
                .commit_with_effect(effect, &TargetGuard(own))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), state.commit_entered.notified())
            .await
            .unwrap();
        if canceled {
            task.abort();
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        } else {
            let result = task.await.unwrap();
            assert!(matches!(
                result,
                Err(crate::SqlError::NativeCommittedUnacknowledged)
            ));
        }
        assert_eq!(state.count().await, 1);
        assert_eq!(state.observed_rollbacks.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read_to_string(&state.journal).unwrap(), "prepared");
        let authority = Original(state.clone());
        let reconstructed = NativeDataEffect::from_native(
            &Pipe::builder("sql:main", "commit-boundary")
                .instance("sql:main")
                .build()
                .unwrap(),
            Value::Int(1),
            &state.window,
            &SyncOptions::default(),
            vec![1],
        )
        .unwrap();
        assert!(authority.prepare_data_commit(&reconstructed).await.is_err());
    }
}
