//! `retry_connect()` for a SQL instance: one attempt is v4's `SELECT 1` (`test_queries`).

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::retry::{Attempt, AttemptCx, RetryProbe};

use super::SqlConnector;

impl SqlConnector {
    /// The connection timeout of this connector: `MRSM_CONNECT_TIMEOUT`, else its
    /// `connect_timeout` attribute (seconds), else 5 seconds.
    pub fn connect_timeout(&self) -> Duration {
        crate::driver::connect::connect_timeout(self.spec())
    }

    /// One `retry_connect()` attempt: the flavor's test query (`SELECT 1`). The error of a failed
    /// attempt is the driver's, described by the first-connection gate (`sql:main: cannot
    /// connect to localhost:5432 (connection refused). Is the database running?`).
    pub async fn connect_attempt(&self) -> Result<(), String> {
        match self.value_params(self.flavor.test_query(), &[]).await {
            Ok(Some(_)) => Ok(()),
            // v4 raised `Failed to connect.` when `exec()` gave nothing back.
            Ok(None) => Err("Failed to connect.".to_string()),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// The [`RetryProbe`] of a SQL connector.
pub struct SqlRetryProbe {
    connector: Arc<SqlConnector>,
}

impl SqlRetryProbe {
    /// A probe over `connector` (its pool is the one the connector keeps using afterwards).
    pub fn new(connector: Arc<SqlConnector>) -> SqlRetryProbe {
        SqlRetryProbe { connector }
    }
}

impl RetryProbe for SqlRetryProbe {
    async fn attempt(&mut self, cx: &mut AttemptCx<'_>) -> Attempt {
        match self.connector.connect_attempt().await {
            Ok(()) => Attempt::Connected,
            Err(message) => {
                cx.detail(&message);
                Attempt::Failed
            }
        }
    }
}
