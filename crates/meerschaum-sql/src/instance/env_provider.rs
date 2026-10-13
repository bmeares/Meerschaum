//! Default [`InstanceProvider`] for cross-instance `{{ Pipe(..., instance='sql:other') }}`
//! references: builds `sql:<label>` connectors from the `MRSM_SQL_<LABEL>` environment URI (the
//! same variable v4 reads into the connectors config), lazily and once per label.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use meerschaum_connector::{InstanceConnector, InstanceProvider};

use crate::flavor::Flavor;
use crate::uri::ConnectionSpec;

use super::{InstanceSettings, SqlConnector};

/// See the module docs.
#[derive(Default)]
pub struct EnvSqlProvider {
    cache: Mutex<HashMap<String, Arc<SqlConnector>>>,
}

impl EnvSqlProvider {
    /// The process-wide provider.
    pub fn shared() -> Arc<EnvSqlProvider> {
        static SHARED: OnceLock<Arc<EnvSqlProvider>> = OnceLock::new();
        SHARED.get_or_init(Default::default).clone()
    }
}

impl InstanceProvider for EnvSqlProvider {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        let label = keys.strip_prefix("sql:")?;
        let uri = std::env::var(format!("MRSM_SQL_{}", label.to_uppercase())).ok()?;
        let cache_key = format!("{label}\0{uri}");
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = cache.get(&cache_key) {
            return Some(c.clone() as Arc<dyn InstanceConnector>);
        }
        let spec = ConnectionSpec::from_uri(&uri, Some(label), None::<Flavor>).ok()?;
        let settings = InstanceSettings::defaults(spec.flavor);
        let conn = Arc::new(SqlConnector::connect_lazy(label, spec, settings).ok()?);
        cache.insert(cache_key, conn.clone());
        Some(conn as Arc<dyn InstanceConnector>)
    }
}
