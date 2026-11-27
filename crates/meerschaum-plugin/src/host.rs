//! Host services available during plugin calls, and the dynamic loader for hosts.
pub use crate::callbacks::calls::{
    available, config, draining, exec_sql, exec_sql_with, mint_token, pipe, plugin_config,
    read_sql, read_sql_with, revoke_token, root_dir, sql_bytes, sql_date, sql_json, sql_timestamp,
    sql_timestamptz, transaction, write_plugin_config,
};
pub use crate::callbacks::{MintedToken, Transaction};

#[cfg(feature = "host")]
mod loader;
#[cfg(feature = "host")]
pub use loader::*;
