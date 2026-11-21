#[path = "../common/mod.rs"]
mod common;

mod api_contract;
mod client_estimates_and_protection;
mod connect_fail_fast;
mod data_limit;
mod execution_alias;
mod exists_checked;
mod raw_parameters;
mod relogin;
mod request_authentication;
mod retry_connect;
mod scoped_transport;
mod source_chunks;
mod strict_rowcount;
mod warnings;
#[cfg(feature = "client")]
mod wss;
