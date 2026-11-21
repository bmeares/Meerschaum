#[path = "../common/mod.rs"]
mod common;

mod api_contract;
mod data_limit;
mod execution_alias;
mod exists_checked;
mod f2_api_client;
mod raw_parameters;
mod relogin;
mod request_authentication;
mod retry_connect;
mod scoped_transport;
mod source_chunks;
mod strict_rowcount;
mod w18_fail_fast;
mod warnings;
#[cfg(feature = "client")]
mod wss;
