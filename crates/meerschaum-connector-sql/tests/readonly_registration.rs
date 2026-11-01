//! Looking up registration metadata never initializes an empty instance.

mod common;

use meerschaum_connector::Connector;
use meerschaum_core::Pipe;

#[tokio::test]
async fn registration_reads_leave_an_empty_instance_empty() {
    let ctx = common::ctx("sqlite", "readonly_registration")
        .await
        .unwrap();
    let conn = &ctx.conn;
    let pipe = Pipe::builder("read_only", &ctx.prefix)
        .instance(&conn.connector_keys())
        .build()
        .unwrap();
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_none());
    assert!(conn.get_pipe_attributes(&pipe).await.unwrap().is_empty());
    assert!(!conn.table_exists("mrsm_pipes", None).await);

    assert!(conn.register_pipe(&pipe).await.success);
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_some());
    assert!(!conn.get_pipe_attributes(&pipe).await.unwrap().is_empty());
}
