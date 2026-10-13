mod common;
use common::*;
use meerschaum_sql::SqlConnector;
use std::time::Duration;

async fn leases(ctx: Ctx) {
    let second = SqlConnector::connect("w5sql_second", ctx.conn.spec().clone())
        .await
        .unwrap();
    let pipe = meerschaum_core::Pipe::builder(&ctx.prefix, "lease")
        .build()
        .unwrap();
    let ttl = Duration::from_secs(2);
    let (a, b) = tokio::join!(
        ctx.conn.acquire_pipe_lease(&pipe, "a", ttl),
        second.acquire_pipe_lease(&pipe, "b", ttl)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a, b, "exactly one independent connection acquires");
    let owner = if a { "a" } else { "b" };
    let other = if a { "b" } else { "a" };
    assert!(!second.release_pipe_lease(&pipe, other).await.unwrap());
    assert!(second.acquire_pipe_lease(&pipe, owner, ttl).await.unwrap());
    assert!(
        !ctx.conn
            .acquire_pipe_lease(&pipe, other, ttl)
            .await
            .unwrap()
    );
    assert!(ctx.conn.release_pipe_lease(&pipe, owner).await.unwrap());
    assert!(
        second
            .acquire_pipe_lease(&pipe, other, Duration::from_millis(50))
            .await
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!second.release_pipe_lease(&pipe, other).await.unwrap());
    let (a, b) = tokio::join!(
        ctx.conn.acquire_pipe_lease(&pipe, "takeover-a", ttl),
        second.acquire_pipe_lease(&pipe, "takeover-b", ttl)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a, b, "exactly one connection takes over an expired lease");
    assert!(!second.release_pipe_lease(&pipe, other).await.unwrap());
    let replacement = if a { "takeover-a" } else { "takeover-b" };
    assert!(
        ctx.conn
            .release_pipe_lease(&pipe, replacement)
            .await
            .unwrap()
    );
}
flavor_tests!(lease_connections, leases);
