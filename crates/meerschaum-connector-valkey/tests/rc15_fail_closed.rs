//! S1 (5.0.0rc15): a user read that fails is an error, never "no such user" or "no attributes"
//! (which grants every scope, v4's rule for a user without a `scopes` attribute), so the API
//! server refuses the request instead of guessing. The read errors are real Valkey errors: a key
//! holding the wrong type answers `WRONGTYPE`, in memory and on a live server
//! (`MRSM_VALKEY_TEST_URL`, default `redis://localhost:6399/12`).

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_valkey::{FredBackend, MemoryBackend, ValkeyBackend, ValkeyConnector};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// Make `key` unreadable as a string: it holds a set (`WRONGTYPE` on `GET`).
async fn break_key(backend: &dyn ValkeyBackend, key: &str) {
    backend.del(&[key.to_string()]).await.unwrap();
    backend.sadd(key, "not a string").await.unwrap();
}

async fn user_reads_fail_closed(backend: Arc<dyn ValkeyBackend>) {
    let conn = ValkeyConnector::with_backend("s1", Map::new(), backend.clone());
    let name = unique("ron");
    let mut user = User::new(&name, Some("password"));
    let mut attributes = Map::new();
    attributes.insert(
        "scopes".into(),
        Value::List(vec![Value::Str("pipes:read".into())]),
    );
    user.attributes = Some(attributes);
    let res = conn.valkey_register_user(&mut user).await;
    assert!(res.success, "{}", res.message);
    let ron = User::new(&name, None);
    let id = conn.valkey_get_user_id(&ron).await.unwrap().unwrap();

    // Reads that succeed: the user's own scopes; a user who does not exist has no record.
    assert_eq!(
        User::scopes_from_read(conn.get_user_attributes(&ron).await).unwrap(),
        ["pipes:read"]
    );
    let nobody = User::new(&unique("nobody"), None);
    assert_eq!(conn.get_user_id(&nobody).await.unwrap(), None);
    assert_eq!(conn.get_user_attributes(&nobody).await.unwrap(), None);
    assert_eq!(conn.get_user_type(&nobody).await.unwrap(), None);
    assert_eq!(conn.get_user_password_hash(&nobody).await.unwrap(), None);

    // Attributes that cannot be read are an error, so no scope is granted.
    let attributes_key = format!("mrsm_user:user_id:{id}:attributes");
    break_key(&*backend, &attributes_key).await;
    let read = conn.get_user_attributes(&ron).await;
    assert!(
        read.as_ref()
            .is_err_and(|e| e.to_string().contains("WRONGTYPE")),
        "{read:?}"
    );
    assert!(User::scopes_from_read(read).is_err());
    // The same for a user whose ID is already known (the principal of a JWT).
    let mut known = User::new(&name, None);
    known.user_id = Some(Value::Str(id.clone()));
    assert!(conn.get_user_attributes(&known).await.is_err());

    // The type and the password hash.
    let type_key = format!("mrsm_user:user_id:{id}:type");
    let hash_key = format!("mrsm_user:user_id:{id}:password_hash");
    let hash = conn.get_user_password_hash(&ron).await.unwrap().unwrap();
    break_key(&*backend, &type_key).await;
    break_key(&*backend, &hash_key).await;
    assert!(conn.get_user_type(&ron).await.is_err());
    assert!(conn.get_user_password_hash(&ron).await.is_err());

    // The ID itself: an error, not "no such user", and nothing is read under the ID `None`.
    let id_key = format!("mrsm_user:username:{name}:user_id");
    break_key(&*backend, &id_key).await;
    assert!(conn.get_user_id(&ron).await.is_err());
    assert!(conn.get_user_attributes(&ron).await.is_err());
    assert!(conn.get_user_type(&ron).await.is_err());
    // A user whose ID cannot be read is not deleted (nothing is removed on a guess).
    let res = conn.delete_user(&ron).await;
    assert!(
        !res.success && res.message.contains("WRONGTYPE"),
        "{}",
        res.message
    );

    // Restore the keys and delete the user.
    backend
        .del(&[
            id_key.clone(),
            attributes_key.clone(),
            type_key.clone(),
            hash_key.clone(),
        ])
        .await
        .unwrap();
    backend.set(&id_key, &id).await.unwrap();
    backend.set(&hash_key, &hash).await.unwrap();
    backend.set(&attributes_key, "{}").await.unwrap();
    assert_eq!(
        conn.get_user_attributes(&ron).await.unwrap(),
        Some(Map::new())
    );
    let mut owned = User::new(&name, None);
    owned.user_id = Some(Value::Str(id));
    let res = conn.valkey_delete_user(&owned).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(conn.get_user_id(&ron).await.unwrap(), None);
}

#[tokio::test]
async fn user_reads_that_fail_are_errors_in_memory() {
    user_reads_fail_closed(Arc::new(MemoryBackend::new())).await;
}

#[tokio::test]
async fn user_reads_that_fail_are_errors_live() {
    let url = std::env::var("MRSM_VALKEY_TEST_URL")
        .unwrap_or_else(|_| "redis://localhost:6399/12".to_string());
    let backend = match tokio::time::timeout(
        Duration::from_secs(3),
        FredBackend::connect(&url, Some(Duration::from_secs(3))),
    )
    .await
    {
        Ok(Ok(b)) => Arc::new(b),
        _ => {
            eprintln!("SKIPPED: no Valkey server reachable at {url}");
            return;
        }
    };
    user_reads_fail_closed(backend).await;
}
