//! A deleted user's tokens never outlive them: `delete_user` deletes them first, and on SQLite
//! (which hands a new row the highest deleted rowid again) a new user's ID is above every token
//! owner's, so a key left by a user deleted before this rule existed is never handed to them.
//!
//! SQLite always; the server flavors when their URIs are set.

mod common;
use chrono::Utc;
use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::users::{Expiration, Token, User};

fn unique(tag: &str) -> String {
    format!(
        "rs_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

async fn token_for(c: &Ctx, user: &User) -> Token {
    let mut token = Token::new(
        Some(&unique("key")),
        Expiration::Default,
        Some(vec!["pipes:read".into()]),
        366,
        Utc::now(),
    );
    token.user = Some(user.clone());
    ok(InstanceConnector::register_token(&c.conn, &mut token).await);
    token
}

flavor_tests!(
    test_deleting_a_user_deletes_their_tokens,
    |c: Ctx| async move {
        let mut keeper = User::new(&unique("keeper"), Some("test1234"));
        ok(c.conn.register_user(&mut keeper).await);
        let kept = token_for(&c, &keeper).await;
        let mut leaver = User::new(&unique("leaver"), Some("test1234"));
        ok(c.conn.register_user(&mut leaver).await);
        let gone = token_for(&c, &leaver).await;

        ok(c.conn.delete_user(&leaver).await);
        let model = InstanceConnector::get_token_model(&c.conn, &gone.id.unwrap())
            .await
            .unwrap();
        assert!(model.is_none(), "a deleted user's token survived");
        // Another user's token is untouched.
        assert!(
            InstanceConnector::get_token_model(&c.conn, &kept.id.unwrap())
                .await
                .unwrap()
                .is_some()
        );
        ok(InstanceConnector::delete_token(&c.conn, &kept).await);
        ok(c.conn.delete_user(&keeper).await);
    }
);

flavor_tests!(
    test_a_new_user_never_inherits_an_orphaned_token,
    |c: Ctx| async move {
        let mut alice = User::new(&unique("alice"), Some("test1234"));
        ok(c.conn.register_user(&mut alice).await);
        let alice_id = c.conn.get_user_id(&alice).await.unwrap().unwrap();
        let key = token_for(&c, &alice).await;
        // A user deleted before deletions removed tokens: only the row goes.
        let users = c.conn.instance_table_name("mrsm_users");
        let removed = c
            .conn
            .exec(&format!(
                "DELETE FROM {users} WHERE user_id = {}",
                alice_id.as_i64().unwrap()
            ))
            .await;
        assert!(removed.is_some());

        let mut bob = User::new(&unique("bob"), Some("test1234"));
        ok(c.conn.register_user(&mut bob).await);
        let bob_id = c.conn.get_user_id(&bob).await.unwrap().unwrap();
        assert_ne!(
            bob_id.as_i64(),
            alice_id.as_i64(),
            "the new user took the ID an orphaned token names"
        );
        let owner_id = InstanceConnector::get_token_model(&c.conn, &key.id.unwrap())
            .await
            .unwrap()
            .unwrap()
            .user_id
            .unwrap();
        assert!(
            InstanceConnector::get_username(&c.conn, &owner_id)
                .await
                .unwrap()
                .is_none(),
            "the orphaned token resolves to a user"
        );
        ok(InstanceConnector::delete_token(&c.conn, &key).await);
        ok(c.conn.delete_user(&bob).await);
    }
);
