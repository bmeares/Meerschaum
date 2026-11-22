//! What each principal sees: listings, hidden pipes answering like missing ones, and the bulk
//! read of the pipes' access behind a listing.

use super::*;

#[tokio::test]
async fn listings_show_each_principal_the_pipes_it_can_see() {
    let f = fixture().await;
    let expected: [(&str, &[(&str, &str)]); 7] = [
        (
            "admin",
            &[TEMPERATURE, SECRET_PIPE, PRESSURE, PUBLIC, LEGACY],
        ),
        ("vera", &[TEMPERATURE, PUBLIC, LEGACY]),
        // The owner of the private pipe sees it, and so does its team's manager.
        ("ed", &[TEMPERATURE, SECRET_PIPE, PUBLIC, LEGACY]),
        ("opal", &[TEMPERATURE, PUBLIC, LEGACY]),
        ("mia", &[TEMPERATURE, SECRET_PIPE, PUBLIC, LEGACY]),
        ("otto", &[PRESSURE, PUBLIC, LEGACY]),
        // Every permission is not every pipe: only administrators see everything.
        ("oscar", &[PRESSURE, PUBLIC, LEGACY]),
    ];
    for (user, keys) in expected {
        let auth = token(&f, user).await;
        let r = get(&f, &auth, "/pipes/keys").await;
        assert_eq!(listed(&r), names(keys), "{user}");
    }
    // The nested listings too.
    let otto = token(&f, "otto").await;
    let r = get(&f, &otto, "/pipes").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(r.json().get("plugin:data").is_none(), "{}", r.text());
    assert!(r.json().get("plugin:ops").is_some(), "{}", r.text());
    let r = get(&f, &otto, "/pipes/plugin:data").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    let r = get(&f, &otto, "/pipes/plugin:data/temperature").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // Anonymous callers see nothing.
    let r = send(&f.app, req("GET", "/pipes/keys", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_hidden_pipe_answers_like_a_missing_one_on_every_route() {
    let f = fixture().await;
    // `oscar` holds every permission but is not in Data.
    let oscar = token(&f, "oscar").await;
    for (method, suffix, body) in [
        ("GET", "", None),
        ("GET", "/attributes", None),
        ("GET", "/id", None),
        ("GET", "/exists", None),
        ("GET", "/data", None),
        ("GET", "/docs", None),
        ("GET", "/csv", None),
        ("GET", "/rowcount", None),
        ("GET", "/sync_time", None),
        ("GET", "/columns/types", None),
        ("GET", "/columns/indices", None),
        ("GET", "/size", None),
        ("GET", "/access", None),
        ("PATCH", "/edit", Some(json!({"tags": ["x"]}))),
        ("POST", "/data", Some(json!([{"ts": "2026-01-01"}]))),
        ("DELETE", "/clear", None),
        ("DELETE", "/drop", None),
        ("DELETE", "/delete", None),
        ("POST", "/lock", Some(json!({}))),
        ("POST", "/unlock", Some(json!({}))),
        ("PATCH", "/access", Some(json!({"visibility": "everyone"}))),
        ("POST", "/analyze", None),
    ] {
        let r = call(&f, method, &oscar, &url(TEMPERATURE, suffix), body).await;
        assert_eq!(
            r.status,
            StatusCode::NOT_FOUND,
            "{method} {suffix}: {}",
            r.text()
        );
        assert!(
            !r.text().contains("Data"),
            "{method} {suffix}: {}",
            r.text()
        );
    }
    // Private: a member who is neither the owner nor a manager.
    let vera = token(&f, "vera").await;
    let r = get(&f, &vera, &url(SECRET_PIPE, "/attributes")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // Nothing changed.
    assert_eq!(stored_access(&f, TEMPERATURE)["team"], "Data");
    assert!(
        f.instance
            .get_pipe_id(&pipe(TEMPERATURE))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn a_listing_reads_the_pipes_in_bulk_and_reuses_them() {
    let f = fixture().await;
    for i in 0..120 {
        let team = if i % 2 == 0 { "Data" } else { "Ops" };
        register(
            &f.instance,
            ("plugin:bulk", &format!("m{i}")),
            json!({"access": {"team": team}}),
        )
        .await;
    }
    let ed = token(&f, "ed").await;
    let (listings, reads) = (
        f.instance.pipes_listings.load(Ordering::SeqCst),
        f.instance.attribute_reads.load(Ordering::SeqCst),
    );
    for _ in 0..5 {
        let r = get(&f, &ed, "/pipes/keys").await;
        assert_eq!(
            listed(&r)
                .iter()
                .filter(|k| k.starts_with("plugin:bulk"))
                .count(),
            60
        );
    }
    // Each listing reads the keys once, and the access index is read once for all of them
    // (twice when the requests outlast `INDEX_TTL`): never once per pipe.
    let listed_now = f.instance.pipes_listings.load(Ordering::SeqCst) - listings;
    assert!(listed_now <= 7, "{listed_now} listings for 5 requests");
    assert_eq!(f.instance.attribute_reads.load(Ordering::SeqCst), reads);
    // One pipe's routes read that pipe, not every pipe.
    let before = f.instance.pipes_listings.load(Ordering::SeqCst);
    for i in (0..20).step_by(2) {
        let r = get(
            &f,
            &ed,
            &url(("plugin:bulk", &format!("m{i}")), "/attributes"),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    }
    assert!(f.instance.pipes_listings.load(Ordering::SeqCst) - before <= 2);
}
