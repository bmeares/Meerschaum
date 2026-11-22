//! Tokens: registering, showing and editing them, and the scopes a token may carry.

use super::*;

#[test]
fn tokens_register_show_and_edit() {
    let home = Home::new();
    let none = home.run(&["register", "tokens"]);
    assert_eq!(none.code, 1, "{}", none.text);
    assert!(
        none.text
            .contains("No users are registered to 'sql:local'."),
        "{}",
        none.text
    );

    // v4 asks for the password twice and the email (v5: the first user of an instance is first
    // offered the admin type; `n` keeps v4's regular user).
    let user = home.ask(&["register", "users", "bob"], "n\npassword1\npassword1\n\n");
    assert_eq!(user.code, 0, "{}", user.text);

    let registered = home.ask(
        &["register", "tokens", "--name", "mytok", "--ttl-days", "10"],
        "1\n",
    );
    assert_eq!(registered.code, 0, "{}", registered.text);
    assert!(
        registered.text.contains("Registered token 'mytok'."),
        "{}",
        registered.text
    );
    assert!(
        registered.text.contains("Client Secret:"),
        "{}",
        registered.text
    );
    assert!(
        registered.text.contains("API Key: mrsm-key:"),
        "{}",
        registered.text
    );
    assert!(
        registered
            .text
            .contains("Successfully registered token 'mytok'."),
        "{}",
        registered.text
    );

    let expiring = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "other",
            "--end",
            "2030-01-01",
        ],
        "1\n",
    );
    assert_eq!(expiring.code, 0, "{}", expiring.text);
    assert!(
        expiring
            .text
            .contains("Expiration: 2030-01-01T00:00:00+00:00")
    );

    let table = home.run(&["show", "tokens"]);
    assert_eq!(table.code, 0, "{}", table.text);
    assert!(
        table
            .text
            .contains("Registered Tokens on instance 'sql:local'"),
        "{}",
        table.text
    );
    assert!(table.text.contains("mytok"), "{}", table.text);
    assert!(table.text.contains("bob"), "{}", table.text);
    let raw = home.run(&["show", "tokens", "--nopretty"]);
    let first = raw
        .text
        .lines()
        .find(|l| l.contains("\"label\": \"mytok\""));
    assert!(first.is_some(), "{}", raw.text);

    // `edit tokens` prompts for every field (label, expiration, scopes, invalidate?).
    // Without a terminal the typed text is appended to each prefilled value (like
    // `prompt_toolkit`), so the empty answers keep the expiration and the scopes.
    let edited = home.ask(&["edit", "tokens", "mytok"], "renamed\n\n\nn\n");
    assert_eq!(edited.code, 0, "{}", edited.text);
    assert!(
        edited.text.contains("Successfully edited 1 token."),
        "{}",
        edited.text
    );
    let after = home.run(&["show", "tokens", "--nopretty"]);
    assert!(
        after.text.contains("\"label\": \"mytokrenamed\""),
        "{}",
        after.text
    );
    assert!(
        !after.text.contains("\"label\": \"mytok\""),
        "{}",
        after.text
    );

    let missing = home.run(&["edit", "tokens"]);
    assert_eq!(missing.code, 1);
    assert!(
        missing
            .text
            .contains("Provide token labels or IDs for the tokens to edit")
    );
}

/// The output with its line wrapping undone.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The `scopes` of the token with `label` in `show tokens --nopretty`.
fn token_scopes(home: &Home, label: &str) -> String {
    let raw = home.run(&["show", "tokens", "--nopretty"]);
    let line = raw
        .text
        .lines()
        .find(|l| l.contains(&format!("\"label\": \"{label}\"")))
        .unwrap_or_else(|| panic!("no token {label}: {}", raw.text));
    let start = line.find("\"scopes\": [").expect("scopes") + "\"scopes\": ".len();
    let end = start + line[start..].find(']').expect("end of scopes") + 1;
    line[start..end].to_string()
}

#[test]
fn a_star_scope_is_stored_literally_for_tokens_and_users() {
    let home = Home::new();
    // The owner holds `*`: a token carries at most its owner's scopes. (v5: the first user is
    // first offered the admin type; `n` keeps a regular user.)
    let user = home.ask(
        &["register", "users", "bob", "--scopes", "*"],
        "n\npassword1\npassword1\n\n",
    );
    assert_eq!(user.code, 0, "{}", user.text);

    // `register tokens --scopes '*'` keeps the wildcard; custom (plugin) scope names are
    // accepted as given.
    let star = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "everything",
            "--scopes",
            "*",
        ],
        "1\n",
    );
    assert_eq!(star.code, 0, "{}", star.text);
    let custom = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "custom",
            "--scopes",
            "myplugin:read",
            "myplugin:write",
        ],
        "1\n",
    );
    assert_eq!(custom.code, 0, "{}", custom.text);
    assert_eq!(token_scopes(&home, "everything"), r#"["*"]"#);
    assert_eq!(
        token_scopes(&home, "custom"),
        r#"["myplugin:read", "myplugin:write"]"#
    );

    // `edit tokens` prefills the stored scopes: keeping them keeps `*` (v4 expanded it to the
    // built-in list), and typing `*` over a custom list stores `*`. The prompt names the
    // semantics.
    let kept = home.ask(&["edit", "tokens", "everything"], "\n\n\nn\n");
    assert_eq!(kept.code, 0, "{}", kept.text);
    assert!(
        one_line(&kept.text).contains("`*` grants all scopes, including custom and future scopes"),
        "{}",
        kept.text
    );
    assert_eq!(token_scopes(&home, "everything"), r#"["*"]"#);
    let kept = home.ask(&["edit", "tokens", "custom"], "\n\n\nn\n");
    assert_eq!(kept.code, 0, "{}", kept.text);
    assert_eq!(
        token_scopes(&home, "custom"),
        r#"["myplugin:read", "myplugin:write"]"#
    );

    // `register users --scopes '*'` and `edit users` keep the wildcard in the user's attributes.
    let scoped = home.ask(
        &["register", "users", "carol", "--scopes", "*"],
        "password1\npassword1\n\n",
    );
    assert_eq!(scoped.code, 0, "{}", scoped.text);
    // `edit users` asks for a role; "Custom permissions" (4) prefills the stored
    // list, so an empty answer keeps `*`. Password, type and email stay (`n`).
    let edited = home.ask(&["edit", "users", "bob"], "n\nn\ny\n4\n\nn\n");
    assert_eq!(edited.code, 0, "{}", edited.text);
    assert!(
        one_line(&edited.text)
            .contains("`*` grants every permission, also plugins' and future ones"),
        "{}",
        edited.text
    );
    for user in ["bob", "carol"] {
        let row = home.run(&[
            "sql",
            "local",
            &format!("SELECT attributes FROM mrsm_users WHERE username = '{user}'"),
            "--nopretty",
        ]);
        assert!(
            row.text.replace('\\', "").contains(r#""scopes": ["*"]"#),
            "{user}: {}",
            row.text
        );
    }
}

#[test]
fn a_token_carries_at_most_its_owners_scopes() {
    let home = Home::new();
    // `ron` holds three scopes (one custom); `ada` is an administrator: `edit users` makes her one.
    let ron = home.ask(
        &[
            "register",
            "users",
            "ron",
            "--scopes",
            "pipes:read",
            "jobs:read",
            "myplugin:read",
        ],
        // The first user is first offered the admin type (`n`: a regular user).
        "n\npassword1\npassword1\n\n",
    );
    assert_eq!(ron.code, 0, "{}", ron.text);
    let ada = home.ask(&["register", "users", "ada"], "password1\npassword1\n\n");
    assert_eq!(ada.code, 0, "{}", ada.text);
    let admin = home.ask(&["edit", "users", "ada"], "n\ny\ny\nn\n\n");
    assert_eq!(admin.code, 0, "{}", admin.text);

    // The owner is the user chosen at the prompt: `1` is `ada`, `2` is `ron` (sorted).
    // A scope the owner holds, or none at all (the owner's own), is fine.
    let ok = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "own",
            "--scopes",
            "pipes:read",
            "myplugin:read",
        ],
        "2\n",
    );
    assert_eq!(ok.code, 0, "{}", ok.text);
    let default = home.ask(&["register", "tokens", "--name", "default"], "2\n");
    assert_eq!(default.code, 0, "{}", default.text);
    assert_eq!(
        token_scopes(&home, "default"),
        r#"["pipes:read", "jobs:read", "myplugin:read"]"#
    );

    // Anything beyond them is refused, naming every disallowed scope, and nothing is registered.
    for (scopes, named) in [
        (vec!["pipes:write"], vec!["pipes:write"]),
        (vec!["*"], vec!["*"]),
        (
            vec!["jobs:read", "other:read", "pipes:drop"],
            vec!["other:read", "pipes:drop"],
        ),
    ] {
        let mut args = vec!["register", "tokens", "--name", "denied", "--scopes"];
        args.extend(scopes.iter().copied());
        let refused = home.ask(&args, "2\n");
        assert_eq!(refused.code, 1, "{}", refused.text);
        let text = one_line(&refused.text);
        assert!(
            text.contains("Cannot grant scopes that user 'ron' does not hold"),
            "{text}"
        );
        for scope in named {
            assert!(text.contains(&format!("'{scope}'")), "{text}");
        }
    }
    assert!(
        !home
            .run(&["show", "tokens", "--nopretty"])
            .text
            .contains("\"denied\""),
    );

    // `edit tokens` applies the cap to the token's owner: keeping scopes works, adding one fails.
    let kept = home.ask(&["edit", "tokens", "own"], "\n\n\nn\n");
    assert_eq!(kept.code, 0, "{}", kept.text);
    let widened = home.ask(&["edit", "tokens", "own"], "\n\n pipes:write\nn\n");
    assert_eq!(widened.code, 1, "{}", widened.text);
    assert!(
        one_line(&widened.text).contains("'pipes:write'"),
        "{}",
        widened.text
    );
    assert_eq!(
        token_scopes(&home, "own"),
        r#"["pipes:read", "myplugin:read"]"#
    );

    // An administrator, and the holder of `*` (see the test above), may grant anything.
    for (name, scopes) in [
        ("grand-star", vec!["*"]),
        ("grand-custom", vec!["brand:new", "pipes:write"]),
    ] {
        let mut args = vec!["register", "tokens", "--name", name, "--scopes"];
        args.extend(scopes.iter().copied());
        let granted = home.ask(&args, "1\n");
        assert_eq!(granted.code, 0, "{scopes:?}: {}", granted.text);
    }
    assert_eq!(token_scopes(&home, "grand-star"), r#"["*"]"#);
}

#[test]
fn a_token_cap_that_cannot_be_read_refuses_instead_of_granting_every_scope() {
    let home = Home::new();
    let ron = home.ask(
        &["register", "users", "ron", "--scopes", "pipes:read"],
        // The first user is first offered the admin type (`n`: a regular user).
        "n\npassword1\npassword1\n\n",
    );
    assert_eq!(ron.code, 0, "{}", ron.text);
    let own = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "own",
            "--scopes",
            "pipes:read",
        ],
        "1\n",
    );
    assert_eq!(own.code, 0, "{}", own.text);
    // Ron's attributes can no longer be read: the read fails, which must not count as "no
    // `scopes` attribute" (every scope).
    let dropped = home.sql("ALTER TABLE mrsm_users DROP COLUMN attributes");
    assert_eq!(dropped.code, 0, "{}", dropped.text);
    let refused = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "wide",
            "--scopes",
            "pipes:write",
        ],
        "1\n",
    );
    assert_eq!(refused.code, 1, "{}", refused.text);
    assert!(
        one_line(&refused.text).contains("Could not read the scopes of user 'ron'"),
        "{}",
        refused.text
    );
    // `edit tokens` refuses too, and the token keeps its scopes.
    let widened = home.ask(&["edit", "tokens", "own"], "\n\n pipes:write\nn\n");
    assert_eq!(widened.code, 1, "{}", widened.text);
    assert!(
        one_line(&widened.text).contains("Could not read the scopes of user 'ron'"),
        "{}",
        widened.text
    );
    assert_eq!(token_scopes(&home, "own"), r#"["pipes:read"]"#);
    assert!(
        !home
            .run(&["show", "tokens", "--nopretty"])
            .text
            .contains("\"wide\""),
    );
}
