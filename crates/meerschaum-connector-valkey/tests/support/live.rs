//! Live tests use only an explicitly supplied disposable fixture, never an ambient server.

pub fn url() -> Option<String> {
    let Ok(url) = std::env::var("MRSM_VALKEY_TEST_URL") else {
        eprintln!("SKIPPED: set MRSM_VALKEY_TEST_URL to an owned disposable Valkey instance");
        return None;
    };
    assert!(
        !url.is_empty(),
        "The disposable Valkey URL must not be empty"
    );
    assert_eq!(std::env::var("MRSM_TEST_ISOLATED").as_deref(), Ok("1"));
    Some(url)
}
