//! Tests for TLS settings, certificate loading and reloading, and the HTTPS listener.

use super::*;
use std::path::Path;

fn settings(patch: &str, key: Option<&str>, cert: Option<&str>) -> (TlsSettings, Vec<String>) {
    let Value::Map(map) = Value::from_json_str(patch).unwrap() else {
        panic!("a map")
    };
    let mut notices = Vec::new();
    (TlsSettings::resolve(key, cert, &map, &mut notices), notices)
}

#[test]
fn flags_win_over_server_keys_and_uvicorn_keys_are_deprecated() {
    let (plain, notices) = settings("{}", None, None);
    assert!(!plain.is_requested() && plain.files().unwrap().is_none() && notices.is_empty());

    let (flags, _) = settings(
        r#"{"api": {"server": {"ssl_keyfile": "/srv/k.pem", "ssl_certfile": "/srv/c.pem"}}}"#,
        Some("/flag/k.pem"),
        None,
    );
    let files = flags.files().unwrap().unwrap();
    assert_eq!(files.keyfile.path, PathBuf::from("/flag/k.pem"));
    assert_eq!(files.keyfile.source, "`--keyfile`");
    assert_eq!(files.certfile.path, PathBuf::from("/srv/c.pem"));
    assert_eq!(files.certfile.source, "`api:server:ssl_certfile`");

    let (uvicorn, notices) = settings(
        r#"{"api": {"uvicorn": {"ssl_keyfile": "/u/k.pem", "ssl_certfile": "/u/c.pem"}}}"#,
        None,
        None,
    );
    let files = uvicorn.files().unwrap().unwrap();
    assert_eq!(files.certfile.source, "`api:uvicorn:ssl_certfile`");
    assert_eq!(notices.len(), 2, "{notices:?}");
    assert!(notices[0].contains("'api:uvicorn:ssl_keyfile' is deprecated"));

    // `api:server` wins over `api:uvicorn`, even to turn it off.
    let (off, notices) = settings(
        r#"{"api": {"server": {"ssl_keyfile": null, "ssl_certfile": ""},
                        "uvicorn": {"ssl_keyfile": "/u/k.pem", "ssl_certfile": "/u/c.pem"}}}"#,
        None,
        None,
    );
    assert!(!off.is_requested() && notices.is_empty());

    // `--keyfile ''` turns off a configured key (and so HTTPS, once both are empty).
    let (empty, _) = settings(
        r#"{"api": {"server": {"ssl_keyfile": "/srv/k.pem", "ssl_certfile": "/srv/c.pem"}}}"#,
        Some(""),
        Some(" "),
    );
    assert!(!empty.is_requested());

    // Relative paths are absolute from the current directory.
    let (relative, _) = settings("{}", Some("k.pem"), Some("c.pem"));
    let files = relative.files().unwrap().unwrap();
    assert!(files.keyfile.path.is_absolute());
    assert!(files.keyfile.path.ends_with("k.pem"));
}

#[test]
fn half_a_pair_is_refused_with_the_way_out() {
    let (key_only, _) = settings("{}", Some("/k.pem"), None);
    let text = key_only.files().unwrap_err().to_string();
    assert!(
        text.contains("only the key is set (/k.pem (`--keyfile`))")
            && text.contains("`--certfile`")
            && text.contains("or neither to serve plain HTTP"),
        "{text}"
    );
    let (cert_only, _) = settings(
        r#"{"api": {"server": {"ssl_certfile": "/c.pem"}}}"#,
        None,
        None,
    );
    let error = cert_only.files().unwrap_err();
    assert!(matches!(error, TlsError::Incomplete(_)));
    let text = error.to_string();
    assert!(
        text.contains("only the certificate is set (/c.pem (`api:server:ssl_certfile`))")
            && text.contains("`--keyfile`, or `api:server:ssl_keyfile`"),
        "{text}"
    );
}

fn write(dir: &Path, name: &str, contents: impl AsRef<[u8]>) -> TlsPath {
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    TlsPath {
        path,
        source: format!("`{name}`"),
    }
}

fn pair(dir: &Path, key: impl AsRef<[u8]>, cert: impl AsRef<[u8]>) -> TlsFiles {
    TlsFiles {
        keyfile: write(dir, "key.pem", key),
        certfile: write(dir, "cert.pem", cert),
    }
}

fn self_signed(names: &[&str]) -> (String, String) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
            .unwrap();
    (signing_key.serialize_pem(), cert.pem())
}

fn load_error(files: TlsFiles) -> String {
    ServerCertificate::load(files).unwrap_err().to_string()
}

#[test]
fn pem_problems_are_named() {
    let dir = tempfile::tempdir().unwrap();
    let (key, cert) = self_signed(&["localhost"]);

    let missing = TlsFiles {
        keyfile: TlsPath {
            path: dir.path().join("nope.pem"),
            source: "`--keyfile`".into(),
        },
        certfile: write(dir.path(), "cert.pem", &cert),
    };
    let text = load_error(missing);
    assert!(
        text.starts_with("Cannot read the TLS private key") && text.contains("does not exist"),
        "{text}"
    );

    let text = load_error(pair(dir.path(), "", &cert));
    assert!(text.contains("is empty"), "{text}");

    let text = load_error(pair(dir.path(), &cert, &key));
    assert!(
        text.contains("holds a certificate and no private key"),
        "{text}"
    );
    assert!(text.contains("swapped"), "{text}");

    let text = load_error(pair(dir.path(), &key, &key));
    assert!(
        text.contains("holds a private key and no certificate"),
        "{text}"
    );

    let der = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .unwrap()
        .cert
        .der()
        .to_vec();
    let text = load_error(pair(dir.path(), &key, der));
    assert!(
        text.contains("binary (DER), not PEM") && text.contains("openssl x509"),
        "{text}"
    );

    let encrypted =
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIB\n-----END ENCRYPTED PRIVATE KEY-----\n";
    let text = load_error(pair(dir.path(), encrypted, &cert));
    assert!(
        text.contains("encrypted with a passphrase") && text.contains("openssl pkey"),
        "{text}"
    );
    // OpenSSL's legacy encryption (headers inside a PKCS#1 block); no key material.
    let label = ["RSA", "PRIVATE", "KEY"].join(" ");
    let legacy = format!(
        "-----BEGIN {label}-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,00\n\nAAAA\n-----END {label}-----\n"
    );
    let text = load_error(pair(dir.path(), &legacy, &cert));
    assert!(text.contains("encrypted with a passphrase"), "{text}");

    let cut = cert.lines().take(3).collect::<Vec<_>>().join("\n");
    let text = load_error(pair(dir.path(), &key, cut));
    assert!(
        text.contains("not valid PEM") && text.contains("cut short"),
        "{text}"
    );

    let corrupt = "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n";
    let text = load_error(pair(dir.path(), &key, corrupt));
    assert!(text.contains("base64 content is corrupt"), "{text}");

    let garbage = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    let text = load_error(pair(dir.path(), &key, garbage));
    assert!(
        text.contains("cannot be read") || text.contains("does not belong"),
        "{text}"
    );

    let text = load_error(pair(dir.path(), "hello\n", &cert));
    assert!(text.contains("holds no private key"), "{text}");

    let not_a_key = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";
    let text = load_error(pair(dir.path(), not_a_key, &cert));
    assert!(
        text.contains("cannot be used") && text.contains("ECDSA"),
        "{text}"
    );
}

#[test]
fn a_key_of_another_certificate_says_which_one_it_belongs_to() {
    let dir = tempfile::tempdir().unwrap();
    let (key_a, cert_a) = self_signed(&["a.example"]);
    let (_, cert_b) = self_signed(&["b.example"]);
    let text = load_error(pair(dir.path(), &key_a, &cert_b));
    assert!(
        text.contains("does not belong to the certificate")
            && text.contains("Use the key the certificate was issued for"),
        "{text}"
    );
    // The server's certificate second: the order is the problem.
    let text = load_error(pair(dir.path(), &key_a, format!("{cert_b}{cert_a}")));
    assert!(text.contains("certificate #2 of that file"), "{text}");
    // In order, a chain loads.
    let loaded =
        ServerCertificate::load(pair(dir.path(), &key_a, format!("{cert_a}{cert_b}"))).unwrap();
    assert_eq!(loaded.summary().chain, 2);
    assert_eq!(loaded.summary().names, vec!["a.example".to_string()]);
}

#[test]
fn key_formats_load() {
    let dir = tempfile::tempdir().unwrap();
    for alg in [
        &rcgen::PKCS_ECDSA_P256_SHA256,
        &rcgen::PKCS_ECDSA_P384_SHA384,
        &rcgen::PKCS_ED25519,
        &rcgen::PKCS_RSA_SHA256,
    ] {
        let key = rcgen::KeyPair::generate_for(alg).unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let pkcs8 = key.serialize_pem();
        assert!(pkcs8.contains("BEGIN PRIVATE KEY"), "{alg:?}");
        ServerCertificate::load(pair(dir.path(), &pkcs8, cert.pem()))
            .unwrap_or_else(|e| panic!("{alg:?} PKCS#8: {e}"));
        // PKCS#1 (`RSA PRIVATE KEY`) and SEC1 (`EC PRIVATE KEY`): the key inside the PKCS#8
        // envelope, as OpenSSL's "traditional" format writes it.
        let label = if *alg == rcgen::PKCS_RSA_SHA256 {
            "RSA PRIVATE KEY"
        } else if *alg == rcgen::PKCS_ED25519 {
            continue;
        } else {
            "EC PRIVATE KEY"
        };
        let inner = testing::traditional_key(&key.serialize_der(), alg == &rcgen::PKCS_RSA_SHA256);
        let traditional = testing::pem(label, &inner);
        ServerCertificate::load(pair(dir.path(), &traditional, cert.pem()))
            .unwrap_or_else(|e| panic!("{alg:?} {label}: {e}"));
    }
}

#[test]
fn summaries_read_names_and_validity() {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec![
        "example.test".to_string(),
        "www.example.test".to_string(),
        "192.0.2.7".to_string(),
        "::1".to_string(),
    ])
    .unwrap();
    params.not_before = rcgen::date_time_ymd(2026, 1, 2);
    params.not_after = rcgen::date_time_ymd(2051, 3, 4);
    let cert = params.self_signed(&key).unwrap();
    let summary = CertificateSummary::from_chain(&[cert.der().clone()]);
    assert_eq!(
        summary.names,
        ["example.test", "www.example.test", "192.0.2.7", "::1"]
    );
    assert_eq!(
        summary.not_before.unwrap().format("%Y-%m-%d").to_string(),
        "2026-01-02"
    );
    // After 2049: `GeneralizedTime`.
    assert_eq!(
        summary.not_after.unwrap().format("%Y-%m-%d").to_string(),
        "2051-03-04"
    );
    assert_eq!(
        summary.describe(),
        "for example.test, www.example.test, 192.0.2.7 and 1 more, valid until 2051-03-04"
    );
    let file = TlsPath {
        path: "/c.pem".into(),
        source: "`--certfile`".into(),
    };
    let at = |y, m, d| Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap();
    assert!(summary.problem(&file, at(2030, 1, 1)).is_none());
    let expired = summary.problem(&file, at(2052, 1, 1)).unwrap();
    assert!(expired.contains("expired on 2051-03-04"), "{expired}");
    let early = summary.problem(&file, at(2025, 1, 1)).unwrap();
    assert!(early.contains("not valid before 2026-01-02"), "{early}");

    // No subject alternative names: the common name.
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "legacy.example");
    let cert = params.self_signed(&key).unwrap();
    assert_eq!(
        CertificateSummary::from_chain(&[cert.der().clone()]).names,
        ["legacy.example"]
    );
    assert_eq!(
        der::time(0x17, b"491231235959Z")
            .unwrap()
            .format("%Y")
            .to_string(),
        "2049"
    );
    assert_eq!(
        der::time(0x17, b"500101000000Z")
            .unwrap()
            .format("%Y")
            .to_string(),
        "1950"
    );
    assert!(der::time(0x17, b"5001010000Z").is_some());
    assert!(der::time(0x17, b"garbage").is_none());
    assert!(
        CertificateSummary::from_chain(&[CertificateDer::from(vec![0x30, 0x01])])
            .names
            .is_empty()
    );
}

#[test]
fn reload_serves_new_files_and_keeps_the_old_on_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (key_a, cert_a) = self_signed(&["a.example"]);
    let files = pair(dir.path(), &key_a, &cert_a);
    let certificate = ServerCertificate::load(files.clone()).unwrap();
    assert_eq!(certificate.reload().unwrap(), Reload::Unchanged);
    let (key_b, cert_b) = self_signed(&["b.example"]);
    // Half a renewal: the new key with the old certificate.
    std::fs::write(&files.keyfile.path, &key_b).unwrap();
    let error = certificate.reload().unwrap_err();
    assert!(error.to_string().contains("does not belong"), "{error}");
    assert_eq!(certificate.summary().names, ["a.example"]);
    std::fs::write(&files.certfile.path, &cert_b).unwrap();
    let Reload::Reloaded(summary) = certificate.reload().unwrap() else {
        panic!("a reload");
    };
    assert_eq!(summary.names, ["b.example"]);
    assert_eq!(certificate.summary().names, ["b.example"]);
    std::fs::remove_file(&files.certfile.path).unwrap();
    assert!(matches!(certificate.reload(), Err(TlsError::Read(_))));
    assert_eq!(certificate.summary().names, ["b.example"]);
}

#[test]
fn urls_and_redirects() {
    let dir = tempfile::tempdir().unwrap();
    let (key, cert) = self_signed(&["*.example.test", "api.example.test"]);
    let https = Https::new(pair(dir.path(), &key, &cert)).unwrap();
    let mut config = ServerConfig::for_root("sql:fixture", dir.path());
    config.port = 8443;
    assert_eq!(https.url(&config), "https://api.example.test:8443");
    config.port = 443;
    assert_eq!(https.url(&config), "https://api.example.test");
    let (key, cert) = self_signed(&["127.0.0.1"]);
    let https = Https::new(pair(dir.path(), &key, &cert)).unwrap();
    config.port = 8443;
    assert_eq!(https.url(&config), "https://localhost:8443");
    config.host = "::1".into();
    assert_eq!(https.url(&config), "https://[::1]:8443");
    let messages = https.startup_messages(&config);
    assert_eq!(
        messages[0].message(),
        "Serving HTTPS on https://[::1]:8443 (certificate for 127.0.0.1, valid until 4096-01-01)."
    );

    assert_eq!(internal_url(&config), "http://[::1]:8443");
    config.internal_addr = Some("127.0.0.1:4321".parse().unwrap());
    assert_eq!(internal_url(&config), "http://127.0.0.1:4321");

    let request = |host: Option<&str>, uri: &str| {
        let mut builder = Request::get(uri);
        if let Some(host) = host {
            builder = builder.header(header::HOST, host);
        }
        https_redirect(&builder.body(()).unwrap())
    };
    let redirect = request(Some("example.test:8443"), "/dash/pipes?q=1");
    assert_eq!(redirect.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        redirect.headers()[header::LOCATION],
        "https://example.test:8443/dash/pipes?q=1"
    );
    assert_eq!(request(None, "/").status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        request(Some("evil@x"), "/").status(),
        StatusCode::BAD_REQUEST
    );

    let mut h2 = Request::get("https://example.test:8443/x")
        .version(Version::HTTP_2)
        .body(())
        .unwrap();
    authority_as_host(&mut h2);
    assert_eq!(h2.headers()[header::HOST], "example.test:8443");
}
