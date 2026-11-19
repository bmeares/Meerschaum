// Append as a private child of factory_quote after native receipt APIs are registered.
// These exercise real Ed25519 transport verification; actual Factory/receipt provenance
// is proved separately by the native Metadata process fixture, not fabricated here.
use super::*;

fn actor() -> Actor {
    Actor {
        owner: "alice".into(),
        username: Some("alice".into()),
        permissions: ["plugins:read".into()].into_iter().collect(),
        administrator: false,
    }
}
fn fixture() -> (Authority, Pending) {
    let (control, _) = crate::executor::control::Authority::generate(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    let artifacts = crate::executor::factory_artifacts::Identity {
        release: env!("CARGO_PKG_VERSION").into(),
        architecture: "x86_64".into(),
        manifest_sha256: "a".repeat(64),
        cli_sha256: "b".repeat(64),
        sdk_sha256: Some("c".repeat(64)),
        compiler_sha256: Some("d".repeat(64)),
    };
    let (factory, _) = Authority::generate(&control.verifier(), None, artifacts).unwrap();
    let pending = Pending::new(
        factory.pin().clone(),
        "e".repeat(64),
        &actor(),
        (Uuid::new_v4(), 1),
        "f".repeat(64),
        1_000_060,
        1_000_000,
    )
    .unwrap();
    (factory, pending)
}
fn response(authority: &Authority, pending: &Pending, message: &[u8], domain: &[u8]) -> Vec<u8> {
    let payload = Payload {
        schema: 1,
        pin_sha256: pin_digest(&pending.pin).unwrap(),
        request_sha256: digest(&serde_json::to_vec(&pending.request).unwrap()),
        configuration_sha256: pending.configuration.clone(),
        actor_sha256: pending.actor_sha256.clone(),
        context_identity_sha256: "1".repeat(64),
        native_receipt_sha256: "2".repeat(64),
        native_launch_id: Uuid::new_v4().simple().to_string(),
        message_sha256: digest(message),
        message_bytes: message.len(),
        completed_at: 999_999,
        issued_at: 1_000_000,
        expires_at: 1_000_030,
    };
    let bytes = serde_json::to_vec(&payload).unwrap();
    let mut signed = domain.to_vec();
    signed.extend(&bytes);
    serde_json::to_vec(&Response {
        payload: URL_SAFE_NO_PAD.encode(bytes),
        signature: URL_SAFE_NO_PAD.encode(authority.key.sign(&signed).as_ref()),
    })
    .unwrap()
}

#[test]
fn independent_quote_is_exact_result_bound_and_single_use_without_burning_bad_attempt() {
    let (factory, pending) = fixture();
    let bytes = response(&factory, &pending, b"actual-native-result", DOMAIN);
    assert!(
        pending
            .verify(&bytes, b"changed-result", 1_000_001)
            .is_err()
    );
    let verified = pending
        .verify(&bytes, b"actual-native-result", 1_000_001)
        .unwrap();
    assert_eq!(verified.pin_sha256(), pin_digest(factory.pin()).unwrap());
    assert_eq!(verified.receipt_sha256(), "2".repeat(64));
    assert!(
        pending
            .verify(&bytes, b"actual-native-result", 1_000_001)
            .is_err()
    );
}
#[test]
fn another_nonce_configuration_actor_generation_artifact_or_signing_domain_refuses() {
    let (factory, pending) = fixture();
    let good = response(&factory, &pending, b"native", DOMAIN);
    let fresh = Pending::new(
        factory.pin().clone(),
        pending.configuration.clone(),
        &actor(),
        (pending.request.context, pending.request.revision),
        pending.request.catalog_sha256.clone(),
        pending.request.deadline,
        1_000_000,
    )
    .unwrap();
    assert!(fresh.verify(&good, b"native", 1_000_001).is_err());
    for which in 0..4 {
        let mut changed = Pending::new(
            factory.pin().clone(),
            pending.configuration.clone(),
            &actor(),
            (pending.request.context, pending.request.revision),
            pending.request.catalog_sha256.clone(),
            pending.request.deadline,
            1_000_000,
        )
        .unwrap();
        changed.request = pending.request.clone();
        match which {
            0 => changed.configuration = "0".repeat(64),
            1 => changed.actor_sha256 = "0".repeat(64),
            2 => changed.pin.generation = Uuid::new_v4(),
            _ => changed.pin.artifacts.compiler_sha256 = Some("0".repeat(64)),
        }
        assert!(changed.verify(&good, b"native", 1_000_001).is_err());
    }
    let wrong = response(
        &factory,
        &pending,
        b"native",
        b"mrsm-control-other-domain\0",
    );
    assert!(pending.verify(&wrong, b"native", 1_000_001).is_err());
    assert!(pending.verify(&good, b"native", 1_000_001).is_ok());
}
#[test]
fn expired_overbound_and_another_response_key_never_become_native_provenance() {
    let (factory, pending) = fixture();
    let good = response(&factory, &pending, b"native", DOMAIN);
    assert!(pending.verify(&good, b"native", 1_000_030).is_err());
    assert!(
        pending
            .verify(&vec![b'x'; MAX_RESPONSE + 1], b"native", 1_000_001)
            .is_err()
    );
    let (other, _) = fixture();
    let wrong = response(&other, &pending, b"native", DOMAIN);
    assert!(pending.verify(&wrong, b"native", 1_000_001).is_err());
    assert!(pending.verify(&good, b"native", 1_000_001).is_ok());
}
