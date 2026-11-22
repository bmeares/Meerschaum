//! All Docker traffic is a typed fake Host; static ELF headers are never executed.
#![cfg(all(test, target_os = "linux"))]

use crate::dispatch::{CommandSpec, Host};
use crate::executor::registry::{Desired, Registry};
use meerschaum_core::access::Team;
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::factory_artifacts as artifacts;
use std::io::{self, Cursor};

fn elf() -> Vec<u8> {
    let mut bytes = vec![0; 20];
    bytes[..6].copy_from_slice(b"\x7fELF\x02\x01");
    bytes[18..].copy_from_slice(&62u16.to_le_bytes());
    bytes
}
fn manifest() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"schema":1,"release":crate::VERSION,"architecture":"x86_64",
        "cli":{"path":artifacts::CLI_PATH,"bytes":20,"sha256":artifacts::digest(&elf())},"sdk":null,"compiler":null})).unwrap()
}
fn tar(single: bool) -> Vec<u8> {
    let mut archive = tar::Builder::new(Vec::new());
    if !single {
        for name in [
            "usr",
            "usr/local",
            "usr/local/bin",
            "usr/local/share",
            "usr/local/share/meerschaum",
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o555);
            header.set_uid(0);
            header.set_gid(0);
            header.set_size(0);
            header.set_cksum();
            archive.append_data(&mut header, name, io::empty()).unwrap();
        }
    }
    let raw = manifest();
    let cli = elf();
    let entries = if single {
        vec![("native-factory.json", raw.as_slice())]
    } else {
        vec![
            (&artifacts::CLI_PATH[1..], cli.as_slice()),
            (&artifacts::MANIFEST_PATH[1..], raw.as_slice()),
        ]
    };
    for (name, bytes) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o444);
        header.set_uid(0);
        header.set_gid(0);
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        archive.append_data(&mut header, name, bytes).unwrap();
    }
    archive.into_inner().unwrap()
}

#[derive(Default)]
struct Docker {
    calls: Vec<Vec<String>>,
    name: Option<String>,
    nonce: Option<String>,
    wrong_cleanup_owner: bool,
}
impl Host for Docker {
    fn run(&mut self, _: &CommandSpec) -> io::Result<i32> {
        panic!("No subprocess may run in this fixture.")
    }
    fn read_line(&mut self, _: &str) -> Option<String> {
        None
    }
    fn output_stream(
        &mut self,
        spec: &CommandSpec,
        _: std::time::Duration,
        maximum: u64,
        consume: &mut (dyn FnMut(&mut dyn io::Read) -> io::Result<()> + Send),
    ) -> io::Result<i32> {
        assert_eq!(spec.program, "docker");
        assert!(spec.scrub_secrets);
        self.calls.push(spec.args.clone());
        let words = spec.args.iter().map(String::as_str).collect::<Vec<_>>();
        let id = format!("sha256:{}", "a".repeat(64));
        let bytes=match words.as_slice(){
            ["image","inspect",..]=>format!("{id}\n").into_bytes(),
            ["container","create",..]=>{
                self.name=spec.args.windows(2).find(|pair|pair[0]=="--name").map(|pair|pair[1].clone());
                self.nonce=spec.args.iter().find_map(|arg|arg.strip_prefix("mrsm.factory.inspection=").map(str::to_string));
                assert!(words.contains(&"--read-only")); assert!(words.contains(&"1000:1000"));
                format!("{}\n","b".repeat(64)).into_bytes()
            },
            ["container","cp",..]=>tar(true),
            ["container","export",..]=>tar(false),
            ["container","inspect",..]=>serde_json::to_vec(&serde_json::json!({"Id":"b".repeat(64),"Image":id,
                "Name":format!("/{}",self.name.as_deref().unwrap()),"State":{"Running":false},
                "Config":{"Labels":{"mrsm.factory.inspection":if self.wrong_cleanup_owner{"somebody-else"}else{self.nonce.as_deref().unwrap()}}}})).unwrap(),
            ["container","rm","--volumes",..]=>"b".repeat(64).into_bytes(),
            _=>panic!("Unexpected non-read Docker operation: {words:?}"),
        };
        assert!(bytes.len() as u64 <= maximum);
        consume(&mut Cursor::new(bytes))?;
        Ok(0)
    }
}

fn fixture() -> (tempfile::TempDir, Team, Desired) {
    let root = tempfile::tempdir().unwrap();
    let mut team = Team::new("factory-fixture");
    team.id = Some(uuid::Uuid::new_v4().to_string());
    team.attributes.insert("isolated".into(), true.into());
    team.attributes.insert(
        "execution".into(),
        Value::Map(Map::from_iter([("mode".into(), "container".into())])),
    );
    let desired = Registry::new(root.path())
        .reconcile(&[team.clone()])
        .unwrap();
    (root, team, desired)
}

#[test]
fn admin_image_cache_uses_complete_immutable_id_proof_and_never_executes_image() {
    let (root, _, _) = fixture();
    let mut docker = Docker::default();
    let first =
        crate::executor::factory::docker::inspect(root.path(), "native:fixture", &[], &mut docker)
            .unwrap();
    assert_eq!(first.image(), format!("sha256:{}", "a".repeat(64)));
    assert!(docker.calls.iter().all(|args| {
        !args
            .iter()
            .any(|arg| matches!(arg.as_str(), "run" | "start" | "exec" | "build" | "pull"))
    }));
    assert!(
        docker
            .calls
            .iter()
            .any(|args| args.get(1).map(String::as_str) == Some("rm"))
    );
    let mut cached = Docker::default();
    let second =
        crate::executor::factory::docker::inspect(root.path(), "native:fixture", &[], &mut cached)
            .unwrap();
    assert_eq!(first.identity(), second.identity());
    assert_eq!(cached.calls.len(), 1);
    use std::os::unix::fs::PermissionsExt;
    let image_directory = crate::executor::factory::install::directory(root.path())
        .join("images")
        .join("a".repeat(64));
    assert_eq!(
        image_directory.metadata().unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["manifest.json", "verified.json"] {
        assert_eq!(
            image_directory
                .join(name)
                .metadata()
                .unwrap()
                .permissions()
                .mode()
                & 0o077,
            0
        );
    }
    // Reuse must still refuse authority material whose privacy was changed later.
    std::fs::set_permissions(&image_directory, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut public_cache = Docker::default();
    assert!(
        crate::executor::factory::docker::inspect(
            root.path(),
            "native:fixture",
            &[],
            &mut public_cache
        )
        .is_err()
    );
    assert_eq!(public_cache.calls.len(), 1);
    std::fs::set_permissions(&image_directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let marker = image_directory.join("verified.json");
    std::fs::write(marker, b"{}").unwrap();
    let mut refused = Docker::default();
    assert!(
        crate::executor::factory::docker::inspect(root.path(), "native:fixture", &[], &mut refused)
            .is_err()
    );
    assert_eq!(refused.calls.len(), 1);
}

#[test]
fn image_inspector_never_removes_a_container_with_another_ownership_label() {
    let (root, _, _) = fixture();
    let mut docker = Docker {
        wrong_cleanup_owner: true,
        ..Default::default()
    };
    assert!(
        crate::executor::factory::docker::inspect(root.path(), "native:fixture", &[], &mut docker)
            .is_err()
    );
    assert!(
        !docker
            .calls
            .iter()
            .any(|args| args.get(1).map(String::as_str) == Some("rm"))
    );
    assert!(
        !crate::executor::factory::install::directory(root.path())
            .join("selected.json")
            .exists()
    );
}

#[test]
fn administrator_keys_are_stable_private_and_never_regenerated_if_lost() {
    let (root, _, desired) = fixture();
    let mut docker = Docker::default();
    let image =
        crate::executor::factory::docker::inspect(root.path(), "native:fixture", &[], &mut docker)
            .unwrap();
    let record = desired.teams.values().next().unwrap();
    let first = crate::executor::factory::install::provision(root.path(), record, &image).unwrap();
    let original = std::fs::read(&first).unwrap();
    let generation = record.verifier.generation.simple();
    let public = crate::executor::factory::install::directory(root.path())
        .join("public")
        .join(format!("{generation}.json"));
    let original_public = std::fs::read(&public).unwrap();
    let private: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let pin: meerschaum_server::executor::factory_attestation::Pin =
        serde_json::from_slice(&original_public).unwrap();
    assert_eq!(private["pin"], serde_json::to_value(&pin).unwrap());
    let second = crate::executor::factory::install::provision(root.path(), record, &image).unwrap();
    assert_eq!(first, second);
    assert_eq!(std::fs::read(&second).unwrap(), original);
    assert_eq!(std::fs::read(&public).unwrap(), original_public);
    let desired_dir = Registry::new(root.path()).desired_dir();
    assert!(!first.starts_with(&desired_dir));
    assert!(!public.starts_with(&desired_dir));
    std::fs::remove_file(&first).unwrap();
    assert!(crate::executor::factory::install::provision(root.path(), record, &image).is_err());
    assert!(!first.exists());
    assert_eq!(std::fs::read(&public).unwrap(), original_public);
}

#[test]
fn compose_projects_only_separate_readonly_mounts_and_exact_inspected_image() {
    let (root, _, desired) = fixture();
    let mut docker = Docker::default();
    let image =
        crate::executor::factory::docker::inspect(root.path(), "native:fixture", &[], &mut docker)
            .unwrap();
    let record = desired.teams.values().next().unwrap();
    let installed =
        crate::executor::factory::install::provision(root.path(), record, &image).unwrap();
    crate::executor::factory::docker::select(root.path(), "native:fixture", &image).unwrap();
    let model = serde_json::json!({"services":{"api":{"volumes":[]},
        format!("executor-{}",record.key):{"user":"1000:1000","image":image.image(),"secrets":[]}},"secrets":{}});
    let rendered = crate::executor::factory::stack::render(
        root.path(),
        &desired,
        "native:fixture",
        Value::from_json_str(&model.to_string()).unwrap(),
    )
    .unwrap();
    let rendered: serde_json::Value = serde_json::from_str(&rendered.to_json_string()).unwrap();
    assert_eq!(
        rendered["services"]["api"]["volumes"][0]["target"],
        meerschaum_server::executor::factory_attestation::PUBLIC_DIRECTORY
    );
    assert_eq!(rendered["services"]["api"]["volumes"][0]["read_only"], true);
    let key = format!("executor-{}-factory", record.key);
    assert_eq!(
        rendered["secrets"][key]["file"],
        installed.to_string_lossy().as_ref()
    );
    assert!(!rendered.to_string().contains("pkcs8"));
    let mut changed = desired.clone();
    changed
        .teams
        .values_mut()
        .next()
        .unwrap()
        .verifier
        .generation = uuid::Uuid::new_v4();
    // A new desired generation has no authority until up provisions it; read-only
    // rendering must remain possible so the administrator can reach that step.
    crate::executor::factory::stack::render(
        root.path(),
        &changed,
        "native:fixture",
        Value::from_json_str(&model.to_string()).unwrap(),
    )
    .unwrap();
}
