use std::{path::PathBuf, process::Command, sync::OnceLock};

pub fn library(name: &str) -> PathBuf {
    const FIXTURE_PROFILE: &str = "release";
    static TARGET: OnceLock<PathBuf> = OnceLock::new();
    let target = TARGET.get_or_init(|| {
        let executable = std::env::current_exe().unwrap();
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| executable.ancestors().nth(3).unwrap().to_path_buf());
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            // The host's artifact limit also applies to test plugins.
            .args([
                "build",
                "--locked",
                "-p",
                "meerschaum-plugin",
                "--example",
                "native_api",
                "--example",
                "native_demo",
                "--example",
                "native_probe",
                "--example",
                "bad_abi",
                "--profile",
                FIXTURE_PROFILE,
            ])
            .arg("--target-dir")
            .arg(&target)
            .env("CARGO_PROFILE_RELEASE_OPT_LEVEL", "z")
            .env("CARGO_PROFILE_RELEASE_STRIP", "symbols")
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
        target
    });
    target.join(FIXTURE_PROFILE).join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}
