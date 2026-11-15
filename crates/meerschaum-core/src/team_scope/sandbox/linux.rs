//! Landlock on Linux (`landlock` crate).

use std::path::{Path, PathBuf};

use landlock::{
    ABI, Access as _, AccessFs, BitFlags, LandlockStatus, PathBeneath, PathFd, RestrictSelfAttr,
    Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus, Scope,
};

use super::{Access, Mode, NEWEST_ABI, Report, Rule, Support, features_of};

/// The ABI whose rights Meerschaum asks for (best effort: the kernel's subset is used).
const ABI_USED: ABI = ABI::V9;

#[allow(unsafe_code)]
pub(super) fn kernel_support() -> Support {
    const LANDLOCK_CREATE_RULESET_VERSION: libc::c_uint = 1;
    // SAFETY: `landlock_create_ruleset(NULL, 0, LANDLOCK_CREATE_RULESET_VERSION)` only returns
    // the kernel's ABI version (or -1 with errno); it takes no pointer to write to, creates no
    // file descriptor and restricts nothing.
    let version = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<libc::c_void>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    if version > 0 {
        return Support::Available {
            kernel_abi: version as u32,
        };
    }
    let error = std::io::Error::last_os_error();
    let reason = match error.raw_os_error() {
        Some(libc::ENOSYS) => "the kernel has no Landlock (older than Linux 5.13 or built \
                               without it), or a seccomp filter hides its system calls"
            .to_string(),
        Some(libc::EOPNOTSUPP) => "Landlock is built into the kernel but not enabled at boot \
                                   (add `landlock` to the kernel's `lsm=` parameter)"
            .to_string(),
        Some(libc::EPERM) | Some(libc::EACCES) => format!(
            "the Landlock system calls are denied ({error}): a container runtime's seccomp \
             profile?"
        ),
        _ => format!("the Landlock system calls failed ({error})"),
    };
    Support::Unsupported { reason }
}

/// The rights of `access`.
fn rights(access: Access) -> BitFlags<AccessFs> {
    let all = AccessFs::from_all(ABI_USED);
    match access {
        Access::ReadExec => AccessFs::ReadFile | AccessFs::ReadDir | AccessFs::Execute,
        Access::Read => AccessFs::ReadFile | AccessFs::ReadDir,
        Access::List => AccessFs::ReadDir.into(),
        Access::ReadWrite => all & !(AccessFs::MakeChar | AccessFs::MakeBlock),
        Access::Device => {
            AccessFs::ReadFile | AccessFs::WriteFile | AccessFs::Truncate | AccessFs::IoctlDev
        }
        Access::SharedMemory => {
            AccessFs::MakeReg
                | AccessFs::ReadFile
                | AccessFs::WriteFile
                | AccessFs::RemoveFile
                | AccessFs::Truncate
        }
        Access::Connect => AccessFs::ResolveUnix.into(),
        Access::Lock => AccessFs::ReadFile | AccessFs::WriteFile,
        Access::DataFile => AccessFs::ReadFile | AccessFs::WriteFile | AccessFs::Truncate,
    }
}

/// The number of threads of this process (`None` when `/proc` cannot tell).
fn threads() -> Option<usize> {
    std::fs::read_dir("/proc/self/task").ok().map(|d| d.count())
}

pub(super) fn restrict(mode: Mode, rules: &[Rule]) -> Report {
    let support = kernel_support();
    let kernel_abi = match &support {
        Support::Available { kernel_abi } => *kernel_abi,
        Support::Unsupported { reason } => return Report::not_enforced(mode, reason.clone()),
    };
    let threads_before = threads();
    let mut added = 0usize;
    let result = (|| -> Result<landlock::RestrictionStatus, landlock::RulesetError> {
        let handled = AccessFs::from_all(ABI_USED);
        let mut ruleset = Ruleset::default()
            .handle_access(handled)?
            .scope(Scope::from_all(ABI_USED))?
            .create()?;
        for rule in rules {
            let path: &Path = &rule.path;
            let Ok(metadata) = std::fs::metadata(path) else {
                continue;
            };
            let Ok(fd) = PathFd::new(path) else {
                continue;
            };
            let mut access = rights(rule.access) & handled;
            if !metadata.is_dir() {
                access &= AccessFs::from_file(ABI_USED);
            }
            if access.is_empty() {
                continue;
            }
            ruleset = ruleset.add_rule(PathBeneath::new(fd, access))?;
            added += 1;
        }
        ruleset.all_threads(true)?.restrict_self()
    })();
    let status = match result {
        Ok(status) => status,
        Err(e) => {
            return Report::not_enforced(
                mode,
                format!(
                    "Landlock refused the sandbox ({e}): a container runtime's seccomp profile?"
                ),
            );
        }
    };
    if status.ruleset == RulesetStatus::NotEnforced {
        return Report::not_enforced(mode, "the kernel did not enforce the sandbox");
    }
    let abi = match status.landlock {
        LandlockStatus::Available { effective_abi, .. } => effective_abi as u32,
        _ => kernel_abi.min(NEWEST_ABI),
    };
    let (mut features, mut unavailable) = features_of(abi);
    let mut reason = None;
    if !status.all_threads {
        features.retain(|f| f != "all_threads");
        if !unavailable.iter().any(|f| f == "all_threads") {
            unavailable.push("all_threads".to_string());
        }
        if threads_before.is_some_and(|n| n > 1) {
            reason = Some(format!(
                "{} threads existed before the sandbox and stay unrestricted (the kernel cannot \
                 restrict all threads before ABI 8)",
                threads_before.unwrap_or_default().saturating_sub(1)
            ));
        }
    }
    Report {
        mode,
        enforced: true,
        abi,
        kernel_abi,
        features,
        unavailable,
        canary: Default::default(),
        reason,
        rules: added,
        dropped: Vec::new(),
    }
}

pub(super) fn private_tmp_dir(name: &str) -> std::io::Result<PathBuf> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let uid = rustix::process::geteuid().as_raw();
    let dir = std::env::temp_dir().join(format!("mrsm-{uid}-{name}"));
    match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let metadata = std::fs::symlink_metadata(&dir)?;
    if !metadata.is_dir() || metadata.uid() != uid {
        return Err(std::io::Error::other(format!(
            "The team's temporary directory `{}` exists but is not a directory of this user's: \
             remove it.",
            dir.display()
        )));
    }
    if metadata.mode() & 0o077 != 0 {
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}
