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

/// The `landlock` crate's ABI of number `abi` (at most [`ABI_USED`]).
fn abi_of(abi: u32) -> ABI {
    match abi {
        0 | 1 => ABI::V1,
        2 => ABI::V2,
        3 => ABI::V3,
        4 => ABI::V4,
        5 => ABI::V5,
        6 => ABI::V6,
        7 => ABI::V7,
        8 => ABI::V8,
        _ => ABI_USED,
    }
}

pub(super) fn restrict(mode: Mode, rules: &[Rule], max_abi: Option<u32>) -> Report {
    let support = kernel_support();
    let kernel_abi = match &support {
        Support::Available { kernel_abi } => *kernel_abi,
        Support::Unsupported { reason } => return Report::not_enforced(mode, reason.clone()),
    };
    let threads_before = threads();
    let mut added = 0usize;
    let used = abi_of(max_abi.unwrap_or(NEWEST_ABI).min(NEWEST_ABI));
    let result = (|| -> Result<landlock::RestrictionStatus, landlock::RulesetError> {
        let handled = AccessFs::from_all(used);
        let mut ruleset = Ruleset::default().handle_access(handled)?;
        // Scopes exist from ABI 6 (an older ABI asked for in tests has none).
        let scopes = Scope::from_all(used);
        if !scopes.is_empty() {
            ruleset = ruleset.scope(scopes)?;
        }
        let mut ruleset = ruleset.create()?;
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
                access &= AccessFs::from_file(used);
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
    }
    .min(max_abi.unwrap_or(NEWEST_ABI));
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
        level: String::new(),
        ..Report::default()
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

/// The errno of `socket(AF_UNIX, ...)` under the filter: what a kernel without Unix sockets
/// answers, which glibc (nscd, nss-resolve), Python and common libraries take as "not available
/// here" and fall back from.
pub(super) const UNIX_SOCKET_ERRNO: i32 = libc::EAFNOSUPPORT;

/// The errno of `io_uring_setup` under the filter (what `kernel.io_uring_disabled` and Docker's
/// default profile answer).
pub(super) const IO_URING_ERRNO: i32 = libc::EPERM;

/// The architecture the filter is built for (`None`: no filter on this one).
fn filter_arch() -> Option<seccompiler::TargetArch> {
    #[cfg(target_arch = "x86_64")]
    {
        Some(seccompiler::TargetArch::x86_64)
    }
    #[cfg(target_arch = "aarch64")]
    {
        Some(seccompiler::TargetArch::aarch64)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        None
    }
}

/// The x32 ABI's numbers on x86-64: the same calls with bit 30 set, under the x86-64 audit
/// architecture (so the architecture check alone does not catch them).
#[cfg(target_arch = "x86_64")]
const X32_SYSCALL_BIT: i64 = 0x4000_0000;

/// `nr` and, on x86-64, its x32 twin.
fn numbers(nr: libc::c_long) -> Vec<i64> {
    // `c_long` is `i32` on 32-bit targets.
    #[allow(clippy::useless_conversion)]
    let nr = i64::from(nr);
    #[cfg(target_arch = "x86_64")]
    {
        vec![nr, nr | X32_SYSCALL_BIT]
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        vec![nr]
    }
}

/// The two programs of the filter: Unix sockets (`.0`, [`UNIX_SOCKET_ERRNO`]) and io_uring
/// (`.1`, [`IO_URING_ERRNO`]). Each first checks the system call's architecture and kills the
/// process on another one (the 32-bit `int 0x80` / `socketcall` path on x86-64, AArch32 on
/// AArch64); everything not named is allowed.
///
/// - `socket(AF_UNIX, ...)`: refused, whatever the type;
/// - `socketpair(AF_UNIX, SOCK_DGRAM, ...)`: refused (a datagram socket of a pair can be
///   re-connected, or `sendto()` a name, and so reach another process's datagram socket); the
///   stream and seqpacket pairs that asyncio and `multiprocessing` use stay allowed: they are
///   connected to each other and cannot be pointed at anyone else;
/// - `io_uring_setup`: refused (io_uring can create sockets without `socket()`).
pub(super) fn unix_socket_programs()
-> Result<(seccompiler::BpfProgram, seccompiler::BpfProgram), String> {
    use seccompiler::{
        SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter, SeccompRule,
    };
    use std::collections::BTreeMap;
    let arch = filter_arch().ok_or("no seccomp filter is built for this architecture")?;
    let err = |e: seccompiler::BackendError| e.to_string();
    let unix = SeccompCondition::new(
        0,
        SeccompCmpArgLen::Dword,
        SeccompCmpOp::Eq,
        libc::AF_UNIX as u64,
    )
    .map_err(err)?;
    let datagram = SeccompCondition::new(
        1,
        SeccompCmpArgLen::Dword,
        SeccompCmpOp::MaskedEq(0xf),
        libc::SOCK_DGRAM as u64,
    )
    .map_err(err)?;
    let mut sockets: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    for nr in numbers(libc::SYS_socket) {
        sockets.insert(nr, vec![SeccompRule::new(vec![unix.clone()]).map_err(err)?]);
    }
    for nr in numbers(libc::SYS_socketpair) {
        sockets.insert(
            nr,
            vec![SeccompRule::new(vec![unix.clone(), datagram.clone()]).map_err(err)?],
        );
    }
    let mut uring: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    for nr in numbers(libc::SYS_io_uring_setup) {
        uring.insert(nr, Vec::new());
    }
    let build = |rules, errno: i32| -> Result<seccompiler::BpfProgram, String> {
        let filter = SeccompFilter::new(
            rules,
            SeccompAction::Allow,
            SeccompAction::Errno(errno as u32),
            arch,
        )
        .map_err(err)?;
        filter
            .try_into()
            .map_err(|e: seccompiler::BackendError| e.to_string())
    };
    Ok((
        build(sockets, UNIX_SOCKET_ERRNO)?,
        build(uring, IO_URING_ERRNO)?,
    ))
}

/// Install the Unix-socket filter on every thread of this process (inherited by everything it
/// starts, irreversible), then try to create a Unix socket: the report says whether it was
/// refused. `no_new_privs` is already set by Landlock.
pub(super) fn deny_unix_sockets(report: &mut Report) {
    use super::{SOCKETS_BY_SECCOMP, SocketProbe};
    let (sockets, uring) = match unix_socket_programs() {
        Ok(programs) => programs,
        Err(why) => {
            report.sockets_reason = Some(why);
            return;
        }
    };
    for program in [&uring, &sockets] {
        if let Err(e) = seccompiler::apply_filter_all_threads(program) {
            report.sockets_reason = Some(format!(
                "the seccomp filter could not be installed ({e}): a container runtime's seccomp \
                 profile?"
            ));
            return;
        }
    }
    report.unix_sockets = SOCKETS_BY_SECCOMP.to_string();
    report.socket_probe = match std::os::unix::net::UnixDatagram::unbound() {
        Ok(_) => SocketProbe::Created,
        Err(e) if e.raw_os_error() == Some(UNIX_SOCKET_ERRNO) => SocketProbe::Denied,
        Err(e) => SocketProbe::Error(e.to_string()),
    };
}

/// A separate filter for credential-free discovery. Ordinary Api and Team execution do not
/// install this filter: their explicitly admitted networking retains its existing behavior.
pub(super) fn deny_network() -> Result<(), String> {
    use seccompiler::{
        SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter, SeccompRule,
    };
    use std::collections::BTreeMap;

    let arch = filter_arch().ok_or("Network denial is unavailable on this architecture.")?;
    let error = |e: seccompiler::BackendError| e.to_string();
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    for syscall in [
        libc::SYS_socket,
        libc::SYS_connect,
        libc::SYS_bind,
        libc::SYS_listen,
        libc::SYS_accept,
        libc::SYS_accept4,
        libc::SYS_io_uring_setup,
    ] {
        for number in numbers(syscall) {
            rules.insert(number, Vec::new());
        }
    }
    let non_unix = SeccompCondition::new(
        0,
        SeccompCmpArgLen::Dword,
        SeccompCmpOp::Ne,
        libc::AF_UNIX as u64,
    )
    .map_err(error)?;
    // Only the connected Unix stream/seqpacket forms and their supported type flags remain.
    let mut invalid_type = Vec::new();
    for kind in [libc::SOCK_STREAM, libc::SOCK_SEQPACKET] {
        for flags in [
            0,
            libc::SOCK_CLOEXEC,
            libc::SOCK_NONBLOCK,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
        ] {
            invalid_type.push(
                SeccompCondition::new(
                    1,
                    SeccompCmpArgLen::Dword,
                    SeccompCmpOp::Ne,
                    (kind | flags) as u64,
                )
                .map_err(error)?,
            );
        }
    }
    for number in numbers(libc::SYS_socketpair) {
        rules.insert(
            number,
            vec![
                SeccompRule::new(vec![non_unix.clone()]).map_err(error)?,
                SeccompRule::new(invalid_type.clone()).map_err(error)?,
            ],
        );
    }
    let program: seccompiler::BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .map_err(error)?
    .try_into()
    .map_err(error)?;
    seccompiler::apply_filter_all_threads(&program)
        .map_err(|e| format!("Could not install credential-free network denial: {e}"))
}

pub(super) fn deny_inbound_network() -> Result<(), String> {
    use seccompiler::{SeccompAction, SeccompFilter, SeccompRule};
    use std::collections::BTreeMap;
    let arch =
        filter_arch().ok_or("Inbound network denial is unavailable on this architecture.")?;
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    for syscall in [
        libc::SYS_bind,
        libc::SYS_listen,
        libc::SYS_accept,
        libc::SYS_accept4,
        libc::SYS_io_uring_setup,
    ] {
        for number in numbers(syscall) {
            rules.insert(number, Vec::new());
        }
    }
    let program: seccompiler::BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .map_err(|e| e.to_string())?
    .try_into()
    .map_err(|e: seccompiler::BackendError| e.to_string())?;
    seccompiler::apply_filter_all_threads(&program)
        .map_err(|e| format!("Could not install API-only listener denial: {e}"))
}
