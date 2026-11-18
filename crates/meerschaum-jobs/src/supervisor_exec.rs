//! A marked native process receives only its selected standard streams.

use std::io;
use std::process::Command;

/// Seal inherited descriptors at a trusted native exec boundary, preserving stdin/out/err.
///
/// Apply only to host-selected supervisor or guardian/native-shell commands. Python worker
/// transports intentionally pass their own connected IPC descriptor and must not use this
/// barrier. The parent's descriptors are unchanged; the syscall runs in the forked child.
/// Unsupported kernels fail the marked launch before its executable starts.
pub fn seal_native_command(command: &mut Command) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: pre_exec runs after standard stream setup in the forked child. close_range
        // is one async-signal-safe kernel syscall. CLOEXEC also preserves std's exec-error
        // pipe until exec succeeds, so unsupported kernels report an ordinary spawn error.
        unsafe {
            command.pre_exec(|| {
                let result = libc::syscall(
                    libc::SYS_close_range,
                    3_u32,
                    u32::MAX,
                    libc::CLOSE_RANGE_CLOEXEC,
                );
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = command;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Marked native execution requires the supported descriptor exec barrier.",
        ))
    }
}
