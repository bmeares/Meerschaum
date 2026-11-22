//! Atomic terminal pending claim, completion and never-handed-off cancellation.
use super::*;
#[cfg(unix)]
pub(super) fn lock(spec: &std::path::Path) -> io::Result<std::fs::File> {
    use rustix::fs::{Mode, OFlags};
    let spec = std::fs::canonicalize(spec)?;
    let generation = spec.parent().ok_or_else(unavailable)?;
    // Real private generations are read-only to their child. Keep the lock one level
    // above even that read grant, so a confined source cannot hold the host's flock.
    let directory = if generation
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("generation-"))
        .and_then(|name| uuid::Uuid::parse_str(name).ok())
        .is_some()
        && generation
            .parent()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str())
            .and_then(|name| uuid::Uuid::parse_str(name).ok())
            .is_some()
    {
        generation.parent().ok_or_else(unavailable)?
    } else {
        generation
    }; // Lifetime-only legacy native fixtures have no private-generation layout.
    let file = std::fs::File::from(rustix::fs::open(
        directory.join(".terminal-launch.lock"),
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != 0 {
        return Err(unavailable());
    }
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < until => {
                std::thread::sleep(std::time::Duration::from_millis(10))
            }
            _ => return Err(unavailable()),
        }
    }
}
/// Retire only this host's newly prepared terminal command which it proves was never
/// handed to a launcher. An existing reattached session never receives this fresh nonce.
/// Claim/update and cancellation take the same private filesystem lock; a claimed,
/// different-owner, malformed or unknown record is preserved and refused.
pub fn cancel_unlaunched_terminal(
    spec: &std::path::Path,
    path: &std::path::Path,
    owner: &LifecycleOwner,
) -> io::Result<()> {
    if LifecycleOwner::current()? != *owner || !owner.alive() {
        return Err(unavailable());
    }
    let _lock = lock(spec)?;
    let record = terminal_launch_read(spec, path)?;
    if &record.owner != owner
        || record.guardian.is_some()
        || record.child.is_some()
        || record.cleanup_complete
    {
        return Err(unavailable());
    }
    std::fs::remove_file(path)
}

#[cfg(not(unix))]
pub(super) fn lock(_: &std::path::Path) -> io::Result<std::fs::File> {
    Err(unavailable())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn never_handed_off_cancellation_and_claim_are_atomic_and_claimed_proof_is_preserved() {
        let root = tempfile::tempdir().unwrap();
        let context = root.path().join(uuid::Uuid::new_v4().simple().to_string());
        let generation = context.join(format!("generation-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&generation).unwrap();
        let spec = generation.join("sandbox.json");
        std::fs::write(&spec, "{}").unwrap();
        let owner = LifecycleOwner::current().unwrap();
        for _ in 0..32 {
            let pending = prepare_terminal_launch(&spec, &owner).unwrap();
            assert!(context.join(".terminal-launch.lock").is_file());
            assert!(
                !generation.join(".terminal-launch.lock").exists(),
                "A private child must not receive read access to the host's lifecycle lock."
            );
            let barrier = std::sync::Barrier::new(3);
            let (canceled, claimed) = std::thread::scope(|threads| {
                let cancel = threads.spawn(|| {
                    barrier.wait();
                    cancel_unlaunched_terminal(&spec, &pending, &owner)
                });
                let claim = threads.spawn(|| {
                    barrier.wait();
                    claim_terminal_launch(&spec, &pending, &owner)
                });
                barrier.wait();
                (cancel.join().unwrap(), claim.join().unwrap())
            });
            assert_ne!(
                canceled.is_ok(),
                claimed.is_ok(),
                "A claimed launch cannot also be canceled as never handed off."
            );
            if claimed.is_ok() {
                let before = std::fs::read(&pending).unwrap();
                assert!(cancel_unlaunched_terminal(&spec, &pending, &owner).is_err());
                assert_eq!(std::fs::read(&pending).unwrap(), before);
                // This fixture claims in the current host and never forks a child.
                complete_terminal_launch(&spec, &pending, &owner).unwrap();
                std::fs::remove_file(&pending).unwrap();
            } else {
                assert!(!pending.exists());
                assert!(claim_terminal_launch(&spec, &pending, &owner).is_err());
                assert!(
                    !pending.exists(),
                    "A canceled nonce must not be recreated by claim."
                );
            }
        }
    }
}
