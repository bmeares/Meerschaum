//! Handing freed heap memory back to the operating system between laps.
//!
//! A `--loop` job runs for months on a multi-threaded runtime. glibc's allocator gives threads
//! their own arenas and keeps each arena's high-water mark after the memory is freed, so a job's
//! resident size follows its largest lap so far, not the current one. Trimming after each lap
//! returns those free pages. Other allocators (musl, macOS, Windows) return memory on their own.
//!
//! The same retention shows within one sync: each fetched chunk's frames, encoded parameters and
//! statements are freed on whichever worker thread finishes them, and the next chunk allocates in
//! other arenas, so a chunked sync's resident size grew with every chunk (a 1,000,000-row SQLite
//! fetch: 117 MiB live at most, 430 MiB resident). [`release_free_memory_throttled`] trims after
//! each sync and each chunk, at most once per interval.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Return free heap memory to the operating system (glibc's `malloc_trim(0)`). A no-op on other
/// platforms. Costs milliseconds; call it between units of work, not inside them.
#[allow(unsafe_code)]
pub fn release_free_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> std::ffi::c_int;
        }
        // SAFETY: `malloc_trim` is glibc's documented, thread-safe function (it takes each
        // arena's lock); it only releases free pages and never touches live allocations.
        unsafe {
            malloc_trim(0);
        }
    }
}

/// [`release_free_memory`] unless the process already trimmed less than `min_interval` ago: for
/// boundaries that can come in quick succession (small chunks, many small pipes), where a trim
/// per call (milliseconds on a large heap) would cost more than it returns. Returns whether it
/// trimmed.
pub fn release_free_memory_throttled(min_interval: Duration) -> bool {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = EPOCH.get_or_init(Instant::now).elapsed();
    if !claim_trim(&LAST, now, min_interval) {
        return false;
    }
    release_free_memory();
    true
}

/// Claim the next trim: `last` holds the milliseconds (plus one; zero is "never") since the
/// process epoch of the last claimed trim. One caller wins when several race.
fn claim_trim(last: &AtomicU64, now: Duration, min_interval: Duration) -> bool {
    let now_ms = u64::try_from(now.as_millis()).unwrap_or(u64::MAX - 1) + 1;
    let min_ms = u64::try_from(min_interval.as_millis()).unwrap_or(u64::MAX);
    let prev = last.load(Ordering::Relaxed);
    if prev != 0 && now_ms.saturating_sub(prev) < min_ms {
        return false;
    }
    last.compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    #[test]
    fn trims_are_throttled_per_interval() {
        let last = AtomicU64::new(0);
        let interval = Duration::from_millis(250);
        let at = Duration::from_millis;
        // The first boundary always trims, even at the epoch.
        assert!(super::claim_trim(&last, at(0), interval));
        assert!(!super::claim_trim(&last, at(100), interval));
        assert!(!super::claim_trim(&last, at(249), interval));
        assert!(super::claim_trim(&last, at(250), interval));
        assert!(!super::claim_trim(&last, at(400), interval));
        assert!(super::claim_trim(&last, at(10_000), interval));
        // A zero interval trims at every boundary.
        assert!(super::claim_trim(&last, at(10_000), Duration::ZERO));
    }

    #[test]
    fn throttled_release_keeps_live_allocations() {
        let kept: Vec<Vec<u8>> = (0..64).map(|i| vec![i as u8; 64 * 1024]).collect();
        drop((0..64).map(|_| vec![0u8; 256 * 1024]).collect::<Vec<_>>());
        super::release_free_memory_throttled(Duration::ZERO);
        assert!(
            kept.iter()
                .enumerate()
                .all(|(i, v)| v.iter().all(|b| *b == i as u8))
        );
    }

    #[test]
    fn release_free_memory_keeps_live_allocations() {
        let kept: Vec<Vec<u8>> = (0..64).map(|i| vec![i as u8; 64 * 1024]).collect();
        drop((0..64).map(|_| vec![0u8; 256 * 1024]).collect::<Vec<_>>());
        super::release_free_memory();
        assert!(
            kept.iter()
                .enumerate()
                .all(|(i, v)| v.iter().all(|b| *b == i as u8))
        );
    }
}
