//! Handing freed heap memory back to the operating system between laps.
//!
//! A `--loop` job runs for months on a multi-threaded runtime. glibc's allocator gives threads
//! their own arenas and keeps each arena's high-water mark after the memory is freed, so a job's
//! resident size follows its largest lap so far, not the current one. Trimming after each lap
//! returns those free pages. Other allocators (musl, macOS, Windows) return memory on their own.

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

#[cfg(test)]
mod tests {
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
