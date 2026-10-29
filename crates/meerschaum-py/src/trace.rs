//! Stage timers for profiling the boundary (`MRSM_CORE_TRACE=1` prints them to stderr).

use std::sync::OnceLock;
use std::time::Instant;

pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("MRSM_CORE_TRACE").is_some())
}

pub struct Stage {
    name: String,
    start: Instant,
}

impl Stage {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start: Instant::now(),
        }
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if enabled() {
            eprintln!(
                "[trace] {:<50} {:9.2} ms",
                self.name,
                self.start.elapsed().as_secs_f64() * 1e3
            );
        }
    }
}
