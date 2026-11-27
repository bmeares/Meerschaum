//! Target selection for trusted plugin installs. Plain downloads always return the source archive.

/// Cargo's full host binary target, including the CPU, operating system and C runtime ABI.
pub const HOST_TARGET: &str = env!("MRSM_PLUGIN_TARGET");
/// A GET with this header accepts a matching native archive or the ordinary source archive.
pub const TARGET_HEADER: &str = "x-mrsm-plugin-target";
/// The repository's maximum compressed archive (including the multipart envelope on upload).
pub const MAX_ARCHIVE_BYTES: usize = 128 * 1024 * 1024;

/// Validate a bounded Rust target key without tying the repository to its compiler's target list.
pub fn valid_target(target: &str) -> bool {
    target.len() <= 96
        && target.contains('-')
        && target.split('-').all(|part| {
            part.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
                && part.bytes().all(|c| {
                    c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'_' | b'.')
                })
                && !part.ends_with('.')
                && !part.contains("..")
        })
}

/// A target archive belongs to the exact bytes of an already registered source release.
pub fn valid_source_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
