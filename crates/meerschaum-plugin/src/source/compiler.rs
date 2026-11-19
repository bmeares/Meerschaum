//! An explicit trusted-host policy for confined single-file compilation.
//!
//! This policy is never deserialized from a native manifest cache, public execution update,
//! job property or environment. Discovery must attach the host-owned Arc to freshly derived
//! single-file SourceBuild values and include identity() in their source/cache fingerprints.

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

/// A trusted host selects this policy before source discovery. Neither variant is decoded
/// from a manifest, callback, job property, or source-controlled Cargo configuration.
#[derive(Clone, Debug, Default)]
pub enum CompilerPolicy {
    #[default]
    Ambient,
    Confined(Option<Arc<dyn SourceCompiler>>),
}

impl CompilerPolicy {
    pub(crate) fn selected(&self) -> Result<Option<Arc<dyn SourceCompiler>>, String> {
        match self {
            Self::Ambient => Ok(None),
            Self::Confined(Some(compiler))
                if compiler.identity().len() == 64
                    && compiler.identity().bytes().all(|byte| {
                        byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                    })
                    && compiler.cargo().is_absolute() =>
            {
                Ok(Some(compiler.clone()))
            }
            Self::Confined(_) => Err(
                "Confined Rust sources require the host's pinned offline compiler; ambient Cargo is unavailable."
                    .into(),
            ),
        }
    }

    pub(crate) fn is_confined(&self) -> bool {
        matches!(self, Self::Confined(_))
    }

    pub(crate) fn build_root(&self, root: &Path) -> Result<PathBuf, String> {
        match self.selected()? {
            Some(compiler) => Ok(root.join(format!("owned-{}", compiler.identity()))),
            None => Ok(root.to_path_buf()),
        }
    }
}

/// Owned generated inputs, not an arbitrary user Cargo manifest/package.
pub struct CompileRequest<'a> {
    pub name: &'a str,
    pub manifest: &'a Path,
    pub target: &'a Path,
}

pub trait SourceCompiler: Debug + Send + Sync {
    /// Immutable exact toolchain/vendor/lock identity, also bound by Main metadata cache.
    fn identity(&self) -> &str;
    /// Absolute host-selected Cargo; confined discovery must never search ambient PATH.
    fn cargo(&self) -> &Path;
    /// Exact dependency lock, with only the generated root package renamed.
    fn lockfile(&self, name: &str) -> Result<String, String>;
    /// Bounded stdout AND stderr, always reap, under the sealed native owner deadline.
    fn compile(&self, request: &CompileRequest<'_>) -> Result<Output, String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::discover_report_in_with_policy;

    #[derive(Debug)]
    struct InspectionCompiler {
        identity: String,
        cargo: PathBuf,
    }

    impl SourceCompiler for InspectionCompiler {
        fn identity(&self) -> &str {
            &self.identity
        }
        fn cargo(&self) -> &Path {
            &self.cargo
        }
        fn lockfile(&self, name: &str) -> Result<String, String> {
            Ok(format!(
                "version = 4\n[[package]]\nname = {name:?}\nversion = \"0.0.0\"\n"
            ))
        }
        fn compile(&self, _: &CompileRequest<'_>) -> Result<Output, String> {
            panic!("discovery and rejected restored policies must not execute a compiler")
        }
    }

    fn policy(root: &Path, digit: char) -> CompilerPolicy {
        CompilerPolicy::Confined(Some(Arc::new(InspectionCompiler {
            identity: std::iter::repeat_n(digit, 64).collect(),
            cargo: root.join("host-cargo"),
        })))
    }

    fn source(root: &Path, declaration: &str) -> PathBuf {
        let plugins = root.join("plugins");
        std::fs::create_dir_all(&plugins).unwrap();
        std::fs::write(
            plugins.join("sample.rs"),
            format!("{declaration}\nfn sync() -> SuccessTuple {{ SuccessTuple::ok(\"x\") }}\n"),
        )
        .unwrap();
        plugins
    }

    #[test]
    fn missing_confined_policy_refuses_before_generating_source_or_finding_cargo() {
        let root = tempfile::tempdir().unwrap();
        let plugins = source(root.path(), "");
        let output = root.path().join("not-created");
        let report =
            discover_report_in_with_policy(&[plugins], &output, &CompilerPolicy::Confined(None));
        assert!(report.candidates.is_empty());
        assert_eq!(report.skipped.len(), 1);
        assert!(report.skipped[0].reason.contains("pinned offline compiler"));
        assert!(!output.exists());
    }

    #[test]
    fn compiler_snapshot_changes_generated_build_and_cache_identity() {
        let root = tempfile::tempdir().unwrap();
        let plugins = source(root.path(), "");
        let build = root.path().join("build");
        let first = discover_report_in_with_policy(
            std::slice::from_ref(&plugins),
            &build,
            &policy(root.path(), 'a'),
        );
        let second = discover_report_in_with_policy(&[plugins], &build, &policy(root.path(), 'b'));
        assert!(first.skipped.is_empty(), "{:?}", first.skipped);
        assert!(second.skipped.is_empty(), "{:?}", second.skipped);
        let first = &first.candidates[0];
        let second = &second.candidates[0];
        assert_ne!(first.fingerprint, second.fingerprint);
        let first_build = first.build.as_ref().unwrap();
        let second_build = second.build.as_ref().unwrap();
        assert_ne!(first_build.target_dir, second_build.target_dir);
        assert_ne!(first_build.shared_target, second_build.shared_target);
        assert_eq!(first_build.cargo, root.path().join("host-cargo"));
        assert!(first_build.compiler_required);
        assert!(
            first_build
                .manifest_path
                .with_file_name("Cargo.lock")
                .is_file()
        );
    }

    #[test]
    fn restored_build_cannot_recover_skipped_host_authority() {
        let root = tempfile::tempdir().unwrap();
        let plugins = source(root.path(), "");
        let report = discover_report_in_with_policy(
            &[plugins],
            &root.path().join("build"),
            &policy(root.path(), 'a'),
        );
        let candidate = &report.candidates[0];
        let encoded = serde_json::to_value(candidate.build.as_ref().unwrap()).unwrap();
        assert!(encoded.get("compiler").is_none());
        let restored: crate::source::SourceBuild = serde_json::from_value(encoded).unwrap();
        assert!(restored.compiler.is_none());
        let error = crate::source::build("sample", &restored, &candidate.library).unwrap_err();
        assert!(error.contains("lost its host-owned compiler policy"));
        assert!(!candidate.library.exists());
    }

    #[test]
    fn confined_dependency_blocks_cannot_select_paths_or_remote_sources() {
        for dependency in [
            "{ path = \"../absent\" }",
            "{ git = \"https://example.invalid/source\" }",
            "{ version = \"1\", registry = \"other\" }",
            "{ workspace = true }",
        ] {
            let root = tempfile::tempdir().unwrap();
            let plugins = source(
                root.path(),
                &format!("//! ```cargo\n//! [dependencies]\n//! extra = {dependency}\n//! ```"),
            );
            let build = root.path().join("not-created");
            let report =
                discover_report_in_with_policy(&[plugins], &build, &policy(root.path(), 'a'));
            assert!(report.candidates.is_empty(), "{dependency}");
            assert_eq!(report.skipped.len(), 1);
            assert!(report.skipped[0].reason.contains("pinned offline registry"));
            assert!(!build.exists());
        }
    }
}
