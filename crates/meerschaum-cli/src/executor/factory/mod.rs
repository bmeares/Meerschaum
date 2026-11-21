//! The Factory: administrator-installed native material in an executor image (SDK, compiler
//! bundle), verified before the main API trusts it. Nothing here starts an image executable or
//! extracts an archive.
//!
//! - [`docker`]: administrator-owned Docker reads and the selected image cache.
//! - [`image`]: streaming verification of a never-started image's installed bytes.
//! - [`install`]: host administrator provisioning, separate from the public desired deployment.
//! - [`local`]: constructor-only installation checks on the executor's host.
//! - [`process`]: the bounded, owned administrator subprocess the other modules run.
//! - [`sdk`]: exact SDK import targets from the constructor's fixed manifest.
//! - [`stack`]: the typed installation projected into generated Compose.

pub(crate) mod docker;
pub(crate) mod image;
pub(crate) mod install;
pub(crate) mod local;
pub(crate) mod process;
#[cfg(test)]
mod provision_tests;
pub(crate) mod sdk;
pub(crate) mod stack;
