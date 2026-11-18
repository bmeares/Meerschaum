//! Bounded public description of administrator-installed native Factory material.
//!
//! Decoding a manifest does not establish authority. Main's administrator must verify
//! its contents against an immutable local image; the Factory independently checks its
//! protected installation. Neither health JSON nor a worker can establish these pins.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const MANIFEST_PATH: &str = "/usr/local/share/meerschaum/native-factory.json";
pub const CLI_PATH: &str = "/usr/local/bin/mrsm";
pub const COMPILER_POLICY_PATH: &str = "/etc/meerschaum/native-runtime.json";
pub const COMPILER_PREFIX: &str = "/usr/local/lib/meerschaum/compiler/";
pub const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_FILES: usize = 32_768;
pub const MAX_FILE_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_TOTAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct File {
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedFile {
    pub path: String,
    pub file: File,
}

impl Serialize for NamedFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut value = serializer.serialize_struct("NamedFile", 3)?;
        value.serialize_field("path", &self.path)?;
        value.serialize_field("bytes", &self.file.bytes)?;
        value.serialize_field("sha256", &self.file.sha256)?;
        value.end()
    }
}
impl<'de> Deserialize<'de> for NamedFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            path: String,
            bytes: u64,
            sha256: String,
        }
        let wire = Wire::deserialize(deserializer)?;
        Ok(Self {
            path: wire.path,
            file: File {
                bytes: wire.bytes,
                sha256: wire.sha256,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sdk {
    pub root: String,
    #[serde(deserialize_with = "unique_files")]
    pub files: BTreeMap<String, File>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compiler {
    pub policy: NamedFile,
    pub snapshot: String,
    pub manifest_sha256: String,
    #[serde(deserialize_with = "unique_files")]
    pub files: BTreeMap<String, File>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema: u8,
    pub release: String,
    pub architecture: String,
    pub cli: NamedFile,
    pub sdk: Option<Sdk>,
    pub compiler: Option<Compiler>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub release: String,
    pub architecture: String,
    pub manifest_sha256: String,
    pub cli_sha256: String,
    pub sdk_sha256: Option<String>,
    pub compiler_sha256: Option<String>,
}

impl Identity {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.release != env!("CARGO_PKG_VERSION")
            || !matches!(self.architecture.as_str(), "x86_64" | "aarch64")
            || !valid_digest(&self.manifest_sha256)
            || !valid_digest(&self.cli_sha256)
            || self
                .sdk_sha256
                .as_deref()
                .is_some_and(|value| !valid_digest(value))
            || self
                .compiler_sha256
                .as_deref()
                .is_some_and(|value| !valid_digest(value))
        {
            return Err(refused());
        }
        Ok(())
    }
}

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn canonical_path(value: &str, absolute: bool) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && value.starts_with('/') == absolute
        && !value.contains('\\')
        && !value.chars().any(char::is_control)
        && value
            .strip_prefix('/')
            .unwrap_or(value)
            .split('/')
            .all(|part| !matches!(part, "" | "." | ".."))
}

fn unique_files<'de, D>(deserializer: D) -> Result<BTreeMap<String, File>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Files;
    impl<'de> serde::de::Visitor<'de> for Files {
        type Value = BTreeMap<String, File>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded map of unique canonical file paths")
        }
        fn visit_map<A>(self, mut input: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut files = BTreeMap::new();
            while let Some((name, file)) = input.next_entry::<String, File>()? {
                if files.len() >= MAX_FILES
                    || !canonical_path(&name, false)
                    || files.insert(name, file).is_some()
                {
                    return Err(serde::de::Error::custom(
                        "Invalid or duplicate native file path.",
                    ));
                }
            }
            Ok(files)
        }
    }
    deserializer.deserialize_map(Files)
}

fn refused() -> &'static str {
    "The administrator-selected native Factory artifact manifest is invalid."
}

impl Manifest {
    pub fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.is_empty() || bytes.len() > MAX_MANIFEST_BYTES {
            return Err(refused());
        }
        let manifest: Self = serde_json::from_slice(bytes).map_err(|_| refused())?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema != 1
            || self.release != env!("CARGO_PKG_VERSION")
            || !matches!(self.architecture.as_str(), "x86_64" | "aarch64")
            || self.cli.path != CLI_PATH
            || self.cli.file.bytes < 20
        {
            return Err(refused());
        }
        let files = self.files()?;
        let mut total = 0u64;
        for file in files.values() {
            if !valid_digest(&file.sha256) || file.bytes > MAX_FILE_BYTES {
                return Err(refused());
            }
            total = total.checked_add(file.bytes).ok_or_else(refused)?;
            if total > MAX_TOTAL_BYTES {
                return Err(refused());
            }
        }
        Ok(())
    }

    /// Exact absolute paths for a streaming image/local installation verifier. This
    /// returns untrusted declarations; it never upgrades them to verified authority.
    pub fn files(&self) -> Result<BTreeMap<String, &File>, &'static str> {
        let mut files = BTreeMap::from([(self.cli.path.clone(), &self.cli.file)]);
        if let Some(sdk) = &self.sdk {
            let parts = sdk.root.split('/').collect::<Vec<_>>();
            if parts.len() != 6
                || parts[..3] != ["", "usr", "local"]
                || !matches!(parts[3], "lib" | "lib64")
                || !parts[4].starts_with("python")
                || parts[5] != "site-packages"
                || !canonical_path(&sdk.root, true)
                || !sdk.files.contains_key("meerschaum/__init__.py")
                || !sdk.files.contains_key("_meerschaum_mrsm.py")
                || !sdk.files.iter().any(|(name, file)| {
                    name.starts_with("meerschaum/_core/_native")
                        && name.ends_with(".so")
                        && file.bytes >= 20
                })
            {
                return Err(refused());
            }
            for (name, file) in &sdk.files {
                if !(name.starts_with("meerschaum/") || name == "_meerschaum_mrsm.py")
                    || name.split('/').any(|part| part == "__pycache__")
                    || !canonical_path(name, false)
                    || files.insert(format!("{}/{name}", sdk.root), file).is_some()
                {
                    return Err(refused());
                }
            }
        }
        if let Some(compiler) = &self.compiler {
            if compiler.policy.path != COMPILER_POLICY_PATH
                || compiler.policy.file.bytes == 0
                || compiler.policy.file.bytes > 16 * 1024
                || !valid_digest(&compiler.manifest_sha256)
                || compiler.snapshot != format!("{COMPILER_PREFIX}{}", compiler.manifest_sha256)
                || compiler.files.get("compiler.json").map(|file| &file.sha256)
                    != Some(&compiler.manifest_sha256)
                || !["Cargo.lock", "toolchain/bin/cargo", "toolchain/bin/rustc"]
                    .iter()
                    .all(|name| compiler.files.contains_key(*name))
            {
                return Err(refused());
            }
            files.insert(compiler.policy.path.clone(), &compiler.policy.file);
            for (name, file) in &compiler.files {
                if !canonical_path(name, false)
                    || files
                        .insert(format!("{}/{name}", compiler.snapshot), file)
                        .is_some()
                {
                    return Err(refused());
                }
            }
        }
        if files.len() > MAX_FILES || files.keys().any(|path| !canonical_path(path, true)) {
            return Err(refused());
        }
        Ok(files)
    }

    pub fn identity(&self, original: &[u8]) -> Result<Identity, &'static str> {
        if Self::decode(original)? != *self {
            return Err(refused());
        }
        Ok(Identity {
            release: self.release.clone(),
            architecture: self.architecture.clone(),
            manifest_sha256: digest(original),
            cli_sha256: self.cli.file.sha256.clone(),
            sdk_sha256: self.sdk.as_ref().map(|sdk| {
                let mut bytes = b"mrsm-factory-sdk-v1\0".to_vec();
                bytes.extend(sdk.root.as_bytes());
                bytes.push(0);
                bytes.extend(serde_json::to_vec(&sdk.files).expect("JSON file map"));
                digest(&bytes)
            }),
            compiler_sha256: self
                .compiler
                .as_ref()
                .map(|compiler| compiler.manifest_sha256.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn native() -> Vec<u8> {
        serde_json::to_vec(
            &serde_json::json!({"schema":1,"release":env!("CARGO_PKG_VERSION"),
            "architecture":"x86_64","cli":{"path":CLI_PATH,"bytes":20,"sha256":"a".repeat(64)},
            "sdk":null,"compiler":null}),
        )
        .unwrap()
    }
    #[test]
    fn native_only_manifest_retains_absent_sdk_compiler_and_exact_raw_identity() {
        let raw = native();
        let manifest = Manifest::decode(&raw).unwrap();
        let identity = manifest.identity(&raw).unwrap();
        identity.validate().unwrap();
        assert_eq!(identity.manifest_sha256, digest(&raw));
        assert_eq!(manifest.files().unwrap().len(), 1);
        assert!(identity.sdk_sha256.is_none() && identity.compiler_sha256.is_none());
        let changed = [raw.as_slice(), b" "].concat();
        assert_ne!(manifest.identity(&changed).unwrap(), identity);
    }
    #[test]
    fn duplicated_sdk_path_cannot_silently_replace_a_pinned_file() {
        let duplicate = format!(
            r#"{{"root":"/usr/local/lib/python3.14/site-packages","files":{{"x":{{"bytes":0,"sha256":"{0}"}},"x":{{"bytes":1,"sha256":"{0}"}}}}}}"#,
            "a".repeat(64)
        );
        assert!(serde_json::from_str::<Sdk>(&duplicate).is_err());
        for path in ["a/../b", "a//b", "./x", "x/", "x\\y", "x\ny"] {
            assert!(!canonical_path(path, false));
        }
    }
    #[test]
    fn wrong_release_unknown_fields_and_file_bounds_refuse() {
        let mut value: serde_json::Value = serde_json::from_slice(&native()).unwrap();
        value["release"] = "another-release".into();
        assert!(Manifest::decode(&serde_json::to_vec(&value).unwrap()).is_err());
        value["release"] = env!("CARGO_PKG_VERSION").into();
        value["extra"] = true.into();
        assert!(Manifest::decode(&serde_json::to_vec(&value).unwrap()).is_err());
        value.as_object_mut().unwrap().remove("extra");
        value["cli"]["bytes"] = (MAX_FILE_BYTES + 1).into();
        assert!(Manifest::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}
