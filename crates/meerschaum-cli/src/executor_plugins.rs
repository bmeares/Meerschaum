//! Host-owned plugin objects and inert catalogs, separate from per-grant source views.
//!
//! Archives are pulled from the authenticated Main plugin API. No URL, source path, tar
//! command, interpreter or library loader is selected by plugin metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::RequestAuthentication;
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::plugins::{self, Expected, Plugin, SourceKind, Validated};

pub const MAX_ARCHIVE_BYTES: usize = 32 * 1024 * 1024;
const MAX_EXPANDED_BYTES: u64 = 128 * 1024 * 1024;
const MAX_MEMBER_BYTES: u64 = 32 * 1024 * 1024;
const MAX_MEMBERS: usize = 4096;

/// The host supplies a short-lived authenticator bound to fresh original-source plugin ACLs.
/// Main must authorize each GET; an object already on disk is never a replacement for that GET.
pub struct Repository(APIConnector);

impl Repository {
    pub fn new(
        origin: &str,
        authentication: Arc<dyn RequestAuthentication>,
    ) -> Result<Self, String> {
        meerschaum_server::executor::environment::validate_callback_url(origin)?;
        let connector = APIConnector::new(
            Some("executor-plugin-repository"),
            Map::from_iter([("uri".into(), Value::Str(origin.into()))]),
        )
        .and_then(|api| api.with_request_authentication(authentication, MAX_ARCHIVE_BYTES))
        .map_err(|_| "The authenticated Main plugin repository is unavailable.")?;
        Ok(Self(connector))
    }

    async fn archive(&self, name: &str) -> Result<Vec<u8>, String> {
        if !plugins::valid_name(name) {
            return Err("Invalid granted plugin name.".into());
        }
        let bytes = self
            .0
            .download_plugin(name)
            .await
            .map_err(|_| "Main refused the current plugin download authority.")?;
        if bytes.len() > MAX_ARCHIVE_BYTES {
            return Err("The plugin archive exceeds its limit.".into());
        }
        Ok(bytes)
    }
}

#[derive(Clone)]
pub struct Store {
    objects: PathBuf,
    catalogs: PathBuf,
    views: PathBuf,
}

pub struct View {
    pub directory: PathBuf,
    /// Canonical immutable entry points, never caller-provided filesystem paths.
    pub sources: BTreeMap<String, PathBuf>,
}

impl Store {
    pub fn new(root: &Path) -> Result<Self, String> {
        if !root.is_absolute() {
            return Err("The plugin runtime root must be absolute.".into());
        }
        let store = Self {
            objects: root.join("plugins/.executor-objects"),
            catalogs: root.join("executor-plugin-catalogs"),
            views: root.join("executor-plugin-views"),
        };
        for path in [&store.objects, &store.catalogs, &store.views] {
            meerschaum_core::team_scope::create_private_dir(path)
                .map_err(|_| "Could not prepare the sealed plugin store.")?;
        }
        Ok(store)
    }

    /// Pull every exact granted archive again, including cached objects, to retain Main ACL
    /// revocation. This method publishes no executable routes and imports no code.
    pub async fn pull(&self, repository: &Repository, catalog: &Validated) -> Result<View, String> {
        require_remote(catalog)?;
        for (name, plugin) in &catalog.catalog().plugins {
            let archive = repository.archive(name).await?;
            let store = self.clone();
            let name = name.clone();
            let plugin = plugin.clone();
            tokio::task::spawn_blocking(move || store.install(&name, &plugin, &archive))
                .await
                .map_err(|_| "Plugin staging was interrupted.")??;
        }
        let store = self.clone();
        let catalog = catalog.clone();
        tokio::task::spawn_blocking(move || {
            store.publish_catalog(&catalog)?;
            store.view(&catalog)
        })
        .await
        .map_err(|_| "Plugin publication was interrupted.".to_string())?
    }

    fn install(&self, name: &str, plugin: &Plugin, archive: &[u8]) -> Result<(), String> {
        if !plugins::valid_name(name)
            || !plugins::valid_digest(&plugin.archive_sha256)
            || archive.len() > MAX_ARCHIVE_BYTES
            || plugins::sha256(archive) != plugin.archive_sha256
        {
            return Err("The plugin archive does not match its approved digest.".into());
        }
        let target = self.objects.join(&plugin.archive_sha256);
        if target.exists() {
            self.object(name, plugin)?;
            return Ok(());
        }
        // A new directory is never exposed to a child before atomic publication. No old
        // child-writable directory or source-view ancestor is reused for host writes.
        let staging = tempfile::Builder::new()
            .prefix(".staging-")
            .tempdir_in(&self.objects)
            .map_err(|_| "Could not stage the plugin archive.")?;
        let source = staging.path().join("source");
        std::fs::create_dir(&source).map_err(|_| "Could not stage plugin sources.")?;
        extract(archive, &source, name, plugin.kind)?;
        entry(&source, name, plugin.kind)?;
        meerschaum_core::team_scope::write_private(&staging.path().join("archive.tar.gz"), "")
            .map_err(|_| "Could not stage the plugin archive.")?;
        let mut saved = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(staging.path().join("archive.tar.gz"))
            .map_err(|_| "Could not stage the plugin archive.")?;
        saved
            .write_all(archive)
            .map_err(|_| "Could not stage the plugin archive.")?;
        drop(saved);
        match std::fs::rename(staging.path(), &target) {
            Ok(()) => {}
            Err(_) if target.is_dir() => {
                self.object(name, plugin)?;
            }
            Err(_) => return Err("Could not publish the immutable plugin object.".into()),
        }
        Ok(())
    }

    fn object(&self, name: &str, plugin: &Plugin) -> Result<PathBuf, String> {
        if !plugins::valid_name(name) || !plugins::valid_digest(&plugin.archive_sha256) {
            return Err("Invalid plugin object identity.".into());
        }
        let directory = self.objects.join(&plugin.archive_sha256);
        let metadata = std::fs::symlink_metadata(&directory)
            .map_err(|_| "The approved plugin object is unavailable.")?;
        if !metadata.is_dir() || metadata.is_symlink() {
            return Err("Invalid plugin object directory.".into());
        }
        let bytes = read_bounded(&directory.join("archive.tar.gz"), MAX_ARCHIVE_BYTES)?;
        if plugins::sha256(&bytes) != plugin.archive_sha256 {
            return Err("The plugin object no longer matches its approved digest.".into());
        }
        entry(&directory.join("source"), name, plugin.kind)
    }

    pub fn publish_catalog(&self, catalog: &Validated) -> Result<String, String> {
        require_remote(catalog)?;
        for (name, plugin) in &catalog.catalog().plugins {
            self.object(name, plugin)?;
        }
        let digest = catalog.digest().map_err(|error| error.to_string())?;
        let bytes = catalog.encode().map_err(|error| error.to_string())?;
        let text = std::str::from_utf8(&bytes).map_err(|_| "Invalid plugin catalog encoding.")?;
        meerschaum_core::team_scope::write_private(
            &self.catalogs.join(format!("{digest}.json")),
            text,
        )
        .map_err(|_| "Could not publish the inert plugin catalog.")?;
        Ok(digest)
    }

    /// A missing, corrupt or stale catalog is unavailable, never an empty manifest or an
    /// instruction to import a plugin in the credentialed control runtime.
    pub fn read_catalog(&self, digest: &str, expected: Expected<'_>) -> Result<Validated, String> {
        if !plugins::valid_digest(digest) {
            return Err("Invalid plugin catalog identity.".into());
        }
        let bytes = read_bounded(
            &self.catalogs.join(format!("{digest}.json")),
            plugins::MAX_CATALOG_BYTES,
        )?;
        let catalog = Validated::decode(&bytes, expected).map_err(|error| error.to_string())?;
        if catalog.digest().map_err(|error| error.to_string())? != digest {
            return Err("The plugin catalog does not match its approved digest.".into());
        }
        for (name, plugin) in &catalog.catalog().plugins {
            self.object(name, plugin)?;
        }
        Ok(catalog)
    }

    /// Only genuine absence may request a fresh exact Main pull. An existing unsafe entry
    /// is a refusal, and full catalog/content validation still precedes any launch.
    pub fn catalog_present(&self, digest: &str) -> Result<bool, String> {
        if !plugins::valid_digest(digest) {
            return Err("Invalid plugin catalog identity.".into());
        }
        match std::fs::symlink_metadata(self.catalogs.join(format!("{digest}.json"))) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Ok(metadata)
                if metadata.is_file() && metadata.len() <= plugins::MAX_CATALOG_BYTES as u64 =>
            {
                Ok(true)
            }
            _ => Err("The selected plugin catalog is unsafe or unavailable.".into()),
        }
    }

    /// Select an exact immutable per-operation subset without changing the Team runtime.
    pub fn read_subset_catalog(
        &self,
        digest: &str,
        team_id: uuid::Uuid,
        generation: uuid::Uuid,
        ceiling: &BTreeSet<String>,
        reserved_actions: &BTreeSet<String>,
    ) -> Result<Validated, String> {
        if !plugins::valid_digest(digest) {
            return Err("Invalid discovery catalog identity.".into());
        }
        let bytes = read_bounded(
            &self.catalogs.join(format!("{digest}.json")),
            plugins::MAX_CATALOG_BYTES,
        )?;
        let catalog: plugins::Catalog = serde_json::from_slice(&bytes)
            .map_err(|_| "The exact discovery catalog is unavailable.")?;
        let grants: BTreeSet<String> = catalog.plugins.keys().cloned().collect();
        if !grants.is_subset(ceiling) {
            return Err("Discovery sources exceed the registered Team grant ceiling.".into());
        }
        self.read_catalog(
            digest,
            Expected {
                team_id,
                generation,
                grants: &grants,
                reserved_actions,
            },
        )
    }

    /// Immutable exact source entry points, without allocating a new view on credential renewal.
    pub fn sources(&self, catalog: &Validated) -> Result<BTreeMap<String, PathBuf>, String> {
        require_remote(catalog)?;
        catalog
            .catalog()
            .plugins
            .iter()
            .map(|(name, plugin)| {
                self.object(name, plugin)
                    .map(|source| (plugin.kind.entry_name(name), source))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
    }

    pub fn view(&self, catalog: &Validated) -> Result<View, String> {
        let sources = self.sources(catalog)?;
        let staging = tempfile::Builder::new()
            .prefix(".staging-")
            .tempdir_in(&self.views)
            .map_err(|_| "Could not prepare a fresh granted plugin view.")?;
        for (filename, source) in &sources {
            #[cfg(unix)]
            std::os::unix::fs::symlink(source, staging.path().join(filename))
                .map_err(|_| "Could not prepare the granted plugin view.")?;
            #[cfg(not(unix))]
            return Err("Confined plugin views require a supported native platform.".into());
        }
        let directory = self.views.join(uuid::Uuid::new_v4().simple().to_string());
        std::fs::rename(staging.path(), &directory)
            .map_err(|_| "Could not publish the granted plugin view.")?;
        Ok(View { directory, sources })
    }
}

fn require_remote(catalog: &Validated) -> Result<(), String> {
    if catalog.catalog().placement.is_remote() {
        Ok(())
    } else {
        Err("A Host file catalog cannot select executor archive storage.".into())
    }
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| "Plugin metadata or source is unavailable.")?;
    if !metadata.is_file() || metadata.is_symlink() || metadata.len() > limit as u64 {
        return Err("Plugin metadata or source is invalid or exceeds its limit.".into());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "Plugin metadata or source is unavailable.")?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "Plugin metadata or source is unavailable.")?;
    if bytes.len() > limit {
        return Err("Plugin metadata or source exceeds its limit.".into());
    }
    Ok(bytes)
}

fn entry(root: &Path, name: &str, kind: SourceKind) -> Result<PathBuf, String> {
    let path = root.join(kind.entry_name(name));
    let marker = match kind {
        SourceKind::PythonModule | SourceKind::NativeSource => path.clone(),
        SourceKind::PythonPackage => path.join("__init__.py"),
        SourceKind::NativePackage => path.join("plugin.toml"),
    };
    if !std::fs::symlink_metadata(&marker)
        .is_ok_and(|metadata| metadata.is_file() && !metadata.is_symlink())
    {
        return Err("The archive lacks its exact declared plugin entry point.".into());
    }
    std::fs::canonicalize(path).map_err(|_| "The plugin entry point is unavailable.".into())
}

fn extract(archive: &[u8], root: &Path, name: &str, kind: SourceKind) -> Result<(), String> {
    // Bound the decompressed stream before tar sees it. Raw iteration exposes extension
    // headers before tar can allocate an attacker-declared GNU/PAX payload.
    let decoder = flate2::read::GzDecoder::new(archive)
        .take(MAX_EXPANDED_BYTES + (MAX_MEMBERS as u64 * 1024));
    let mut tar = tar::Archive::new(decoder);
    let mut members = BTreeSet::new();
    let mut expanded = 0u64;
    let mut headers = 0;
    let mut long_path = None;
    let top = kind.entry_name(name);
    for item in tar
        .entries()
        .map_err(|_| "Invalid plugin archive.")?
        .raw(true)
    {
        let mut item = item.map_err(|_| "Invalid plugin archive member.")?;
        headers += 1;
        if headers > MAX_MEMBERS {
            return Err("The plugin archive contains too many headers.".into());
        }
        let entry_type = item.header().entry_type();
        if entry_type.is_gnu_longname() {
            if long_path.is_some() || item.size() > 4096 {
                return Err("Invalid long plugin archive path.".into());
            }
            let mut bytes = Vec::new();
            item.by_ref()
                .take(4097)
                .read_to_end(&mut bytes)
                .map_err(|_| "Invalid long plugin archive path.")?;
            if bytes.len() > 4096 {
                return Err("Invalid long plugin archive path.".into());
            }
            if bytes.last() == Some(&0) {
                bytes.pop();
            }
            let name = String::from_utf8(bytes).map_err(|_| "Invalid long plugin archive path.")?;
            if name.contains('\0') {
                return Err("Invalid long plugin archive path.".into());
            }
            long_path = Some(PathBuf::from(name));
            continue;
        }
        let path = match long_path.take() {
            Some(path) => path,
            None => item
                .path()
                .map_err(|_| "Invalid plugin archive path.")?
                .into_owned(),
        };
        let parts = path.components().collect::<Vec<_>>();
        if parts.is_empty()
            || parts.len() > 32
            || path.as_os_str().len() > 4096
            || parts
                .iter()
                .any(|part| !matches!(part, Component::Normal(_)))
            || parts[0].as_os_str() != std::ffi::OsStr::new(&top)
            || (matches!(kind, SourceKind::PythonModule | SourceKind::NativeSource)
                && parts.len() != 1)
            || !members.insert(path.clone())
            || members.len() > MAX_MEMBERS
        {
            return Err("The archive contains an invalid or ungranted member path.".into());
        }
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err("Plugin archive links and special files are refused.".into());
        }
        let destination = root.join(path);
        if entry_type.is_dir() {
            std::fs::create_dir_all(destination)
                .map_err(|_| "Could not stage plugin directories.")?;
            continue;
        }
        let size = item
            .header()
            .size()
            .map_err(|_| "Invalid plugin archive size.")?;
        expanded = expanded
            .checked_add(size)
            .ok_or("The plugin archive exceeds its expanded limit.")?;
        if size > MAX_MEMBER_BYTES || expanded > MAX_EXPANDED_BYTES {
            return Err("The plugin archive exceeds its expanded limit.".into());
        }
        std::fs::create_dir_all(destination.parent().ok_or("Invalid plugin archive path.")?)
            .map_err(|_| "Could not stage plugin directories.")?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(|_| "Could not stage a unique plugin source file.")?;
        let copied = std::io::copy(&mut item.by_ref().take(size + 1), &mut file)
            .map_err(|_| "Could not stage plugin sources.")?;
        if copied != size {
            return Err("The plugin archive has a truncated member.".into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = item.header().mode().unwrap_or(0) & 0o111;
            file.set_permissions(std::fs::Permissions::from_mode(0o400 | mode))
                .map_err(|_| "Could not seal plugin source permissions.")?;
        }
    }
    if long_path.is_some() {
        return Err("The plugin archive ends with an unused long path.".into());
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    #[test]
    fn host_files_catalogs_cannot_publish_or_select_executor_archive_objects_even_if_empty() {
        use meerschaum_server::executor::plugin_host::{HostIdentity, Placement};
        let root = tempfile::tempdir().unwrap();
        let identity = HostIdentity {
            team_id: None,
            generation: uuid::Uuid::new_v4(),
            files: BTreeMap::new(),
            view_sha256: "a".repeat(64),
        };
        let catalog = plugins::Catalog {
            schema: plugins::SCHEMA,
            release: env!("CARGO_PKG_VERSION").into(),
            team_id: uuid::Uuid::nil(),
            generation: identity.generation,
            placement: Placement::Host(identity.clone()),
            plugins: BTreeMap::new(),
        };
        let validated = Validated::decode_host(
            &serde_json::to_vec(&catalog).unwrap(),
            &identity,
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();
        let store = Store::new(root.path()).unwrap();
        assert!(store.publish_catalog(&validated).is_err());
        assert!(store.sources(&validated).is_err());
        assert!(store.view(&validated).is_err());
        assert_eq!(std::fs::read_dir(&store.catalogs).unwrap().count(), 0);
        assert_eq!(std::fs::read_dir(&store.views).unwrap().count(), 0);
    }

    fn archive(items: &[(&str, &[u8])]) -> Vec<u8> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut tar = tar::Builder::new(encoder);
        for (name, bytes) in items {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o600);
            header.set_cksum();
            tar.append_data(&mut header, name, *bytes).unwrap();
        }
        tar.into_inner().unwrap().finish().unwrap()
    }

    fn catalog(archive: &[u8], team: uuid::Uuid, generation: uuid::Uuid) -> Validated {
        let grants = BTreeSet::from(["probe".into()]);
        let value = serde_json::json!({
            "schema":plugins::SCHEMA,"release":env!("CARGO_PKG_VERSION"),"team_id":team,"generation":generation,
            "plugins":{"probe":{"archive_sha256":plugins::sha256(archive),"kind":"python_module",
                "metadata":{"actions":[{"name":"profile_probe","words":["profile","probe"]}]}}}
        });
        Validated::decode(
            &serde_json::to_vec(&value).unwrap(),
            Expected {
                team_id: team,
                generation,
                grants: &grants,
                reserved_actions: &BTreeSet::new(),
            },
        )
        .unwrap()
    }

    #[test]
    fn stable_objects_and_inert_metadata_survive_grant_view_changes_without_loading_sources() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let marker = root.join("must-not-run");
        let source = format!(
            "open({:?}, 'w').write('unsafe')\nraise RuntimeError('never import for metadata')\n",
            marker.to_string_lossy()
        );
        let bytes = archive(&[("probe.py", source.as_bytes())]);
        let team = uuid::Uuid::new_v4();
        let generation = uuid::Uuid::new_v4();
        let metadata = catalog(&bytes, team, generation);
        let store = Store::new(&root).unwrap();
        store
            .install("probe", &metadata.catalog().plugins["probe"], &bytes)
            .unwrap();
        let digest = store.publish_catalog(&metadata).unwrap();
        let grants = BTreeSet::from(["probe".into()]);
        let loaded = store
            .read_catalog(
                &digest,
                Expected {
                    team_id: team,
                    generation,
                    grants: &grants,
                    reserved_actions: &BTreeSet::new(),
                },
            )
            .unwrap();
        assert_eq!(loaded.action_names().unwrap(), ["profile_probe"]);
        let first = store.view(&loaded).unwrap();
        let second = store.view(&loaded).unwrap();
        assert_ne!(first.directory, second.directory);
        assert_eq!(first.sources, second.sources);
        assert_eq!(
            std::fs::read_to_string(first.directory.join("probe.py")).unwrap(),
            source
        );
        assert!(!marker.exists());
        let none = BTreeSet::new();
        assert!(
            store
                .read_catalog(
                    &digest,
                    Expected {
                        team_id: team,
                        generation,
                        grants: &none,
                        reserved_actions: &BTreeSet::new(),
                    }
                )
                .is_err()
        );
        let mut empty = metadata.catalog().clone();
        empty.plugins.clear();
        let empty = Validated::decode(
            &serde_json::to_vec(&empty).unwrap(),
            Expected {
                team_id: team,
                generation,
                grants: &none,
                reserved_actions: &BTreeSet::new(),
            },
        )
        .unwrap();
        let empty = store.view(&empty).unwrap();
        assert_eq!(std::fs::read_dir(empty.directory).unwrap().count(), 0);
        assert!(
            first.sources["probe.py"].is_file(),
            "Revoking a view must retain stable storage."
        );
        assert!(
            store
                .read_catalog(
                    &"f".repeat(64),
                    Expected {
                        team_id: team,
                        generation,
                        grants: &grants,
                        reserved_actions: &BTreeSet::new(),
                    }
                )
                .is_err(),
            "A missing catalog must not become empty success."
        );
        assert!(!marker.exists());
    }

    #[test]
    fn archive_staging_refuses_links_duplicates_ungranted_paths_and_declared_expansion_bombs() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::new(&temp.path().canonicalize().unwrap()).unwrap();
        let good = archive(&[("probe.py", b"raise RuntimeError('inert')")]);
        let metadata = catalog(&good, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let plugin = &metadata.catalog().plugins["probe"];
        assert!(store.install("probe", plugin, b"wrong archive").is_err());
        let mut evil = vec![
            archive(&[("other.py", b"outside")]),
            archive(&[("probe.py", b"first"), ("probe.py", b"replacement")]),
        ];
        for entry_type in [
            tar::EntryType::Symlink,
            tar::EntryType::GNULongName,
            tar::EntryType::Regular,
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_path("probe.py").unwrap();
            header.set_entry_type(entry_type);
            header.set_link_name("/outside").unwrap();
            header.set_mode(0o600);
            header.set_size(if entry_type == tar::EntryType::Symlink {
                0
            } else {
                MAX_MEMBER_BYTES + 1
            });
            header.set_cksum();
            let mut raw = header.as_bytes().to_vec();
            raw.resize(raw.len() + 1024, 0);
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&raw).unwrap();
            evil.push(encoder.finish().unwrap());
        }
        for bytes in evil {
            let mut selected = plugin.clone();
            selected.archive_sha256 = plugins::sha256(&bytes);
            assert!(store.install("probe", &selected, &bytes).is_err());
            assert!(!store.objects.join(&selected.archive_sha256).exists());
            assert_eq!(
                std::fs::read_dir(&store.objects).unwrap().count(),
                0,
                "Failed staging leaked files."
            );
        }
        store.install("probe", plugin, &good).unwrap();
        let first = store.object("probe", plugin).unwrap();
        store.install("probe", plugin, &good).unwrap();
        assert_eq!(store.object("probe", plugin).unwrap(), first);
    }

    struct Auth(Arc<AtomicUsize>);
    impl RequestAuthentication for Auth {
        fn authorization(
            &self,
            request: meerschaum_connector_api::client::authentication::Request<'_>,
        ) -> meerschaum_connector_api::traits::ConnectorResult<String> {
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, "/plugins/probe");
            let count = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(format!("Bearer synthetic-plugin-{count}"))
        }
    }

    struct Main {
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Main {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(thread) = self.thread.take() {
                let joined = thread.join();
                if !std::thread::panicking() {
                    joined.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn pulls_reauthenticate_cached_objects_and_refuse_main_revocation_and_redirects() {
        let bytes = archive(&[("probe.py", b"raise RuntimeError('never import')")]);
        let selected = catalog(&bytes, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let thread = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut count = 0;
            while !stopping.load(Ordering::SeqCst) && Instant::now() < deadline {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut buffer).unwrap();
                    assert!(read > 0 && request.len() < 16 * 1024);
                    request.extend_from_slice(&buffer[..read]);
                }
                count += 1;
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with("GET /plugins/probe HTTP/1.1\r\n"));
                assert!(request.to_ascii_lowercase().contains(&format!(
                    "authorization: bearer synthetic-plugin-{count}\r\n"
                )));
                let (status, extra, body) = match count {
                    1 => (
                        "200 OK",
                        "Content-Type: application/gzip\r\n",
                        bytes.as_slice(),
                    ),
                    2 => (
                        "403 Forbidden",
                        "Content-Type: application/json\r\n",
                        b"{\"detail\":\"revoked\"}".as_slice(),
                    ),
                    3 => (
                        "302 Found",
                        "Location: /must-not-follow\r\n",
                        b"redirect".as_slice(),
                    ),
                    _ => panic!("Repository followed a redirect or retried a refused pull."),
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(body).unwrap();
            }
            assert_eq!(count, 3);
        });
        let _main = Main {
            stop,
            thread: Some(thread),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let repository = Repository::new(&origin, Arc::new(Auth(calls.clone()))).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let store = Store::new(&temp.path().canonicalize().unwrap()).unwrap();
        let first = store.pull(&repository, &selected).await.unwrap();
        assert!(first.directory.join("probe.py").is_file());
        assert!(
            store.pull(&repository, &selected).await.is_err(),
            "A cached object bypassed Main revocation."
        );
        assert!(
            store.pull(&repository, &selected).await.is_err(),
            "A redirect was accepted."
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(first.sources["probe.py"].is_file());
    }
}
#[test]
fn only_genuine_catalog_absence_requests_a_pull() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path()).unwrap();
    let digest = "a".repeat(64);
    assert!(!store.catalog_present(&digest).unwrap());
    assert!(store.catalog_present("../catalog").is_err());
    let path = store.catalogs.join(format!("{digest}.json"));
    std::fs::create_dir(&path).unwrap();
    assert!(store.catalog_present(&digest).is_err());
    std::fs::remove_dir(&path).unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(root.path().join("absent"), &path).unwrap();
        assert!(store.catalog_present(&digest).is_err());
        std::fs::remove_file(&path).unwrap();
    }
    std::fs::write(&path, "malformed existing catalog").unwrap();
    assert!(store.catalog_present(&digest).unwrap());
    assert!(
        store
            .read_subset_catalog(
                &digest,
                uuid::Uuid::new_v4(),
                uuid::Uuid::new_v4(),
                &Default::default(),
                &Default::default()
            )
            .is_err()
    );
}
