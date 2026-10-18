//! Downloading DuckDB's official shared library: pinned checksums, a size-capped download, an
//! extraction that only ever writes the one library file, an atomic rename into the cache and a
//! cross-process lock.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::StreamExt;
use sha2::{Digest, Sha256};

use super::{Reporter, VERSION, cache_dir};

/// One official release asset (`libduckdb-<plat>.zip`).
pub(crate) struct Asset {
    pub plat: &'static str,
    /// SHA-256 of the zip. Taken from the GitHub release API `digest` field of DuckDB v1.5.6 and
    /// verified by downloading every asset.
    pub sha256: &'static str,
    pub size: u64,
}

const ASSETS: &[Asset] = &[
    Asset {
        plat: "linux-amd64",
        sha256: "b845005f5132a7d8180057c35e14a7626632258782f871a90861b19c1c03841b",
        size: 41_378_782,
    },
    Asset {
        plat: "linux-amd64-musl",
        sha256: "9b82ed3720135c9821f5b8fa83744500bdc0146d3f88cdbc86dace35f0249e70",
        size: 42_706_663,
    },
    Asset {
        plat: "linux-arm64",
        sha256: "b72ed9f05003f5e9d2015f7ceada6416b377d9dd33169cdde3c9e33897856eee",
        size: 37_763_835,
    },
    Asset {
        plat: "linux-arm64-musl",
        sha256: "a568750e1bd07d7e1d9dfe78ed89bb6ea4d945a66c5302799f39987890eea57b",
        size: 40_218_738,
    },
    Asset {
        plat: "osx-universal",
        sha256: "e0bc007d9b0094c0970ac1847a8601d10aad07cbd2910ce586ec810f77b638d6",
        size: 36_216_941,
    },
    Asset {
        plat: "windows-amd64",
        sha256: "44cf59583f9951d2cb09b1bf115a63ecb2d8901e363903029d86c7d8683fe96a",
        size: 13_408_787,
    },
    Asset {
        plat: "windows-arm64",
        sha256: "c63285647e94c46d309b60d5dd1ff3cb2cdf9ca03e415a818ff7d8dbf838e0e8",
        size: 14_366_908,
    },
];

/// The asset platform name of this build's target, as DuckDB names its release files.
pub(crate) fn platform() -> Option<&'static str> {
    Some(
        if cfg!(all(
            target_os = "linux",
            target_arch = "x86_64",
            target_env = "musl"
        )) {
            "linux-amd64-musl"
        } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
            "linux-amd64"
        } else if cfg!(all(
            target_os = "linux",
            target_arch = "aarch64",
            target_env = "musl"
        )) {
            "linux-arm64-musl"
        } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
            "linux-arm64"
        } else if cfg!(target_os = "macos") {
            "osx-universal"
        } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
            "windows-amd64"
        } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
            "windows-arm64"
        } else {
            return None;
        },
    )
}

pub(crate) fn asset_for(plat: &str) -> Option<&'static Asset> {
    ASSETS.iter().find(|a| a.plat == plat)
}

/// The asset of this build's target.
pub(crate) fn current_asset() -> Option<&'static Asset> {
    platform().and_then(asset_for)
}

pub(crate) fn release_url(a: &Asset) -> String {
    format!(
        "https://github.com/duckdb/duckdb/releases/download/v{VERSION}/libduckdb-{}.zip",
        a.plat
    )
}

/// The size of the pinned download in decimal megabytes (rounded), for messages.
pub(crate) fn size_mb(a: &Asset) -> u64 {
    (a.size + 500_000) / 1_000_000
}

/// Upper bound for the extracted library (the largest official one is ~120 MB).
const MAX_LIBRARY_BYTES: u64 = 1 << 30;

/// Extract only the entry named exactly `wanted` (a bare file name) from the zip into `dest`,
/// never trusting any path inside the archive and never writing more than `max` bytes. Returns
/// the written file.
pub(crate) fn extract_library(
    zip_path: &Path,
    wanted: &str,
    dest: &Path,
    max: u64,
) -> Result<PathBuf, String> {
    if wanted.is_empty() || wanted == ".." || wanted.contains(['/', '\\']) {
        return Err(format!("Refusing the library name '{wanted}'."));
    }
    let file = fs::File::open(zip_path).map_err(|e| e.to_string())?;
    let mut zip = zip::ZipArchive::new(file)
        .map_err(|e| format!("The download is not a zip archive: {e}"))?;
    let mut idx = None;
    for i in 0..zip.len() {
        let entry = zip.by_index_raw(i).map_err(|e| e.to_string())?;
        // The literal entry name only (no normalization): `../x`, `dir/libduckdb.so` and absolute
        // names never equal the bare wanted file name.
        if entry.name_raw() == wanted.as_bytes() && entry.is_file() {
            idx = Some(i);
            break;
        }
    }
    let idx = idx.ok_or_else(|| format!("The archive does not contain '{wanted}'."))?;
    let mut entry = zip.by_index(idx).map_err(|e| e.to_string())?;
    if entry.size() > max {
        return Err(format!(
            "'{wanted}' in the archive is {} bytes, over the {max} byte limit.",
            entry.size()
        ));
    }
    let out = dest.join(wanted);
    let mut f = fs::File::create(&out).map_err(|e| e.to_string())?;
    let mut limited = (&mut entry).take(max + 1);
    let n = std::io::copy(&mut limited, &mut f).map_err(|e| e.to_string())?;
    if n > max {
        let _ = fs::remove_file(&out);
        return Err(format!("'{wanted}' expanded past the {max} byte limit."));
    }
    f.flush().map_err(|e| e.to_string())?;
    f.sync_all().map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&out, fs::Permissions::from_mode(0o755)).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

/// Download `url`, verify its SHA-256, and install the library atomically into the cache of
/// `root`. With `force` an existing library is replaced.
pub(crate) async fn install_from(
    root: &Path,
    url: &str,
    sha256: &str,
    size_hint: u64,
    lib_file: &str,
    force: bool,
    reporter: &dyn Reporter,
) -> Result<PathBuf, String> {
    let dir = cache_dir(root);
    let target = dir.join(lib_file);
    fs::create_dir_all(&dir).map_err(|e| format!("Could not create '{}': {e}", dir.display()))?;
    // One installer at a time; others wait here, then find the library in place.
    let lock_path = dir.parent().unwrap_or(&dir).join(".install.lock");
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|e| format!("Could not open '{}': {e}", lock_path.display()))?;
    let lock = tokio::task::spawn_blocking(move || fs4::FileExt::lock(&lock).map(|_| lock))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("Could not lock '{}': {e}", lock_path.display()))?;

    let result = async {
        // Scratch directories of installs that died (any pid) are removed while we hold the lock.
        remove_stale_scratch(dir.parent().unwrap_or(&dir), STALE_SCRATCH);
        if !force && target.is_file() {
            return Ok(target.clone());
        }
        let scratch = dir
            .parent()
            .unwrap_or(&dir)
            .join(format!(".tmp-{}-{VERSION}", std::process::id()));
        let _ = fs::remove_dir_all(&scratch);
        fs::create_dir_all(&scratch).map_err(|e| e.to_string())?;
        let downloaded = download_and_extract(url, sha256, size_hint, lib_file, &scratch, reporter)
            .await
            .and_then(|extracted| {
                if force {
                    let _ = fs::remove_file(&target);
                }
                fs::rename(&extracted, &target)
                    .map_err(|e| format!("Could not install '{}': {e}", target.display()))
            });
        let _ = fs::remove_dir_all(&scratch);
        downloaded.map(|_| target.clone())
    }
    .await;
    let _ = fs4::FileExt::unlock(&lock);
    result
}

/// Install scratch directories (`.tmp-<pid>-<version>`) older than this are leftovers of a killed
/// install.
const STALE_SCRATCH: Duration = Duration::from_secs(3600);

/// Remove `.tmp-*` directories of `drivers/duckdb` last modified more than `max_age` ago. Called
/// with the install lock held, so no live install owns them (a live one touches its directory
/// continuously while downloading, and finishes well inside an hour).
pub(crate) fn remove_stale_scratch(parent: &Path, max_age: Duration) {
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with(".tmp-") {
            continue;
        }
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > max_age);
        if old {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

async fn download_and_extract(
    url: &str,
    sha256: &str,
    size_hint: u64,
    lib_file: &str,
    scratch: &Path,
    reporter: &dyn Reporter,
) -> Result<PathBuf, String> {
    // reqwest reads HTTP_PROXY / HTTPS_PROXY / ALL_PROXY / NO_PROXY from the environment.
    // The pinned asset is an https URL and so must every redirect be: the SHA-256 check would
    // reject a swapped body, but nothing should travel in the clear. The unit tests serve the
    // archive from a plain-http local server, so only they may start with an http URL (the
    // redirect rule applies to them too).
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            "meerschaum-driver-installer/",
            env!("CARGO_PKG_VERSION")
        ))
        .https_only(!cfg!(test))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 10 {
                attempt.error("too many redirects")
            } else if attempt.url().scheme() != "https" {
                let target = attempt.url().to_string();
                attempt.error(format!("refusing a redirect to a non-https URL ({target})"))
            } else {
                attempt.follow()
            }
        }))
        .connect_timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .get(url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| format!("Could not download {url}: {}", error_chain(&e)))?;
    reporter.progress(0, resp.content_length().unwrap_or(size_hint));
    let zip_path = scratch.join("download.zip");
    let mut f = fs::File::create(&zip_path).map_err(|e| e.to_string())?;
    let mut hasher = Sha256::new();
    let mut got = 0u64;
    let total = resp.content_length().unwrap_or(size_hint);
    let mut stream = resp.bytes_stream();
    // At most twice the pinned size, so a hostile mirror cannot fill the disk.
    let cap = size_hint.saturating_mul(2).max(1 << 20);
    loop {
        let next = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .map_err(|_| "The download stalled for 60 seconds.".to_string())?;
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|e| format!("The download failed: {}", error_chain(&e)))?;
        got += chunk.len() as u64;
        if got > cap {
            return Err("The download is larger than expected; refusing it.".into());
        }
        hasher.update(&chunk);
        f.write_all(&chunk).map_err(|e| e.to_string())?;
        reporter.progress(got, total);
    }
    drop(f);
    let digest = hex::encode(hasher.finalize());
    if !digest.eq_ignore_ascii_case(sha256) {
        return Err(format!(
            "Checksum mismatch for {url}: expected {sha256}, got {digest}. The download was discarded."
        ));
    }
    extract_library(&zip_path, lib_file, scratch, MAX_LIBRARY_BYTES)
}

fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut src = e.source();
    while let Some(s) = src {
        out.push_str(": ");
        out.push_str(&s.to_string());
        src = s.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::duckdb_lib::NullReporter;
    fn make_zip(path: &Path, entries: &[(&str, &[u8])]) {
        let f = fs::File::create(path).unwrap();
        let mut w = zip::ZipWriter::new(f);
        for (name, data) in entries {
            w.start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            w.write_all(data).unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn every_supported_platform_has_a_pinned_checksum() {
        for a in ASSETS {
            assert_eq!(a.sha256.len(), 64, "{}", a.plat);
            assert!(
                a.sha256.chars().all(|c| c.is_ascii_hexdigit()),
                "{}",
                a.plat
            );
            assert!(a.size > 1_000_000, "{}", a.plat);
        }
        assert!(
            release_url(asset_for("linux-amd64").unwrap())
                .ends_with("/v1.5.6/libduckdb-linux-amd64.zip")
        );
    }

    #[test]
    fn extracts_only_the_named_entry() {
        let d = tempfile::tempdir().unwrap();
        let z = d.path().join("a.zip");
        make_zip(
            &z,
            &[
                ("duckdb.h", b"h"),
                ("libduckdb.so", b"lib"),
                ("../evil", b"x"),
            ],
        );
        let out = tempfile::tempdir().unwrap();
        let p = extract_library(&z, "libduckdb.so", out.path(), 1000).unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"lib");
        assert_eq!(fs::read_dir(out.path()).unwrap().count(), 1);
    }

    #[test]
    fn rejects_zip_slip_and_nested_names() {
        let d = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let z = d.path().join("a.zip");
        make_zip(
            &z,
            &[
                ("../libduckdb.so", b"x"),
                ("sub/libduckdb.so", b"y"),
                ("/abs/libduckdb.so", b"z"),
            ],
        );
        assert!(extract_library(&z, "libduckdb.so", out.path(), 1000).is_err());
        assert!(extract_library(&z, "../libduckdb.so", out.path(), 1000).is_err());
        assert!(extract_library(&z, "sub/libduckdb.so", out.path(), 1000).is_err());
        assert_eq!(fs::read_dir(out.path()).unwrap().count(), 0);
        assert!(!d.path().parent().unwrap().join("libduckdb.so").exists());
    }

    #[test]
    fn enforces_the_size_limit() {
        let d = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let z = d.path().join("a.zip");
        make_zip(&z, &[("libduckdb.so", &vec![0u8; 5000])]);
        assert!(
            extract_library(&z, "libduckdb.so", out.path(), 4000)
                .unwrap_err()
                .contains("limit")
        );
    }

    async fn serve(body: Vec<u8>) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 2048];
                    let _ = s.read(&mut buf).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = s.write_all(head.as_bytes()).await;
                    let _ = s.write_all(&body).await;
                });
            }
        });
        format!("http://{addr}/libduckdb-test.zip")
    }

    fn zip_bytes() -> Vec<u8> {
        let d = tempfile::tempdir().unwrap();
        let z = d.path().join("a.zip");
        make_zip(&z, &[("libduckdb.so", b"fake library")]);
        fs::read(z).unwrap()
    }

    #[tokio::test]
    async fn a_checksum_mismatch_is_refused_and_leaves_nothing() {
        let url = serve(zip_bytes()).await;
        let root = tempfile::tempdir().unwrap();
        let err = install_from(
            root.path(),
            &url,
            &"0".repeat(64),
            100,
            "libduckdb.so",
            false,
            &NullReporter,
        )
        .await
        .unwrap_err();
        assert!(err.contains("Checksum mismatch"), "{err}");
        assert!(!cache_dir(root.path()).join("libduckdb.so").exists());
        let leftovers: Vec<_> = fs::read_dir(root.path().join("drivers/duckdb"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !leftovers.iter().any(|n| n.starts_with(".tmp")),
            "{leftovers:?}"
        );
    }

    #[tokio::test]
    async fn stale_scratch_directories_are_cleaned_and_fresh_ones_kept() {
        let body = zip_bytes();
        let sha = hex::encode(Sha256::digest(&body));
        let url = serve(body).await;
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("drivers").join("duckdb");
        fs::create_dir_all(&parent).unwrap();
        let old = parent.join(".tmp-99999-1.0.0");
        let fresh = parent.join(".tmp-88888-1.0.0");
        let unrelated = parent.join("keep-me");
        for dir in [&old, &fresh, &unrelated] {
            fs::create_dir_all(dir).unwrap();
            fs::write(dir.join("partial.zip"), b"x").unwrap();
        }
        // `filetime` sets the time of a directory on every platform (`File::open(dir)` +
        // `set_modified` fails on Windows).
        let two_hours_ago = filetime::FileTime::from_system_time(
            std::time::SystemTime::now() - Duration::from_secs(7200),
        );
        filetime::set_file_mtime(&old, two_hours_ago).unwrap();
        filetime::set_file_mtime(&unrelated, two_hours_ago).unwrap();
        install_from(
            root.path(),
            &url,
            &sha,
            100,
            "libduckdb.so",
            false,
            &NullReporter,
        )
        .await
        .unwrap();
        assert!(!old.exists(), "the stale scratch directory stays");
        assert!(
            fresh.exists(),
            "a recent scratch directory is another install's"
        );
        assert!(unrelated.exists(), "only `.tmp-*` directories are removed");
    }

    /// A server answering every request with a redirect to `location`.
    async fn redirect_to(location: String) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                let head = format!(
                    "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 2048];
                    let _ = s.read(&mut buf).await;
                    let _ = s.write_all(head.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}/libduckdb-test.zip")
    }

    #[tokio::test]
    async fn a_redirect_to_plain_http_is_refused() {
        let body = zip_bytes();
        let sha = hex::encode(Sha256::digest(&body));
        // The target serves a valid archive with the right checksum, so only the scheme rule
        // can make the install fail.
        let target = serve(body).await;
        let url = redirect_to(target).await;
        let root = tempfile::tempdir().unwrap();
        let err = install_from(
            root.path(),
            &url,
            &sha,
            100,
            "libduckdb.so",
            false,
            &NullReporter,
        )
        .await
        .unwrap_err();
        assert!(err.contains("non-https"), "{err}");
        assert!(!cache_dir(root.path()).join("libduckdb.so").exists());
    }

    #[tokio::test]
    async fn parallel_first_uses_install_once_without_racing() {
        let body = zip_bytes();
        let sha = hex::encode(Sha256::digest(&body));
        let url = serve(body).await;
        let root = tempfile::tempdir().unwrap();
        let mut handles = Vec::new();
        for _ in 0..6 {
            let (r, u, s) = (root.path().to_path_buf(), url.clone(), sha.clone());
            handles.push(tokio::spawn(async move {
                install_from(&r, &u, &s, 100, "libduckdb.so", false, &NullReporter).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(
            fs::read(cache_dir(root.path()).join("libduckdb.so")).unwrap(),
            b"fake library"
        );
        let entries: Vec<_> = fs::read_dir(root.path().join("drivers/duckdb"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !entries.iter().any(|n| n.starts_with(".tmp")),
            "{entries:?}"
        );
    }
}
