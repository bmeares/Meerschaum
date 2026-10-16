//! Shared pieces of the plugin lifecycle tests: an isolated home, a synthetic wheel, plugin
//! archives, a loopback-only plugin repository and the native demo plugin.
//!
//! Everything is synthetic and offline: the installers are pointed at a local wheel directory
//! (`UV_NO_INDEX` / `PIP_NO_INDEX` / `*_FIND_LINKS`) and told they are offline, and the
//! repository is a thread listening on `127.0.0.1` on a random port (never 8989).
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// The repository root (what `PYTHONPATH` points the Python worker at).
pub fn repo_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.canonicalize().unwrap_or(root)
}

/// What a run printed.
pub struct Run {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
}

impl Run {
    /// Both streams, ANSI codes removed.
    pub fn text(&self) -> String {
        strip_ansi(&format!("{}{}", self.stdout, self.stderr))
    }

    /// [`Run::text`] with runs of whitespace collapsed (the formatter wraps long lines).
    pub fn flat(&self) -> String {
        self.text().split_whitespace().collect::<Vec<_>>().join(" ")
    }
}

fn strip_ansi(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// An isolated root and plugins directory, a wheel directory and the environment to run in.
pub struct Home {
    _dir: tempfile::TempDir,
    pub root: PathBuf,
    pub plugins: PathBuf,
    pub wheels: PathBuf,
}

impl Home {
    pub fn new() -> Home {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let root = base.join("root");
        let plugins = base.join("plugins");
        let wheels = base.join("wheels");
        for path in [&root, &plugins, &wheels] {
            std::fs::create_dir_all(path).unwrap();
        }
        let home = Home {
            _dir: dir,
            root,
            plugins,
            wheels,
        };
        write_wheel(&home.wheels, "mrsm_syn_pkg", "1.0.0");
        home
    }

    pub fn write_plugin(&self, file: &str, text: &str) {
        let path = self.plugins.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    /// The environment a run gets: nothing but what is listed here.
    pub fn env(&self) -> Vec<(String, String)> {
        let wheels = self.wheels.to_string_lossy().into_owned();
        let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
        vec![
            pair("PATH", &std::env::var("PATH").unwrap_or_default()),
            pair("HOME", &self.root.to_string_lossy()),
            pair("PYTHONPATH", &repo_root().to_string_lossy()),
            pair("MRSM_ROOT_DIR", &self.root.to_string_lossy()),
            pair("MRSM_PLUGINS_DIR", &self.plugins.to_string_lossy()),
            pair("MRSM_NO_UPDATE_CHECK", "1"),
            pair("MRSM_NOASK", "1"),
            pair("UV_OFFLINE", "1"),
            pair("UV_NO_INDEX", "1"),
            pair("UV_FIND_LINKS", &wheels),
            pair("PIP_NO_INDEX", "1"),
            pair("PIP_FIND_LINKS", &wheels),
        ]
    }

    pub fn run(&self, args: &[&str]) -> Run {
        self.run_with(args, &[])
    }

    /// Run `mrsm <args>` with `extra` environment variables (they replace same-named ones).
    pub fn run_with(&self, args: &[&str], extra: &[(&str, &str)]) -> Run {
        let mut env = self.env();
        for (key, value) in extra {
            env.retain(|(k, _)| k != key);
            env.push((key.to_string(), value.to_string()));
        }
        run_binary(args, &env, None)
    }

    /// `venvs/<name>/lib/python*/site-packages` of an installed venv.
    pub fn site_packages(&self, venv: &str) -> Option<PathBuf> {
        let lib = self.root.join("venvs").join(venv).join("lib");
        std::fs::read_dir(lib)
            .ok()?
            .flatten()
            .map(|e| e.path().join("site-packages"))
            .find(|p| p.is_dir())
    }

    pub fn has_package(&self, venv: &str, package: &str) -> bool {
        self.site_packages(venv)
            .is_some_and(|site| site.join(package).join("__init__.py").is_file())
    }
}

pub fn run_binary(args: &[&str], env: &[(String, String)], cwd: Option<&Path>) -> Run {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm"));
    command
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output: Output = command.output().unwrap();
    Run {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        success: output.status.success(),
    }
}

/// A wheel with one package, written as an uncompressed zip (no Python needed).
pub fn write_wheel(dir: &Path, name: &str, version: &str) -> PathBuf {
    let dist_info = format!("{name}-{version}.dist-info");
    let files: Vec<(String, Vec<u8>)> = vec![
        (
            format!("{name}/__init__.py"),
            b"VALUE = 'synthetic'\n".to_vec(),
        ),
        (
            format!("{dist_info}/METADATA"),
            format!(
                "Metadata-Version: 2.1\nName: {}\nVersion: {version}\n",
                name.replace('_', "-")
            )
            .into_bytes(),
        ),
        (
            format!("{dist_info}/WHEEL"),
            b"Wheel-Version: 1.0\nGenerator: synthetic\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
                .to_vec(),
        ),
        (format!("{dist_info}/RECORD"), Vec::new()),
    ];
    let path = dir.join(format!("{name}-{version}-py3-none-any.whl"));
    std::fs::write(&path, zip_stored(&files)).unwrap();
    path
}

/// A zip archive with every entry stored (not compressed).
fn zip_stored(files: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for (name, data) in files {
        let mut crc = flate2::Crc::new();
        crc.update(data);
        let offset = out.len() as u32;
        let le32 = |v: u32| v.to_le_bytes();
        let le16 = |v: u16| v.to_le_bytes();
        out.extend(le32(0x04034b50));
        out.extend(le16(20));
        out.extend(le16(0));
        out.extend(le16(0));
        out.extend(le16(0));
        out.extend(le16(0x21));
        out.extend(le32(crc.sum()));
        out.extend(le32(data.len() as u32));
        out.extend(le32(data.len() as u32));
        out.extend(le16(name.len() as u16));
        out.extend(le16(0));
        out.extend(name.as_bytes());
        out.extend(data);
        central.extend(le32(0x02014b50));
        central.extend(le16(20));
        central.extend(le16(20));
        central.extend(le16(0));
        central.extend(le16(0));
        central.extend(le16(0));
        central.extend(le16(0x21));
        central.extend(le32(crc.sum()));
        central.extend(le32(data.len() as u32));
        central.extend(le32(data.len() as u32));
        central.extend(le16(name.len() as u16));
        central.extend(le16(0));
        central.extend(le16(0));
        central.extend(le16(0));
        central.extend(le16(0));
        central.extend(le32(0));
        central.extend(le32(offset));
        central.extend(name.as_bytes());
    }
    let central_offset = out.len() as u32;
    out.extend(&central);
    out.extend(0x06054b50u32.to_le_bytes());
    out.extend([0u8; 4]);
    out.extend((files.len() as u16).to_le_bytes());
    out.extend((files.len() as u16).to_le_bytes());
    out.extend((central.len() as u32).to_le_bytes());
    out.extend(central_offset.to_le_bytes());
    out.extend([0u8; 2]);
    out
}

/// A plugin archive: `(path inside the archive, text)` entries, gzip-compressed.
pub fn archive(entries: &[(&str, &str)]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, text) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(text.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(1_700_000_000);
        header.set_cksum();
        builder
            .append_data(&mut header, name, text.as_bytes())
            .unwrap();
    }
    gzip(&builder.into_inner().unwrap())
}

pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

/// A tar.gz whose only member escapes the extraction directory.
pub fn traversal_archive() -> Vec<u8> {
    let mut header = tar::Header::new_gnu();
    header.as_old_mut().name[..11].copy_from_slice(b"../evil.txt");
    header.set_size(1);
    header.set_mode(0o644);
    header.set_cksum();
    let mut builder = tar::Builder::new(Vec::new());
    builder.append(&header, &b"x"[..]).unwrap();
    gzip(&builder.into_inner().unwrap())
}

/// A plugin repository on `127.0.0.1` serving `GET /plugins/<name>` like the API does: the
/// archive when there is one, else `200` with `[false, "Archive for plugin ... not found."]`.
pub struct Repo {
    pub port: u16,
    archives: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
}

impl Repo {
    pub fn start() -> Repo {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        assert_ne!(port, 8989);
        let repo = Repo {
            port,
            archives: Arc::default(),
            requests: Arc::default(),
            stop: Arc::default(),
        };
        let (archives, requests, stop) = (
            repo.archives.clone(),
            repo.requests.clone(),
            repo.stop.clone(),
        );
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                serve(stream, &archives, &requests);
            }
        });
        repo
    }

    /// Publish (or replace) a plugin archive.
    pub fn publish(&self, name: &str, archive: Vec<u8>) {
        self.archives
            .lock()
            .unwrap()
            .insert(name.to_string(), archive);
    }

    /// `MRSM_API_<LABEL>` for this repository.
    pub fn connector(&self, label: &str) -> (String, String) {
        (
            format!("MRSM_API_{}", label.to_uppercase()),
            format!("http://127.0.0.1:{}", self.port),
        )
    }

    /// The request paths received so far.
    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    pub fn count(&self, path: &str) -> usize {
        self.requests().iter().filter(|p| *p == path).count()
    }
}

impl Drop for Repo {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
    }
}

fn serve(
    mut stream: TcpStream,
    archives: &Mutex<HashMap<String, Vec<u8>>>,
    requests: &Mutex<Vec<String>>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buffer.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
        }
    }
    let head = String::from_utf8_lossy(&buffer).into_owned();
    let path = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_string();
    requests.lock().unwrap().push(path.clone());
    let name = path.rsplit('/').next().unwrap_or_default().to_string();
    let (content_type, body) = match archives.lock().unwrap().get(&name) {
        Some(bytes) => ("application/x-tar", bytes.clone()),
        None => (
            "application/json",
            serde_json::json!([
                false,
                format!("Archive for plugin '{name}' could not be found.")
            ])
            .to_string()
            .into_bytes(),
        ),
    };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(&body);
}

/// A TCP port nobody listens on.
pub fn closed_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// The built `native_demo` example library (a C-ABI plugin with no `setup`).
pub fn native_demo_library() -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            .args([
                "build",
                "-p",
                "meerschaum-plugin",
                "--example",
                "native_demo",
            ])
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
    });
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}native_demo.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

/// Install the native demo plugin into `plugins`.
pub fn install_native_demo(plugins: &Path) {
    let dir = plugins.join("native_demo");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("plugin.toml"),
        include_str!("../../../meerschaum-plugin/examples/plugin.toml"),
    )
    .unwrap();
    std::fs::copy(
        native_demo_library(),
        dir.join(meerschaum_plugin::discovery::library_name("native_demo")),
    )
    .unwrap();
}
