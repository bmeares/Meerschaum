//! Native HTTPS for the API server: `start api --keyfile KEY --certfile CERT`, or
//! `api:server:ssl_keyfile` / `api:server:ssl_certfile` (4.1's `api:uvicorn:ssl_keyfile` /
//! `ssl_certfile` keep working, with a notice).
//!
//! The server terminates TLS itself with [rustls](https://docs.rs/rustls) (no OpenSSL) and
//! negotiates HTTP/2 or HTTP/1.1 by ALPN. Without a key and a certificate nothing here runs: the
//! server speaks plain HTTP exactly as before.
//!
//! - [`TlsSettings`] say where the files come from; [`TlsSettings::files`] refuses a key without a
//!   certificate (or the reverse) and says how to fix it.
//! - [`ServerCertificate`] reads and checks the PEM files (PKCS#8, PKCS#1 RSA and SEC1 EC keys; the
//!   server's own certificate followed by its intermediates) and is what every handshake presents.
//!   It is reloaded, without a restart and without dropping connections, when the files change
//!   (they are read again every [`DEFAULT_RELOAD_INTERVAL`]) and on `SIGHUP`. A replacement that
//!   does not load (a renewal caught half-written, a key that does not match) keeps the previous
//!   certificate in service.
//! - [`prepare`] makes HTTPS ready before the server starts: it loads the certificate and binds the
//!   **internal listener**, plain HTTP on a port of its own on `127.0.0.1` (never exposed), which
//!   the server's own callers use: the Python API host's calls back into the server and the jobs
//!   it starts (plugin installs from this server). Its address goes into the host's configuration
//!   through the `internal_addr` field of [`ServerConfig`].
//! - [`serve`] runs the HTTPS listener and the internal one. Plain HTTP that reaches the HTTPS port
//!   gets `/healthcheck` (and `/healthcheck/ready`) answered, so container healthchecks such as
//!   `curl --fail http://localhost:8000/healthcheck` keep working, and a redirect to `https://`
//!   for everything else. [`serve_until`] also drains (see [`crate::lifecycle`]).
//!
//! HTTP/2 requests carry their authority in the URI, not in a `Host` header; [`serve`] adds the
//! header so routes and the API host see the same request over HTTP/1.1 and HTTP/2. WebSockets use
//! HTTP/1.1 (the server does not advertise HTTP/2's extended CONNECT, so browsers open their
//! websocket on a connection of its own).

use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{HeaderValue, Method, Request, Response, StatusCode, Version, header};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use meerschaum_core::{Map, Value};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::{PemObject, SectionKind};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use crate::config::ServerConfig;

/// How often the key and certificate files are read again to pick up a renewal.
pub const DEFAULT_RELOAD_INTERVAL: Duration = Duration::from_secs(30);

/// How long a client has to send its first byte and finish the TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The protocols offered by ALPN, preferred first.
pub const ALPN_PROTOCOLS: &[&[u8]] = &[b"h2", b"http/1.1"];

/// The path plain HTTP may still reach on the HTTPS port (container healthchecks).
pub const HEALTHCHECK_PATH: &str = "/healthcheck";

/// The readiness check plain HTTP may also reach on the HTTPS port (load balancers' probes).
pub const READY_PATH: &str = "/healthcheck/ready";

/// The first byte of every TLS connection (a handshake record).
const TLS_HANDSHAKE_RECORD: u8 = 0x16;

// ------------------------------------------------------------------------------------------------
// Settings

/// One file of the pair and where it was set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPath {
    /// The absolute path.
    pub path: PathBuf,
    /// Where it was set: `` `--keyfile` ``, `` `api:server:ssl_keyfile` `` or
    /// `` `api:uvicorn:ssl_keyfile` `` (and the same for the certificate).
    pub source: String,
}

impl std::fmt::Display for TlsPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.path.display(), self.source)
    }
}

/// Where the private key and the certificate come from (either may be missing).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsSettings {
    /// The private key (`--keyfile`, `api:server:ssl_keyfile`).
    pub keyfile: Option<TlsPath>,
    /// The certificate chain (`--certfile`, `api:server:ssl_certfile`).
    pub certfile: Option<TlsPath>,
}

/// Both files of a complete pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// The private key.
    pub keyfile: TlsPath,
    /// The certificate chain: the server's certificate first, then its intermediates.
    pub certfile: TlsPath,
}

fn config_get<'a>(map: &'a Map, keys: &[&str]) -> Option<&'a Value> {
    let mut cur = map.get(*keys.first()?)?;
    for key in &keys[1..] {
        cur = cur.get(key)?;
    }
    Some(cur)
}

/// A configured path: `~/` expanded, made absolute against the current directory (so a reload
/// never depends on where the process is later).
fn absolute(path: &str) -> PathBuf {
    let path = match path.strip_prefix("~/") {
        Some(rest) => match std::env::var_os("HOME").filter(|h| !h.is_empty()) {
            Some(home) => PathBuf::from(home).join(rest),
            None => PathBuf::from(path),
        },
        None => PathBuf::from(path),
    };
    std::path::absolute(&path).unwrap_or(path)
}

impl TlsSettings {
    /// Resolve the files: a flag wins (`--keyfile ''` turns off a key set in the config), then
    /// `api:server:<key>` (present but empty or null: none, without looking further), then 4.1's
    /// `api:uvicorn:<key>`, which adds a deprecation notice to `notices`.
    pub fn resolve(
        keyfile_flag: Option<&str>,
        certfile_flag: Option<&str>,
        cfg: &Map,
        notices: &mut Vec<String>,
    ) -> TlsSettings {
        let mut pick = |flag: Option<&str>, option: &str, key: &str| -> Option<TlsPath> {
            if let Some(value) = flag {
                let value = value.trim();
                return (!value.is_empty()).then(|| TlsPath {
                    path: absolute(value),
                    source: format!("`{option}`"),
                });
            }
            for section in ["server", "uvicorn"] {
                let Some(value) = config_get(cfg, &["api", section, key]) else {
                    continue;
                };
                let text = match value {
                    Value::Null => String::new(),
                    other => other.py_str().trim().to_string(),
                };
                if text.is_empty() {
                    // `api:server:<key>: null` overrides an `api:uvicorn` value.
                    if section == "server" {
                        return None;
                    }
                    continue;
                }
                if section == "uvicorn" {
                    notices.push(format!(
                        "The config key 'api:uvicorn:{key}' is deprecated; use 'api:server:{key}'."
                    ));
                }
                return Some(TlsPath {
                    path: absolute(&text),
                    source: format!("`api:{section}:{key}`"),
                });
            }
            None
        };
        TlsSettings {
            keyfile: pick(keyfile_flag, "--keyfile", "ssl_keyfile"),
            certfile: pick(certfile_flag, "--certfile", "ssl_certfile"),
        }
    }

    /// Whether either file is set (HTTPS was asked for).
    pub fn is_requested(&self) -> bool {
        self.keyfile.is_some() || self.certfile.is_some()
    }

    /// The pair: `None` for plain HTTP, an error when only one of the two is set.
    pub fn files(&self) -> Result<Option<TlsFiles>, TlsError> {
        match (&self.keyfile, &self.certfile) {
            (None, None) => Ok(None),
            (Some(keyfile), Some(certfile)) => Ok(Some(TlsFiles {
                keyfile: keyfile.clone(),
                certfile: certfile.clone(),
            })),
            (Some(keyfile), None) => Err(TlsError::Incomplete(format!(
                "HTTPS needs a private key and its certificate, but only the key is set ({keyfile}). \
                 Set the certificate too (`--certfile`, or `api:server:ssl_certfile`), or neither \
                 to serve plain HTTP."
            ))),
            (None, Some(certfile)) => Err(TlsError::Incomplete(format!(
                "HTTPS needs a private key and its certificate, but only the certificate is set \
                 ({certfile}). Set the key too (`--keyfile`, or `api:server:ssl_keyfile`), or \
                 neither to serve plain HTTP."
            ))),
        }
    }
}

/// Why HTTPS cannot be served: the message says what is wrong and how to fix it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsError {
    /// Only one of the key and the certificate is set.
    Incomplete(String),
    /// A file cannot be read.
    Read(String),
    /// A file holds no usable key or certificate, or they do not belong together.
    Invalid(String),
    /// The internal listener cannot be bound.
    Listener(String),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Incomplete(m)
            | TlsError::Read(m)
            | TlsError::Invalid(m)
            | TlsError::Listener(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for TlsError {}

// ------------------------------------------------------------------------------------------------
// Loading

/// The crypto provider: aws-lc-rs, explicitly (rustls' process default is ambiguous when another
/// crate in the tree enables its `ring` feature too).
fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn read_file(file: &TlsPath, what: &str) -> Result<Vec<u8>, TlsError> {
    std::fs::read(&file.path).map_err(|e| {
        let reason = match e.kind() {
            std::io::ErrorKind::NotFound => "the file does not exist".to_string(),
            std::io::ErrorKind::PermissionDenied => "permission denied: this user may not read \
                 it (Let's Encrypt's private keys are readable only by root: give this user read \
                 access, for example through a group, or copy the files where it can read them)"
                .to_string(),
            std::io::ErrorKind::IsADirectory => {
                "it is a directory: point to the file itself".to_string()
            }
            _ => e.to_string(),
        };
        TlsError::Read(format!("Cannot read the TLS {what} {file}: {reason}."))
    })
}

/// The PEM sections rustls knows (certificates, keys, ...), in file order.
fn pem_sections(
    bytes: &[u8],
    file: &TlsPath,
    what: &str,
) -> Result<Vec<(SectionKind, Vec<u8>)>, TlsError> {
    let mut sections = Vec::new();
    for item in <(SectionKind, Vec<u8>) as PemObject>::pem_slice_iter(bytes) {
        match item {
            Ok(section) => sections.push(section),
            Err(e) => {
                let detail = match e {
                    rustls::pki_types::pem::Error::MissingSectionEnd { .. } => {
                        "a block has no `-----END ...-----` line (is the file cut short?)"
                            .to_string()
                    }
                    rustls::pki_types::pem::Error::IllegalSectionStart { .. } => {
                        "a `-----BEGIN ...-----` line is malformed".to_string()
                    }
                    rustls::pki_types::pem::Error::Base64Decode(_) => {
                        "a block's base64 content is corrupt".to_string()
                    }
                    other => other.to_string(),
                };
                return Err(TlsError::Invalid(format!(
                    "The TLS {what} file {file} is not valid PEM: {detail}."
                )));
            }
        }
    }
    Ok(sections)
}

fn is_key(kind: SectionKind) -> bool {
    matches!(
        kind,
        SectionKind::PrivateKey | SectionKind::RsaPrivateKey | SectionKind::EcPrivateKey
    )
}

/// Why a file has nothing of what was looked for: empty, binary (DER), or (`other`) the other
/// half of the pair.
fn missing(
    bytes: &[u8],
    file: &TlsPath,
    what: &str,
    other: Option<&str>,
    none: String,
) -> TlsError {
    let convert = if what == "certificate" {
        "x509"
    } else {
        "pkey"
    };
    let path = file.path.display();
    let message = if bytes.iter().all(u8::is_ascii_whitespace) {
        format!("The TLS {what} file {file} is empty.")
    } else if bytes.first() == Some(&0x30) {
        format!(
            "The TLS {what} file {file} is binary (DER), not PEM. Convert it with \
             `openssl {convert} -inform der -in {path} -out {path}.pem` and use the `.pem` file."
        )
    } else if let Some(other) = other {
        format!(
            "The TLS {what} file {file} holds {other} and no {what}: are the key and the \
             certificate (`--keyfile` and `--certfile`, or `ssl_keyfile` and `ssl_certfile`) \
             swapped?"
        )
    } else {
        none
    };
    TlsError::Invalid(message)
}

/// The certificate chain of a PEM file.
fn certificates(bytes: &[u8], file: &TlsPath) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let sections = pem_sections(bytes, file, "certificate")?;
    let certs: Vec<CertificateDer<'static>> = sections
        .iter()
        .filter(|(kind, _)| *kind == SectionKind::Certificate)
        .map(|(_, der)| CertificateDer::from(der.clone()))
        .collect();
    if certs.is_empty() {
        let other = sections
            .iter()
            .any(|(kind, _)| is_key(*kind))
            .then_some("a private key");
        return Err(missing(
            bytes,
            file,
            "certificate",
            other,
            format!(
                "The TLS certificate file {file} holds no certificate (no \
                 `-----BEGIN CERTIFICATE-----` block)."
            ),
        ));
    }
    Ok(certs)
}

/// The private key of a PEM file (the first one): PKCS#8, PKCS#1 (RSA) or SEC1 (EC).
fn private_key(bytes: &[u8], file: &TlsPath) -> Result<PrivateKeyDer<'static>, TlsError> {
    // An encrypted key is skipped (PKCS#8) or unreadable (OpenSSL's legacy `Proc-Type` headers)
    // by the PEM reader: say what it is before reading.
    let text = String::from_utf8_lossy(bytes);
    if text.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----")
        || text.contains("Proc-Type: 4,ENCRYPTED")
    {
        return Err(TlsError::Invalid(format!(
            "The TLS private key in {file} is encrypted with a passphrase, which the server \
             cannot ask for. Write an unencrypted copy that only the server's user can read: \
             `openssl pkey -in {} -out key.pem` (then `chmod 600 key.pem`).",
            file.path.display()
        )));
    }
    let sections = pem_sections(bytes, file, "private key")?;
    let key = sections.iter().find_map(|(kind, der)| {
        let der = der.clone();
        match kind {
            SectionKind::PrivateKey => Some(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der))),
            SectionKind::RsaPrivateKey => Some(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(der))),
            SectionKind::EcPrivateKey => Some(PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(der))),
            _ => None,
        }
    });
    key.ok_or_else(|| {
        let other = sections
            .iter()
            .any(|(kind, _)| *kind == SectionKind::Certificate)
            .then_some("a certificate");
        missing(
            bytes,
            file,
            "private key",
            other,
            format!(
                "The TLS private key file {file} holds no private key (no `-----BEGIN PRIVATE \
                 KEY-----`, `RSA PRIVATE KEY` or `EC PRIVATE KEY` block)."
            ),
        )
    })
}

/// A loaded pair: what handshakes present, what the logs say about it, and the digest of the
/// files it came from (to see whether they changed).
#[derive(Debug, Clone)]
struct Loaded {
    key: Arc<CertifiedKey>,
    summary: CertificateSummary,
    digest: [u8; 32],
}

fn digest(key: &[u8], cert: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((key.len() as u64).to_le_bytes());
    hasher.update(key);
    hasher.update(cert);
    hasher.finalize().into()
}

/// Check and assemble the pair from the files' bytes.
fn assemble(files: &TlsFiles, key_bytes: &[u8], cert_bytes: &[u8]) -> Result<Loaded, TlsError> {
    let digest = digest(key_bytes, cert_bytes);
    let key = private_key(key_bytes, &files.keyfile)?;
    let certs = certificates(cert_bytes, &files.certfile)?;
    let signing_key = provider().key_provider.load_private_key(key).map_err(|e| {
        TlsError::Invalid(format!(
            "The TLS private key in {} cannot be used ({e}): use an RSA key of 2048 bits or \
                 more, an ECDSA P-256, P-384 or P-521 key, or an Ed25519 key.",
            files.keyfile
        ))
    })?;
    let certified = CertifiedKey::new(certs.clone(), signing_key);
    match certified.keys_match() {
        // The key cannot tell its public half: nothing to compare (rustls accepts it too).
        Ok(()) | Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::Unknown)) => {}
        Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::KeyMismatch)) => {
            let position = certs.iter().position(|cert| {
                CertifiedKey::new(vec![cert.clone()], certified.key.clone())
                    .keys_match()
                    .is_ok()
            });
            let hint = match position {
                Some(index) => format!(
                    " It belongs to certificate #{} of that file: the server's own certificate \
                     must come first, followed by its intermediates.",
                    index + 1
                ),
                None => " Use the key the certificate was issued for.".to_string(),
            };
            return Err(TlsError::Invalid(format!(
                "The TLS private key {} does not belong to the certificate {}.{hint}",
                files.keyfile, files.certfile
            )));
        }
        Err(e) => {
            return Err(TlsError::Invalid(format!(
                "The TLS certificate in {} cannot be read: {e}.",
                files.certfile
            )));
        }
    }
    let summary = CertificateSummary::from_chain(&certs);
    Ok(Loaded {
        key: Arc::new(certified),
        summary,
        digest,
    })
}

/// What the logs say about a certificate: the names it is for and its validity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CertificateSummary {
    /// The DNS names and addresses of the server's certificate (else its subject's common name).
    pub names: Vec<String>,
    /// The first moment it is valid.
    pub not_before: Option<DateTime<Utc>>,
    /// The last moment it is valid.
    pub not_after: Option<DateTime<Utc>>,
    /// The certificates in the file (the server's and its intermediates).
    pub chain: usize,
}

impl CertificateSummary {
    /// Read the server's certificate (the first of `chain`). Fields that cannot be read stay
    /// empty: the summary is for messages only.
    pub fn from_chain(chain: &[CertificateDer<'_>]) -> CertificateSummary {
        let mut summary = chain
            .first()
            .and_then(|cert| der::summarize(cert.as_ref()))
            .unwrap_or_default();
        summary.chain = chain.len();
        summary
    }

    /// `for example.com, www.example.com, valid until 2027-01-25`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if !self.names.is_empty() {
            const SHOWN: usize = 3;
            let mut names = self.names[..self.names.len().min(SHOWN)].join(", ");
            if self.names.len() > SHOWN {
                names.push_str(&format!(" and {} more", self.names.len() - SHOWN));
            }
            parts.push(format!("for {names}"));
        }
        if let Some(until) = self.not_after {
            parts.push(format!("valid until {}", until.format("%Y-%m-%d")));
        }
        parts.join(", ")
    }

    /// The warning for a certificate clients refuse at `now`: expired or not valid yet.
    pub fn problem(&self, file: &TlsPath, now: DateTime<Utc>) -> Option<String> {
        if let Some(until) = self.not_after.filter(|until| *until < now) {
            return Some(format!(
                "The TLS certificate in {file} expired on {}: browsers and clients refuse it. \
                 Renew it and replace the file (the server picks the new one up without a \
                 restart).",
                until.format("%Y-%m-%d")
            ));
        }
        if let Some(from) = self.not_before.filter(|from| *from > now) {
            return Some(format!(
                "The TLS certificate in {file} is not valid before {}: clients refuse it until \
                 then (is this machine's clock right?).",
                from.format("%Y-%m-%d %H:%M UTC")
            ));
        }
        None
    }
}

mod der;

// ------------------------------------------------------------------------------------------------
// The served certificate

/// What [`ServerCertificate::reload`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reload {
    /// The files hold what is already served.
    Unchanged,
    /// The new certificate is served from now on.
    Reloaded(CertificateSummary),
}

/// The certificate every handshake presents (a rustls certificate resolver), replaceable while
/// the server runs.
pub struct ServerCertificate {
    files: TlsFiles,
    current: RwLock<Loaded>,
}

impl std::fmt::Debug for ServerCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerCertificate")
            .field("files", &self.files)
            .field("summary", &self.summary())
            .finish()
    }
}

impl ServerCertificate {
    /// Read and check both files.
    pub fn load(files: TlsFiles) -> Result<ServerCertificate, TlsError> {
        let key = read_file(&files.keyfile, "private key")?;
        let cert = read_file(&files.certfile, "certificate")?;
        let loaded = assemble(&files, &key, &cert)?;
        Ok(ServerCertificate {
            files,
            current: RwLock::new(loaded),
        })
    }

    /// The files it is read from.
    pub fn files(&self) -> &TlsFiles {
        &self.files
    }

    fn current(&self) -> Loaded {
        self.current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The names and validity of the certificate served now.
    pub fn summary(&self) -> CertificateSummary {
        self.current().summary
    }

    /// The certificate chain served now (DER, the server's certificate first).
    pub fn chain(&self) -> Vec<CertificateDer<'static>> {
        self.current().key.cert.clone()
    }

    /// Read the files again and serve what they hold when it changed. On an error the previous
    /// certificate stays in service.
    pub fn reload(&self) -> Result<Reload, TlsError> {
        let key = read_file(&self.files.keyfile, "private key")?;
        let cert = read_file(&self.files.certfile, "certificate")?;
        if digest(&key, &cert) == self.current().digest {
            return Ok(Reload::Unchanged);
        }
        let loaded = assemble(&self.files, &key, &cert)?;
        let summary = loaded.summary.clone();
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = loaded;
        Ok(Reload::Reloaded(summary))
    }
}

impl ResolvesServerCert for ServerCertificate {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(
            self.current
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .key
                .clone(),
        )
    }
}

/// The rustls configuration of the HTTPS listener: TLS 1.3 and 1.2, ALPN `h2` then `http/1.1`,
/// and the certificate `certificate` serves at the time of each handshake.
pub fn rustls_config(
    certificate: Arc<ServerCertificate>,
) -> Result<Arc<rustls::ServerConfig>, TlsError> {
    let mut config = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Invalid(format!("The TLS configuration is invalid: {e}.")))?
        .with_no_client_auth()
        .with_cert_resolver(certificate);
    config.alpn_protocols = ALPN_PROTOCOLS.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(config))
}

// ------------------------------------------------------------------------------------------------
// Events

/// A message about the certificate while the server runs (a reload, a reload that failed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsEvent {
    /// Something happened as expected.
    Info(String),
    /// Something needs the operator's attention.
    Warning(String),
}

impl TlsEvent {
    /// The message.
    pub fn message(&self) -> &str {
        match self {
            TlsEvent::Info(m) | TlsEvent::Warning(m) => m,
        }
    }
}

/// Where [`TlsEvent`]s go (the CLI prints them like its other messages; without one they are
/// `tracing` events).
pub type TlsEvents = Arc<dyn Fn(TlsEvent) + Send + Sync>;

fn emit(events: &Option<TlsEvents>, event: TlsEvent) {
    match events {
        Some(sink) => sink(event),
        None => match &event {
            TlsEvent::Info(m) => tracing::info!("{m}"),
            TlsEvent::Warning(m) => tracing::warn!("{m}"),
        },
    }
}

// ------------------------------------------------------------------------------------------------
// Preparing and serving

/// HTTPS made ready before the server starts ([`prepare`]).
pub struct Https {
    certificate: Arc<ServerCertificate>,
    config: Arc<rustls::ServerConfig>,
    internal: Option<std::net::TcpListener>,
    events: Option<TlsEvents>,
    reload_interval: Duration,
}

impl std::fmt::Debug for Https {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Https")
            .field("certificate", &self.certificate)
            .field("internal", &self.internal_addr())
            .field("reload_interval", &self.reload_interval)
            .finish_non_exhaustive()
    }
}

/// Bind the internal listener: a free port on `127.0.0.1` (else `[::1]`), never another address.
pub fn bind_internal_listener() -> Result<std::net::TcpListener, TlsError> {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .or_else(|_| std::net::TcpListener::bind((Ipv6Addr::LOCALHOST, 0)))
        .and_then(|listener| {
            listener.set_nonblocking(true)?;
            Ok(listener)
        })
        .map_err(|e| {
            TlsError::Listener(format!(
                "Cannot open the loopback listener the API host and jobs call the server on: {e}."
            ))
        })?;
    Ok(listener)
}

impl Https {
    /// Load the certificate (no internal listener: see [`Https::with_internal_listener`]).
    pub fn new(files: TlsFiles) -> Result<Https, TlsError> {
        let certificate = Arc::new(ServerCertificate::load(files)?);
        let config = rustls_config(certificate.clone())?;
        Ok(Https {
            certificate,
            config,
            internal: None,
            events: None,
            reload_interval: DEFAULT_RELOAD_INTERVAL,
        })
    }

    /// Also serve the internal listener (plain HTTP on loopback, [`bind_internal_listener`]).
    pub fn with_internal_listener(mut self) -> Result<Https, TlsError> {
        self.internal = Some(bind_internal_listener()?);
        Ok(self)
    }

    /// Send reload messages to `events` instead of `tracing`.
    pub fn with_events(mut self, events: TlsEvents) -> Https {
        self.events = Some(events);
        self
    }

    /// Read the files again every `interval` instead of [`DEFAULT_RELOAD_INTERVAL`].
    pub fn with_reload_interval(mut self, interval: Duration) -> Https {
        self.reload_interval = interval.max(Duration::from_millis(10));
        self
    }

    /// The certificate served.
    pub fn certificate(&self) -> &Arc<ServerCertificate> {
        &self.certificate
    }

    /// The rustls configuration of the HTTPS listener.
    pub fn rustls_config(&self) -> Arc<rustls::ServerConfig> {
        self.config.clone()
    }

    /// Where the internal listener listens.
    pub fn internal_addr(&self) -> Option<SocketAddr> {
        self.internal.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// The address clients use: `https://<name>[:port]`, the first DNS name of the certificate,
    /// else the bind host when it is a specific one, else `localhost`.
    pub fn url(&self, config: &ServerConfig) -> String {
        let summary = self.certificate.summary();
        let named = summary
            .names
            .iter()
            .find(|name| !name.contains('*') && name.parse::<IpAddr>().is_err())
            .cloned();
        let bound = match config.host.trim_matches(['[', ']']) {
            "" | "0.0.0.0" | "::" => None,
            host => Some(host.to_string()),
        };
        let host = named.or(bound).unwrap_or_else(|| "localhost".into());
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host
        };
        if config.port == 443 {
            format!("https://{host}")
        } else {
            format!("https://{host}:{}", config.port)
        }
    }

    /// What `start api` prints when it starts: where it serves HTTPS with which certificate, and
    /// a warning when clients will refuse the certificate.
    pub fn startup_messages(&self, config: &ServerConfig) -> Vec<TlsEvent> {
        let summary = self.certificate.summary();
        let described = summary.describe();
        let mut line = format!("Serving HTTPS on {}", self.url(config));
        if !described.is_empty() {
            line.push_str(&format!(" (certificate {described})"));
        }
        line.push('.');
        let mut messages = vec![TlsEvent::Info(line)];
        if let Some(problem) = summary.problem(&self.certificate.files.certfile, Utc::now()) {
            messages.push(TlsEvent::Warning(problem));
        }
        messages
    }
}

/// Make HTTPS ready for `config` when it asks for it: check the key and certificate, bind the
/// internal listener and record its address in `config` (for the API host's configuration, which
/// must be built after this). `Ok(None)` for plain HTTP.
pub fn prepare(config: &mut ServerConfig) -> Result<Option<Https>, TlsError> {
    let Some(files) = config.tls.files()? else {
        return Ok(None);
    };
    let https = Https::new(files)?.with_internal_listener()?;
    config.internal_addr = https.internal_addr();
    Ok(Some(https))
}

/// The URL the server's own callers on this machine use: the internal listener when HTTPS is
/// served (`http://127.0.0.1:<port>`), else the server's own port on loopback.
pub fn internal_url(config: &ServerConfig) -> String {
    match config.internal_addr {
        Some(addr) => format!("http://{addr}"),
        None => {
            let host = match config.host.as_str() {
                "" | "0.0.0.0" => "127.0.0.1".to_string(),
                "::" | "[::]" => "[::1]".to_string(),
                h if h.contains(':') && !h.starts_with('[') => format!("[{h}]"),
                h => h.to_string(),
            };
            format!("http://{host}:{}", config.port)
        }
    }
}

/// Serve `app` over HTTPS on `listener` (and plain HTTP on the internal listener, when `https`
/// has one) until the future is dropped or a listener fails. The certificate is read again every
/// reload interval and on `SIGHUP`.
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    https: Https,
) -> std::io::Result<()> {
    serve_until(listener, app, https, crate::lifecycle::DrainSignal::never()).await
}

/// [`serve`] until `drain` fires, then drain: both listeners stop accepting, requests in progress
/// finish, idle connections close, and the future completes once no connection is left.
pub async fn serve_until(
    listener: tokio::net::TcpListener,
    app: Router,
    https: Https,
    drain: crate::lifecycle::DrainSignal,
) -> std::io::Result<()> {
    let Https {
        certificate,
        config,
        internal,
        events,
        reload_interval,
    } = https;
    let internal = internal
        .map(tokio::net::TcpListener::from_std)
        .transpose()?;
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    // Every connection of the HTTPS listener holds a sender: once all are dropped, `recv` ends.
    let (connections, mut all_closed) = tokio::sync::mpsc::channel::<()>(1);
    let public = accept_loop(
        listener,
        acceptor,
        app.clone(),
        drain.clone(),
        connections.clone(),
    );
    let internal_drain = drain.clone();
    let internal = async move {
        match internal {
            Some(listener) => {
                axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .with_graceful_shutdown(internal_drain.wait())
                .await
            }
            None => Ok(()),
        }
    };
    let listeners = async { tokio::try_join!(public, internal).map(|_| ()) };
    tokio::select! {
        result = listeners => result?,
        () = reload_loop(certificate, events, reload_interval) => return Ok(()),
    }
    // Drained: wait for the connections of the HTTPS listener.
    drop(connections);
    let _ = all_closed.recv().await;
    Ok(())
}

/// Accept connections until `drain` fires (accept errors are retried, as `axum::serve` does).
async fn accept_loop(
    listener: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    app: Router,
    drain: crate::lifecycle::DrainSignal,
    connections: tokio::sync::mpsc::Sender<()>,
) -> std::io::Result<()> {
    let stop = drain.clone().wait();
    tokio::pin!(stop);
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            // The listener is dropped on return: new connections are refused.
            () = &mut stop => return Ok(()),
        };
        let (tcp, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(e) => {
                if !matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionReset
                ) {
                    tracing::error!("accept error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        let drain = drain.clone();
        let held = connections.clone();
        tokio::spawn(async move {
            connection(tcp, peer, acceptor, app, drain).await;
            drop(held);
        });
    }
}

/// One connection: TLS (served as HTTP/2 or HTTP/1.1 with upgrades), or plain HTTP (the
/// healthcheck and a redirect to `https://`), told apart by the first byte. When `drain` fires
/// it shuts down gracefully (the request in progress finishes, then the connection closes).
async fn connection(
    tcp: tokio::net::TcpStream,
    peer: SocketAddr,
    acceptor: tokio_rustls::TlsAcceptor,
    app: Router,
    drain: crate::lifecycle::DrainSignal,
) {
    let _ = tcp.set_nodelay(true);
    let deadline = tokio::time::Instant::now() + HANDSHAKE_TIMEOUT;
    let mut first = [0u8; 1];
    match tokio::time::timeout_at(deadline, tcp.peek(&mut first)).await {
        Ok(Ok(n)) if n > 0 => {}
        _ => return,
    }
    if first[0] != TLS_HANDSHAKE_RECORD {
        let service = TowerToHyperService::new(plain_service(app, peer));
        let conn =
            hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(tcp), service);
        tokio::pin!(conn);
        let result = tokio::select! {
            result = conn.as_mut() => result,
            () = drain.wait() => {
                conn.as_mut().graceful_shutdown();
                conn.await
            }
        };
        if let Err(e) = result {
            tracing::trace!("plain HTTP connection on the HTTPS port: {e}");
        }
        return;
    }
    let stream = match tokio::time::timeout_at(deadline, acceptor.accept(tcp)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            tracing::debug!("TLS handshake with {peer} failed: {e}");
            return;
        }
        Err(_) => {
            tracing::debug!("TLS handshake with {peer} timed out");
            return;
        }
    };
    let service = TowerToHyperService::new(tls_service(app, peer));
    let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
    tokio::pin!(conn);
    let result = tokio::select! {
        result = conn.as_mut() => result,
        () = drain.wait() => {
            conn.as_mut().graceful_shutdown();
            conn.await
        }
    };
    if let Err(e) = result {
        tracing::trace!("HTTPS connection: {e}");
    }
}

/// The application with the peer address (`ConnectInfo`, as `axum::serve` adds it) and, for
/// HTTP/2, a `Host` header from the request's authority.
fn tls_service(
    app: Router,
    peer: SocketAddr,
) -> impl tower::Service<
    Request<Incoming>,
    Response = Response<Body>,
    Error = Infallible,
    Future = impl Send,
> + Clone
+ Send {
    tower::service_fn(move |mut req: Request<Incoming>| {
        let app = app.clone();
        async move {
            req.extensions_mut().insert(ConnectInfo(peer));
            if req.version() == Version::HTTP_2 {
                authority_as_host(&mut req);
            }
            app.oneshot(req).await
        }
    })
}

/// HTTP/2 names the host in the `:authority` pseudo-header (the request's URI), not in `Host`:
/// add it, so the routes (the web terminal's origin check) and the API host see what HTTP/1.1
/// gives them.
pub fn authority_as_host<B>(req: &mut Request<B>) {
    if req.headers().contains_key(header::HOST) {
        return;
    }
    if let Some(value) = req
        .uri()
        .authority()
        .and_then(|authority| HeaderValue::from_str(authority.as_str()).ok())
    {
        req.headers_mut().insert(header::HOST, value);
    }
}

/// Plain HTTP on the HTTPS port: `/healthcheck` (and `/healthcheck/ready`) is answered by the
/// application, everything else
/// is redirected to `https://` on the same host and port.
fn plain_service(
    app: Router,
    peer: SocketAddr,
) -> impl tower::Service<
    Request<Incoming>,
    Response = Response<Body>,
    Error = Infallible,
    Future = impl Send,
> + Clone
+ Send {
    tower::service_fn(move |mut req: Request<Incoming>| {
        let app = app.clone();
        async move {
            if matches!(req.uri().path(), HEALTHCHECK_PATH | READY_PATH)
                && matches!(*req.method(), Method::GET | Method::HEAD)
            {
                req.extensions_mut().insert(ConnectInfo(peer));
                return app.oneshot(req).await;
            }
            Ok(https_redirect(&req))
        }
    })
}

/// `308 Permanent Redirect` to the request's URL with `https://`, or `400` without a usable
/// `Host`.
pub fn https_redirect<B>(req: &Request<B>) -> Response<Body> {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .filter(|h| !h.contains('@') && h.parse::<axum::http::uri::Authority>().is_ok());
    let path = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let location = host
        .map(|host| format!("https://{host}{path}"))
        .and_then(|url| HeaderValue::from_str(&url).ok().map(|value| (url, value)));
    let mut response = match location {
        Some((url, value)) => {
            let mut response =
                Response::new(Body::from(format!("This server speaks HTTPS: {url}\n")));
            *response.status_mut() = StatusCode::PERMANENT_REDIRECT;
            response.headers_mut().insert(header::LOCATION, value);
            response
        }
        None => {
            let mut response = Response::new(Body::from("This server speaks HTTPS.\n"));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            response
        }
    };
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Read the files again every `interval` and on `SIGHUP`, forever. A failed reload of a poll is
/// reported once it is seen twice in a row (a renewal writes the key and the certificate one
/// after the other) and not again until something changes; one asked for with `SIGHUP` is always
/// reported.
async fn reload_loop(
    certificate: Arc<ServerCertificate>,
    events: Option<TlsEvents>,
    interval: Duration,
) {
    #[cfg(unix)]
    let mut hangup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(signal) => Some(signal),
        Err(e) => {
            emit(
                &events,
                TlsEvent::Warning(format!(
                    "SIGHUP will not reload the TLS certificate ({e}); the files are still \
                         read again every {} seconds.",
                    interval.as_secs()
                )),
            );
            None
        }
    };
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut seen: Option<String> = None;
    let mut reported: Option<String> = None;
    loop {
        // `Some(true)`: SIGHUP; `Some(false)`: the interval; `None`: the signal stream ended.
        #[cfg(unix)]
        let asked = tokio::select! {
            _ = ticker.tick() => Some(false),
            received = async {
                match hangup.as_mut() {
                    Some(signal) => signal.recv().await,
                    None => std::future::pending().await,
                }
            } => received.map(|()| true),
        };
        #[cfg(not(unix))]
        let asked = {
            ticker.tick().await;
            Some(false)
        };
        let Some(asked) = asked else {
            #[cfg(unix)]
            {
                hangup = None;
            }
            continue;
        };
        let reloading = certificate.clone();
        let outcome = tokio::task::spawn_blocking(move || reloading.reload())
            .await
            .unwrap_or_else(|e| Err(TlsError::Read(format!("The reload failed: {e}."))));
        let certfile = &certificate.files.certfile;
        match outcome {
            Ok(Reload::Unchanged) => {
                if asked {
                    let mut line = format!(
                        "SIGHUP: the TLS key and certificate files are unchanged; still serving {}",
                        certfile.path.display()
                    );
                    let described = certificate.summary().describe();
                    if !described.is_empty() {
                        line.push_str(&format!(" ({described})"));
                    }
                    line.push('.');
                    emit(&events, TlsEvent::Info(line));
                }
                seen = None;
                reported = None;
            }
            Ok(Reload::Reloaded(summary)) => {
                let mut line = format!(
                    "Reloaded the TLS certificate from {}",
                    certfile.path.display()
                );
                let described = summary.describe();
                if !described.is_empty() {
                    line.push_str(&format!(" ({described})"));
                }
                line.push('.');
                emit(&events, TlsEvent::Info(line));
                if let Some(problem) = summary.problem(certfile, Utc::now()) {
                    emit(&events, TlsEvent::Warning(problem));
                }
                seen = None;
                reported = None;
            }
            Err(error) => {
                let text = error.to_string();
                let persists = seen.as_deref() == Some(text.as_str());
                if asked || (persists && reported.as_deref() != Some(text.as_str())) {
                    emit(
                        &events,
                        TlsEvent::Warning(format!(
                            "Could not reload the TLS certificate; still serving the previous \
                             one. {text}"
                        )),
                    );
                    reported = Some(text.clone());
                }
                seen = Some(text);
            }
        }
    }
}

#[cfg(test)]
mod tests;

/// Helpers for tests (this crate's and others'): PEM text and OpenSSL's "traditional" keys.
#[doc(hidden)]
pub mod testing {
    use base64::Engine as _;

    /// `-----BEGIN <label>-----`, the base64 of `der` in 64-column lines, `-----END <label>-----`.
    pub fn pem(label: &str, der: &[u8]) -> String {
        let encoded = base64::engine::general_purpose::STANDARD.encode(der);
        let mut text = format!("-----BEGIN {label}-----\n");
        for chunk in encoded.as_bytes().chunks(64) {
            text.push_str(std::str::from_utf8(chunk).unwrap_or_default());
            text.push('\n');
        }
        text.push_str(&format!("-----END {label}-----\n"));
        text
    }

    fn length(len: usize) -> Vec<u8> {
        if len < 0x80 {
            return vec![len as u8];
        }
        let bytes: Vec<u8> = len
            .to_be_bytes()
            .into_iter()
            .skip_while(|b| *b == 0)
            .collect();
        let mut out = vec![0x80 | bytes.len() as u8];
        out.extend(bytes);
        out
    }

    fn tlv(tag: u8, value: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend(length(value.len()));
        out.extend_from_slice(value);
        out
    }

    /// The key inside a PKCS#8 `PrivateKeyInfo`: PKCS#1 `RSAPrivateKey` for RSA, else SEC1
    /// `ECPrivateKey` with the curve (from the envelope's algorithm) added as its `parameters`.
    pub fn traditional_key(pkcs8: &[u8], rsa: bool) -> Vec<u8> {
        let (_, info, _) = super::der::read(pkcs8).expect("PrivateKeyInfo");
        let (_, _version, rest) = super::der::read(info).expect("version");
        let (_, algorithm, rest) = super::der::read(rest).expect("algorithm");
        let (_, key, _) = super::der::read(rest).expect("privateKey");
        if rsa {
            return key.to_vec();
        }
        let (_, _oid, rest) = super::der::read(algorithm).expect("algorithm oid");
        let (_, curve, _) = super::der::read(rest).expect("curve");
        let (_, ec, _) = super::der::read(key).expect("ECPrivateKey");
        // ECPrivateKey ::= SEQUENCE { version, privateKey, [0] parameters, [1] publicKey }
        let mut fields = Vec::new();
        let mut rest = ec;
        let mut inserted = false;
        while let Some((tag, value, next)) = super::der::read(rest) {
            if (tag == 0xa1 || tag == 0xa0) && !inserted {
                if tag == 0xa1 {
                    fields.extend(tlv(0xa0, &tlv(0x06, curve)));
                }
                inserted = true;
            }
            fields.extend(tlv(tag, value));
            rest = next;
        }
        if !inserted {
            fields.extend(tlv(0xa0, &tlv(0x06, curve)));
        }
        tlv(0x30, &fields)
    }
}
