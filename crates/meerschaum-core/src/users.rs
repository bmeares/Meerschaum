//! Users and tokens (`meerschaum/core/User/_User.py`, `meerschaum/core/Token/_Token.py`) and
//! passlib-compatible password hashing.
//!
//! Hashes use passlib's `pbkdf2-sha256` modular-crypt format:
//! `$pbkdf2-sha256$<rounds>$<salt>$<checksum>`, where salt and checksum use passlib's "adapted
//! base64" (standard alphabet with `.` instead of `+`, no padding).

use blake2::{Blake2b512, Blake2s256};
use chrono::{DateTime, Duration, TimeZone, Utc};
use hmac::{Hmac, SimpleHmac};
use md5::Md5;
use ripemd::Ripemd160;
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512, Sha512_224, Sha512_256};
use sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512};
use sm3::Sm3;
use uuid::Uuid;

use crate::static_config::static_config;
use crate::success::SuccessTuple;
use crate::value::{Map, Value};

/// Password hashing errors that Python raises (rather than returning `False`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PasswordError {
    /// `hashlib.pbkdf2_hmac` does not support the digest named in the hash.
    #[error("unsupported hash type {0}")]
    UnsupportedDigest(String),
    /// The number of rounds must be at least 1.
    #[error("iteration value must be greater than 0.")]
    InvalidRounds,
    /// Random bytes could not be generated.
    #[error("could not generate random bytes: {0}")]
    Random(String),
}

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 encoding with padding (`base64.b64encode`).
pub fn b64encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(B64_ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(B64_ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64_ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64_ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// A base64 decoding error (`binascii.Error`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Base64Error(pub String);

/// `binascii.a2b_base64` in its default non-strict mode: characters outside the alphabet are
/// skipped and decoding stops at valid padding.
pub fn a2b_base64(data: &[u8]) -> Result<Vec<u8>, Base64Error> {
    let mut out = Vec::new();
    let mut quad_pos = 0u8;
    let mut left_char = 0u8;
    let mut pads = 0u8;
    let mut data_chars = 0usize;
    for &c in data {
        if c == b'=' {
            if quad_pos >= 2 {
                pads += 1;
                if quad_pos + pads >= 4 {
                    return Ok(out);
                }
            }
            continue;
        }
        let Some(v) = B64_ALPHABET.iter().position(|&a| a == c) else {
            continue;
        };
        let v = v as u8;
        pads = 0;
        data_chars += 1;
        match quad_pos {
            0 => {
                quad_pos = 1;
                left_char = v;
            }
            1 => {
                quad_pos = 2;
                out.push((left_char << 2) | (v >> 4));
                left_char = v & 0x0f;
            }
            2 => {
                quad_pos = 3;
                out.push((left_char << 4) | (v >> 2));
                left_char = v & 0x03;
            }
            _ => {
                quad_pos = 0;
                out.push((left_char << 6) | v);
                left_char = 0;
            }
        }
    }
    match quad_pos {
        0 => Ok(out),
        1 => Err(Base64Error(format!(
            "Invalid base64-encoded string: number of data characters ({data_chars}) cannot be 1 more than a multiple of 4"
        ))),
        _ => Err(Base64Error("Incorrect padding".to_string())),
    }
}

/// `ab64_encode`: base64 without padding, `+` replaced by `.`.
pub fn ab64_encode(data: &[u8]) -> String {
    b64encode(data).trim_end_matches('=').replace('+', ".")
}

/// `ab64_decode` (via `b64s_decode`): accepts `.` or `+`, restores padding.
pub fn ab64_decode(data: &str) -> Result<Vec<u8>, Base64Error> {
    if !data.is_ascii() {
        return Err(Base64Error(
            "string argument should contain only ASCII characters".into(),
        ));
    }
    let mut bytes = data.replace('.', "+").into_bytes();
    match bytes.len() & 3 {
        0 => {}
        2 => bytes.extend_from_slice(b"=="),
        3 => bytes.push(b'='),
        _ => return Err(Base64Error("Invalid base64 input".into())),
    }
    a2b_base64(&bytes)
}

/// `hashlib.pbkdf2_hmac(name, password, salt, rounds)`.
pub fn pbkdf2_hmac(
    algorithm: &str,
    password: &[u8],
    salt: &[u8],
    rounds: u32,
) -> Result<Vec<u8>, PasswordError> {
    if rounds < 1 {
        return Err(PasswordError::InvalidRounds);
    }
    macro_rules! derive {
        ($mac:ty, $size:expr) => {{
            let mut buf = vec![0u8; $size];
            pbkdf2::pbkdf2::<$mac>(password, salt, rounds, &mut buf)
                .map_err(|e| PasswordError::UnsupportedDigest(e.to_string()))?;
            buf
        }};
    }
    let out = match algorithm.to_lowercase().as_str() {
        "sha1" | "1.3.14.3.2.26" => derive!(Hmac<Sha1>, 20),
        "sha224" | "2.16.840.1.101.3.4.2.4" => derive!(Hmac<Sha224>, 28),
        "sha256" | "2.16.840.1.101.3.4.2.1" => derive!(Hmac<Sha256>, 32),
        "sha384" | "2.16.840.1.101.3.4.2.2" => derive!(Hmac<Sha384>, 48),
        "sha512" | "2.16.840.1.101.3.4.2.3" => derive!(Hmac<Sha512>, 64),
        "sha512_224" | "2.16.840.1.101.3.4.2.5" => derive!(Hmac<Sha512_224>, 28),
        "sha512_256" | "2.16.840.1.101.3.4.2.6" => derive!(Hmac<Sha512_256>, 32),
        "sha3_224" | "2.16.840.1.101.3.4.2.7" => derive!(SimpleHmac<Sha3_224>, 28),
        "sha3_256" | "2.16.840.1.101.3.4.2.8" => derive!(SimpleHmac<Sha3_256>, 32),
        "sha3_384" | "2.16.840.1.101.3.4.2.9" => derive!(SimpleHmac<Sha3_384>, 48),
        "sha3_512" | "2.16.840.1.101.3.4.2.10" => derive!(SimpleHmac<Sha3_512>, 64),
        "md5" | "1.2.840.113549.2.5" => derive!(Hmac<Md5>, 16),
        "ripemd160" | "ripemd" | "rmd160" | "1.3.36.3.2.1" => derive!(Hmac<Ripemd160>, 20),
        "sm3" | "1.2.156.10197.1.401" => derive!(Hmac<Sm3>, 32),
        // BLAKE2 uses lazy block buffering, so the generic Digest-based HMAC is required.
        "blake2b" | "blake2b512" | "1.3.6.1.4.1.1722.12.2.1.16" => {
            derive!(SimpleHmac<Blake2b512>, 64)
        }
        "blake2s" | "blake2s256" | "1.3.6.1.4.1.1722.12.2.2.8" => {
            derive!(SimpleHmac<Blake2s256>, 32)
        }
        other => return Err(PasswordError::UnsupportedDigest(other.to_string())),
    };
    Ok(out)
}

/// Random bytes from the OS (`os.urandom`).
pub fn urandom(n: usize) -> Result<Vec<u8>, PasswordError> {
    let mut buf = vec![0u8; n];
    getrandom::fill(&mut buf).map_err(|e| PasswordError::Random(e.to_string()))?;
    Ok(buf)
}

/// Port of `hash_password(password, salt, rounds)`. Defaults: 16 random salt bytes and
/// 1,000,000 rounds (`STATIC_CONFIG['users']['password_hash']`).
pub fn hash_password(
    password: &str,
    salt: Option<&[u8]>,
    rounds: Option<u32>,
) -> Result<String, PasswordError> {
    let cfg = &static_config().users.password_hash;
    let salt = match salt {
        Some(s) => s.to_vec(),
        None => urandom(cfg.salt_bytes)?,
    };
    let rounds = rounds.unwrap_or(cfg.pbkdf2_sha256_default_rounds);
    let checksum = pbkdf2_hmac(&cfg.algorithm_name, password.as_bytes(), &salt, rounds)?;
    Ok(format!(
        "$pbkdf2-{}${}${}${}",
        cfg.algorithm_name,
        rounds,
        ab64_encode(&salt),
        ab64_encode(&checksum)
    ))
}

/// Port of `verify_password(password, password_hash)`.
///
/// Malformed hashes return `Ok(false)` (Python warns). An unsupported digest or zero rounds
/// is an error, because Python raises there.
pub fn verify_password(password: &str, password_hash: &str) -> Result<bool, PasswordError> {
    let parts: Vec<&str> = password_hash.split('$').collect();
    if parts.len() != 5 {
        return Ok(false);
    }
    let (digest, rounds_str, salt_str, checksum_str) = (parts[1], parts[2], parts[3], parts[4]);
    let algorithm = digest.rsplit('-').next().unwrap_or(digest);
    let (Ok(salt), Ok(checksum)) = (ab64_decode(salt_str), ab64_decode(checksum_str)) else {
        return Ok(false);
    };
    let Some(rounds) = crate::pyfmt::parse_py_int(rounds_str) else {
        return Ok(false);
    };
    let rounds = u32::try_from(rounds).map_err(|_| PasswordError::InvalidRounds)?;
    let computed = pbkdf2_hmac(algorithm, password.as_bytes(), &salt, rounds)?;
    Ok(constant_time_eq(&checksum, &computed))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Port of `valid_username` (`connectors/sql/_users.py`).
///
/// PARITY: Python lists the allowed special characters from a `set`, so their order in the
/// message varies between processes; here it is fixed.
pub fn valid_username(username: &str) -> SuccessTuple {
    let users = &static_config().users;
    let mut reasons = Vec::new();
    let len = username.chars().count() as i64;
    if len < users.min_username_length {
        reasons.push(format!(
            "Usernames must have at least {} characters.",
            users.min_username_length
        ));
    }
    if len > users.max_username_length {
        reasons.push(format!(
            "Usernames must contain {} or fewer characters.",
            users.max_username_length
        ));
    }
    let acceptable = ['_', '-', '.', '@'];
    if username
        .chars()
        .any(|c| !c.is_alphanumeric() && !acceptable.contains(&c))
    {
        reasons.push(
            "Usernames may only contain alphanumeric characters and the following special characters: ['_', '-', '.', '@']"
                .to_string(),
        );
    }
    if reasons.is_empty() {
        return SuccessTuple::success();
    }
    let mut msg = format!("Username '{username}' is invalid for the following reasons:\n");
    for r in reasons {
        msg.push_str(&format!(" - {r}\n"));
    }
    SuccessTuple::fail(msg)
}

/// A Meerschaum user (`meerschaum.core.User`).
#[derive(Debug, Clone, PartialEq)]
pub struct User {
    /// The username.
    pub username: String,
    /// The plaintext password (`''` when not given).
    pub password: String,
    /// The email address.
    pub email: Option<String>,
    /// The user type (`'admin'`, ...). Python's attribute is `type`.
    pub user_type: Option<String>,
    /// `_attributes`.
    pub attributes: Option<Map>,
    /// `_user_id`.
    pub user_id: Option<Value>,
    /// `_instance_keys`.
    pub instance_keys: Option<String>,
    /// `_password_hash`, computed lazily from the password.
    pub password_hash: Option<String>,
}

impl User {
    /// `User(username, password=None, ...)`.
    pub fn new(username: &str, password: Option<&str>) -> User {
        User {
            username: username.to_string(),
            password: password.unwrap_or("").to_string(),
            email: None,
            user_type: None,
            attributes: None,
            user_id: None,
            instance_keys: None,
            password_hash: None,
        }
    }

    /// `user.attributes` (an empty dict when unset).
    pub fn attributes(&mut self) -> &mut Map {
        self.attributes.get_or_insert_with(Map::new)
    }

    /// `user.password_hash`: the cached hash, or a new hash of the password.
    pub fn get_password_hash(&mut self) -> Result<String, PasswordError> {
        if let Some(h) = &self.password_hash {
            return Ok(h.clone());
        }
        let h = hash_password(&self.password, None, None)?;
        self.password_hash = Some(h.clone());
        Ok(h)
    }

    /// `user.get_scopes(refresh=False)`: `attributes['scopes']` if truthy, else every scope.
    pub fn get_scopes(&self) -> Vec<String> {
        match self.attributes.as_ref().and_then(|a| a.get("scopes")) {
            Some(Value::List(items)) if !items.is_empty() => {
                items.iter().map(crate::pyfmt::py_str).collect()
            }
            _ => static_config().scope_names(),
        }
    }
}

impl std::fmt::Display for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.username)
    }
}

/// A token's expiration argument: Python distinguishes "not given" (a placeholder) from `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiration {
    /// Not given: `now + api:tokens:default_expiration_days`, rounded down to the day.
    Default,
    /// `None`: never expires.
    Never,
    /// An explicit timestamp.
    At(DateTime<Utc>),
}

/// A long-lived access token (`meerschaum.core.Token`).
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    /// The label (Python generates a random daemon-style name when not given).
    pub label: Option<String>,
    /// The creation timestamp.
    pub creation: Option<DateTime<Utc>>,
    /// The expiration timestamp (`None` never expires).
    pub expiration: Option<DateTime<Utc>>,
    /// `_instance_keys`.
    pub instance_keys: Option<String>,
    /// `_user`.
    pub user: Option<User>,
    /// `_user_id`.
    pub user_id: Option<Value>,
    /// The scopes (all scopes by default).
    pub scopes: Vec<String>,
    /// Whether the token is valid.
    pub is_valid: bool,
    /// The client ID.
    pub id: Option<Uuid>,
    /// The client secret.
    pub secret: Option<String>,
    /// The stored hash of the secret.
    pub secret_hash: Option<String>,
}

/// `round_time(dt, timedelta(days=1))` (round down to midnight).
fn floor_to_day(dt: DateTime<Utc>) -> DateTime<Utc> {
    let date = dt.date_naive();
    Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap_or_default())
}

impl Token {
    /// `Token(label, creation, expiration, ...)`. `default_expiration_days` is
    /// `api:tokens:default_expiration_days` (366); `now` is the current time.
    pub fn new(
        label: Option<&str>,
        expiration: Expiration,
        scopes: Option<Vec<String>>,
        default_expiration_days: i64,
        now: DateTime<Utc>,
    ) -> Token {
        let days = if default_expiration_days == 0 {
            366
        } else {
            default_expiration_days
        };
        let expiration = match expiration {
            Expiration::Default => Some(floor_to_day(now + Duration::days(days))),
            Expiration::Never => None,
            Expiration::At(dt) => Some(dt),
        };
        Token {
            label: label.filter(|l| !l.is_empty()).map(String::from),
            creation: None,
            expiration,
            instance_keys: None,
            user: None,
            user_id: None,
            scopes: scopes
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| static_config().scope_names()),
            is_valid: true,
            id: None,
            secret: None,
            secret_hash: None,
        }
    }

    /// Port of `generate_credentials`: keep existing credentials, else a UUID4 id and a random
    /// alphanumeric secret of 24 to 33 characters.
    ///
    /// PARITY: `randint(min_len, max_len + 1)` is inclusive, so secrets can be one character
    /// longer than `maximum_length` (`core/Token/_Token.py:76`).
    pub fn generate_credentials(&mut self) -> Result<(Uuid, String), PasswordError> {
        if let (Some(id), Some(secret)) = (self.id, &self.secret) {
            if !secret.is_empty() {
                return Ok((id, secret.clone()));
            }
        }
        let tokens = &static_config().tokens;
        let span = (tokens.maximum_length + 1 - tokens.minimum_length + 1) as u64;
        let len = tokens.minimum_length as u64 + random_below(span)?;
        let secret = generate_password(len as usize)?;
        let id = Uuid::new_v4();
        self.secret = Some(secret.clone());
        self.id = Some(id);
        Ok((id, secret))
    }

    /// Port of `get_api_key`: `'mrsm-key:' + base64("{id}:{secret}")`.
    pub fn get_api_key(&self) -> String {
        let id = self
            .id
            .map(|u| u.to_string())
            .unwrap_or_else(|| "None".into());
        let secret = self.secret.clone().unwrap_or_else(|| "None".into());
        format!(
            "mrsm-key:{}",
            b64encode(format!("{id}:{secret}").as_bytes())
        )
    }

    /// The hash stored for a new token's secret (`hash_password(secret, rounds=100_000)`).
    pub fn hash_secret(secret: &str) -> Result<String, PasswordError> {
        hash_password(secret, None, Some(static_config().tokens.hash_rounds))
    }

    /// The in-memory part of `get_expiration_status`: `true` (and `is_valid = false`) when the
    /// token has expired. Invalidating it on the instance is the caller's job.
    pub fn check_expiration(&mut self, now: DateTime<Utc>) -> bool {
        match self.expiration {
            None => false,
            Some(exp) => {
                let expired = exp <= now;
                if expired {
                    self.is_valid = false;
                }
                expired
            }
        }
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label.as_deref().unwrap_or(""))
    }
}

/// A uniform random integer in `[0, n)`.
fn random_below(n: u64) -> Result<u64, PasswordError> {
    if n == 0 {
        return Ok(0);
    }
    let zone = u64::MAX - (u64::MAX % n);
    loop {
        let bytes = urandom(8)?;
        let v = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
        if v < zone {
            return Ok(v % n);
        }
    }
}

/// Port of `generate_password(length)`: random ASCII letters and digits.
pub fn generate_password(length: usize) -> Result<String, PasswordError> {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut out = String::with_capacity(length);
    for _ in 0..length {
        let i = random_below(CHARS.len() as u64)? as usize;
        out.push(CHARS[i] as char);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ab64_round_trip() {
        for data in [&b""[..], b"a", b"ab", b"abc", &[0xfb, 0xff, 0xfe]] {
            assert_eq!(ab64_decode(&ab64_encode(data)).unwrap(), data);
        }
        assert_eq!(ab64_encode(&[0xfb, 0xff]), "./8");
        assert_eq!(ab64_decode("+/8").unwrap(), vec![0xfb, 0xff]);
        assert!(ab64_decode("a").is_err());
    }

    #[test]
    fn hash_and_verify() {
        let h = hash_password("hunter2", Some(b"0123456789abcdef"), Some(1000)).unwrap();
        assert!(h.starts_with("$pbkdf2-sha256$1000$MDEyMzQ1Njc4OWFiY2RlZg$"));
        assert!(verify_password("hunter2", &h).unwrap());
        assert!(!verify_password("hunter3", &h).unwrap());
        assert!(!verify_password("x", "garbage").unwrap());
        assert!(verify_password("x", "$pbkdf2-md4$1$AA$AA").is_err());
    }

    #[test]
    fn verifies_legacy_sha1_hash() {
        // Released v4 accepts passlib's SHA1 format as well as the current SHA256 default.
        let hash = "$pbkdf2-sha1$1$YQ$Fjdb0.tvFFKt9C1qqYPG1X541e0";
        assert!(verify_password("p", hash).unwrap());
        assert!(!verify_password("wrong", hash).unwrap());
    }

    #[test]
    fn verifies_released_python_digest_schemes() {
        let fixture =
            Value::from_json_str(include_str!("../tests/fixtures/password_schemes.json")).unwrap();
        for case in fixture.get("cases").and_then(Value::as_list).unwrap() {
            let hash = case.get("hash").and_then(Value::as_str).unwrap();
            let expected = case.get("valid").and_then(Value::as_bool).unwrap();
            assert_eq!(verify_password("p", hash).unwrap(), expected, "{hash}");
            assert!(!verify_password("wrong", hash).unwrap(), "{hash}");
        }
    }

    #[test]
    fn usernames_and_tokens() {
        assert!(valid_username("foo.bar@x").success);
        assert!(!valid_username("bad name").success);
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 30, 0).unwrap();
        let mut t = Token::new(None, Expiration::Default, None, 366, now);
        assert_eq!(
            t.expiration,
            Some(Utc.with_ymd_and_hms(2027, 1, 2, 0, 0, 0).unwrap())
        );
        assert_eq!(t.scopes.len(), 23);
        let (_, secret) = t.generate_credentials().unwrap();
        assert!((24..=33).contains(&secret.len()));
        assert!(t.get_api_key().starts_with("mrsm-key:"));
    }
}
