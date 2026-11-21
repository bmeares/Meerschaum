//! The stack gives its containers their credentials in files.
//!
//! Up to 5.0 the compose file `mrsm stack` writes held the `api` service's whole configuration
//! (`MRSM_CONFIG`: every connector's credentials, and `MRSM_PATCH`: Grafana's administrator
//! password) in its `environment`, so the credentials were on disk in `stack/docker-compose.yaml`
//! and in `docker inspect`. Whenever `mrsm stack` writes the compose file:
//!
//! - **The files.** What a service's credential variables would hold is written to
//!   `<root>/stack/secrets/<service>/`, and the service gets the variable's file form pointing at
//!   a read-only mount of that directory ([`CONTAINER_DIR`]): `MRSM_CONFIG_FILE` /
//!   `MRSM_PATCH_FILE` for `api`, `GF_SECURITY_ADMIN_PASSWORD__FILE` for `grafana`, and the
//!   one-shot `grafana_readonly` service reads its two passwords from files. The `valkey`
//!   service, whose images ignore `VALKEY_PASSWORD`, is started with a configuration file holding
//!   `requirepass` ([`externalize_valkey`]). The compose file and `docker inspect` then hold
//!   paths, not credentials.
//! - **Who can read them.** `<root>/stack/secrets/` is `0700`: no other host user can reach the
//!   files. Each service's subdirectory is `0755` with `0644` files, because the container's user
//!   (uid 1000 in Meerschaum's image, 472 in Grafana's, 1000 in TimescaleDB's) need not be the host
//!   user: a bind mount is resolved by the Docker daemon, and inside the container only the
//!   mounted subdirectory is traversed, never the `0700` parent. Each service mounts its own
//!   subdirectory only.
//! - **What is moved.** Every `MRSM_CONFIG` / `MRSM_PATCH` entry of the `api` service, the
//!   default one (as `MRSM{...}` references or materialized) and a customized one alike: the file
//!   holds exactly the text the variable would have held, so a customized entry's keys reach the
//!   container unchanged. An entry that uses Docker Compose interpolation (`<DOLLAR>NAME` in the
//!   config) stays in the environment, as does everything when the service already sets a `_FILE`
//!   form itself. The user's config file is never rewritten.
//! - **Which images.** Only a 5.1.0 or newer Meerschaum image reads `MRSM_CONFIG_FILE`. The `api`
//!   service gets the file form when its image is `bmeares/meerschaum:<version>[-variant]` with
//!   `<version>` 5.1.0 or newer (a 5.1.0 pre-release included), or when `stack:api_config_files`
//!   is `true`; otherwise it keeps the environment form, with one warning naming the reason
//!   (`false` keeps it without the warning). The API container never starts without its
//!   configuration: when the files cannot be written, the environment form is kept too.
//! - **Changes.** The files are written on every `mrsm stack` command, and each service carries a
//!   label ([`REVISION_LABEL`]) that changes with its files (a salted hash, so it reveals nothing
//!   about them): `mrsm stack up -d` recreates a container whose credentials changed, as it did
//!   when they were in its environment.

use std::path::{Path, PathBuf};

use meerschaum_core::value::{Map, Value};

/// The directory under the stack directory (`<root>/stack/secrets`), readable by its owner only.
pub const SECRETS_DIRNAME: &str = "secrets";

/// Where each service sees its own subdirectory of the secrets directory.
pub const CONTAINER_DIR: &str = "/run/secrets/meerschaum";

/// The compose label that changes with a service's files.
pub const REVISION_LABEL: &str = "io.meerschaum.secrets-revision";

/// The `stack` setting that overrides the image rule for the `api` service.
pub const API_SETTING: &str = "api_config_files";

/// The first version whose image reads `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE`.
pub const FIRST_VERSION: &str = "5.1.0.dev0";

/// Meerschaum's image repository.
pub const OFFICIAL_REPOSITORY: &str = "bmeares/meerschaum";

/// The salt of the revision labels (in the secrets directory).
const SALT_FILENAME: &str = ".revision_salt";

/// The custom API services whose directories this module manages (one name per line).
const MANAGED_MANIFEST: &str = ".managed_services";

/// The `api` service's files.
pub const API_CONFIG_FILENAME: &str = "mrsm_config.json";
/// The `api` service's patch file.
pub const API_PATCH_FILENAME: &str = "mrsm_patch.json";
/// Grafana's administrator password.
pub const GRAFANA_ADMIN_FILENAME: &str = "admin_password";
/// The `grafana_readonly` service's files: the database owner's password and the read-only
/// role's.
pub const OWNER_PASSWORD_FILENAME: &str = "owner_password";
/// The read-only role's password.
pub const READONLY_PASSWORD_FILENAME: &str = "readonly_password";
/// The `valkey` service's configuration file (its `requirepass`).
pub const VALKEY_CONF_FILENAME: &str = "valkey.conf";
/// The password generated for the stack's Valkey when `valkey:main` has none (in the secrets
/// directory, owner-only, never mounted).
pub const VALKEY_GENERATED_FILENAME: &str = ".valkey_password";

/// The services this module writes files for, with the files each may have.
const SERVICES: &[(&str, &[&str])] = &[
    ("api", &[API_CONFIG_FILENAME, API_PATCH_FILENAME]),
    ("grafana", &[GRAFANA_ADMIN_FILENAME]),
    (
        super::stack_readonly::SERVICE,
        &[OWNER_PASSWORD_FILENAME, READONLY_PASSWORD_FILENAME],
    ),
    ("valkey", &[VALKEY_CONF_FILENAME]),
];

/// How the `api` service gets its configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiForm {
    /// `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` and a mounted directory.
    Files,
    /// `MRSM_CONFIG` / `MRSM_PATCH` in its environment, with the warning to show (`None` when
    /// `stack:api_config_files` asks for it).
    Environment(Option<String>),
}

fn setting_bool(setting: Option<&Value>) -> Option<bool> {
    match setting? {
        Value::Bool(b) => Some(*b),
        Value::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "on" | "1" => Some(true),
            "false" | "no" | "off" | "0" => Some(false),
            _ => None,
        },
        Value::Int(i) => Some(*i != 0),
        _ => None,
    }
}

/// Why `image` cannot be known to read `MRSM_CONFIG_FILE` (`None`: it can).
pub fn image_reason(image: Option<&str>) -> Option<String> {
    let Some(image) = image.map(str::trim).filter(|i| !i.is_empty()) else {
        return Some("the service has no `image`".into());
    };
    // `repo:tag@digest`: the tag still says the version.
    let named = image.split('@').next().unwrap_or(image);
    let (repository, tag) = match named.rfind(':') {
        Some(ix) if !named[ix..].contains('/') => (&named[..ix], &named[ix + 1..]),
        _ => (named, ""),
    };
    let repository = ["docker.io/", "index.docker.io/", "registry-1.docker.io/"]
        .iter()
        .find_map(|prefix| repository.strip_prefix(prefix))
        .unwrap_or(repository);
    if repository != OFFICIAL_REPOSITORY {
        return Some(format!(
            "its image `{image}` is not `{OFFICIAL_REPOSITORY}:<version>`, so whether it reads \
             `MRSM_CONFIG_FILE` cannot be known"
        ));
    }
    let version = tag.split('-').next().unwrap_or_default();
    match meerschaum_core::version::parse_version(version) {
        Err(_) => Some(format!(
            "its image `{image}` has no version in its tag, so whether it reads \
             `MRSM_CONFIG_FILE` cannot be known"
        )),
        Ok(_) if !meerschaum_core::version::version_ge(version, FIRST_VERSION) => Some(format!(
            "its image `{image}` is older than 5.1.0, which reads `MRSM_CONFIG_FILE`"
        )),
        Ok(_) => None,
    }
}

/// How a Meerschaum API service (`service`) with `image` gets its configuration, given
/// `stack:api_config_files`.
pub fn api_form_for_service(
    service: &str,
    image: Option<&str>,
    setting: Option<&Value>,
) -> ApiForm {
    match setting_bool(setting) {
        Some(true) => return ApiForm::Files,
        Some(false) => return ApiForm::Environment(None),
        None => {}
    }
    match image_reason(image) {
        None => ApiForm::Files,
        Some(reason) => ApiForm::Environment(Some(format!(
            "The stack's `{service}` service gets its configuration in `MRSM_CONFIG` / `MRSM_PATCH`, so \
             every connector's credentials are in `stack/docker-compose.yaml` and in `docker \
             inspect`: {reason}. Use a 5.1.0 or newer image, or set \
             `stack:{API_SETTING}: true` if this image reads `MRSM_CONFIG_FILE` (`false` keeps \
             the environment without this warning)."
        ))),
    }
}

/// How the `api` service with `image` gets its configuration, given `stack:api_config_files`.
pub fn api_form(image: Option<&str>, setting: Option<&Value>) -> ApiForm {
    api_form_for_service("api", image, setting)
}

/// The value of `name` in a compose `environment` (a map or a `KEY=value` list), as text. `None`
/// when unset or not a scalar.
pub fn env_get(environment: Option<&Value>, name: &str) -> Option<String> {
    match environment? {
        Value::Map(map) => match map.get(name)? {
            Value::Null | Value::Map(_) | Value::List(_) => None,
            other => Some(other.py_str()),
        },
        Value::List(items) => items.iter().find_map(|item| match item {
            Value::Str(s) => s
                .strip_prefix(name)
                .and_then(|rest| rest.strip_prefix('='))
                .map(str::to_string),
            _ => None,
        }),
        _ => None,
    }
}

/// Whether a compose `environment` names `name` at all.
fn env_has(environment: Option<&Value>, name: &str) -> bool {
    match environment {
        Some(Value::Map(map)) => map.contains_key(name),
        Some(Value::List(items)) => items.iter().any(
            |item| matches!(item, Value::Str(s) if s == name || s.starts_with(&format!("{name}="))),
        ),
        _ => false,
    }
}

fn env_remove(environment: &mut Value, name: &str) {
    match environment {
        Value::Map(map) => {
            map.shift_remove(name);
        }
        Value::List(items) => items.retain(
            |item| !matches!(item, Value::Str(s) if s == name || s.starts_with(&format!("{name}="))),
        ),
        _ => {}
    }
}

/// Mount `host` at [`CONTAINER_DIR`] (read-only) in a service, replacing whatever was mounted
/// there.
fn mount(service: &mut Map, host: &Path) {
    let host = host.to_string_lossy().replace('\\', "/");
    let mut volumes = match service.get("volumes") {
        Some(Value::List(items)) => items.clone(),
        _ => Vec::new(),
    };
    volumes.retain(|volume| match volume {
        Value::Str(s) => {
            let mut parts = s.rsplitn(3, ':').collect::<Vec<_>>();
            parts.reverse();
            !parts.contains(&CONTAINER_DIR)
        }
        Value::Map(map) => map.get("target").map(Value::py_str).as_deref() != Some(CONTAINER_DIR),
        _ => true,
    });
    volumes.push(Value::Str(format!("{host}:{CONTAINER_DIR}:z,ro")));
    service.insert("volumes".into(), Value::List(volumes));
}

fn set_label(service: &mut Map, value: &str) {
    match service.get_mut("labels") {
        Some(Value::Map(labels)) => {
            labels.insert(REVISION_LABEL.into(), Value::Str(value.to_string()));
        }
        Some(Value::List(items)) => {
            let prefix = format!("{REVISION_LABEL}=");
            items.retain(|item| !matches!(item, Value::Str(s) if s.starts_with(&prefix)));
            items.push(Value::Str(format!("{REVISION_LABEL}={value}")));
        }
        _ => {
            let mut labels = Map::new();
            labels.insert(REVISION_LABEL.into(), Value::Str(value.to_string()));
            service.insert("labels".into(), Value::Map(labels));
        }
    }
}

/// A short salted hash of a service's files: changes with them, reveals nothing about them.
pub fn revision(salt: &str, service: &str, files: &[(String, String)]) -> String {
    use sha2::Digest;
    let mut hash = sha2::Sha256::new();
    hash.update(salt.as_bytes());
    hash.update([0]);
    hash.update(service.as_bytes());
    for (name, contents) in files {
        hash.update([0]);
        hash.update(name.as_bytes());
        hash.update([0]);
        hash.update(contents.as_bytes());
    }
    hex::encode(hash.finalize())[..16].to_string()
}

/// The files of one service.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServiceFiles {
    /// The compose service.
    pub service: String,
    /// `(file name, contents)`.
    pub files: Vec<(String, String)>,
}

/// What [`externalize`] did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Externalized {
    /// The files to write, by service (a service with no files has its old ones removed).
    pub services: Vec<ServiceFiles>,
    /// What to tell the user.
    pub warnings: Vec<String>,
}

/// Whether a Grafana image reads `GF_<SECTION>_<KEY>__FILE` (Grafana's own images do).
fn grafana_reads_files(image: Option<&str>) -> bool {
    let Some(image) = image else {
        return false;
    };
    let image = ["docker.io/", "index.docker.io/", "registry-1.docker.io/"]
        .iter()
        .find_map(|prefix| image.strip_prefix(prefix))
        .unwrap_or(image);
    image.starts_with("grafana/grafana")
}

/// The variables each service gets from a file: `(variable, file form, file name)`. The
/// `grafana_readonly` service reads its files in its command instead (no file form).
const API_VARIABLES: &[(&str, &str, &str)] = &[
    ("MRSM_CONFIG", "MRSM_CONFIG_FILE", API_CONFIG_FILENAME),
    ("MRSM_PATCH", "MRSM_PATCH_FILE", API_PATCH_FILENAME),
];

fn is_safe_service_dir(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', '\0'])
}

fn externalize_api_service(
    name: &str,
    service: &mut Map,
    host_dir: &Path,
    form: &ApiForm,
    salt: &str,
    warnings: &mut Vec<String>,
) -> ServiceFiles {
    let mut service_files = ServiceFiles {
        service: name.to_string(),
        files: Vec::new(),
    };
    let environment = service.get("environment");
    let present: Vec<_> = API_VARIABLES
        .iter()
        .filter(|(var, _, _)| env_get(environment, var).is_some())
        .collect();
    let own_files = API_VARIABLES
        .iter()
        .any(|(_, file_var, _)| env_has(environment, file_var));
    if !present.is_empty() {
        match form {
            ApiForm::Environment(warning) => warnings.extend(warning.clone()),
            ApiForm::Files if own_files => warnings.push(format!(
                "The stack's `{name}` service sets `MRSM_CONFIG_FILE` or `MRSM_PATCH_FILE` \
                 itself: its `MRSM_CONFIG` / `MRSM_PATCH` stay in its environment."
            )),
            ApiForm::Files => {
                let mut environment = service
                    .get("environment")
                    .cloned()
                    .unwrap_or_else(|| Value::Map(Map::new()));
                for (var, file_var, filename) in present {
                    let Some(text) = env_get(Some(&environment), var) else {
                        continue;
                    };
                    if text.contains("<DOLLAR>") {
                        warnings.push(format!(
                            "The stack's `{name}` service's `{var}` uses Docker Compose \
                             interpolation: it stays in the service's environment."
                        ));
                        continue;
                    }
                    service_files.files.push((filename.to_string(), text));
                    env_remove(&mut environment, var);
                    super::os_sql_stack::set_environment(
                        &mut environment,
                        file_var,
                        &format!("{CONTAINER_DIR}/{filename}"),
                        true,
                    );
                }
                service.insert("environment".into(), environment);
            }
        }
    }
    if !service_files.files.is_empty() {
        mount(service, &host_dir.join(name));
        set_label(service, &revision(salt, name, &service_files.files));
    }
    service_files
}

fn externalize_inner(
    compose: &mut Value,
    host_dir: &Path,
    form_for: &dyn Fn(&str, Option<&str>) -> ApiForm,
    salt: &str,
) -> Externalized {
    let mut out = Externalized::default();
    let Some(services) = compose
        .as_map_mut()
        .and_then(|c| c.get_mut("services"))
        .and_then(Value::as_map_mut)
    else {
        return out;
    };

    // `api`: MRSM_CONFIG / MRSM_PATCH (kept at index 0 for fixed service ordering).
    let api_files = if let Some(Value::Map(service)) = services.get_mut("api") {
        let image = service.get("image").map(Value::py_str);
        let form = form_for("api", image.as_deref());
        externalize_api_service("api", service, host_dir, &form, salt, &mut out.warnings)
    } else {
        ServiceFiles {
            service: "api".into(),
            files: Vec::new(),
        }
    };
    out.services.push(api_files);

    // `grafana`: the administrator password (`with_grafana_sign_in`).
    let mut grafana_files = ServiceFiles {
        service: "grafana".into(),
        files: Vec::new(),
    };
    if let Some(Value::Map(service)) = services.get_mut("grafana") {
        let image = service.get("image").map(Value::py_str);
        let environment = service.get("environment");
        if grafana_reads_files(image.as_deref())
            && !env_has(environment, "GF_SECURITY_ADMIN_PASSWORD__FILE")
            && let Some(password) = env_get(environment, "GF_SECURITY_ADMIN_PASSWORD")
            && !password.contains("<DOLLAR>")
        {
            let mut environment = environment.cloned().unwrap_or(Value::Map(Map::new()));
            grafana_files
                .files
                .push((GRAFANA_ADMIN_FILENAME.to_string(), password));
            env_remove(&mut environment, "GF_SECURITY_ADMIN_PASSWORD");
            super::os_sql_stack::set_environment(
                &mut environment,
                "GF_SECURITY_ADMIN_PASSWORD__FILE",
                &format!("{CONTAINER_DIR}/{GRAFANA_ADMIN_FILENAME}"),
                true,
            );
            service.insert("environment".into(), environment);
            mount(service, &host_dir.join("grafana"));
            set_label(service, &revision(salt, "grafana", &grafana_files.files));
        }
    }
    out.services.push(grafana_files);

    // `grafana_readonly`: the service `stack_readonly` generates, with both passwords.
    let readonly = super::stack_readonly::SERVICE;
    let mut readonly_files = ServiceFiles {
        service: readonly.into(),
        files: Vec::new(),
    };
    if let Some(Value::Map(service)) = services.get_mut(readonly) {
        let environment = service.get("environment");
        if let (Some(owner), Some(password)) = (
            env_get(environment, "PGPASSWORD"),
            env_get(environment, super::stack_readonly::PASSWORD_VARIABLE),
        ) {
            let mut environment = environment.cloned().unwrap_or(Value::Map(Map::new()));
            readonly_files
                .files
                .push((OWNER_PASSWORD_FILENAME.to_string(), owner));
            readonly_files
                .files
                .push((READONLY_PASSWORD_FILENAME.to_string(), password));
            env_remove(&mut environment, "PGPASSWORD");
            env_remove(&mut environment, super::stack_readonly::PASSWORD_VARIABLE);
            service.insert("environment".into(), environment);
            service.insert(
                "command".into(),
                Value::List(vec![Value::Str(super::stack_readonly::file_command(
                    &format!("{CONTAINER_DIR}/{OWNER_PASSWORD_FILENAME}"),
                    &format!("{CONTAINER_DIR}/{READONLY_PASSWORD_FILENAME}"),
                ))]),
            );
            mount(service, &host_dir.join(readonly));
            set_label(service, &revision(salt, readonly, &readonly_files.files));
        }
    }
    out.services.push(readonly_files);

    // Additional custom Meerschaum API services (e.g. `api-sra`, `api-bwg`) that set
    // `MRSM_CONFIG` or `MRSM_PATCH`.
    let extra_names: Vec<String> = services
        .iter()
        .filter(|(name, svc)| {
            !matches!(name.as_str(), "api" | "grafana" | "valkey")
                && name.as_str() != readonly
                && is_safe_service_dir(name)
                && svc.as_map().is_some_and(|m| {
                    let env = m.get("environment");
                    API_VARIABLES
                        .iter()
                        .any(|(var, _, _)| env_get(env, var).is_some())
                })
        })
        .map(|(name, _)| name.clone())
        .collect();
    for name in extra_names {
        if let Some(Value::Map(service)) = services.get_mut(&name) {
            let image = service.get("image").map(Value::py_str);
            let form = form_for(&name, image.as_deref());
            let files =
                externalize_api_service(&name, service, host_dir, &form, salt, &mut out.warnings);
            out.services.push(files);
        }
    }
    out
}

/// Move the services' credentials out of the compose config into files under `host_dir` (see
/// the module documentation). `compose` is changed in place; nothing is written here.
pub fn externalize(
    compose: &mut Value,
    host_dir: &Path,
    api: &ApiForm,
    salt: &str,
) -> Externalized {
    externalize_inner(compose, host_dir, &|_, _| api.clone(), salt)
}

/// Like [`externalize`], computing each API service's [`ApiForm`] from its own `image` and
/// `stack:api_config_files`.
pub fn externalize_with_setting(
    compose: &mut Value,
    host_dir: &Path,
    api_setting: Option<&Value>,
    salt: &str,
) -> Externalized {
    externalize_inner(
        compose,
        host_dir,
        &|service, image| api_form_for_service(service, image, api_setting),
        salt,
    )
}

/// Whether `image` is one of Valkey's own images (`valkey/valkey`, `valkey/valkey-bundle`), whose
/// server takes a configuration file as its first argument.
pub fn is_valkey_image(image: Option<&str>) -> bool {
    let Some(image) = image else {
        return false;
    };
    let image = ["docker.io/", "index.docker.io/", "registry-1.docker.io/"]
        .iter()
        .find_map(|prefix| image.strip_prefix(prefix))
        .unwrap_or(image);
    let repository = image.split(['@', ':']).next().unwrap_or_default();
    matches!(repository, "valkey/valkey" | "valkey/valkey-bundle")
}

/// `requirepass` for a Valkey configuration file, quoted as Valkey reads it.
pub fn valkey_conf(password: &str) -> String {
    let quoted = password.replace('\\', "\\\\").replace('"', "\\\"");
    format!(
        "# Written by `mrsm stack` from `valkey:main`'s password; rewritten on every command.\n\
         requirepass \"{quoted}\"\n"
    )
}

/// The `valkey` service's password: Valkey's images ignore `VALKEY_PASSWORD`, so the stack
/// starts the server with a configuration file holding `requirepass` (in the service's secrets
/// subdirectory, mounted read-only like the others) and drops the variable. `password` is what
/// `VALKEY_PASSWORD` stands for (`valkey:main`'s password, `None` when it has none: the caller
/// generates one). A service with its own `command`, another image or a `VALKEY_PASSWORD` given
/// through another variable is left as it is, with a warning.
pub fn externalize_valkey(
    compose: &mut Value,
    host_dir: &Path,
    password: Option<&str>,
    salt: &str,
) -> (ServiceFiles, Vec<String>) {
    let mut files = ServiceFiles {
        service: "valkey".into(),
        files: Vec::new(),
    };
    let mut warnings = Vec::new();
    let Some(Value::Map(service)) = compose
        .as_map_mut()
        .and_then(|c| c.get_mut("services"))
        .and_then(Value::as_map_mut)
        .and_then(|services| services.get_mut("valkey"))
    else {
        return (files, warnings);
    };
    let image = service.get("image").map(Value::py_str);
    if !is_valkey_image(image.as_deref()) {
        return (files, warnings);
    }
    let environment = service.get("environment");
    let given = env_get(environment, "VALKEY_PASSWORD");
    let password = match given.as_deref() {
        // The stack's variable: `valkey:main`'s password.
        Some("<DOLLAR>VALKEY_PASSWORD") | Some("<DOLLAR>{VALKEY_PASSWORD}") | None => password,
        Some(other) if other.contains("<DOLLAR>") => {
            warnings.push(
                "The stack's `valkey` service takes `VALKEY_PASSWORD` from another variable: \
                 it is left as it is, and Valkey ignores that variable. Give the server \
                 `--requirepass` yourself."
                    .into(),
            );
            return (files, warnings);
        }
        Some(literal) => Some(literal),
    };
    let Some(password) = password.filter(|p| !p.is_empty()) else {
        return (files, warnings);
    };
    if password.contains(['\n', '\r']) {
        warnings.push(
            "`valkey:main`'s password holds a line break, which a Valkey configuration file \
             cannot: the stack's Valkey runs without a password."
                .into(),
        );
        return (files, warnings);
    }
    if service.get("command").is_some_and(|c| !c.is_null()) {
        warnings.push(
            "The stack's `valkey` service has its own `command`: it is left as it is. Make sure \
             it sets `requirepass` (Valkey ignores `VALKEY_PASSWORD`)."
                .into(),
        );
        return (files, warnings);
    }
    files
        .files
        .push((VALKEY_CONF_FILENAME.to_string(), valkey_conf(password)));
    if let Some(environment) = service.get_mut("environment") {
        env_remove(environment, "VALKEY_PASSWORD");
    }
    service.insert(
        "command".into(),
        Value::List(vec![
            Value::Str("valkey-server".into()),
            Value::Str(format!("{CONTAINER_DIR}/{VALKEY_CONF_FILENAME}")),
        ]),
    );
    mount(service, &host_dir.join("valkey"));
    set_label(service, &revision(salt, "valkey", &files.files));
    (files, warnings)
}

/// `<root>/stack/secrets`.
pub fn secrets_dir(stack_dir: &Path) -> PathBuf {
    stack_dir.join(SECRETS_DIRNAME)
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
}

/// Create the secrets directory (owner-only) and return the salt of its revision labels.
pub fn prepare_dir(dir: &Path) -> Result<String, String> {
    let failed = |e: &dyn std::fmt::Display| {
        format!(
            "Cannot prepare the stack's secrets directory {}: {e}",
            dir.display()
        )
    };
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| failed(&e))?;
    }
    if !dir.is_dir() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(dir)
                .map_err(|e| failed(&e))?;
        }
        #[cfg(not(unix))]
        std::fs::create_dir(dir).map_err(|e| failed(&e))?;
    }
    // Every time: a directory created earlier (or copied) must not stay readable by others.
    set_mode(dir, 0o700).map_err(|e| failed(&e))?;
    meerschaum_core::config::grafana_readonly::read_or_generate_secret(
        &dir.join(SALT_FILENAME),
        "the salt of the stack's secret revisions",
    )
}

/// Write one file readable through its service's mount (`0644`, atomically: a container
/// reading it never sees half a file).
fn write_readable(path: &Path, contents: &str) -> std::io::Result<()> {
    if std::fs::read_to_string(path).is_ok_and(|existing| existing == contents) {
        return set_mode(path, 0o644);
    }
    let dir = path.parent().unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temporary = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    std::fs::write(&temporary, contents)?;
    set_mode(&temporary, 0o644)?;
    std::fs::rename(&temporary, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&temporary);
    })
}

/// Write the services' files under `dir` (prepared by [`prepare_dir`]) and remove the ones no
/// longer wanted (a service back on its environment keeps no copy of its credentials).
pub fn write_files(dir: &Path, services: &[ServiceFiles]) -> Result<(), String> {
    let write_one =
        |service: &str, known: &[&str], wanted: &[(String, String)]| -> Result<(), String> {
            let subdir = dir.join(service);
            let failed =
                |e: &dyn std::fmt::Display| format!("Cannot write {}: {e}", subdir.display());
            if !wanted.is_empty() {
                std::fs::create_dir_all(&subdir).map_err(|e| failed(&e))?;
                set_mode(&subdir, 0o755).map_err(|e| failed(&e))?;
                for (name, contents) in wanted {
                    write_readable(&subdir.join(name), contents).map_err(|e| failed(&e))?;
                }
            }
            for name in known {
                if !wanted.iter().any(|(wanted, _)| wanted == name) {
                    let _ = std::fs::remove_file(subdir.join(name));
                }
            }
            if wanted.is_empty() {
                // Only when empty: something the user put there is kept.
                let _ = std::fs::remove_dir(&subdir);
            }
            Ok(())
        };

    for (service, known) in SERVICES {
        let wanted = services
            .iter()
            .find(|s| s.service == *service)
            .map(|s| s.files.as_slice())
            .unwrap_or_default();
        write_one(service, known, wanted)?;
    }

    // Custom API services (`api-sra`, ...): only directories this module wrote before (listed in
    // its manifest) are ever cleaned, so a user's own files under `stack/secrets/` are kept.
    let manifest = dir.join(MANAGED_MANIFEST);
    let previous: Vec<String> = std::fs::read_to_string(&manifest)
        .map(|text| {
            text.lines()
                .map(str::trim)
                .filter(|l| is_safe_service_dir(l))
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    let api_known = &[API_CONFIG_FILENAME, API_PATCH_FILENAME];
    let builtin: Vec<&str> = SERVICES.iter().map(|(s, _)| *s).collect();
    let mut managed: Vec<String> = Vec::new();
    for entry in services {
        let name = entry.service.as_str();
        if builtin.contains(&name)
            || !is_safe_service_dir(name)
            || managed.iter().any(|m| m == name)
        {
            continue;
        }
        if !entry.files.is_empty() {
            write_one(name, api_known, &entry.files)?;
            managed.push(name.to_string());
        } else if previous.iter().any(|p| p == name) {
            write_one(name, api_known, &[])?;
        }
    }
    for name in &previous {
        if managed.contains(name) || builtin.contains(&name.as_str()) {
            continue;
        }
        let path = dir.join(name);
        // Never follow a symlink out of the secrets directory.
        if !std::fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir()) {
            continue;
        }
        write_one(name, api_known, &[])?;
    }
    if managed.is_empty() {
        let _ = std::fs::remove_file(&manifest);
    } else {
        meerschaum_core::team_scope::write_private(&manifest, &(managed.join("\n") + "\n"))
            .map_err(|e| format!("Cannot write {}: {e}", manifest.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(json: &str) -> Value {
        Value::from_json_str(json).unwrap()
    }

    #[test]
    fn only_a_5_1_meerschaum_image_is_known_to_read_the_files() {
        for image in [
            "bmeares/meerschaum:5.1.0-full",
            "bmeares/meerschaum:5.1.0",
            "bmeares/meerschaum:5.1.0rc1-rust",
            "docker.io/bmeares/meerschaum:5.2.3-slim",
            "bmeares/meerschaum:6.0.0-full@sha256:0123",
        ] {
            assert_eq!(image_reason(Some(image)), None, "{image}");
            assert_eq!(api_form(Some(image), None), ApiForm::Files, "{image}");
        }
        for (image, reason) in [
            ("bmeares/meerschaum:5.0.2-full", "older than 5.1.0"),
            ("bmeares/meerschaum:4.1.7", "older than 5.1.0"),
            ("bmeares/meerschaum:latest", "no version in its tag"),
            ("bmeares/meerschaum:full", "no version in its tag"),
            ("bmeares/meerschaum", "no version in its tag"),
            ("bmeares/meerschaum@sha256:0123", "no version in its tag"),
            ("myorg/meerschaum:5.1.0-full", "cannot be known"),
            ("registry.example.com:5000/mrsm:5.1.0", "cannot be known"),
        ] {
            let found = image_reason(Some(image)).unwrap();
            assert!(found.contains(reason), "{image}: {found}");
            match api_form(Some(image), None) {
                ApiForm::Environment(Some(warning)) => {
                    assert!(warning.contains(&found), "{warning}");
                    assert!(warning.contains("stack:api_config_files"), "{warning}");
                }
                other => panic!("{image}: {other:?}"),
            }
        }
        assert!(image_reason(None).unwrap().contains("no `image`"));
        // The setting overrides the rule both ways (and silences the warning).
        assert_eq!(
            api_form(Some("myorg/meerschaum:x"), Some(&Value::Bool(true))),
            ApiForm::Files
        );
        assert_eq!(
            api_form(
                Some("bmeares/meerschaum:5.1.0-full"),
                Some(&Value::Str("false".into()))
            ),
            ApiForm::Environment(None)
        );
    }

    fn compose(api_environment: &str) -> Value {
        value(&format!(
            r#"{{"services": {{
                "api": {{"image": "bmeares/meerschaum:5.1.0-full",
                         "environment": {api_environment},
                         "volumes": ["api_root:/meerschaum"]}},
                "grafana": {{"image": "grafana/grafana:latest",
                             "environment": {{"GF_SECURITY_ADMIN_PASSWORD": "gf-secret",
                                              "GF_AUTH_ANONYMOUS_ENABLED": "true"}}}},
                "grafana_readonly": {{"image": "timescale/timescaledb-ha:pg18",
                    "command": ["printf ..."],
                    "environment": {{"PGUSER": "mrsm", "PGPASSWORD": "owner-secret",
                                     "MRSM_READONLY_PASSWORD": "ro-secret",
                                     "MRSM_READONLY_SQL": "SELECT 1"}}}}}}}}"#
        ))
    }

    const CONFIG: &str = r#"{"meerschaum": {"connectors": {"sql": {"main": {"password": "pg-secret"}}}}, "extra": {"mine": 1}}"#;

    #[test]
    fn the_credentials_move_to_files_and_the_compose_file_keeps_paths() {
        let environment = serde_json::json!({
            "MRSM_CONFIG": CONFIG,
            "MRSM_PATCH": r#"{"api": {"grafana": {"admin_password": "gf-secret"}}}"#,
            "OTHER": "kept",
        })
        .to_string();
        let mut compose = compose(&environment);
        let dir = Path::new("/srv/root/stack/secrets");
        let done = externalize(&mut compose, dir, &ApiForm::Files, "salt");
        assert!(done.warnings.is_empty(), "{:?}", done.warnings);
        let text = meerschaum_core::yaml::dump(&compose).unwrap();
        for secret in ["pg-secret", "gf-secret", "owner-secret", "ro-secret"] {
            assert!(!text.contains(secret), "{secret} in:\n{text}");
        }
        let api = compose.get("services").unwrap().get("api").unwrap();
        let env = api.get("environment");
        assert_eq!(env_get(env, "MRSM_CONFIG"), None);
        assert_eq!(
            env_get(env, "MRSM_CONFIG_FILE").as_deref(),
            Some("/run/secrets/meerschaum/mrsm_config.json")
        );
        assert_eq!(
            env_get(env, "MRSM_PATCH_FILE").as_deref(),
            Some("/run/secrets/meerschaum/mrsm_patch.json")
        );
        assert_eq!(env_get(env, "OTHER").as_deref(), Some("kept"));
        assert_eq!(
            api.get("volumes").unwrap(),
            &value(
                r#"["api_root:/meerschaum", "/srv/root/stack/secrets/api:/run/secrets/meerschaum:z,ro"]"#
            )
        );
        assert_eq!(
            api.get("labels")
                .unwrap()
                .get(REVISION_LABEL)
                .unwrap()
                .py_str(),
            revision("salt", "api", &done.services[0].files)
        );
        // The file holds exactly what the variable held: customized keys reach the container.
        assert_eq!(
            done.services[0].files,
            vec![
                (API_CONFIG_FILENAME.to_string(), CONFIG.to_string()),
                (
                    API_PATCH_FILENAME.to_string(),
                    r#"{"api": {"grafana": {"admin_password": "gf-secret"}}}"#.to_string()
                ),
            ]
        );

        let grafana = compose.get("services").unwrap().get("grafana").unwrap();
        let env = grafana.get("environment");
        assert_eq!(env_get(env, "GF_SECURITY_ADMIN_PASSWORD"), None);
        assert_eq!(
            env_get(env, "GF_SECURITY_ADMIN_PASSWORD__FILE").as_deref(),
            Some("/run/secrets/meerschaum/admin_password")
        );
        assert_eq!(
            done.services[1].files,
            vec![("admin_password".to_string(), "gf-secret".to_string())]
        );

        let readonly = compose
            .get("services")
            .unwrap()
            .get("grafana_readonly")
            .unwrap();
        let env = readonly.get("environment");
        assert_eq!(env_get(env, "PGPASSWORD"), None);
        assert_eq!(env_get(env, "MRSM_READONLY_PASSWORD"), None);
        assert_eq!(env_get(env, "PGUSER").as_deref(), Some("mrsm"));
        let command = readonly.get("command").unwrap().as_list().unwrap()[0].py_str();
        assert!(
            command.contains("/run/secrets/meerschaum/owner_password"),
            "{command}"
        );
        assert_eq!(done.services[2].files.len(), 2);
    }

    #[test]
    fn list_environments_and_interpolation_are_handled() {
        let environment = serde_json::json!([
            format!("MRSM_CONFIG={CONFIG}"),
            "MRSM_PATCH=<DOLLAR>MY_PATCH",
        ])
        .to_string();
        let mut compose = compose(&environment);
        let done = externalize(&mut compose, Path::new("/d"), &ApiForm::Files, "s");
        let env = compose
            .get("services")
            .unwrap()
            .get("api")
            .unwrap()
            .get("environment")
            .cloned();
        assert_eq!(env_get(env.as_ref(), "MRSM_CONFIG"), None);
        assert!(env_get(env.as_ref(), "MRSM_CONFIG_FILE").is_some());
        // Interpolated by Docker Compose: only the environment can carry it.
        assert_eq!(
            env_get(env.as_ref(), "MRSM_PATCH").as_deref(),
            Some("<DOLLAR>MY_PATCH")
        );
        assert_eq!(env_get(env.as_ref(), "MRSM_PATCH_FILE"), None);
        assert_eq!(done.services[0].files.len(), 1);
        assert!(
            done.warnings[0].contains("interpolation"),
            "{:?}",
            done.warnings
        );
    }

    #[test]
    fn an_old_image_keeps_the_environment_form_with_a_warning() {
        let environment = serde_json::json!({"MRSM_CONFIG": CONFIG}).to_string();
        let mut compose = compose(&environment);
        let before = compose.get("services").unwrap().get("api").cloned();
        let form = api_form(Some("bmeares/meerschaum:5.0.2-full"), None);
        let done = externalize(&mut compose, Path::new("/d"), &form, "s");
        assert_eq!(compose.get("services").unwrap().get("api").cloned(), before);
        assert!(done.services[0].files.is_empty());
        assert_eq!(done.warnings.len(), 1);
        assert!(done.warnings[0].contains("5.0.2"), "{:?}", done.warnings);
        // A service that sets the file form itself is left as it is.
        let environment =
            serde_json::json!({"MRSM_CONFIG": CONFIG, "MRSM_PATCH_FILE": "/mine.json"}).to_string();
        let mut compose = self::compose(&environment);
        let before = compose.get("services").unwrap().get("api").cloned();
        let done = externalize(&mut compose, Path::new("/d"), &ApiForm::Files, "s");
        assert_eq!(compose.get("services").unwrap().get("api").cloned(), before);
        assert!(done.warnings[0].contains("itself"), "{:?}", done.warnings);
    }

    #[test]
    fn a_custom_grafana_image_keeps_its_password_in_the_environment() {
        let mut compose = compose("{}");
        if let Some(Value::Map(grafana)) = compose
            .as_map_mut()
            .and_then(|c| c.get_mut("services"))
            .and_then(Value::as_map_mut)
            .and_then(|s| s.get_mut("grafana"))
        {
            grafana.insert("image".into(), Value::Str("example/dashboards:1".into()));
        }
        let done = externalize(&mut compose, Path::new("/d"), &ApiForm::Files, "s");
        assert!(done.services[1].files.is_empty());
        let grafana = compose.get("services").unwrap().get("grafana").unwrap();
        assert_eq!(
            env_get(grafana.get("environment"), "GF_SECURITY_ADMIN_PASSWORD").as_deref(),
            Some("gf-secret")
        );
    }

    #[test]
    fn the_revision_follows_the_files_and_the_salt() {
        let files = vec![("a".to_string(), "1".to_string())];
        let other = vec![("a".to_string(), "2".to_string())];
        assert_eq!(revision("s", "api", &files), revision("s", "api", &files));
        assert_ne!(revision("s", "api", &files), revision("s", "api", &other));
        assert_ne!(revision("s", "api", &files), revision("t", "api", &files));
        assert_eq!(revision("s", "api", &files).len(), 16);
    }

    #[cfg(unix)]
    #[test]
    fn the_directory_is_owner_only_and_the_files_readable_through_their_mount() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let dir = secrets_dir(root.path());
        std::fs::create_dir_all(&dir).unwrap();
        set_mode(&dir, 0o755).unwrap();
        let salt = prepare_dir(&dir).unwrap();
        assert_eq!(salt, prepare_dir(&dir).unwrap());
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&dir.join(SALT_FILENAME)), 0o600);
        let services = vec![ServiceFiles {
            service: "api".into(),
            files: vec![(API_CONFIG_FILENAME.into(), "{}".into())],
        }];
        write_files(&dir, &services).unwrap();
        assert_eq!(mode(&dir.join("api")), 0o755);
        assert_eq!(mode(&dir.join("api").join(API_CONFIG_FILENAME)), 0o644);
        assert_eq!(
            std::fs::read_to_string(dir.join("api").join(API_CONFIG_FILENAME)).unwrap(),
            "{}"
        );
        // Back on the environment: the copy is removed.
        write_files(&dir, &[]).unwrap();
        assert!(!dir.join("api").exists());
    }

    #[test]
    fn the_valkey_service_reads_its_password_from_a_file() {
        let compose_text = r#"{"services": {"valkey": {
            "image": "valkey/valkey-bundle:latest",
            "environment": {"VALKEY_PASSWORD": "<DOLLAR>VALKEY_PASSWORD", "OTHER": "1"},
            "volumes": ["valkey_data:/data"]}}}"#;
        let mut compose = value(compose_text);
        let (files, warnings) =
            externalize_valkey(&mut compose, Path::new("/d"), Some("p\\w\"d"), "s");
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(files.files[0].0, VALKEY_CONF_FILENAME);
        assert_eq!(
            files.files[0].1.lines().last().unwrap(),
            r#"requirepass "p\\w\"d""#
        );
        let service = compose.get("services").unwrap().get("valkey").unwrap();
        assert_eq!(
            service.get("command").unwrap(),
            &value(r#"["valkey-server", "/run/secrets/meerschaum/valkey.conf"]"#)
        );
        let environment = service.get("environment").unwrap();
        assert!(environment.get("VALKEY_PASSWORD").is_none());
        assert_eq!(environment.get("OTHER"), Some(&Value::Str("1".into())));
        assert!(
            service
                .get("volumes")
                .unwrap()
                .py_str()
                .contains("/d/valkey:/run/secrets/meerschaum:z,ro")
        );

        // No password, another image, or a command of the user's own: left as it is.
        for (text, password, warned) in [
            (compose_text, None, false),
            (
                r#"{"services": {"valkey": {"image": "bitnami/valkey:8"}}}"#,
                Some("p"),
                false,
            ),
            (
                r#"{"services": {"valkey": {"image": "valkey/valkey:8", "command": "valkey-server"}}}"#,
                Some("p"),
                true,
            ),
            (
                r#"{"services": {"valkey": {"image": "valkey/valkey:8",
                    "environment": {"VALKEY_PASSWORD": "<DOLLAR>OTHER"}}}}"#,
                Some("p"),
                true,
            ),
        ] {
            let mut compose = value(text);
            let before = compose.clone();
            let (files, warnings) =
                externalize_valkey(&mut compose, Path::new("/d"), password, "s");
            assert!(files.files.is_empty(), "{text}");
            assert_eq!(compose, before, "{text}");
            assert_eq!(!warnings.is_empty(), warned, "{text}: {warnings:?}");
        }
        for image in ["valkey/valkey", "docker.io/valkey/valkey-bundle:9@sha256:0"] {
            assert!(is_valkey_image(Some(image)), "{image}");
        }
        assert!(!is_valkey_image(Some("redis:7")));
    }

    #[test]
    fn custom_api_services_are_externalized_and_cleaned_up() {
        let mut compose = value(&format!(
            r#"{{"services": {{
                "api": {{"image": "bmeares/meerschaum:5.1.0-full",
                         "environment": {{"MRSM_CONFIG": {cfg}}}}},
                "api-sra": {{"image": "bmeares/meerschaum:5.1.0-full",
                             "environment": {{"MRSM_CONFIG": {cfg}, "MRSM_PATCH": "{{\"a\": 1}}"}}}},
                "api-legacy": {{"image": "bmeares/meerschaum:5.0.2-full",
                                "environment": {{"MRSM_CONFIG": {cfg}}}}}
            }}}}"#,
            cfg = serde_json::to_string(CONFIG).unwrap(),
        ));
        let tmp = tempfile::tempdir().unwrap();
        let dir = secrets_dir(tmp.path());
        let salt = prepare_dir(&dir).unwrap();
        let done = externalize_with_setting(&mut compose, &dir, None, &salt);
        assert_eq!(done.warnings.len(), 1, "{:?}", done.warnings);
        assert!(
            done.warnings[0].contains("`api-legacy`"),
            "{:?}",
            done.warnings
        );

        let services = compose.get("services").unwrap();
        for name in ["api", "api-sra"] {
            let svc = services.get(name).unwrap();
            let env = svc.get("environment");
            assert_eq!(env_get(env, "MRSM_CONFIG"), None, "{name}");
            assert_eq!(
                env_get(env, "MRSM_CONFIG_FILE").as_deref(),
                Some("/run/secrets/meerschaum/mrsm_config.json"),
                "{name}"
            );
            assert!(
                svc.get("volumes")
                    .unwrap()
                    .py_str()
                    .contains(&format!("/{name}:/run/secrets/meerschaum:z,ro")),
                "{name}"
            );
        }
        assert_eq!(
            env_get(
                services.get("api-sra").unwrap().get("environment"),
                "MRSM_PATCH_FILE"
            )
            .as_deref(),
            Some("/run/secrets/meerschaum/mrsm_patch.json")
        );
        // Legacy custom API stays on environment.
        let legacy = services.get("api-legacy").unwrap();
        assert_eq!(
            env_get(legacy.get("environment"), "MRSM_CONFIG").as_deref(),
            Some(CONFIG)
        );
        assert_eq!(env_get(legacy.get("environment"), "MRSM_CONFIG_FILE"), None);

        write_files(&dir, &done.services).unwrap();
        assert!(dir.join("api").join(API_CONFIG_FILENAME).is_file());
        assert!(dir.join("api-sra").join(API_CONFIG_FILENAME).is_file());
        assert!(dir.join("api-sra").join(API_PATCH_FILENAME).is_file());
        assert!(!dir.join("api-legacy").exists());

        // A user's own directory (never written by this module) is never touched.
        let mine = dir.join("my-service");
        std::fs::create_dir_all(&mine).unwrap();
        std::fs::write(mine.join(API_CONFIG_FILENAME), "{}").unwrap();

        // Removing `api-sra` cleans up its managed secret files and empty directory.
        let remaining: Vec<_> = done
            .services
            .into_iter()
            .filter(|s| s.service != "api-sra")
            .collect();
        write_files(&dir, &remaining).unwrap();
        assert!(dir.join("api").join(API_CONFIG_FILENAME).is_file());
        assert!(!dir.join("api-sra").exists());
        assert!(mine.join(API_CONFIG_FILENAME).is_file());
        assert!(!dir.join(MANAGED_MANIFEST).exists());
    }
}
