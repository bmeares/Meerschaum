//! Where `upgrade meerschaum` installs into a Python interpreter: where the `meerschaum`
//! distribution that interpreter imports already lives, never somewhere else.
//!
//! [`probe`] asks the interpreter for the facts ([`HostFacts`]): the installed distributions on
//! its `sys.path` in import order, its install locations (`purelib` / `platlib`, the user site),
//! whether it is a virtual environment, whether it is marked externally managed (PEP 668) and
//! whether `pip` is importable. [`decide`] turns the facts into the installer invocation
//! ([`HostInstall`]) or a refusal that names the location and the command to run instead, before
//! anything is changed. [`verify_location`] checks afterwards that the copy the interpreter
//! imports is the one just installed.
//!
//! What the platforms do (checked in throwaway containers, see the 5.1.0 changelog):
//!
//! - Fedora marks nothing externally managed; `pip install` as a user puts packages in the user
//!   site and `sudo pip install` in `/usr/local/lib*/python3.X/site-packages` (the interpreter's
//!   `purelib` / `platlib`); RPMs live in `/usr/lib*/python3.X/site-packages` with
//!   `INSTALLER` = `rpm`.
//! - Debian and Ubuntu ship `EXTERNALLY-MANAGED`; pip then refuses both system and `--user`
//!   installs unless `--break-system-packages` is given. `apt` packages live in
//!   `/usr/lib/python3/dist-packages`, outside the interpreter's install locations.
//!
//! Hence the rules are detection, not distribution names: a marker means pip refuses the user
//! site too, and a distribution outside every install location (or recorded as installed by a
//! package manager) belongs to that manager.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::Value as Json;

/// One installed `meerschaum` distribution on the interpreter's `sys.path`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Copy {
    /// The directory holding its `.dist-info` (a `site-packages`), resolved.
    pub site: PathBuf,
    /// Its version.
    pub version: String,
    /// Its `INSTALLER` record (`pip`, `uv`, `rpm`, ...), empty when it has none.
    pub installer: String,
    /// `direct_url.json` marks it editable (a development installation).
    pub editable: bool,
    /// `direct_url.json`'s `url`.
    pub url: Option<String>,
    /// The current user may write its site directory and its package directory.
    pub writable: bool,
}

/// What an interpreter says about itself and its `meerschaum`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostFacts {
    /// The interpreter as it was named (a virtual environment's `python` keeps its path).
    pub executable: String,
    /// `sys.prefix != sys.base_prefix`.
    pub in_venv: bool,
    /// `sysconfig.get_path("purelib")`, resolved.
    pub purelib: PathBuf,
    /// `sysconfig.get_path("platlib")`, resolved.
    pub platlib: PathBuf,
    /// `site.getusersitepackages()`, resolved.
    pub user_site: Option<PathBuf>,
    /// `site.ENABLE_USER_SITE` is true.
    pub user_site_enabled: bool,
    /// The `EXTERNALLY-MANAGED` marker (PEP 668) of the interpreter, outside a virtual environment.
    pub externally_managed: Option<PathBuf>,
    /// `import pip` would work.
    pub has_pip: bool,
    /// The directory `import meerschaum` loads from, resolved.
    pub module: Option<PathBuf>,
    /// Every `meerschaum` distribution on `sys.path`, in import order: the first is the one the
    /// interpreter uses.
    pub copies: Vec<Copy>,
}

/// The install scheme of the distribution to upgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// A virtual environment's own `site-packages`.
    Venv,
    /// The user site (`pip install --user`).
    User,
    /// The interpreter's system-wide `site-packages`.
    System,
}

/// How to upgrade the distribution in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostInstall {
    /// The scheme the installer uses.
    pub scheme: Scheme,
    /// Where the distribution lives now.
    pub site: PathBuf,
    /// Where the installer may put the new one within the same scheme: `purelib` and `platlib`
    /// (a pure-Python 4.x wheel in `lib` is replaced by a 5.x platform wheel in `lib64`), or the
    /// user site.
    pub sites: Vec<PathBuf>,
}

impl HostInstall {
    /// The user scheme needs `pip --user` (uv has no user scheme).
    pub fn user(&self) -> bool {
        self.scheme == Scheme::User
    }
}

const PROBE: &str = r#"
import json, os, site, sys, sysconfig
import importlib.util
import importlib.metadata as md
def real(p):
    return os.path.realpath(p) if p else None
paths = sysconfig.get_paths()
in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
marker = os.path.join(paths.get("stdlib") or "", "EXTERNALLY-MANAGED")
try:
    user_site = site.getusersitepackages()
except Exception:
    user_site = None
try:
    has_pip = importlib.util.find_spec("pip") is not None
except Exception:
    has_pip = False
module = None
try:
    spec = importlib.util.find_spec("meerschaum")
except Exception:
    spec = None
if spec is not None:
    if spec.origin and spec.origin not in ("namespace", "built-in"):
        module = os.path.dirname(spec.origin)
    elif spec.submodule_search_locations:
        module = list(spec.submodule_search_locations)[0]
copies, seen = [], set()
for entry in sys.path:
    if not entry:
        continue
    try:
        dists = list(md.distributions(name="meerschaum", path=[entry]))
    except Exception:
        dists = []
    for d in dists:
        try:
            where = real(str(d.locate_file("")))
        except Exception:
            continue
        if where in seen:
            continue
        seen.add(where)
        try:
            direct = json.loads(d.read_text("direct_url.json") or "{}")
        except Exception:
            direct = {}
        package = os.path.join(where, "meerschaum")
        copies.append({
            "site": where,
            "version": d.version,
            "installer": (d.read_text("INSTALLER") or "").strip(),
            "editable": bool((direct.get("dir_info") or {}).get("editable")),
            "url": direct.get("url"),
            "writable": os.access(where, os.W_OK)
                and (not os.path.isdir(package) or os.access(package, os.W_OK)),
        })
print(json.dumps({
    "in_venv": in_venv,
    "purelib": real(paths.get("purelib")),
    "platlib": real(paths.get("platlib")),
    "user_site": real(user_site),
    "user_site_enabled": site.ENABLE_USER_SITE is True,
    "externally_managed": marker if (not in_venv and os.path.isfile(marker)) else None,
    "has_pip": has_pip,
    "module": real(module),
    "copies": copies,
}))
"#;

/// Ask `python` (isolated from `PYTHON*` variables and the current directory, as the
/// verification after the install is) where its `meerschaum` lives.
pub fn probe(python: &Path) -> Result<HostFacts, String> {
    let output = meerschaum_core::child_env::scrubbed(Command::new(python))
        .args(["-E", "-P", "-c", PROBE])
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("Cannot run {}: {e}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "Cannot inspect {}: {}",
            python.display(),
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .last()
                .unwrap_or_default()
                .trim()
        ));
    }
    parse(
        &python.to_string_lossy(),
        &String::from_utf8_lossy(&output.stdout),
    )
}

/// Read the probe's answer (the last line that is a JSON object).
pub fn parse(python: &str, stdout: &str) -> Result<HostFacts, String> {
    let line = stdout
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with('{'))
        .ok_or_else(|| format!("{python} gave no answer about its installation"))?;
    let json: Json = serde_json::from_str(line)
        .map_err(|e| format!("{python} gave an unreadable answer: {e}"))?;
    let path = |v: &Json, key: &str| v.get(key).and_then(Json::as_str).map(PathBuf::from);
    let truth = |v: &Json, key: &str| v.get(key).and_then(Json::as_bool).unwrap_or(false);
    let copies = json
        .get("copies")
        .and_then(Json::as_array)
        .map(|list| {
            list.iter()
                .map(|c| Copy {
                    site: path(c, "site").unwrap_or_default(),
                    version: c
                        .get("version")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .into(),
                    installer: c
                        .get("installer")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .into(),
                    editable: truth(c, "editable"),
                    url: c.get("url").and_then(Json::as_str).map(str::to_string),
                    writable: truth(c, "writable"),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(HostFacts {
        executable: python.into(),
        in_venv: truth(&json, "in_venv"),
        purelib: path(&json, "purelib").unwrap_or_default(),
        platlib: path(&json, "platlib").unwrap_or_default(),
        user_site: path(&json, "user_site"),
        user_site_enabled: truth(&json, "user_site_enabled"),
        externally_managed: path(&json, "externally_managed"),
        has_pip: truth(&json, "has_pip"),
        module: path(&json, "module"),
        copies,
    })
}

/// The package manager an `INSTALLER` record names, when it is not a Python installer.
fn package_manager(installer: &str) -> Option<&'static str> {
    Some(match installer.trim().to_lowercase().as_str() {
        "rpm" | "dnf" | "yum" => "the system package manager (RPM: `dnf` or `yum`)",
        "dpkg" | "debian" | "apt" => "the system package manager (`apt`)",
        "pacman" => "the system package manager (`pacman`)",
        "apk" => "the system package manager (`apk`)",
        "portage" | "emerge" => "the system package manager (`emerge`)",
        "conda" | "mamba" => "conda",
        _ => return None,
    })
}

/// The pip command that upgrades `package` in place, as a shell line.
fn pip_line(
    python: &str,
    package: &str,
    passthrough: &[String],
    sudo: bool,
    user: bool,
    break_system: bool,
) -> String {
    let mut words: Vec<String> = Vec::new();
    if sudo {
        words.push("sudo".into());
    }
    words.extend([python.into(), "-m".into(), "pip".into(), "install".into()]);
    if user {
        words.push("--user".into());
    }
    if break_system && !passthrough.iter().any(|a| a == "--break-system-packages") {
        words.push("--break-system-packages".into());
    }
    words.push("--upgrade".into());
    words.push(package.into());
    words.extend(passthrough.iter().cloned());
    meerschaum_core::shlex::join(&words)
}

const UNCHANGED: &str = "No installation was changed.";

/// Choose how to upgrade the `meerschaum` the interpreter imports, in the location it already
/// lives in, or refuse with the reason and the command to run instead. `package` is the
/// requirement to install (`meerschaum[extras]==X`); `passthrough` are the installer arguments
/// the user gave (`--break-system-packages` among them is consent to modify an externally
/// managed interpreter).
pub fn decide(
    facts: &HostFacts,
    package: &str,
    passthrough: &[String],
) -> Result<HostInstall, String> {
    let python = facts.executable.as_str();
    let Some(copy) = facts.copies.first() else {
        return Err(match &facts.module {
            Some(dir) => format!(
                "{python} imports `meerschaum` from {} without an installed distribution (a source tree on its path). Update that source tree instead (`git pull`, then `scripts/rust/build_py.sh`). {UNCHANGED}",
                dir.display()
            ),
            None => format!(
                "{python} has no installed `meerschaum` distribution to upgrade. {UNCHANGED}"
            ),
        });
    };
    let site = copy.site.display();
    let version = &copy.version;
    if copy.editable {
        return Err(format!(
            "Meerschaum {version} in {python} is an editable (development) installation of {}. Update its source tree and rebuild it (`git pull`, then `scripts/rust/build_py.sh`) instead. {UNCHANGED}",
            copy.url.as_deref().unwrap_or(&site.to_string())
        ));
    }
    if let Some(module) = &facts.module
        && !module.starts_with(&copy.site)
    {
        return Err(format!(
            "{python} imports `meerschaum` from {}, not from its installed distribution (Meerschaum {version} in {site}), so upgrading the distribution would not change what runs. Remove the copy in {} first. {UNCHANGED}",
            module.display(),
            module.display(),
        ));
    }
    if let Some(manager) = package_manager(&copy.installer) {
        return Err(format!(
            "Meerschaum {version} in {site} was installed by {manager}. Upgrade it with {manager}, or remove it and install Meerschaum with pipx or in a virtual environment. {UNCHANGED}"
        ));
    }
    let in_scheme = copy.site == facts.purelib || copy.site == facts.platlib;
    let scheme = if in_scheme && facts.in_venv {
        Scheme::Venv
    } else if in_scheme {
        Scheme::System
    } else if !facts.in_venv
        && facts.user_site_enabled
        && facts.user_site.as_ref() == Some(&copy.site)
    {
        Scheme::User
    } else {
        let installer = if copy.installer.is_empty() {
            "a tool that left no `INSTALLER` record (a system package, most likely)".to_string()
        } else {
            format!("`{}`", copy.installer)
        };
        return Err(format!(
            "Meerschaum {version} in {site} is outside the locations {python} installs to ({}{}), so it belongs to {installer}. Upgrade it with that tool, or remove it and install Meerschaum with pipx or in a virtual environment. {UNCHANGED}",
            facts.platlib.display(),
            match (&facts.user_site, facts.in_venv) {
                (Some(user), false) => format!(" or the user site {}", user.display()),
                _ => String::new(),
            },
        ));
    };
    let consent = passthrough.iter().any(|a| a == "--break-system-packages");
    if let Some(marker) = &facts.externally_managed
        && scheme != Scheme::Venv
        && !consent
    {
        let user = scheme == Scheme::User;
        let line = pip_line(
            python,
            package,
            passthrough,
            !user && !copy.writable,
            user,
            true,
        );
        return Err(format!(
            "{python} is marked externally managed ({}, PEP 668): its packages belong to the system package manager, and pip refuses to install into it{} without `--break-system-packages`. Meerschaum {version} is in {site}. To upgrade it there anyway, run `{line}` (or pass `-A --break-system-packages` to this action). Installing Meerschaum with pipx or in a virtual environment avoids this. {UNCHANGED}",
            marker.display(),
            if user { " or its user site" } else { "" },
        ));
    }
    if !copy.writable {
        let line = pip_line(
            python,
            package,
            passthrough,
            scheme != Scheme::User,
            scheme == Scheme::User,
            false,
        );
        return Err(format!(
            "Meerschaum {version} is installed in {site}, which this user cannot write. Run `{line}` to upgrade it there, or install Meerschaum for this user only (pipx, a virtual environment or `pip install --user`) after removing that copy. {UNCHANGED}"
        ));
    }
    if scheme == Scheme::User && !facts.has_pip {
        let line = pip_line(python, package, passthrough, false, true, false);
        return Err(format!(
            "Meerschaum {version} is installed in the user site ({site}), which only pip can upgrade (uv has no user scheme), and pip is not available for {python}. Install pip (`{python} -m ensurepip --user`, or the distribution's python3-pip package), then run `{line}`. {UNCHANGED}"
        ));
    }
    let mut sites = match scheme {
        Scheme::User => vec![copy.site.clone()],
        Scheme::Venv | Scheme::System => vec![facts.purelib.clone(), facts.platlib.clone()],
    };
    sites.dedup();
    Ok(HostInstall {
        scheme,
        site: copy.site.clone(),
        sites,
    })
}

/// After installing: the copy the interpreter imports must be the one just installed in one of
/// `target.sites`. A copy found first elsewhere shadows it (an error naming both); copies behind
/// it are only reported (`Ok(Some(note))`).
pub fn verify_location(
    facts: &HostFacts,
    target: &HostInstall,
    version: &str,
) -> Result<Option<String>, String> {
    let python = facts.executable.as_str();
    let ours = |site: &Path| target.sites.iter().any(|s| s == site);
    let new = facts.copies.iter().find(|c| ours(&c.site));
    let (installed, new_site) = match new {
        Some(c) => (c.version.clone(), c.site.display().to_string()),
        None => (
            "missing".into(),
            target
                .sites
                .iter()
                .map(|s| s.display().to_string())
                .collect::<Vec<_>>()
                .join(" or "),
        ),
    };
    let Some(first) = facts.copies.first() else {
        return Err(format!(
            "{python} finds no `meerschaum` distribution after the upgrade (expected {version} in {new_site})."
        ));
    };
    if !ours(&first.site) {
        return Err(format!(
            "{} (Meerschaum {}) shadows {new_site} (Meerschaum {installed}, just installed): {python} imports the first. Remove the stale copy (`{}` removes the copy found first), then check with `mrsm doctor`.",
            first.site.display(),
            first.version,
            meerschaum_core::shlex::join(&[python, "-m", "pip", "uninstall", "meerschaum"]),
        ));
    }
    if let Some(module) = &facts.module
        && !module.starts_with(&first.site)
    {
        return Err(format!(
            "{} shadows {new_site} (Meerschaum {installed}, just installed): {python} imports `meerschaum` from the first. Remove it, then check with `mrsm doctor`.",
            module.display(),
        ));
    }
    let others: Vec<String> = facts
        .copies
        .iter()
        .skip(1)
        .map(|c| format!("{} (Meerschaum {})", c.site.display(), c.version))
        .collect();
    Ok((!others.is_empty()).then(|| {
        format!(
            "{python} imports Meerschaum {installed} from {new_site}; other copies behind it: {}.",
            others.join(", ")
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PKG: &str = "meerschaum==5.1.0";

    fn copy(site: &str) -> Copy {
        Copy {
            site: site.into(),
            version: "5.0.2".into(),
            installer: "pip".into(),
            editable: false,
            url: None,
            writable: true,
        }
    }

    /// Fedora's system Python run by a user: user site enabled, nothing externally managed.
    fn fedora(copies: Vec<Copy>) -> HostFacts {
        HostFacts {
            executable: "/usr/bin/python3".into(),
            in_venv: false,
            purelib: "/usr/local/lib/python3.15/site-packages".into(),
            platlib: "/usr/local/lib64/python3.15/site-packages".into(),
            user_site: Some("/home/u/.local/lib/python3.15/site-packages".into()),
            user_site_enabled: true,
            externally_managed: None,
            has_pip: true,
            module: copies.first().map(|c| c.site.join("meerschaum")),
            copies,
        }
    }

    const USER: &str = "/home/u/.local/lib/python3.15/site-packages";
    const LOCAL64: &str = "/usr/local/lib64/python3.15/site-packages";

    #[test]
    fn a_user_site_installation_is_upgraded_with_the_user_scheme() {
        let facts = fedora(vec![copy(USER)]);
        let plan = decide(&facts, PKG, &[]).unwrap();
        assert_eq!(plan.scheme, Scheme::User);
        assert!(plan.user());
        assert_eq!(plan.site, PathBuf::from(USER));
        assert_eq!(plan.sites, [PathBuf::from(USER)]);
    }

    #[test]
    fn a_user_site_installation_without_pip_says_how_to_get_pip() {
        let mut facts = fedora(vec![copy(USER)]);
        facts.has_pip = false;
        let error = decide(&facts, PKG, &[]).unwrap_err();
        assert!(error.contains("uv has no user scheme"), "{error}");
        assert!(
            error.contains("/usr/bin/python3 -m ensurepip --user"),
            "{error}"
        );
        assert!(
            error.contains("`/usr/bin/python3 -m pip install --user --upgrade meerschaum==5.1.0`"),
            "{error}"
        );
        assert!(error.ends_with(UNCHANGED), "{error}");
    }

    #[test]
    fn a_writable_system_installation_is_upgraded_in_place() {
        for site in [LOCAL64, "/usr/local/lib/python3.15/site-packages"] {
            let plan = decide(&fedora(vec![copy(site)]), PKG, &[]).unwrap();
            assert_eq!(plan.scheme, Scheme::System);
            assert_eq!(plan.site, PathBuf::from(site));
            // A 4.x pure-Python copy in `lib` is replaced by the platform wheel in `lib64`.
            assert_eq!(
                plan.sites,
                [
                    PathBuf::from("/usr/local/lib/python3.15/site-packages"),
                    PathBuf::from(LOCAL64)
                ]
            );
        }
    }

    #[test]
    fn a_system_installation_this_user_cannot_write_stops_with_the_sudo_command() {
        let mut system = copy(LOCAL64);
        system.writable = false;
        let error = decide(&fedora(vec![system]), PKG, &["--no-index".into()]).unwrap_err();
        assert!(error.contains(LOCAL64), "{error}");
        assert!(
            error.contains(
                "`sudo /usr/bin/python3 -m pip install --upgrade meerschaum==5.1.0 --no-index`"
            ),
            "{error}"
        );
        assert!(error.ends_with(UNCHANGED), "{error}");
    }

    #[test]
    fn a_virtual_environment_is_upgraded_as_before() {
        let site = "/home/u/env/lib/python3.13/site-packages";
        let facts = HostFacts {
            executable: "/home/u/env/bin/python".into(),
            in_venv: true,
            purelib: site.into(),
            platlib: site.into(),
            user_site: Some("/home/u/.local/lib/python3.13/site-packages".into()),
            user_site_enabled: false,
            // A virtual environment is never externally managed, whatever its base says.
            externally_managed: None,
            has_pip: false,
            module: Some(PathBuf::from(site).join("meerschaum")),
            copies: vec![copy(site)],
        };
        let plan = decide(&facts, PKG, &[]).unwrap();
        assert_eq!(plan.scheme, Scheme::Venv);
        // A read-only environment (owned by root) stops with the command to run.
        let mut read_only = facts.clone();
        read_only.copies[0].writable = false;
        let error = decide(&read_only, PKG, &[]).unwrap_err();
        assert!(
            error.contains("`sudo /home/u/env/bin/python -m pip install --upgrade"),
            "{error}"
        );
        // A venv with system site packages whose `meerschaum` is the base interpreter's: not
        // this environment's to change (installing would leave two copies).
        let mut base = facts.clone();
        base.copies = vec![copy(LOCAL64)];
        base.module = Some(PathBuf::from(LOCAL64).join("meerschaum"));
        let error = decide(&base, PKG, &[]).unwrap_err();
        assert!(error.contains("outside the locations"), "{error}");
        assert!(!error.contains("user site"), "{error}");
    }

    /// Debian and Ubuntu: marked externally managed, pip refuses the user site too.
    fn debian(copies: Vec<Copy>) -> HostFacts {
        HostFacts {
            executable: "/usr/bin/python3".into(),
            in_venv: false,
            purelib: "/usr/local/lib/python3.13/dist-packages".into(),
            platlib: "/usr/local/lib/python3.13/dist-packages".into(),
            user_site: Some("/home/u/.local/lib/python3.13/site-packages".into()),
            user_site_enabled: true,
            externally_managed: Some("/usr/lib/python3.13/EXTERNALLY-MANAGED".into()),
            has_pip: true,
            module: copies.first().map(|c| c.site.join("meerschaum")),
            copies,
        }
    }

    #[test]
    fn an_externally_managed_interpreter_stops_unless_the_user_consents() {
        let user = "/home/u/.local/lib/python3.13/site-packages";
        let error = decide(&debian(vec![copy(user)]), PKG, &[]).unwrap_err();
        assert!(error.contains("EXTERNALLY-MANAGED"), "{error}");
        assert!(error.contains("or its user site"), "{error}");
        assert!(
            error.contains(
                "`/usr/bin/python3 -m pip install --user --break-system-packages --upgrade meerschaum==5.1.0`"
            ),
            "{error}"
        );
        assert!(error.ends_with(UNCHANGED), "{error}");
        // `-A --break-system-packages` is the user's consent: the user scheme, in place.
        let consent = ["--break-system-packages".to_string()];
        let plan = decide(&debian(vec![copy(user)]), PKG, &consent).unwrap();
        assert_eq!(plan.scheme, Scheme::User);

        // The system site (`sudo pip install --break-system-packages` put it there).
        let mut system = copy("/usr/local/lib/python3.13/dist-packages");
        system.writable = false;
        let error = decide(&debian(vec![system.clone()]), PKG, &[]).unwrap_err();
        assert!(
            error.contains(
                "`sudo /usr/bin/python3 -m pip install --break-system-packages --upgrade meerschaum==5.1.0`"
            ),
            "{error}"
        );
        // Consent does not make an unwritable location writable.
        let error = decide(&debian(vec![system]), PKG, &consent).unwrap_err();
        assert!(error.contains("cannot write"), "{error}");
    }

    #[test]
    fn a_package_manager_installation_is_refused() {
        // An RPM: recorded as such, outside the interpreter's install locations.
        let mut rpm = copy("/usr/lib64/python3.15/site-packages");
        rpm.installer = "rpm".into();
        let error = decide(&fedora(vec![rpm]), PKG, &[]).unwrap_err();
        assert!(error.contains("RPM"), "{error}");
        assert!(error.ends_with(UNCHANGED), "{error}");
        // A Debian package: no INSTALLER record, in `dist-packages` outside every location.
        let mut deb = copy("/usr/lib/python3/dist-packages");
        deb.installer = String::new();
        let error = decide(&debian(vec![deb]), PKG, &[]).unwrap_err();
        assert!(error.contains("outside the locations"), "{error}");
        assert!(error.contains("no `INSTALLER` record"), "{error}");
        // Even inside an install location, a package manager's record wins.
        let mut conda = copy(LOCAL64);
        conda.installer = "conda".into();
        assert!(
            decide(&fedora(vec![conda]), PKG, &[])
                .unwrap_err()
                .contains("conda")
        );
    }

    #[test]
    fn development_installations_are_refused() {
        let mut editable = copy(USER);
        editable.editable = true;
        editable.url = Some("file:///home/u/src/Meerschaum".into());
        let error = decide(&fedora(vec![editable]), PKG, &[]).unwrap_err();
        assert!(error.contains("editable"), "{error}");
        assert!(error.contains("file:///home/u/src/Meerschaum"), "{error}");
        // A source tree on the path, no distribution at all.
        let mut tree = fedora(vec![]);
        tree.module = Some("/home/u/src/Meerschaum/meerschaum".into());
        let error = decide(&tree, PKG, &[]).unwrap_err();
        assert!(
            error.contains("without an installed distribution"),
            "{error}"
        );
        // Nothing at all.
        let mut nothing = fedora(vec![]);
        nothing.module = None;
        assert!(
            decide(&nothing, PKG, &[])
                .unwrap_err()
                .contains("no installed `meerschaum` distribution")
        );
        // The module imported is not the distribution's (an editable finder, a stray copy).
        let mut stray = fedora(vec![copy(USER)]);
        stray.module = Some("/home/u/src/meerschaum".into());
        assert!(
            decide(&stray, PKG, &[])
                .unwrap_err()
                .contains("not from its installed distribution")
        );
    }

    #[test]
    fn the_first_copy_on_the_path_is_the_one_upgraded() {
        // A stale user-site copy in front of a system one: the user copy is what runs.
        let facts = fedora(vec![copy(USER), copy(LOCAL64)]);
        assert_eq!(decide(&facts, PKG, &[]).unwrap().scheme, Scheme::User);
    }

    #[test]
    fn a_copy_found_first_elsewhere_shadows_the_new_one() {
        let target = HostInstall {
            scheme: Scheme::System,
            site: "/usr/local/lib/python3.15/site-packages".into(),
            sites: vec![
                "/usr/local/lib/python3.15/site-packages".into(),
                LOCAL64.into(),
            ],
        };
        let mut new = copy(LOCAL64);
        new.version = "5.1.0".into();
        let facts = fedora(vec![copy(USER), new.clone()]);
        let error = verify_location(&facts, &target, "5.1.0").unwrap_err();
        assert!(
            error.starts_with(&format!(
                "{USER} (Meerschaum 5.0.2) shadows {LOCAL64} (Meerschaum 5.1.0, just installed)"
            )),
            "{error}"
        );
        assert!(error.contains("/usr/bin/python3 -m pip uninstall meerschaum"));
        // In front: fine, and the copy behind it is reported.
        let facts = fedora(vec![new.clone(), copy(USER)]);
        let note = verify_location(&facts, &target, "5.1.0").unwrap().unwrap();
        assert!(note.contains("other copies behind it"), "{note}");
        let facts = fedora(vec![new]);
        assert_eq!(verify_location(&facts, &target, "5.1.0").unwrap(), None);
    }

    #[test]
    fn the_probe_answer_is_read() {
        let facts = parse(
            "/usr/bin/python3",
            "noise\n{\"in_venv\": false, \"purelib\": \"/p\", \"platlib\": \"/q\", \"user_site\": \"/u\", \"user_site_enabled\": true, \"externally_managed\": null, \"has_pip\": true, \"module\": \"/u/meerschaum\", \"copies\": [{\"site\": \"/u\", \"version\": \"5.0.2\", \"installer\": \"pip\", \"editable\": false, \"url\": null, \"writable\": true}]}\n",
        )
        .unwrap();
        assert_eq!(
            facts.copies,
            vec![{
                let mut c = copy("/u");
                c.version = "5.0.2".into();
                c
            }]
        );
        assert_eq!(facts.user_site, Some(PathBuf::from("/u")));
        assert!(facts.externally_managed.is_none());
        assert!(parse("/usr/bin/python3", "nothing").is_err());
    }
}
