//! Main chooses names only; immutable descriptor-based copying reads the actual source bytes.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::PathBuf;

use meerschaum_server::access::Access;
use meerschaum_server::executor::{plugins, source_ceiling};

pub(crate) type SelectedSources = (Vec<(OsString, PathBuf)>, BTreeSet<String>);

pub(crate) fn select(caller: &Access, directories: &[PathBuf]) -> Result<SelectedSources, String> {
    let mut sources = Vec::new();
    let mut filenames = BTreeSet::new();
    let mut grants = BTreeSet::new();
    for directory in directories {
        // Administrators may select an external source directory, including a configured
        // directory alias. Individual plugin entries and package members may never redirect
        // the subsequent source copier to some other host path.
        let anchor = match std::fs::canonicalize(directory) {
            Ok(anchor) => anchor,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err("The trusted Host plugin directory is unavailable.".into()),
        };
        let entries = std::fs::read_dir(&anchor)
            .map_err(|_| "The trusted Host plugin directory is unavailable.")?;
        for entry in entries {
            let entry = entry.map_err(|_| "The trusted Host plugin entry is unavailable.")?;
            let filename = entry.file_name();
            let Some(text) = filename.to_str() else {
                continue;
            };
            let name = text
                .strip_suffix(".py")
                .or_else(|| text.strip_suffix(".rs"))
                .unwrap_or(text);
            if !plugins::valid_name(name)
                || !meerschaum_core::access::can_use_plugin(&caller.principal, name)
                || !source_ceiling::permits_name(caller, name)
                || text == "__init__.py"
            {
                continue;
            }
            let kind = entry
                .file_type()
                .map_err(|_| "The granted Host plugin entry is unavailable.")?;
            if kind.is_symlink() {
                return Err(
                    "A granted Host plugin entry cannot redirect to another host path.".into(),
                );
            }
            let source = anchor.join(&filename);
            let importable = if kind.is_dir() && text == name {
                let mut marker = false;
                for filename in ["__init__.py", "plugin.toml"] {
                    match std::fs::symlink_metadata(source.join(filename)) {
                        Ok(metadata) if metadata.is_file() => marker = true,
                        Ok(metadata) if metadata.is_symlink() => {
                            return Err("A Host source entry-point marker cannot be a link.".into());
                        }
                        Ok(_) => {
                            return Err(
                                "A Host source entry-point marker must be a regular file.".into()
                            );
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => {
                            return Err(
                                "The granted Host package entry point is unavailable.".into()
                            );
                        }
                    }
                }
                marker
            } else {
                kind.is_file() && (text.ends_with(".py") || text.ends_with(".rs"))
            };
            if !importable {
                continue;
            }
            if grants.len() >= plugins::MAX_PLUGINS && !grants.contains(name) {
                return Err("The Host source grant set exceeds its bound.".into());
            }
            if !filenames.insert(text.to_string()) {
                continue;
            }
            // Preserve this raw anchored entry. Canonicalizing it here would erase a leaf
            // symlink race before the copier's held NOFOLLOW component walk can reject it.
            sources.push((filename.clone(), source));
            grants.insert(name.into());
        }
    }
    Ok((sources, grants))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn administrator_directory_alias_is_allowed_but_raw_entries_cannot_redirect_after_selection() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("approved-external-sources");
        std::fs::create_dir(&directory).unwrap();
        let entry = directory.join("probe.py");
        std::fs::write(
            &entry,
            "raise AssertionError('Selection must not import source')\n",
        )
        .unwrap();
        let alias = root.path().join("configured-source-directory");
        std::os::unix::fs::symlink(&directory, &alias).unwrap();
        let (sources, names) = select(&Access::everything(), std::slice::from_ref(&alias)).unwrap();
        assert_eq!(names, BTreeSet::from(["probe".into()]));
        assert_eq!(sources, vec![(OsString::from("probe.py"), entry.clone())]);
        let outside = root.path().join("ungranted-file");
        std::fs::write(&outside, "never select this host file").unwrap();
        std::fs::remove_file(&entry).unwrap();
        std::os::unix::fs::symlink(&outside, &entry).unwrap();
        assert!(
            crate::executor_plugin_files::Store::new(root.path())
                .unwrap()
                .snapshot(&sources, None)
                .is_err()
        );
        assert!(select(&Access::everything(), &[alias]).is_err());
        assert_eq!(
            std::fs::read_to_string(outside).unwrap(),
            "never select this host file"
        );
    }
}
