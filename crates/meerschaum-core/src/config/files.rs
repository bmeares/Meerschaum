//! Reading and writing the config files: which file holds each key (`.yaml`, `.yml` or
//! `.json`), `read_config`, `write_config` and reloading the connectors from disk.

use super::*;

/// The file names `read_config` chose to parse for a request, with what v4 would have printed.
pub(super) struct FileSelection {
    /// The file to parse for each key, in order.
    pub(super) filekeys: Vec<String>,
    /// Requested keys with no file.
    missing: Vec<String>,
    /// `(stem, chosen file)` for every stem with more than one candidate.
    duplicates: Vec<(String, String)>,
}

/// `os.path.splitext(name)[0]`.
fn stem_of(name: &str) -> String {
    match name.rfind('.') {
        Some(i) if i > 0 => name[..i].to_string(),
        _ => name.to_string(),
    }
}

/// `UnicodeDecodeError`'s text for a config file that is not UTF-8.
fn decode_error_text(bytes: &[u8], err: &std::str::Utf8Error) -> String {
    let pos = err.valid_up_to();
    let byte = bytes[pos];
    let reason = if err.error_len().is_none() {
        "unexpected end of data"
    } else if (0x80..=0xBF).contains(&byte) || byte >= 0xF5 || byte == 0xC0 || byte == 0xC1 {
        "invalid start byte"
    } else {
        "invalid continuation byte"
    };
    format!("'utf-8' codec can't decode byte 0x{byte:02x} in position {pos}: {reason}")
}

impl ConfigState {
    /// The file names in a config directory (sorted, for determinism).
    fn list_dir(&self, directory: &Path) -> Vec<String> {
        if !directory.exists() && self.allow_writes {
            let _ = std::fs::create_dir_all(directory);
        }
        let mut names: Vec<String> = std::fs::read_dir(directory)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Choose which files to parse for the requested keys (`read_config`'s filekey logic).
    pub(super) fn select_filekeys(filenames: &[String], keys: Option<&[String]>) -> FileSelection {
        let mut missing: Vec<String> = Vec::new();
        let mut found: Vec<String> = Vec::new();
        let candidates: Vec<String> = match keys {
            None => filenames.to_vec(),
            Some(keys) => {
                let mut out = Vec::new();
                for k in keys {
                    for ft in FILETYPES {
                        let name = format!("{k}.{ft}");
                        if filenames.contains(&name) {
                            out.push(name);
                            if !found.contains(k) {
                                found.push(k.clone());
                            }
                            missing.retain(|m| m != k);
                        } else if !found.contains(k) && !missing.contains(k) {
                            missing.push(k.clone());
                        }
                    }
                }
                out
            }
        };

        // Group by stem (stable sort), preferring the default filetype when duplicated.
        let mut sorted = candidates;
        sorted.sort_by_key(|f| stem_of(f));
        let default_ft = &static_config().config.default_filetype;
        let mut filekeys = Vec::new();
        let mut duplicates = Vec::new();
        let mut i = 0;
        while i < sorted.len() {
            let s = stem_of(&sorted[i]);
            let mut group = vec![sorted[i].clone()];
            let mut j = i + 1;
            while j < sorted.len() && stem_of(&sorted[j]) == s {
                group.push(sorted[j].clone());
                j += 1;
            }
            let preferred = format!("{s}.{default_ft}");
            let chosen = if group.len() > 1 && group.contains(&preferred) {
                preferred
            } else {
                group[0].clone()
            };
            if group.len() > 1 {
                duplicates.push((s.clone(), chosen.clone()));
            }
            filekeys.push(chosen);
            i = j;
        }
        FileSelection {
            filekeys,
            missing,
            duplicates,
        }
    }

    /// Port of `read_config` as the JSON-like projection (see [`read_config_lossless`]).
    ///
    /// [`read_config_lossless`]: Self::read_config_lossless
    pub fn read_config(
        &mut self,
        directory: Option<&Path>,
        keys: Option<&[String]>,
        write_missing: bool,
        substitute: bool,
        raise_parsing_errors: bool,
    ) -> Result<(Map, Vec<String>), ConfigError> {
        let (config, names) = self.read_config_lossless(
            directory,
            keys,
            write_missing,
            substitute,
            raise_parsing_errors,
        )?;
        Ok((ConfigValue::map_to_value(&config), names))
    }

    /// Port of `read_config`. Returns the config read from `directory` (default
    /// `CONFIG_DIR_PATH`) and the file names that were parsed.
    ///
    /// PARITY: a file whose extension has no loader (`foo.txt`) is announced as unknown and then
    /// still "parsed": v4 does not skip it, the missing loader is a `KeyError` that becomes the
    /// parse warning, and the key gets its defaults (`_read_config.py:178-194`).
    pub fn read_config_lossless(
        &mut self,
        directory: Option<&Path>,
        keys: Option<&[String]>,
        write_missing: bool,
        substitute: bool,
        raise_parsing_errors: bool,
    ) -> Result<(ConfigMap, Vec<String>), ConfigError> {
        let symlinks_key = Self::symlinks_key();
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let filenames = self.list_dir(&directory);
        let FileSelection {
            filekeys,
            missing: missing_keys,
            duplicates,
        } = Self::select_filekeys(&filenames, keys);

        let mut config = ConfigMap::new();
        let mut config_to_write = ConfigMap::new();

        for mk in &missing_keys {
            if !self.defaults.contains_key(mk) {
                continue;
            }
            let raw_default = self.defaults.get(mk).cloned().unwrap_or(ConfigValue::Null);
            let default_dict = if substitute {
                let defaults = self.defaults.clone();
                match self.search_and_substitute_lossless(&defaults) {
                    Ok(m) => m,
                    Err(e) => {
                        self.warn(e);
                        defaults
                    }
                }
            } else {
                self.defaults.clone()
            };
            let default_symlinks = default_dict
                .get(symlinks_key)
                .and_then(|s| s.get(mk))
                .cloned()
                .unwrap_or_else(ConfigValue::new_map);
            let value = default_dict.get(mk).cloned().unwrap_or(ConfigValue::Null);
            config.insert(mk.clone(), value);
            if default_symlinks.truthy() {
                let links = config
                    .entry(symlinks_key.to_string())
                    .or_insert_with(ConfigValue::new_map);
                if let Some(m) = links.as_map_mut() {
                    let existing = m.get(mk).cloned().unwrap_or_else(ConfigValue::new_map);
                    m.insert(mk.clone(), apply_patch(&existing, &default_symlinks));
                }
            }
            // Write the raw unsubstituted default so `MRSM{...}` references inside lists
            // (such as `stack.yaml` ports, healthchecks, and Grafana datasources) as well as
            // maps are preserved verbatim on disk.
            config_to_write.insert(mk.clone(), raw_default);
        }

        if !config_to_write.is_empty() && write_missing {
            self.write_config_lossless(Some(&config_to_write), Some(&directory));
        }

        for (stem, chosen) in &duplicates {
            self.notice(Notice::PrintOnly(format!(
                "Found multiple config files named '{stem}'. Will attempt to parse '{chosen}' for key '{stem}'."
            )));
        }

        for filename in &filekeys {
            let filepath = directory.join(filename);
            let parts: Vec<&str> = filename.split('.').collect();
            let filetype = parts[parts.len() - 1].to_string();
            let key = parts[..parts.len() - 1].join(".");
            if parts.len() < 2 || !FILETYPES.contains(&filetype.as_str()) {
                self.print(format!(
                    "Unknown file '{filename}' in '{}'. Skipping...",
                    paths::posix(&directory)
                ));
            }
            let bytes = std::fs::read(&filepath)?;
            let parsed: Result<ConfigValue, LoadError> = if !FILETYPES.contains(&filetype.as_str())
            {
                Err(LoadError::Key(filetype.clone()))
            } else {
                match std::str::from_utf8(&bytes) {
                    Err(e) => Err(LoadError::Decode(decode_error_text(&bytes, &e))),
                    Ok(text) if filetype == "json" => json_loads(text).map_err(LoadError::Json),
                    Ok(text) => load_text(
                        text,
                        &paths::posix(&filepath),
                        false,
                        LoadOptions {
                            safe: false,
                            project: false,
                        },
                    )
                    .map(|node| ConfigValue::from_node(&node))
                    .map_err(LoadError::Yaml),
                }
            };
            let config_key = match parsed {
                Ok(v) => v,
                Err(error) => {
                    if raise_parsing_errors {
                        return Err(ConfigError::Load {
                            file: filename.clone(),
                            error,
                        });
                    }
                    self.warn(format!(
                        "Could not parse '{filename}':\n    {}\nUsing default values in memory; the file on disk is unchanged.\nFix it manually or run `edit config {key} --rollback` to restore a backup.",
                        error.message()
                    ));
                    self.defaults
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(ConfigValue::new_map)
                }
            };
            if secrets::RESERVED_KEYS.contains(&key.as_str())
                && secrets::first_time(&format!("reserved\u{0}{}", filepath.display()))
            {
                let source = if key == secrets::FILE_SOURCE {
                    "a file's contents"
                } else {
                    "an environment variable"
                };
                self.warn(format!(
                    "The config file '{filename}' defines the top-level key '{key}', which is \
                     reserved for secret references: `MRSM{{{key}:...}}` reads {source}, not this \
                     key. `get_config('{key}', ...)` still reads it; rename the key to refer to \
                     it with `MRSM{{}}`."
                ));
            }
            if directory == self.paths.config_dir {
                secrets::warn_if_exposed(&filepath, &key, &config_key);
            }
            let mut single = ConfigMap::new();
            single.insert(key.clone(), config_key);
            let single = if substitute {
                match self.search_and_substitute_lossless(&single) {
                    Ok(m) => m,
                    Err(e) => {
                        self.warn(e);
                        single
                    }
                }
            } else {
                single
            };
            config.insert(
                key.clone(),
                single.get(&key).cloned().unwrap_or(ConfigValue::Null),
            );
            if let Some(link) = single.get(symlinks_key).and_then(|s| s.get(&key)) {
                let links = config
                    .entry(symlinks_key.to_string())
                    .or_insert_with(ConfigValue::new_map);
                if let Some(m) = links.as_map_mut() {
                    m.insert(key.clone(), link.clone());
                }
            }
        }

        Ok((config, filekeys))
    }

    /// See [`Config::refresh_connectors`].
    pub fn refresh_connectors(&mut self) -> bool {
        let key = "meerschaum".to_string();
        let Ok((fresh, _)) =
            self.read_config_lossless(None, Some(std::slice::from_ref(&key)), false, true, false)
        else {
            return false;
        };
        let Some(fresh) = fresh
            .get(&key)
            .and_then(|section| section.get("connectors"))
            .cloned()
        else {
            return false;
        };
        let Some(section) = self.config.get_mut(&key).and_then(|m| m.as_map_mut()) else {
            return false;
        };
        let existing = section
            .get("connectors")
            .cloned()
            .unwrap_or_else(ConfigValue::new_map);
        let mut aliases = connector_alias_sections(&existing);
        aliases.extend(connector_alias_sections(&fresh));
        let mut merged = apply_patch(&existing, &fresh);
        restore_connector_alias_sections(&mut merged, aliases);
        if merged == existing {
            return false;
        }
        section.insert("connectors".to_string(), merged);
        true
    }

    /// Port of `get_keyfile_path`.
    ///
    /// PARITY: the file is looked up in the **config directory** and only its name is joined
    /// onto `directory` (v4 calls `read_config(keys=[key], ...)` without a directory,
    /// `_read_config.py:446`), so a write into another directory keeps the filetype the key
    /// already has in the config directory. Unlike v4 it does not parse the file (no warnings
    /// for a malformed one).
    pub fn get_keyfile_path(
        &self,
        key: &str,
        create_new: bool,
        directory: Option<&Path>,
    ) -> Option<PathBuf> {
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let filenames = self.list_dir(&self.paths.config_dir);
        let selection = Self::select_filekeys(&filenames, Some(&[key.to_string()]));
        match selection.filekeys.first() {
            Some(f) => Some(directory.join(f)),
            None if create_new => {
                Some(directory.join(format!("{key}.{}", static_config().config.default_filetype)))
            }
            None => None,
        }
    }

    /// Port of `write_config` for a JSON-like config. Returns `false` when writes are disabled
    /// or a write failed.
    pub fn write_config(&mut self, config: Option<&Map>, directory: Option<&Path>) -> bool {
        let lossless = config.map(ConfigValue::map_from_value);
        self.write_config_lossless(lossless.as_ref(), directory)
    }

    /// Port of `write_config`: one file per top-level key. Returns `false` when writes are
    /// disabled or a write failed.
    pub fn write_config_lossless(
        &mut self,
        config: Option<&ConfigMap>,
        directory: Option<&Path>,
    ) -> bool {
        self.write_config_with(config, directory, &|v: &ConfigValue| {
            dump_node(
                &v.to_node(),
                &DumpOptions {
                    sort_keys: false,
                    ..DumpOptions::default()
                },
            )
            .map_err(|e| e.message)
        })
    }

    /// `write_config` with the YAML emitter injectable. When it fails for a key, that key's
    /// file is **not touched** (an emitter error must never truncate a user's config) and the
    /// write returns `false` after the messages v4 prints.
    ///
    /// PARITY: v4 opens the file for writing before dumping and removes it when the dump fails;
    /// the file is left as it was here (also for JSON, which cannot hold a date or `bytes`).
    pub fn write_config_with(
        &mut self,
        config: Option<&ConfigMap>,
        directory: Option<&Path>,
        dump_yaml: &dyn Fn(&ConfigValue) -> Result<String, String>,
    ) -> bool {
        self.write_error = None;
        if !self.allow_writes {
            return false;
        }
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let config_dict = match config {
            Some(c) => c.clone(),
            None => self.config.clone(),
        };
        // 5.1: into the config directory, only the file as written plus the caller's change
        // (never what the environment, a patch or a secret reference holds; see `provenance`).
        // Another directory (a patch directory, the defaults copy) gets what the caller passed.
        let config_dict = if directory == self.paths.config_dir {
            self.persistable_config(&config_dict)
        } else {
            revert_symlinks_lossless(&config_dict)
        };
        let default_ft = static_config().config.default_filetype.clone();
        for (k, v) in &config_dict {
            let filetype = if k == "meerschaum" {
                "yaml".to_string()
            } else if let Some(ft) = v.get("filetype") {
                py_str(ft)
            } else {
                match self.get_keyfile_path(k, false, Some(&directory)) {
                    None => default_ft.clone(),
                    Some(path) => {
                        let ft = path
                            .extension()
                            .map(|e| e.to_string_lossy().to_string())
                            .unwrap_or_default();
                        if FILETYPES.contains(&ft.as_str()) {
                            ft
                        } else {
                            self.print(format!(
                                "Invalid filetype '{ft}' for '{k}'. Assuming {default_ft}..."
                            ));
                            default_ft.clone()
                        }
                    }
                }
            };
            let filepath = directory.join(format!("{k}.{filetype}"));
            if let Some(parent) = filepath.parent() {
                let _ = std::fs::create_dir(parent);
            }
            let mut text = String::new();
            if k == "meerschaum" {
                text.push_str(DEFAULT_HEADER_COMMENT);
            }
            match filetype.as_str() {
                "json" => match json_dumps(v, JsonStyle::Indent(2), true) {
                    Ok(dumped) => text.push_str(&dumped),
                    Err(e) => {
                        self.print(format!(
                            "FAILED TO WRITE!\n{e}\n{{'sort_keys': False, 'indent': 2}}"
                        ));
                        return false;
                    }
                },
                "yaml" | "yml" => match dump_yaml(v) {
                    Ok(dumped) => text.push_str(&dumped),
                    Err(e) => {
                        self.print(format!(
                            "FAILED TO WRITE! Could not dump '{k}':\n{e}\n{{'sort_keys': False, 'indent': 2}}"
                        ));
                        return false;
                    }
                },
                other => {
                    self.print(format!(
                        "FAILED TO WRITE!\n{}\n{{'sort_keys': False, 'indent': 2}}",
                        str_repr(other)
                    ));
                    // PARITY: v4 removes the file here; a config file holds credentials, so it is
                    // never deleted on a failed write.
                    return false;
                }
            }
            if let Err(e) = write_file_atomically(&filepath, text.as_bytes()) {
                self.write_error = Some((e.kind(), e.to_string()));
                return false;
            }
        }
        true
    }

    /// The I/O error the last failed [`write_config_with`](Self::write_config_with) hit, if any
    /// (v4 raises it, e.g. `PermissionError` for a read-only file). Cleared by this call.
    pub fn take_write_error(&mut self) -> Option<(std::io::ErrorKind, String)> {
        self.write_error.take()
    }
}
