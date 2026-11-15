//! The two config environment variables and their file forms (5.1): `MRSM_CONFIG` /
//! `MRSM_CONFIG_FILE` and `MRSM_PATCH` / `MRSM_PATCH_FILE`.
//!
//! A `*_FILE` variable names a file whose contents are what the variable would hold, parsed the
//! same way. Setting a variable and its file form is an error naming both ([`Config::new`]
//! refuses to start); an empty `*_FILE` counts as unset, which is how a child process that is
//! given its own `MRSM_CONFIG` drops an inherited `MRSM_CONFIG_FILE`. There is deliberately no
//! `MRSM_<TYPE>_<LABEL>_FILE`: `MRSM_SQL_MAIN_FILE` is the connector `sql:main_file`.
//!
//! Child processes Meerschaum starts as itself inherit the environment, so they read the same
//! file and see the same configuration.
//!
//! [`Config::new`]: super::Config::new

use super::ConfigState;
use super::GetOptions;
use super::lossless::{ConfigValue, apply_patch_maps};
use crate::pyfmt::string_to_dict;
use crate::static_config::static_config;

/// The variable names of the two config patches and their file forms:
/// `[(MRSM_CONFIG, MRSM_CONFIG_FILE), (MRSM_PATCH, MRSM_PATCH_FILE)]`.
pub fn patch_variables() -> [(String, String); 2] {
    let env = &static_config().environment;
    [
        (env.config.clone(), env.config_file.clone()),
        (env.patch.clone(), env.patch_file.clone()),
    ]
}

/// The `MRSM_CONFIG` / `MRSM_PATCH` texts `env` holds, in that order, each with the variable it
/// came from: the variable itself, or the contents of the file its `*_FILE` form names (5.1; an
/// empty `*_FILE` counts as unset). `Err` naming both variables when both forms are set, or naming
/// the file when it cannot be read.
pub fn environment_patch_texts(env: &[(String, String)]) -> Result<Vec<(String, String)>, String> {
    let lookup = |name: &str| env.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    let mut texts = Vec::new();
    for (var, file_var) in patch_variables() {
        let value = lookup(&var);
        let file = lookup(&file_var).filter(|v| !v.is_empty());
        match (value, file) {
            (Some(value), Some(_)) if !value.is_empty() => {
                return Err(format!(
                    "Both {var} and {file_var} are set. Set one of them: {file_var} names a file \
                     holding what {var} would hold."
                ));
            }
            (_, Some(path)) => {
                let text = std::fs::read_to_string(&path).map_err(|e| {
                    format!("{file_var} names the file '{path}', which cannot be read: {e}.")
                })?;
                texts.push((file_var, text));
            }
            (Some(value), None) => texts.push((var, value)),
            (None, None) => {}
        }
    }
    Ok(texts)
}

/// The text `MRSM_CONFIG` (or `MRSM_PATCH`) holds in `env`, from the variable or the file its
/// `*_FILE` form names: what code that inspects the patches (the console's connector listing,
/// `doctor`) reads. `None` when neither is set or the file cannot be read.
pub fn environment_patch_text(env: &[(String, String)], var: &str) -> Option<String> {
    let (_, file_var) = patch_variables().into_iter().find(|(v, _)| v == var)?;
    if let Some((_, value)) = env.iter().find(|(k, _)| k == var)
        && !value.is_empty()
    {
        return Some(value.clone());
    }
    let (_, path) = env.iter().find(|(k, v)| k == &file_var && !v.is_empty())?;
    std::fs::read_to_string(path).ok()
}

impl ConfigState {
    /// Apply `MRSM_CONFIG` / `MRSM_PATCH` texts (see [`environment_patch_texts`]).
    pub(super) fn apply_patch_texts(&mut self, texts: &[(String, String)]) {
        for (var, raw) in texts {
            let patch = match string_to_dict(raw.trim_start()) {
                Ok(p) => ConfigValue::map_from_value(&p),
                Err(_) => {
                    let shown = if var.ends_with("_FILE") {
                        "<the file's contents>".to_string()
                    } else {
                        raw.clone()
                    };
                    self.print(format!(
                        "Environment variable {var} is set but cannot be parsed.\nUnset {var} or change to JSON or simplified dictionary format (see --help, under params for formatting)\n{var} is set to:\n{shown}\nSkipping patching os environment into config..."
                    ));
                    continue;
                }
            };
            for key in patch.keys() {
                let opts = GetOptions {
                    warn: false,
                    ..GetOptions::default()
                };
                let _ = self.get_config_lossless(std::slice::from_ref(key), opts);
            }
            self.config = apply_patch_maps(&self.config, &patch);
        }
    }
}
