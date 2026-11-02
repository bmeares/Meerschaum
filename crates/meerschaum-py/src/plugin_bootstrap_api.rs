//! `bootstrap plugins` for the web console's New plugin: `_core.bootstrap_plugin_choices(name)`
//! and `_core.bootstrap_plugin(...)`, over the CLI's own scaffold
//! (`meerschaum_cli::native::bootstrap_plugins`): the same choices, checks and files, with the
//! answers given up front instead of asked for.

use std::path::PathBuf;

use meerschaum_cli::native::bootstrap_plugins::{
    DEFAULT_FEATURE, LANGUAGES, PluginAnswers, create_plugin, packaging_choices,
    plugin_feature_choices,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// The configured plugins directories, first first.
fn plugins_dirs(py: Python<'_>) -> PyResult<Vec<PathBuf>> {
    Ok(crate::facade::merged_config(py)?
        .paths()
        .plugins_dirs
        .clone())
}

/// What New plugin offers: `languages` (the first is the default), `features`
/// (`[(key, description), ...]`), `default_features`, `packaging` (`{language: [single file,
/// package]}`, worded for `name`) and `directories` (the plugins directories, first first).
#[pyfunction]
#[pyo3(signature = (name = ""))]
pub fn bootstrap_plugin_choices<'py>(py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyDict>> {
    let dirs = plugins_dirs(py)?;
    let out = PyDict::new(py);
    out.set_item("languages", LANGUAGES.to_vec())?;
    out.set_item("features", plugin_feature_choices())?;
    out.set_item("default_features", vec![DEFAULT_FEATURE])?;
    let packaging = PyDict::new(py);
    for language in LANGUAGES {
        let (single, package) = packaging_choices(name, language == "Rust");
        packaging.set_item(language, vec![single, package])?;
    }
    out.set_item("packaging", packaging)?;
    out.set_item(
        "directories",
        dirs.iter()
            .map(|dir| dir.to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
    )?;
    Ok(out)
}

/// Create a plugin: `(success, message, created)`. `language` is `Python` or `Rust`;
/// `directory` is one of `bootstrap_plugin_choices()['directories']` (the first by default).
/// `created` is `{path, files, edit, source}` (`source`: the file to edit, relative to the
/// plugin, as the console's source view names it), or `None` when nothing was written.
#[pyfunction]
#[pyo3(signature = (name, language, package, features, action = String::new(), dt_col = None, directory = None))]
#[allow(clippy::too_many_arguments)]
pub fn bootstrap_plugin<'py>(
    py: Python<'py>,
    name: String,
    language: &str,
    package: bool,
    features: Vec<String>,
    action: String,
    dt_col: Option<String>,
    directory: Option<String>,
) -> PyResult<(bool, String, Option<Bound<'py, PyDict>>)> {
    let Some(language) = LANGUAGES
        .iter()
        .find(|known| known.eq_ignore_ascii_case(language.trim()))
    else {
        return Ok((
            false,
            format!("Unknown language '{language}'. Choose Python or Rust."),
            None,
        ));
    };
    let dirs = plugins_dirs(py)?;
    let target = match directory {
        None => dirs.first().cloned(),
        Some(chosen) => dirs
            .iter()
            .find(|dir| dir.to_string_lossy() == chosen.as_str())
            .cloned()
            .or(Some(PathBuf::from(chosen))),
    };
    let Some(target) = target else {
        return Ok((false, "No plugin dir path could be found.".into(), None));
    };
    let answers = PluginAnswers {
        name,
        rust: *language == "Rust",
        package,
        features,
        action,
        dt_col,
    };
    let result = py.detach(|| create_plugin(&dirs, &target, &answers));
    let created = match result {
        Ok(created) => created,
        Err(message) => return Ok((false, message, None)),
    };
    let message = format!(
        "Successfully created {} '{}'.",
        if answers.package { "package" } else { "file" },
        created.path.display()
    );
    let source = if answers.package {
        created
            .edit
            .strip_prefix(&created.path)
            .unwrap_or(&created.edit)
            .to_path_buf()
    } else {
        PathBuf::from(created.edit.file_name().unwrap_or_default())
    };
    let info = PyDict::new(py);
    info.set_item("path", created.path.to_string_lossy().into_owned())?;
    info.set_item(
        "files",
        created
            .files
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
    )?;
    info.set_item("edit", created.edit.to_string_lossy().into_owned())?;
    info.set_item(
        "source",
        source
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/"),
    )?;
    Ok((true, message, Some(info)))
}

/// Register the bindings.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bootstrap_plugin_choices, m)?)?;
    m.add_function(wrap_pyfunction!(bootstrap_plugin, m)?)?;
    Ok(())
}
