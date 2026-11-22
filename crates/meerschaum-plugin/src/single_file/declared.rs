//! What a single-file plugin declares about itself (`VERSION`, `REQUIRED`) and the Cargo
//! dependencies its `use` lines and `REQUIRED` entries add to the generated crate.

use super::*;

/// What a single-file plugin declares about itself: `const VERSION: &str = "1.2.0";` and
/// `const REQUIRED: &[&str] = &["package", "plugin:name"];`, the Rust spelling of Python's
/// `__version__` and `required`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SingleFileMetadata {
    /// `VERSION`, when declared.
    pub version: Option<String>,
    /// `REQUIRED` (empty when not declared).
    pub required: Vec<String>,
}

pub(super) fn declared_metadata(file: &Path, parsed: &syn::File) -> Result<SingleFileMetadata> {
    let mut metadata = SingleFileMetadata::default();
    for item in &parsed.items {
        let Item::Const(constant) = item else {
            continue;
        };
        // Only the documented types declare metadata; a `const VERSION: u32` stays a helper.
        match constant.ident.to_string().as_str() {
            "VERSION" if str_type(&constant.ty) => {
                let version = string_literal(&constant.expr)
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| {
                        diagnostic(
                            file,
                            constant.expr.span(),
                            "VERSION must be a non-empty string literal, e.g. const VERSION: &str = \"1.0.0\";",
                        )
                    })?;
                metadata.version = Some(version);
            }
            "REQUIRED" if str_slice_type(&constant.ty) => {
                let invalid = || {
                    diagnostic(
                        file,
                        constant.expr.span(),
                        "REQUIRED must be a list of string literals, e.g. const REQUIRED: &[&str] = &[\"requests\"];",
                    )
                };
                let Expr::Reference(reference) = constant.expr.as_ref() else {
                    return Err(invalid());
                };
                let Expr::Array(array) = reference.expr.as_ref() else {
                    return Err(invalid());
                };
                metadata.required = array
                    .elems
                    .iter()
                    .map(|e| string_literal(e).ok_or_else(invalid))
                    .collect::<Result<_>>()?;
            }
            _ => {}
        }
    }
    Ok(metadata)
}

/// `&str` / `&'static str`.
fn str_type(ty: &Type) -> bool {
    matches!(ty, Type::Reference(r) if r.mutability.is_none() && plain_type(&r.elem, "str"))
}

/// `&[&str]` / `&'static [&'static str]`.
pub(super) fn str_slice_type(ty: &Type) -> bool {
    matches!(ty, Type::Reference(r) if r.mutability.is_none()
        && matches!(r.elem.as_ref(), Type::Slice(slice) if str_type(&slice.elem)))
}

/// Read a single-file plugin's `VERSION` and `REQUIRED` without generating or building it.
pub(crate) fn metadata(file: &Path) -> Result<SingleFileMetadata> {
    let source = std::fs::read_to_string(file).map_err(error)?;
    let parsed = syn::parse_file(&source)
        .map_err(|e| diagnostic(file, e.span(), format!("invalid Rust plugin: {e}")))?;
    declared_metadata(file, &parsed)
}

/// Optional Cargo dependencies in a Rust doc-comment fence; never infer dependencies from use.
pub(super) fn dependencies(parsed: &syn::File, file: &Path) -> Result<toml::Table> {
    dependencies_for(parsed, file, false)
}

pub(super) fn dependencies_for(
    parsed: &syn::File,
    file: &Path,
    confined: bool,
) -> Result<toml::Table> {
    let mut block = None;
    let mut current: Option<String> = None;
    let doc_lines: Vec<String> = parsed
        .attrs
        .iter()
        .filter_map(|attr| {
            if attr.path().is_ident("doc")
                && let syn::Meta::NameValue(meta) = &attr.meta
                && let syn::Expr::Lit(expr) = &meta.value
                && let syn::Lit::Str(text) = &expr.lit
            {
                return Some(text.value());
            }
            None
        })
        .collect();
    for doc in doc_lines.iter().flat_map(|doc| doc.lines()) {
        let doc = doc.strip_prefix(' ').unwrap_or(doc);
        if doc.trim() == "```cargo" {
            if current.is_some() || block.is_some() {
                return Err(error("only one ```cargo dependency block is allowed"));
            }
            current = Some(String::new());
        } else if doc.trim() == "```" && current.is_some() {
            block = current.take();
        } else if let Some(text) = &mut current {
            text.push_str(doc);
            text.push('\n');
        }
    }
    if current.is_some() {
        return Err(error("unclosed ```cargo dependency block"));
    }
    let Some(block) = block else {
        return Ok(toml::Table::new());
    };
    let mut cargo: toml::Table = toml::from_str(&block).map_err(|e| {
        error(format!(
            "invalid Cargo dependency block in '{}': {e}",
            file.display()
        ))
    })?;
    if cargo.keys().any(|k| k != "dependencies") {
        return Err(error(
            "single-file Cargo blocks support only [dependencies]; use a packaged plugin for other Cargo settings",
        ));
    }
    let Some(deps) = cargo.remove("dependencies") else {
        return Ok(toml::Table::new());
    };
    let mut deps = deps
        .as_table()
        .cloned()
        .ok_or_else(|| error("dependencies must be a Cargo table"))?;
    for (name, spec) in &mut deps {
        if ["meerschaum_plugin", "meerschaum-plugin"].contains(&name.as_str()) {
            return Err(error(format!(
                "'{name}' is supplied by Meerschaum; do not declare it in [dependencies]"
            )));
        }
        if !(spec.is_str() || spec.is_table()) {
            return Err(error(format!(
                "dependency '{name}' needs a version string or a Cargo dependency table"
            )));
        }
        if let Some(table) = spec.as_table_mut() {
            if confined
                && [
                    "path",
                    "git",
                    "branch",
                    "tag",
                    "rev",
                    "registry",
                    "index",
                    "workspace",
                ]
                .iter()
                .any(|key| table.contains_key(*key))
            {
                return Err(error(format!(
                    "confined dependency '{name}' must use the host's pinned offline registry inputs"
                )));
            }
            if table.contains_key("workspace")
                || table.get("optional").and_then(toml::Value::as_bool) == Some(true)
            {
                return Err(error(format!(
                    "dependency '{name}' cannot use workspace or optional; use a packaged plugin"
                )));
            }
            if let Some(path) = table.get_mut("path") {
                let relative = path
                    .as_str()
                    .ok_or_else(|| error("dependency path must be a string"))?;
                let resolved = file.parent().unwrap_or(Path::new(".")).join(relative);
                *path = toml::Value::String(
                    resolved
                        .canonicalize()
                        .map_err(|e| error(format!("dependency '{name}': {e}")))?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    Ok(deps)
}
