//! `Pipe.get_parameters()` with symlinks applied: merge reference pipes' parameters, resolve
//! `{{ Pipe() }}` / `{{ self }}` templates, then `MRSM{}` symlinks
//! (`meerschaum/core/Pipe/_attributes.py`).

use super::{Pipe, PipeError};
use crate::config::Config;
use crate::template::{TemplateResolver, replace_in_parameters_at_depth};
use crate::value::{Map, apply_patch_maps};

/// The resolved parameters and the warnings Python would have emitted.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedParameters {
    /// The resolved parameters. When `MRSM{}` symlinks were substituted, this includes the
    /// `_symlinks` key, as in Python.
    pub parameters: Map,
    /// Warnings (failed templates, circular references, unresolvable references).
    pub warnings: Vec<String>,
}

impl Pipe {
    /// Port of `get_parameters(apply_symlinks=True)`.
    ///
    /// `resolver` loads reference pipes and evaluates templates; `config` (or else
    /// [`TemplateResolver::config`]) resolves `MRSM{}` symlinks.
    pub fn resolved_parameters(
        &self,
        resolver: &dyn TemplateResolver,
        config: Option<&Config>,
    ) -> Result<ResolvedParameters, PipeError> {
        let mut warnings = Vec::new();
        let mut visited = vec![self.clone()];
        let config = config.or_else(|| resolver.config());
        let parameters = resolve_inner(self, resolver, config, &mut visited, &mut warnings, 0)?;
        Ok(ResolvedParameters {
            parameters,
            warnings,
        })
    }
}

/// [`Pipe::resolved_parameters`] at a template nesting depth (see
/// [`crate::template::MAX_TEMPLATE_DEPTH`]).
pub(crate) fn resolve_at_depth(
    pipe: &Pipe,
    resolver: &dyn TemplateResolver,
    depth: usize,
) -> Result<ResolvedParameters, PipeError> {
    let mut warnings = Vec::new();
    let mut visited = vec![pipe.clone()];
    let parameters = resolve_inner(
        pipe,
        resolver,
        resolver.config(),
        &mut visited,
        &mut warnings,
        depth,
    )?;
    Ok(ResolvedParameters {
        parameters,
        warnings,
    })
}

fn substitute(config: Option<&Config>, params: Map, warnings: &mut Vec<String>) -> Map {
    match config {
        None => params,
        Some(cfg) => match cfg.search_and_substitute(&params) {
            Ok(m) => m,
            Err(e) => {
                warnings.push(e);
                params
            }
        },
    }
}

fn resolve_inner(
    pipe: &Pipe,
    resolver: &dyn TemplateResolver,
    config: Option<&Config>,
    visited: &mut Vec<Pipe>,
    warnings: &mut Vec<String>,
    depth: usize,
) -> Result<Map, PipeError> {
    let raw = resolver.raw_parameters(pipe);
    let defaults = resolver.defaults();
    let mut parameters = Map::new();
    let mut with_raw = pipe.clone();
    with_raw.set_parameters(raw.clone());
    with_raw.resolution_snapshot = true;
    for ref_pipe in with_raw.references(&defaults)? {
        let Some(ref_pipe) = ref_pipe else {
            warnings.push(format!(
                "Failed to resolve reference pipe for {pipe}: 'NoneType' object has no attribute 'get_parameters'"
            ));
            continue;
        };
        if visited.contains(&ref_pipe) {
            warnings.push(format!(
                "Circular reference detected in {pipe}: chain involves {ref_pipe}."
            ));
            return Ok(substitute(config, raw, warnings));
        }
        visited.push(ref_pipe.clone());
        let loaded = resolver.load_pipe(&ref_pipe);
        let base = resolve_inner(&loaded, resolver, config, visited, warnings, depth)?;
        parameters = apply_patch_maps(&parameters, &base);
    }
    parameters = apply_patch_maps(&parameters, &raw);
    let replaced =
        replace_in_parameters_at_depth(&parameters, Some(pipe), resolver, warnings, depth);
    Ok(substitute(config, replaced, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe::PipeDefaults;
    use crate::template::InMemoryResolver;
    use crate::value::Value;

    #[test]
    fn references_merge() {
        let base = Pipe::builder("sql:main", "base")
            .parameters(
                Value::from_json_str(r#"{"columns": {"datetime": "ts"}, "tags": ["a"]}"#)
                    .unwrap()
                    .as_map()
                    .unwrap()
                    .clone(),
            )
            .build()
            .unwrap();
        let child = Pipe::builder("sql:main", "child")
            .parameters(
                Value::from_json_str(
                    r#"{"reference": "Pipe('sql:main', 'base')", "tags": ["b"], "sql": "SELECT * FROM {{ Pipe('sql:main', 'base') }}"}"#,
                )
                .unwrap()
                .as_map()
                .unwrap()
                .clone(),
            )
            .build()
            .unwrap();
        let resolver = InMemoryResolver {
            defaults: PipeDefaults::default(),
            pipes: vec![base],
        };
        let out = child.resolved_parameters(&resolver, None).unwrap();
        assert_eq!(
            out.parameters.get("columns"),
            Some(&Value::from_json_str(r#"{"datetime": "ts"}"#).unwrap())
        );
        assert_eq!(
            out.parameters.get("sql"),
            Some(&Value::from("SELECT * FROM sql_main_base"))
        );
    }
}
