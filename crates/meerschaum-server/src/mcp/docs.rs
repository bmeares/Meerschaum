//! The MCP reference served as the `mrsm://docs` resource (`meerschaum/mcp/_docs.py`), rendered
//! from the registry (built-in and federated entries) so it cannot drift from the code.

use std::collections::BTreeMap;

use super::McpConfig;
use super::dispatch::{all_prompts, all_resources, all_tools};
use super::registry::{Prompt, Resource, Tool, primer};
use super::tools::ToolCx;

/// Abbreviations whose trailing period does not end a sentence.
const ABBREVIATIONS: &[&str] = &["e.g", "i.e", "etc", "vs", "cf"];

/// `_first_sentence(text)`.
pub fn first_sentence(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let bytes = collapsed.as_bytes();
    for (i, _) in collapsed.match_indices('.') {
        let next_is_space = collapsed[i + 1..]
            .chars()
            .next()
            .is_some_and(char::is_whitespace);
        if !next_is_space {
            continue;
        }
        let head = &collapsed[..i];
        if ABBREVIATIONS.iter().any(|a| head.ends_with(a)) {
            continue;
        }
        return format!("{head}.");
    }
    let _ = bytes;
    if collapsed.ends_with('.') {
        collapsed
    } else {
        format!("{collapsed}.")
    }
}

fn code_list<'a>(items: impl IntoIterator<Item = &'a String>) -> String {
    items
        .into_iter()
        .map(|i| format!("`{i}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `render_reference()`: the reference of this server's entries (built-in, native and federated).
pub async fn render_reference(cx: &ToolCx<'_>) -> String {
    let tools = all_tools(cx.server).await;
    let resources = all_resources(cx.server).await;
    let prompts = all_prompts(cx.server).await;
    render(&tools, &resources, &prompts, primer(), &cx.server.config)
}

/// The reference of these entries, with `primer` as its data model (what
/// [`render_reference`] renders; the contract test renders v4's registry with it too).
pub fn render(
    tools: &[Tool],
    resources: &[Resource],
    prompts: &[Prompt],
    primer: &str,
    config: &McpConfig,
) -> String {
    let tools_table = tools_table(tools);
    let resources_table = resources_table(resources);
    let prompts_table = prompts_table(prompts);
    let scopes_table = scopes_table(tools);
    render_sections(
        &tools_table,
        &resources_table,
        &prompts_table,
        &scopes_table,
        primer,
        config,
    )
}

/// The tools table (sorted by name).
pub fn tools_table(tools: &[Tool]) -> Vec<String> {
    let mut sorted_tools = tools.to_vec();
    sorted_tools.sort_by(|a, b| a.name.cmp(&b.name));
    let mut tools_table = vec![
        "| Tool | Scope | Read-only | Destructive | Description |".to_string(),
        "| --- | --- | :-: | :-: | --- |".to_string(),
    ];
    for t in &sorted_tools {
        let scopes = if t.scopes.is_empty() {
            "—".to_string()
        } else {
            code_list(&t.scopes)
        };
        tools_table.push(format!(
            "| `{}` | {} | {} | {} | {} |",
            t.name,
            scopes,
            if t.read_only { "✅" } else { "" },
            if t.destructive { "⚠️" } else { "" },
            first_sentence(&t.description)
        ));
    }
    tools_table
}

/// The resources table.
pub fn resources_table(resources: &[Resource]) -> Vec<String> {
    let mut resources_table = vec![
        "| URI | Scope | Description |".to_string(),
        "| --- | --- | --- |".to_string(),
    ];
    for r in resources {
        let uri = r
            .uri_template
            .clone()
            .or_else(|| r.uri.clone())
            .unwrap_or_default();
        let scopes = if r.scopes.is_empty() {
            "none".to_string()
        } else {
            code_list(&r.scopes)
        };
        resources_table.push(format!(
            "| `{uri}` | {scopes} | {} |",
            first_sentence(&r.description)
        ));
    }
    resources_table
}

/// The prompts table (sorted by name).
pub fn prompts_table(prompts: &[Prompt]) -> Vec<String> {
    let mut sorted_prompts = prompts.to_vec();
    sorted_prompts.sort_by(|a, b| a.name.cmp(&b.name));
    let mut prompts_table = vec![
        "| Prompt | Arguments | Description |".to_string(),
        "| --- | --- | --- |".to_string(),
    ];
    for p in &sorted_prompts {
        let args: Vec<String> = p
            .arguments
            .iter()
            .map(|a| {
                let name = a.get("name").map(|n| n.py_str()).unwrap_or_default();
                let required = a.get("required").is_some_and(|r| r.truthy());
                format!("`{name}`{}", if required { "" } else { " *(optional)*" })
            })
            .collect();
        let args = if args.is_empty() {
            "—".to_string()
        } else {
            args.join(", ")
        };
        prompts_table.push(format!(
            "| `{}` | {args} | {} |",
            p.name,
            first_sentence(&p.description)
        ));
    }
    prompts_table
}

/// The scopes table: each scope with the tools it unlocks.
pub fn scopes_table(tools: &[Tool]) -> Vec<String> {
    let scope_descriptions = &meerschaum_core::static_config::static_config()
        .tokens
        .scopes;
    let mut tools_by_scope: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for t in tools {
        let scopes = if t.scopes.is_empty() {
            vec!["(none)".to_string()]
        } else {
            t.scopes.clone()
        };
        for scope in scopes {
            tools_by_scope
                .entry(scope)
                .or_default()
                .push(t.name.clone());
        }
    }
    let mut scopes_table = vec![
        "| Scope | Unlocks | Meaning |".to_string(),
        "| --- | --- | --- |".to_string(),
    ];
    for (scope, names) in &tools_by_scope {
        let mut names = names.clone();
        names.sort();
        scopes_table.push(format!(
            "| `{scope}` | {} | {} |",
            code_list(&names),
            scope_descriptions.get(scope).cloned().unwrap_or_default()
        ));
    }
    scopes_table
}

fn render_sections(
    tools_table: &[String],
    resources_table: &[String],
    prompts_table: &[String],
    scopes_table: &[String],
    primer: &str,
    config: &McpConfig,
) -> String {
    let mut notes = Vec::new();
    if !config.action_allowlist.is_empty() {
        let mut a = config.action_allowlist.clone();
        a.sort();
        notes.push(format!(
            "`execute_action` is restricted to an allowlist: {}.",
            code_list(&a)
        ));
    }
    if !config.action_denylist.is_empty() {
        let mut d = config.action_denylist.clone();
        d.sort();
        notes.push(format!(
            "These actions are blocked because they execute arbitrary code on the API host: {}.",
            code_list(&d)
        ));
    }
    if config.read_only {
        notes.push(
            "**This server is running in read-only mode**, so only read-only tools are exposed \
             regardless of your token's scopes."
                .to_string(),
        );
    }

    let mut sections: Vec<String> = vec![
        "# Meerschaum MCP reference".into(),
        String::new(),
        "## Data model".into(),
        String::new(),
        primer.to_string(),
        String::new(),
        "## Tools".into(),
        String::new(),
        tools_table.join("\n"),
        String::new(),
        "Tools marked ⚠️ delete or overwrite data. Tools marked ✅ never modify anything. \
         Every tool also carries these as MCP annotations (`readOnlyHint`, \
         `destructiveHint`, `idempotentHint`), so a client can prompt before a \
         destructive call."
            .into(),
        String::new(),
        "## Resources".into(),
        String::new(),
        resources_table.join("\n"),
        String::new(),
        "## Prompts".into(),
        String::new(),
        prompts_table.join("\n"),
        String::new(),
        "## Scopes".into(),
        String::new(),
        "Over HTTP, each tool requires an OAuth2 scope on your token — the same scopes the \
         REST routes enforce. A token with the `*` scope may call anything. Tools your token \
         cannot call are hidden from `tools/list` rather than failing at call time."
            .into(),
        String::new(),
        scopes_table.join("\n"),
        String::new(),
        "## Limits".into(),
        String::new(),
        "- `read_sql` accepts a single read-only statement. Writes, DDL, and stacked \
         statements are refused. For untrusted callers, back the connector with a read-only \
         database role as well."
            .into(),
        "- `execute_action` grants broad control over the instance and its host, which is \
         what the `actions:execute` scope means."
            .into(),
    ];
    sections.extend(notes.into_iter().map(|n| format!("- {n}")));
    sections.push(
        "- List results are paginated. Pass a response's `next_cursor` back as `cursor` to \
         continue; a null `next_cursor` means there is nothing more."
            .into(),
    );
    sections.push(String::new());
    sections.join("\n")
}

/// The head of `docs/zensical/reference/mcp/tools.md` ([`render_tool_reference`]).
const TOOL_REFERENCE_HEADER: &str = "<!--
    GENERATED FILE — DO NOT EDIT.
    Regenerate with `python scripts/generate_mcp_docs.py` after changing the MCP registry
    (`crates/meerschaum-server/src/mcp/`: v4's `registry.json` and v5's additions in `v5.rs`).
-->

# MCP Tool Reference

Every tool, resource, and prompt the [MCP server](index.md) exposes, generated
from the server's registry.

Over HTTP each tool requires an OAuth2 scope, and tools your token does not
cover are hidden from `tools/list` rather than refused at call time. A tool of
Pipelines, runs, jobs or connectors is also hidden when the server does not serve
the routes of its HTTP equivalent (`api:permissions:routes`). The stdio
transport grants everything, because it runs as the user who launched it.
";

/// The end of `docs/zensical/reference/mcp/tools.md`.
const TOOL_REFERENCE_FOOTER: &str = "
## Annotations

Every tool carries the MCP annotations a client uses to decide whether to prompt
before calling:

| Annotation | Meaning |
| --- | --- |
| `readOnlyHint` | The tool never modifies anything. |
| `destructiveHint` | The tool deletes or overwrites data. |
| `idempotentHint` | Calling it twice with the same arguments has the same effect as once. |
| `openWorldHint` | The tool can reach beyond this Meerschaum instance. |

## Pagination

`list_pipes`, `get_pipe_data`, and `read_sql` return a `next_cursor`. Pass it
back as `cursor` for the next page; a `null` cursor means there is nothing more.

On a pipe with a datetime axis, narrowing with `begin` and `end` is far cheaper
than paging — cursors re-read the rows they skip.
";

/// The published tool reference (`docs/zensical/reference/mcp/tools.md`): the tables of
/// [`render`] and each tool's scopes, annotations and arguments.
pub fn render_tool_reference(tools: &[Tool], resources: &[Resource], prompts: &[Prompt]) -> String {
    let mut sections: Vec<String> = vec![
        TOOL_REFERENCE_HEADER.to_string(),
        "## Tools".into(),
        String::new(),
        tools_table(tools).join("\n"),
        String::new(),
        "## Resources".into(),
        String::new(),
        resources_table(resources).join("\n"),
        String::new(),
        "## Prompts".into(),
        String::new(),
        prompts_table(prompts).join("\n"),
        String::new(),
        "## Scopes".into(),
        String::new(),
        scopes_table(tools).join("\n"),
        String::new(),
        "## Tool details".into(),
        String::new(),
    ];
    let mut sorted = tools.to_vec();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    for tool in &sorted {
        sections.push(format!("### `{}`", tool.name));
        sections.push(String::new());
        sections.push(tool.description.clone());
        sections.push(String::new());
        let scopes = if tool.scopes.is_empty() {
            "none".to_string()
        } else {
            code_list(&tool.scopes)
        };
        sections.push(format!("**Scope:** {scopes}"));
        sections.push(String::new());
        let flags: Vec<&str> = [
            ("read-only", tool.read_only),
            ("destructive", tool.destructive),
            ("idempotent", tool.idempotent),
            ("open-world", tool.open_world),
        ]
        .iter()
        .filter(|(_, on)| *on)
        .map(|(flag, _)| *flag)
        .collect();
        if !flags.is_empty() {
            sections.push(format!("**Annotations:** {}", flags.join(", ")));
            sections.push(String::new());
        }
        let properties = tool
            .input_schema
            .get("properties")
            .and_then(meerschaum_core::Value::as_map)
            .cloned()
            .unwrap_or_default();
        let required = tool.required();
        if !properties.is_empty() {
            sections.push("| Argument | Type | Required | Description |".into());
            sections.push("| --- | --- | :-: | --- |".into());
            for (name, schema) in &properties {
                let ty = match (schema.get("enum"), schema.get("type")) {
                    (Some(meerschaum_core::Value::List(values)), _) if !values.is_empty() => values
                        .iter()
                        .map(|v| format!("`{}`", v.py_str()))
                        .collect::<Vec<_>>()
                        .join(" \\| "),
                    (_, Some(meerschaum_core::Value::List(types))) => types
                        .iter()
                        .map(meerschaum_core::Value::py_str)
                        .collect::<Vec<_>>()
                        .join(" \\| "),
                    (_, Some(t)) => t.py_str(),
                    _ => "any".into(),
                };
                let description = schema
                    .get("description")
                    .map(meerschaum_core::Value::py_str)
                    .unwrap_or_default()
                    .replace('\n', " ");
                let is_required = if required.contains(name) { "✅" } else { "" };
                sections.push(format!(
                    "| `{name}` | {ty} | {is_required} | {description} |"
                ));
            }
            sections.push(String::new());
        }
    }
    sections.push(TOOL_REFERENCE_FOOTER.to_string());
    sections.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sentences() {
        assert_eq!(first_sentence("A b. C d."), "A b.");
        assert_eq!(first_sentence("Use e.g. this. Then"), "Use e.g. this.");
        assert_eq!(first_sentence("No period"), "No period.");
        assert_eq!(first_sentence("  Spaced\n  out.  "), "Spaced out.");
    }
}
