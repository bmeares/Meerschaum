//! The MCP reference served as the `mrsm://docs` resource (`meerschaum/mcp/_docs.py`), rendered
//! from the registry (built-in and federated entries) so it cannot drift from the code.

use std::collections::BTreeMap;

use super::dispatch::{all_prompts, all_resources, all_tools};
use super::registry::builtin;
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

/// `render_reference()`.
pub async fn render_reference(cx: &ToolCx<'_>) -> String {
    let tools = all_tools(cx.server).await;
    let resources = all_resources(cx.server).await;
    let prompts = all_prompts(cx.server).await;

    let mut sorted_tools = tools.clone();
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

    let mut resources_table = vec![
        "| URI | Scope | Description |".to_string(),
        "| --- | --- | --- |".to_string(),
    ];
    for r in &resources {
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

    let mut sorted_prompts = prompts.clone();
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

    let scope_descriptions = &meerschaum_core::static_config::static_config()
        .tokens
        .scopes;
    let mut tools_by_scope: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for t in &tools {
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

    let config = &cx.server.config;
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
        builtin().primer.clone(),
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
