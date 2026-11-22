//! Action-permission guardrails for MCP's `execute_action` (`meerschaum/mcp/_security.py`):
//! the action allow/deny lists and subaction chain check.
//!
//! SQL scanning and query policies live in [`crate::sql_guard`].

use super::McpConfig;

/// `is_action_permitted(action_name)`.
pub fn is_action_permitted(config: &McpConfig, action: &str) -> Result<(), String> {
    if !config.action_allowlist.is_empty() && !config.action_allowlist.iter().any(|a| a == action) {
        let mut allowed = config.action_allowlist.clone();
        allowed.sort();
        return Err(format!(
            "Action '{action}' is not in this server's MCP action allowlist. \
             Permitted actions: {}.",
            allowed.join(", ")
        ));
    }
    if config.action_denylist.iter().any(|a| a == action) {
        return Err(format!(
            "Action '{action}' is blocked by this server's MCP action denylist because it \
             executes arbitrary code on the API host. Set `api:mcp:actions:denylist` in the \
             Meerschaum config to change this."
        ));
    }
    Ok(())
}

/// `check_action_chain(words)`: no denylisted action as another action's subaction.
pub fn check_action_chain(config: &McpConfig, words: &[String]) -> Result<(), String> {
    for word in words {
        if config.action_denylist.iter().any(|a| a == word) {
            return Err(format!(
                "Action '{word}' is blocked by this server's MCP action denylist and may not be \
                 run through another action either."
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions() {
        let cfg = McpConfig::default();
        assert!(is_action_permitted(&cfg, "show").is_ok());
        assert!(is_action_permitted(&cfg, "sh").is_err());
        assert!(check_action_chain(&cfg, &["pipes".into()]).is_ok());
        assert!(check_action_chain(&cfg, &["python".into()]).is_err());
        let cfg = McpConfig {
            action_allowlist: vec!["sync".into()],
            ..McpConfig::default()
        };
        assert!(is_action_permitted(&cfg, "show").is_err());
    }
}
