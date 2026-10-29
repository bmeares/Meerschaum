//! The published MCP tool reference (`docs/zensical/reference/mcp/tools.md`) is rendered from
//! the server's registry (`mcp::docs::render_tool_reference`), so it cannot drift from it.
//! `python scripts/generate_mcp_docs.py` rewrites it (this test with `MRSM_UPDATE_MCP_DOCS=1`).

use std::path::PathBuf;

use meerschaum_server::mcp::docs::render_tool_reference;
use meerschaum_server::mcp::registry::{builtin, tools};

fn page_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/zensical/reference/mcp/tools.md")
}

#[test]
fn the_tool_reference_page_is_current() {
    let b = builtin();
    let page = render_tool_reference(tools(), &b.resources, &b.prompts);
    let path = page_path();
    if std::env::var_os("MRSM_UPDATE_MCP_DOCS").is_some() {
        std::fs::write(&path, &page).expect("write the tool reference");
        return;
    }
    let Ok(current) = std::fs::read_to_string(&path) else {
        // Outside the repository (a packaged crate) there is no page to compare.
        eprintln!("SKIP: no {}", path.display());
        return;
    };
    assert!(
        current == page,
        "{} is stale: run `python scripts/generate_mcp_docs.py`",
        path.display()
    );
}

#[test]
fn the_reference_lists_v5_tools_with_their_arguments() {
    let b = builtin();
    let page = render_tool_reference(tools(), &b.resources, &b.prompts);
    for needle in [
        "### `list_pipelines`",
        "### `sync_pipeline`",
        "| `sync_pipeline` | `pipes:write`, `jobs:execute` |",
        "| `parents` | boolean \\| integer |  |",
        "| `begin` | string \\| integer |  |",
        "| `status` | `success` \\| `failed` \\| `running` |  |",
    ] {
        assert!(page.contains(needle), "{needle}");
    }
}
