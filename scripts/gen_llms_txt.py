#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate `llms.txt` (link index) and `llms-full.txt` (concatenated docs) from the
zensical nav, so agents/IDEs get a digestible map instead of the full 14k-line docs.

Run via `scripts/docs.sh`; reads `docs/zensical.toml`, writes into `docs/zensical/`
(zensical copies them to the site root).
"""
from __future__ import annotations
import re
import sys
import tomllib
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"
CONFIG = DOCS / "zensical.toml"


def doc_url(site_url: str, rel: str) -> str:
    """Map a nav file path to its clean published URL."""
    if rel.endswith("/index.md"):
        rel = rel[: -len("index.md")]
    elif rel == "index.md":
        rel = ""
    elif rel.endswith(".md"):
        rel = rel[:-3] + "/"
    return site_url.rstrip("/") + "/" + rel.lstrip("/")


_CSS_LINE = re.compile(r"^[.#]?[\w-]+.*\{\s*$|^\s*[\w-]+\s*:\s*.+;\s*$|^\}\s*$")


def doc_title_and_desc(md_path: Path) -> tuple[str, str]:
    """First H1 (title) and first real paragraph (description) of a markdown file."""
    if not md_path.exists():
        return "", ""
    text = md_path.read_text(encoding="utf-8")
    ### Strip YAML frontmatter.
    if text.startswith("---"):
        text = text.split("---", 2)[-1]
    title, desc, in_block, in_fence = "", "", False, False
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if "<style" in s or "<script" in s:
            in_block = True
        if in_block:
            in_block = not ("</style>" in s or "</script>" in s)
            continue
        if s.startswith("# ") and not title:
            title = s[2:].strip()
            continue
        if desc:
            continue
        if not s or s.startswith(("#", "<", "!", "|", "```", "[//]")) or _CSS_LINE.match(s):
            continue
        ### Drop markdown link/emphasis syntax, collapse whitespace.
        s = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", s)
        s = re.sub(r"[*_`]", "", s)
        desc = re.sub(r"\s+", " ", s).strip()[:200]
    return title, desc


def walk(node, section: list) -> None:
    """Collect (title, rel_path) leaves from a nav node into `section`."""
    if isinstance(node, str):
        if node.endswith(".md"):
            section.append((None, node))
        return
    if isinstance(node, list):
        for item in node:
            walk(item, section)
        return
    if isinstance(node, dict):
        for title, value in node.items():
            if isinstance(value, str):
                if value.endswith(".md"):
                    section.append((title, value))
                ### http(s) external links are skipped.
            else:
                walk(value, section)


def main() -> int:
    cfg = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    project = cfg["project"]
    site_url = project["site_url"]
    docs_dir = DOCS / project.get("docs_dir", "zensical")
    nav = project["nav"]

    ### Group leaves under their top-level nav section.
    sections: list[tuple[str, list]] = []
    for item in nav:
        if not isinstance(item, dict):
            continue
        for title, value in item.items():
            leaves: list = []
            walk(value, leaves)
            if leaves:
                sections.append((title, leaves))

    index_lines = [
        f"# {project['site_name']}",
        "",
        f"> {project['site_description']}",
        "",
    ]
    full_parts = [f"# {project['site_name']}\n\n> {project['site_description']}\n"]

    for section_title, leaves in sections:
        index_lines.append(f"## {section_title}")
        for title, rel in leaves:
            md_path = docs_dir / rel
            h1, desc = doc_title_and_desc(md_path)
            name = title or h1 or (section_title if len(leaves) == 1 else rel)
            url = doc_url(site_url, rel)
            index_lines.append(f"- [{name}]({url})" + (f": {desc}" if desc else ""))
            full_parts.append(
                f"\n\n---\n\n# {name}\n\nSource: {url}\n\n"
                + (md_path.read_text(encoding="utf-8") if md_path.exists() else "")
            )
        index_lines.append("")

    ### Fail the build rather than ship an empty/broken index.
    assert sections, "no nav sections found — check zensical.toml nav structure"
    assert any(line.startswith("- [") for line in index_lines), "no doc links generated"

    (docs_dir / "llms.txt").write_text("\n".join(index_lines), encoding="utf-8")
    (docs_dir / "llms-full.txt").write_text("".join(full_parts), encoding="utf-8")
    print(f"Wrote llms.txt ({len(sections)} sections) and llms-full.txt to {docs_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
