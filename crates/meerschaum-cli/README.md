# meerschaum-cli

The `mrsm` binary: the argument parser (`spec`, `argparse`, `parse`), action dispatch, the native
actions, the formatting toolkit, and the interactive shell. Design: `docs/design/v5.0.0.md` §4.3.

## The interactive shell (`src/shell/`)

`mrsm` (or `mrsm shell`) reproduces v4.1.4's prompt_toolkit shell on `reedline`:

| Module | Ported from |
|---|---|
| `shell/mod.rs` | `Shell.py`: banner, prompt, `precmd`, shell commands, history file, the read loop |
| `shell/completer.rs` | `ShellCompleter.get_completions`, `_check_complete_keys`, `default_action_completer`, `Shell.complete_*`, the `_complete_*` functions in `meerschaum/actions/*.py` |
| `shell/editor.rs` | `PromptSession(complete_while_typing=True, reserve_space_for_menu=False)`: completion state, Tab / Shift-Tab / Up / Down, `ValidAutoSuggest`, reverse-i-search, the menu and toolbar rendering |
| `shell/toolbar.rs` | `input_with_sigint.bottom_toolbar()` |
| `shell/updates.rs` | `updates.py`: the version check thread and the update messages |

### How the toolbar and the completion menu are drawn

reedline has no toolbar, no complete-while-typing, and its menus only change the buffer on accept,
so the shell keeps prompt_toolkit's own state machine in `editor::Shared` and uses three reedline
extension points (details in the `editor.rs` module docs):

- **Hook menus**: never-active menus whose `update_values(&mut Editor, ..)` reedline calls
  synchronously when a `ReedlineEvent::Menu(name)` is handled (quick completions on). The edit mode
  (emacs plus routing) sends Tab, Shift-Tab, Up/Down, suggestion accepts, every text insertion and
  the search keys to these hooks, which update the buffer exactly as prompt_toolkit's `Buffer` would.
- **The hinter**: reedline prints the hint after the buffer on every paint and counts its lines
  when it scrolls. The hint carries the ghost text, the menu rows below the cursor (one line each,
  placed with `CHA`), and a last line that jumps to the bottom row (`CUP`) and draws the toolbar.
  reedline therefore keeps the prompt above the toolbar exactly as prompt_toolkit does (scrolling
  after long output), and every repaint, including resize and Ctrl-L, redraws the toolbar in the
  same frame. On submit reedline repaints without the hint, which removes the menu and toolbar the
  way prompt_toolkit does when input is accepted; command output then scrolls normally.
- **The prompt**: when there is no room below the cursor, prompt_toolkit opens the menu above it,
  over the prompt's leading blank line; those rows are drawn as part of the prompt string. The
  prompt is also where `(reverse-i-search)` appears.

The hinter tracks the prompt's screen row like reedline's painter (measured once per prompt with a
cursor position query, then scrolled by the same rule) and applies prompt_toolkit's float placement
(`FloatContainer._draw_float`) and menu scrolling (`Window` scroll offsets, `ScrollbarMargin`).

### The CLI daemon

v5 has no CLI daemon: actions run natively in the `mrsm` process, so there is no Python process to
keep warm. `daemon` keeps v4's toggle and message (`CLI daemon mode is on.`), reads its initial
state from `system:experimental:cli_daemon`, and adds one info line saying the daemon is not used
in v5. The toolbar keeps showing the instance (v4 shows `daemon` there while the mode is on).
`--no-daemon` is accepted and has nothing to disable.

### Plugin completers

The plugin manifest (`meerschaum-worker`) records plugin actions but not their `complete_<action>`
functions, and the CLI does not load plugin actions yet. `completer::PluginCompleter` is the seam:
plugin actions fall back to `default_action_completer` (their subactions), as in v4 for a plugin
without a completer. `plugin:<name>` labels (data plugins) need the manifest's `data_functions`
and are not listed yet.

### Tests and fixtures

- `tests/shell_completions.rs`: every input in `fixtures/shell_completions.json` (239 inputs:
  actions, subactions, flags, flag `choices`, connector keys, config keys, jobs, compose, the
  empty fallback, chained `+` and `:` pipelines) must give v4's completions exactly: text, start
  position, display and highlighted prefix.
- `tests/shell_screens.rs`: drives `mrsm` in a pseudo-terminal (`portable-pty`), renders it with
  `vt100`, and compares every snapshot of the 23 scenarios in `fixtures/shell_screens.json`
  (recorded from v4 with `pexpect` + `pyte`) cell by cell: text, cursor, and styles (colors in
  pyte's vocabulary, bold, reverse, underline). Scenarios: startup with the toolbar, the menu for
  `sh` / `show p` / `show ` / `sync pipes --` / flag choices, Tab / Shift-Tab / Up / Down cycling
  and scrolling, typing after a selection, Backspace, auto-suggest with Right and End, `instance`
  updating the prompt and toolbar, Ctrl-C, Ctrl-D, `exit`, resize, a 40x8 terminal, long output
  with the menu opening above the prompt, Ctrl-L, reverse-i-search, a trailing backslash, `daemon`,
  no ANSI, the update message, and the toolbar disabled.

Regenerate both fixtures with the reference harness (`pexpect`, `pyte` and `prompt-toolkit` are
pinned in `scripts/parity/requirements.txt`):

```bash
scripts/parity/run.sh crates/meerschaum-cli/tests/fixtures/gen_shell_screens.py [scenario ...]
scripts/parity/check_fixture_hygiene.sh crates/meerschaum-cli/tests/fixtures
```

Both the generator and the tests run every process with `MRSM_ROOT_DIR` and `HOME` in a fresh
temporary directory, an empty temporary `MRSM_PLUGINS_DIR`, SQLite connectors only (`sql:main` is
pointed at a temporary SQLite file too) and no update checks, and assert that the root is under the
temporary directory before spawning anything.

### Tolerances

The screen comparison is exact except for these, each applied only where named in
`tests/shell_screens.rs`:

1. **Version string.** `v4.1.4` and `v5.x` differ in length, so the banner's version line is
   compared with leading spaces trimmed. In the 40-column scenario (`tiny`) the banner wraps
   differently, so only the rows from the prompt down are compared there.
2. **Reverse-i-search cursor.** prompt_toolkit puts the cursor in the search field; reedline keeps
   it in the buffer at the match. The cursor is not compared while searching (the text, the match
   highlight and the toolbar are).
3. **`daemon`.** The extra info line (see above) moves the prompt down one row and the toolbar
   shows the instance; while the mode is on only the prompt row is compared.
4. **No-ANSI toolbar overflow.** v4's no-ANSI toolbar is one cell wider than the terminal (the
   status icon is counted as one column but is two wide); prompt_toolkit draws the icon half off
   the edge and v5 clips it, so the icon and the cells after column 97 are not compared in
   `no_ansi`.

The harness emulates pyte's resize (shrinking deletes lines at the top and keeps the cursor row) so
both shells are measured against the same terminal model.

### Known gaps

- A menu that must open above the cursor is only drawn over prompt lines; when the rows above the
  cursor are input (a wrapped or multi-line buffer) it is not shown (prompt_toolkit overlays them).
  With a wrapped buffer the menu opens below the end of the buffer rather than below the anchor.
- Ctrl-C leaves a menu drawn above the prompt on screen until the screen is cleared (it is, unless
  `shell:clear_screen` is off), because reedline does not repaint before returning.
- In reverse-i-search, keys other than the search keys accept the search and then run (prompt_toolkit
  edits the search text with them, e.g. Left/Right).
- Escape-F accepts the next word of a suggestion but does not reopen the completion menu.
- Plugin completers and data-plugin connector labels (see above).
- `api:` executors list no jobs during completion (v4 queries the remote executor).
