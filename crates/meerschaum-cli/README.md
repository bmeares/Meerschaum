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

### Venv accounting (`src/native/venvs.rs`, D20)

`attempt_import` and `pip_install` record who requested each package in
`venvs/<name>/.mrsm_requests.json` (schema in `meerschaum/utils/venv/_requests.py`). `show venvs`
lists each venv's path, size on disk and packages with versions and requesters. `prune venvs`
(v5-only; `--dry` lists, asks unless `-y`/`-f`/`--noask`) removes packages whose recorded
requesters are all uninstalled plugins and that nothing needed depends on (`Requires-Dist`),
through `uninstall packages --venv`. Packages with no ledger entry, `pip`/`setuptools`/`wheel`/`uv`/
`PyYAML`, anything requested by `meerschaum` and anything in an installed plugin's `required` list
are kept. `upgrade` prints the same plan afterwards. `prune` is in `actions::V5_ONLY_ACTIONS`: it
resolves and completes subactions, but `show actions` and the top-level completion list keep v4's
contents for the parity fixtures.

### Engine surfaces (`src/native/engine_views.rs`, `src/native/sync.rs`)

- `plan pipes` prints the engine's plan (spec 5.3) without syncing; `show graph` draws the
  selection as a DAG; `show runs [--last N]` lists recorded runs and `show run <id>` (or
  `show runs <id>`) draws a run's Gantt chart. All take the `sync pipes` selection and bound
  flags; `--nopretty` prints JSON.
- A graph `sync pipes` run (`--parents`, `--children`, `--resume`) with more than one node ends
  with the Gantt chart; on a terminal (`Runtime::live`, set only by `Runtime::system`) it also
  prints one progress line per node transition. Flat runs and `--nopretty` keep v4's output.
- `--chunk-pipelining` and `--last N` (only for `show run[s]`) are parsed by
  `parse::engine_arguments` like `--parents`. `parameters['schedule']` reaches the engine through
  a `Cadence` built on `meerschaum-jobs` (`native/engine.rs`).
- `show plugins` lists native (C-ABI) plugins as `name (native)` (bare names with `--nopretty`).

### v5-only actions, subactions and flags

v4's listings are fixtures, so v5 additions stay out of them: `actions::V5_ONLY_ACTIONS`
(`prune`, `plan`) and `actions::V5_ONLY_SUBACTIONS` (`show graph`, `show runs`) resolve and run
but are left out of `show actions`, the bare top-level and `show <TAB>` completions and the registry
parity test (they complete once a prefix is typed). v5-only flags are listed in
`help::v5_additions`: `mrsm sync pipes --help` prints v4's text, then a `v5 additions (not in v4):`
block; `help::strip_v5_additions` removes it so the help fixtures still compare byte for byte.

### The CLI daemon

v5 has no CLI daemon: `mrsm` dispatches actions directly and pools any Python workers within
the process. `daemon` keeps v4's toggle and message (`CLI daemon mode is on.`), reads its initial
state from `system:experimental:cli_daemon`, and adds one info line saying the daemon is not used
in v5. The toolbar keeps showing the instance (v4 shows `daemon` there while the mode is on).
`--no-daemon` is accepted and has nothing to disable.

### Plugin completers

The process session owns one `WorkerPool` and refreshes `ensure_manifest` at startup. An empty
plugin directory and a fresh cache need no Python process. Manifest actions, data connectors,
arguments, docstrings and `complete_<action>` functions feed help and shell completion. Plugin
argument parsing uses the plugin's Python argparse declarations. Actions run through the pool,
with stdout/stderr streamed to the CLI and `Pipe.sync` calls forwarded into the native engine.

`native/sync.rs` runs selected pipes through the engine, including graph traversal, bounds,
worker limits and continuous/listening mode. `native/engine.rs` resolves native connectors and
wraps them in `PythonRegistry`. Same-instance SQL uses a compatibility adapter which preserves
v4's in-place output and writes the engine's delta ledger around the commit.

`server.rs` hosts `start api` and `start mcp`: native HTTP/MCP backends, a shared worker session,
and a supervised Python API host selected by the manifest. MCP stdout contains protocol messages
only. Every standard action is native (`src/native/`); Python runs only a plugin's own code,
through the worker. `python` keeps its native interpreter launch.

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
- `api:` executors list no jobs during completion (v4 queries the remote executor).

### Wiring regression tests

`tests/wiring_parity.rs` compares plugin actions, custom flags, fetch results, callback writes,
and a parent graph run against `tests/fixtures/wiring_transcript.json`. Regenerate it with
`scripts/parity/run.sh crates/meerschaum-cli/tests/fixtures/gen_actions.py --wiring-only`.
The original actions transcript remains a separate unchanged fixture. `tests/server_modes.rs`
checks API startup on a disposable port and MCP stdout isolation with temporary SQLite roots.
