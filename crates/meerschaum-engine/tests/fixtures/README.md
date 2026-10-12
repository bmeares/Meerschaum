# Planner golden fixtures (§14)

These are the acceptance tests for the planner in `docs/design/v5.0.0.md` §5.3–§5.4. Each JSON
file is a pipe graph, a selection, an engine state and the **expected plan, written by hand from
the spec** (not generated from the implementation). `tests/golden.rs` runs every file; run it with
`cargo test -p meerschaum-engine --test golden -- --nocapture` to also print each plan table.

All data is synthetic.

## Format

| Key | Meaning |
|---|---|
| `description` | What the fixture proves. |
| `records` | `PipeRecord`s: `instance`, `connector`, `metric`, `location`, `parameters`, and `streaming` for stream sources. The graph is built from these exactly as from a `fetch_pipes_keys` dict. |
| `implicit_edges` | `system.engine.implicit_edges` (default `true`). |
| `selection` | `{"targets": {"refs": [pipe_ref, ...]} \| {"filter": {...}}, "parents": "all" \| {"n": N}, "children": ...}`. |
| `options` | `PlanOptions`: `begin`, `end`, `interactive`, `yes`, `auto_rebuild`, `completed`. |
| `state` | `MemoryStateData`: `watermarks`, `definition_hashes`, `ledger` and `cursors` keyed by the **state instance** (the child's instance), `unreachable_instances`. A definition hash of `"@current"` is replaced by the node's current hash (unchanged definition). |
| `expected.error` | A substring of the planning error, when planning must fail. |
| `expected.warnings` | Substrings that must appear in the plan's warnings. When empty, the plan must have none. |
| `expected.nodes` | Every node, in dependency order: `action`, `bounds` (`begin` is `""` for v4's `begin=''`, `null` for unbounded), optionally `ranges`, `chunks` / `chunk_count`, `consumes` (the cursors written after success), `provisional`, `requires_confirmation`, and `reason_contains` substrings. |

`provisional: true` means a parent runs first in this run: the static plan shows the action
against the state **as it is now**, and the executor re-plans the node once its parents finish.

Times are UTC. Unless a fixture says otherwise, `fetch.backtrack_minutes` is the default 1440.

## Fixtures

### `daily_rollup_cursor_skip.json` — cursor-first `skip` for an aggregate

`daily_rollup` reads `raw_events` through `{{ Pipe() }}` (an implicit edge). Its watermark is the
day floor (`2026-10-12 00:00`) and is always below `raw_events`' (`09:45`), so a watermark-only rule
would plan `incremental` on every run. The edge cursor says the rollup consumed seq 41, the latest
entry, at parent watermark `09:45`, which is still the parent's watermark: no unconsumed entries,
no advance, definition unchanged → **`skip`**. `raw_events` has no parents in the selection →
`root` with v4 bounds `['', None)`. The rollup is provisional because `raw_events` runs first; the
executor test `rollup_is_replanned_after_its_parent_syncs` shows the re-plan.

### `six_level_chain.json` — six primary-keyed layers without a datetime column

A GIS-style chain with no datetime axis anywhere; ledger entries carry only sequence numbers.
Edges are declared four ways: `parents` strings, a `{{ Pipe() }}` in `sql`, and a singular `parent`
in the `keys()` dict form.

| Node | Edge state | Plan |
|---|---|---|
| `parcels_raw` | no parents | `root` |
| `parcels_clean` | cursor 2, entry 3 unconsumed | `backfill`, full range (no axis to bound it), consumes seq 3 |
| `parcels_joined` | cursor 7 = latest | `skip` |
| `zoning_overlay` | **no cursor** on a new edge | `incremental`, full range (first engine run; no watermarks to compare), consumes the parent's latest seq 4 |
| `districts` | cursor 2, entry 3 unconsumed | `backfill`, full range |
| `district_summary` | cursor 10 = latest | `skip` |

`zoning_overlay` is not provisional: its parent is `skip`, so it does not run first.

### `diamond.json` — branches, a join, and the capping rule

`orders → orders_by_store`, `orders → orders_by_sku`, both → `store_sku_join` (one `parents`
edge, two template edges, one in `fetch.definition`).

- `orders_by_store` (backtrack 60 min, watermark 11:00): entry 21 is late data for 10-03, and the
  parent watermark advanced 11:00 → 12:00 since the cursor. 10-03 starts before the tail (10:00), so
  the action is **`backfill`** over `[10-03, 10-04) ∪ [10-12 10:00, 10-12 12:00)`: the dirty chunk plus
  the tail, capped at the parent's watermark (12:00). Two chunks.
- `orders_by_sku`: cursor at the latest entry and watermark → `skip`.
- `store_sku_join` (backtrack 60 min, watermark 10:00): `orders_by_sku` advanced 11:00 → 12:00
  with no entries → **`incremental`** `[09:00, 11:00)`. The end is `min(11:00, 12:00)`: the capping
  rule holds the join at the slower branch.

### `cross_instance.json` — an edge across instances

`readings` lives on `sql:landing`; `readings_hourly` on `sql:warehouse`. The edge's ledger entries
and cursor are read from **`sql:warehouse`** (the child's instance). A decoy entry (seq 99, 2026-09-01)
on `sql:landing` would force a backfill if the planner read the parent's instance. On the warehouse,
entry 7 covers 10-12, which is inside the tail (`07:00 − 1 day`) → `incremental`
`[10-11 07:00, 10-12 08:30)`, capped at the parent's watermark read through `sql:landing`. Two daily
chunks. The cursor to write afterwards is on `sql:warehouse`, seq 7.

### `cross_instance_unreachable.json` — the parent's instance is down

Same edge plus a grandchild. `sql:landing` is unreachable, so the child cannot read the parent's
watermark → **`upstream_failed`** (§5.1), and its own child is `upstream_failed` because its parent
failed. The root is still `root`: it would fail at run time on its own.

### `stale_reference.json` — references to pipes that no longer exist

`report` lists `legacy_feed` in `parents` and joins `customers_v1` through `{{ Pipe() }}`; neither is
registered. Both are **warnings**, not errors, and the remaining edge from `orders` plans normally
(`incremental`, the parent watermark advanced 05:00 → 06:00). The `reference` key is parameter
inheritance and produces neither an edge nor a warning.

### `cycle.json` — a cycle is a planning error

`a → b` (`b.parents`), `b → c` (`{{ Pipe() }}` in `c.sql`), `c → a` (`a.parents`). Planning
`a --parents` fails with the cycle path `sql:main/a → sql:main/b → sql:main/c → sql:main/a`.

### `cycle_flat_selection.json` — the same cycle under a flat selection

`sync pipes -c sql:main` without `--parents`/`--children` keeps v4 semantics (R1): every node is
`root`, no capping, and the cycle becomes a warning with registration order as the fallback.

### `stream_source_one_shot.json` — a stream source in a one-shot run

`telemetry` comes from a streaming connector, so `sync pipes --parents` never runs it: **`observe`**.
Its flushes left entries 102 (the current hour) and 103 (backloaded data from 10-10). The child
(15-minute buckets, backtrack 15 min, watermark 09:30) backfills `[10-10, 10-11)` and the tail of
10-12, capped at the stream's watermark `09:59:30`. It is **not** provisional: an observed parent
does not run first.

### `integer_axis_parent.json` — integer-axis parents under a datetime child

`meter_readings` is epoch milliseconds with `precision: millisecond`; `gauge_readings` is epoch
seconds **without** `precision`, so its edge declares `axis: {unit: s}` (no inference warning).
Entry 9 (`[1791158400000, 1791244800000)` ms) maps to `[10-05, 10-06)`; entry 4
(`[1791331200, 1791332640)` s, a 24-minute legacy chunk) maps to `10-07 00:00–00:24`, widened to the
child's daily grid `[10-07, 10-08)`. Both start before the tail (10-12, backtrack 0) → `backfill`
over the two days. Watermarks are converted the same way for the cap (`min` = 10-12 05:00).

### `rebuild_interactive.json` / `rebuild_job.json` — a changed definition

`orders_clean`'s stored hash differs from the hash of its current `sql`/`dtypes`.

- **Interactive, no `--yes`:** `rebuild` with `requires_confirmation`; bounds `[None, 10-12 10:00)`
  (the full range, capped at the parent), cleared first. The report below is provisional on it.
- **Job context (non-interactive), no `--yes`:** **`rebuild_pending`**: no sync, no cursor advance,
  and a warning on every run. Its child `orders_report` still runs (`incremental`, the clean
  pipe's watermark advanced 08:00 → 09:00) against the stale data, and is not provisional because
  a pending node does not run.

### `edge_lookback.json` — a trailing-window edge

`rolling_7d` declares `{connector: plugin:sensors, metric: raw_events, lookback_days: 7}`: its
row for day `d` reads `raw_events` over `[d − 7 days, d]`. One late hour on 10-04 13:00 therefore
dirties the child from 10-04 through 10-11: `[10-04 13:00, 10-11 14:00)` widened to the daily grid
is `[10-04, 10-12)`, eight chunks. (The edge is declared both in `parents` and through
`{{ Pipe() }}`; it is deduplicated and keeps the declared options.)
