# 0005. Jobs start through a per-root admission gate

Status: accepted (5.0.0rc2). Source: [`docs/design/v5.0.0.md`](../design/v5.0.0.md) §3 D23, §6.4.

## Context

Starting many jobs at once (`compose up`, `restart jobs`, a reboot starting every systemd unit)
made them all boot, import plugins and run their first lap on top of each other.

## Decision

Every job passes a per-root, cross-process startup gate before its worker starts: `flock` slots
under `<root>/jobs/.startup/` held through the first lap, random jitter and load/memory waits for
bulk and automatic starts, and an optional fixed per-job schedule jitter. Compose-only staggering
and a fixed concurrency cap without jitter were rejected.

## Consequences

- A job "stuck at starting" for 10 to 30 seconds is usually waiting at the gate.
- Tests that wait for bulk-started jobs pass `--no-stagger` (or set `jobs.startup.enabled: false`).
- The gate lives in `crates/meerschaum-jobs/src/startup.rs` and is shared by local daemons,
  systemd units and compose.
