# Architecture decision records

Decisions that later architecture work builds on rather than reopens. Read the ones that touch
the area you are changing; if a change contradicts one, say so explicitly and why. Format and
when to add one: [`docs/agents/domain.md`](../agents/domain.md). The 5.0 decision log these
summarize is §3 of [`docs/design/v5.0.0.md`](../design/v5.0.0.md).

| ADR | Decision | Source |
|---|---|---|
| [0001](0001-python-reached-only-through-workers-and-hosts.md) | Python is reached from Rust only through workers and hosts | D12 |
| [0002](0002-no-python-core-in-the-package.md) | No Python core in the package; released v4 is the parity oracle | D14 |
| [0003](0003-user-sql-runs-on-the-rust-driver.md) | User SQL from Python runs on the Rust driver on every flavor | D16 |
| [0004](0004-zero-dependency-base-wheel.md) | The base wheel has no runtime dependencies | D20 |
| [0005](0005-job-startup-admission-gate.md) | Jobs start through a per-root admission gate | D23 |
| [0006](0006-runtime-images-and-size-budgets.md) | Runtime image variants and size budgets | D25 |
| [0007](0007-isolated-teams-scoped-processes.md) | Isolated teams run in scoped processes under the server's user | 5.1 teams |
| [0008](0008-main-api-keeps-authority-over-team-executors.md) | The main API keeps all authority; team executors are untrusted and fail closed | 5.1 teams |
| [0009](0009-team-execution-requires-enforced-sandbox.md) | Team execution runs only where the sandbox covers sockets and signals | 5.1 teams |
| [0010](0010-flags-modify-actions.md) | A flag modifies an action; new behavior is a new action | D26 |
