# Startup Stagger

Starting many jobs together can overload a host. Meerschaum spreads bulk starts with random delay, host load checks, and a per-root limit on starting workers.

| Start | Delay and load check | Startup slot |
|---|---|---|
| `compose up`, `restart jobs`, multiple jobs, systemd restarts | Yes | Yes |
| `start job NAME` | No | Yes |
| Any start with `--no-stagger` | No | No |

A waiting job shows `Startup: ...` in its logs. It can be stopped before its worker runs.

??? info "Limits and configuration"

    Configure `jobs:startup`:

    | Setting | Default | Purpose |
    |---|---|---|
    | `enabled` | `true` | Turn the gate on or off. |
    | `jitter_seconds` | `30` | Maximum random startup delay. |
    | `max_load` | `1.5` | Load average per CPU; `0` disables the check. |
    | `min_free_memory_percent` | `10` | Minimum free memory; `0` disables the check. |
    | `max_concurrent` | `max(2, CPUs / 2)` | Starting workers per root; `0` is unlimited. |
    | `slot_timeout_seconds` | `300` | Maximum wait or slot hold. |
    | `settle_seconds` | `15` | Linux idle period which releases a slot early. |

    Slots are shared lock files under `<root>/jobs/.startup/` and are released if their owner exits. A worker releases its slot after its first lap, at the timeout, or after settling below 10% of a CPU on Linux. Unsupported host load or memory checks pass.

    A job may override startup properties, or use `--startup-jitter 5s` when created. Set `jobs:startup:enabled: false` to disable startup staggering globally.
