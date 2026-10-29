# Job Health

Repeating jobs record each completed run, called a lap. `mrsm show jobs` and the console show the last successful sync and whether work is failing or stalled.

| State | Meaning |
|---|---|
| `healthy` | The last lap succeeded. |
| `failing` | The last lap failed; the status includes consecutive failures. |
| `stalled` | The process is alive, but no lap has finished when expected. |
| `stopped` | The job is stopped or paused. |
| `unknown` | The first lap is running or the action does not report laps. |

```python
import meerschaum as mrsm

job = mrsm.Job('syncing-engine')
print(job.health)
# {'state': 'healthy', ..., 'message': 'last good sync 2 min ago'}
```

??? info "When a job becomes stalled"

    A repeating job is stalled after three expected intervals, with a minimum of one minute past the next due time. A first lap gets at least an hour. A `--listen` job is stalled after an hour without a sync.

    The expected interval comes from `--min-seconds`, its schedule, or its Pipes' schedules. If a lap takes longer than the loop interval, its duration is used instead.

    Lap records live in `$MRSM_ROOT_DIR/jobs/<name>/laps.json`. Successful laps are saved at most every few seconds; failures are saved immediately.
