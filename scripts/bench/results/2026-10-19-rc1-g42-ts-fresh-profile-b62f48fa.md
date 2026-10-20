# Sync benchmark: rc1-g42-ts-fresh-profile-b62f48fa (2026-10-19)

- v4: meerschaum 4.1.4 (reference venv)
- v5: 5.0.0a1
- Python 3.14.8, Linux x86_64, 40 CPUs
- measured interpreters: v4 Python 3.14.8; v5 Python 3.14.8
- repeats: 1, including every 1M case (one extra warm-up up to 100k); the table shows the median seconds
- `py` columns are timed around the call in-process; `cli` columns are whole-process wall time (startup included)
- `v5cli` uses the installed wheel console script; `v5bin` uses the standalone release binary. Historical reports named the standalone binary `v5cli`.
- `x` columns: v5 time / v4 time for the same style (`py`, `cli`). Below 1.00 is faster; **bold** marks a regression (> 10% slower)
`lat_*` cases are per call, in milliseconds

- installed wheel extension SHA-256: `6a1c7006fd292f7f00f777d8844f593e5e5c250589c828fd59863007ce5232a3`
- wheel console script SHA-256: `8786c398038440dc3b0c29c1e654910dab4711c9b6f7126893b49287e4245661`

## timescaledb

| case | v4py | v5py | x | v4cli | v5cli (wheel) | x | v5bin | x |
|---|---|---|---|---|---|---|---|---|
| lat_first_call | 552.27 ms | 205.77 ms | 0.37 | n/a | n/a |  | n/a |  |
| lat_get_sync_time | 3.36 ms | 2.28 ms | 0.68 | n/a | n/a |  | n/a |  |
| lat_get_rowcount | 3.06 ms | 1.51 ms | 0.49 | n/a | n/a |  | n/a |  |
| lat_get_data | 18.99 ms | 9.42 ms | 0.50 | n/a | n/a |  | n/a |  |
| lat_fresh_get_sync_time | 8.22 ms | 6.48 ms | 0.79 | n/a | n/a |  | n/a |  |
| lat_template_get_sync_time | 20.87 ms | 14.37 ms | 0.69 | n/a | n/a |  | n/a |  |

## Regressions

None above the threshold.

## Wave 10 comparison

No matched v5 case exceeded the 20% slowdown threshold. The wheel CLI has no historical counterpart.

## Machine load

- timescaledb: load averages [1.18115234375, 2.32373046875, 3.9462890625]; rustc processes 0
