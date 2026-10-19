# Sync benchmark: rc1-g42-ts-fresh-confirm-3c05f9c0 (2026-10-19)

- v4: meerschaum 4.1.4 (reference venv)
- v5: 5.0.0a1
- Python 3.14.8, Linux x86_64, 40 CPUs
- measured interpreters: v4 Python 3.14.8; v5 Python 3.14.8
- repeats: 3, including every 1M case (one extra warm-up up to 100k); the table shows the median seconds
- `py` columns are timed around the call in-process; `cli` columns are whole-process wall time (startup included)
- `v5cli` uses the installed wheel console script; `v5bin` uses the standalone release binary. Historical reports named the standalone binary `v5cli`.
- `x` columns: v5 time / v4 time for the same style (`py`, `cli`). Below 1.00 is faster; **bold** marks a regression (> 10% slower)
`lat_*` cases are per call, in milliseconds

- installed wheel extension SHA-256: `6a1c7006fd292f7f00f777d8844f593e5e5c250589c828fd59863007ce5232a3`
- wheel console script SHA-256: `8786c398038440dc3b0c29c1e654910dab4711c9b6f7126893b49287e4245661`

## timescaledb

| case | v4py | v5py | x | v4cli | v5cli (wheel) | x | v5bin | x |
|---|---|---|---|---|---|---|---|---|
| lat_first_call | 312.71 ms | 110.89 ms | 0.35 | n/a | n/a |  | n/a |  |
| lat_get_sync_time | 1.78 ms | 1.56 ms | 0.88 | n/a | n/a |  | n/a |  |
| lat_get_rowcount | 1.77 ms | 1.90 ms | 1.07 | n/a | n/a |  | n/a |  |
| lat_get_data | 9.50 ms | 5.59 ms | 0.59 | n/a | n/a |  | n/a |  |
| lat_fresh_get_sync_time | 4.58 ms | 5.38 ms | **1.18** | n/a | n/a |  | n/a |  |
| lat_template_get_sync_time | 12.18 ms | 11.85 ms | 0.97 | n/a | n/a |  | n/a |  |

## Regressions

- timescaledb `lat_fresh_get_sync_time`: v5py is 1.18x v4py

## Wave 10 comparison

No matched v5 case exceeded the 20% slowdown threshold. The wheel CLI has no historical counterpart.

## Machine load

- timescaledb: load averages [1.3037109375, 2.83251953125, 4.359375]; rustc processes 0
