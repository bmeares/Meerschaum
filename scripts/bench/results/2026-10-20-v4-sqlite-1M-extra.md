# Sync benchmark: v4-sqlite-1M-extra (2026-10-20)

- v4: meerschaum 4.1.4 (reference venv)
- v5: unavailable
- Python 3.14.8, Linux x86_64, 40 CPUs
- hardware: Intel(R) Xeon(R) Gold 5215 CPU @ 2.50GHz, 40 logical CPUs, 377 GiB RAM
- measured interpreters: v4 Python 3.14.8; v5 Python unrecorded
- repeats: 5, including every 1M case (one extra warm-up up to 100k); the table shows the median seconds
- `py` columns are timed around the call in-process; `cli` columns are whole-process wall time (startup included)
- `v5cli` uses the installed wheel console script; `v5bin` uses the standalone release binary. Historical reports named the standalone binary `v5cli`.
- `x` columns: v5 time / v4 time for the same style (`py`, `cli`). Below 1.00 is faster; **bold** marks a regression (> 10% slower)
`lat_*` cases are per call, in milliseconds


## sqlite

| case | v4py | v5py | x | v4cli | v5cli (wheel) | x | v5bin | x |
|---|---|---|---|---|---|---|---|---|
| sync_1M | 301.477 | n/a |  | n/a | n/a |  | n/a |  |

## Regressions

None above the threshold.

## Wave 10 comparison

No matched v5 case exceeded the 20% slowdown threshold. The wheel CLI has no historical counterpart.

## Machine load

- sqlite: load averages [7.201171875, 5.857421875, 4.4306640625]; rustc processes 0
