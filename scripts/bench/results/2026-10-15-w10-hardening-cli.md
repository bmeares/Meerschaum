# Sync benchmark: w10-hardening-cli (2026-10-15)

- v4: meerschaum 4.1.4 (reference venv)
- v5: `mrsm` release build (Meerschaum v5.0.0-a1), façade at cd9c340f + working tree
- Python 3.14.8, Linux x86_64, 40 CPUs
- repeats: 3 (1 extra warm-up per case up to 100k; 1M uses at most 2); the table shows the median seconds
- `py` columns are timed around the call in-process; `cli` columns are whole-process wall time (startup included)
- `x` columns: v5 time / v4 time for the same style (`py`, `cli`). Below 1.00 is faster; **bold** marks a regression (> 10% slower)
- `v5pyp` (when run): the façade with `MRSM_CORE=python`, for reference only (no `x`); `lat_*` cases are per call, in milliseconds

## sqlite

| case | v4py | v5pyp | v5py | x | v4cli | v5cli | x |
|---|---|---|---|---|---|---|---|
| sync_10k | n/a | n/a | n/a |  | 3.884 | 0.377 | 0.10 |
| sync_100k | n/a | n/a | n/a |  | 28.007 | 1.343 | 0.05 |
| sync_1M | n/a | n/a | n/a |  | 361.368 | 10.348 | 0.03 |
| inc_100k | n/a | n/a | n/a |  | 19.800 | 1.146 | 0.06 |
| inc_1M | n/a | n/a | n/a |  | 19.764 | 1.312 | 0.07 |
| chain_3 | n/a | n/a | n/a |  | 16.113 | 2.180 | 0.14 |
| chain_3_engine | n/a | n/a | n/a |  | n/a | 2.255 |  |

## postgis

| case | v4py | v5pyp | v5py | x | v4cli | v5cli | x |
|---|---|---|---|---|---|---|---|
| sync_10k | n/a | n/a | n/a |  | 2.155 | 0.601 | 0.28 |
| sync_100k | n/a | n/a | n/a |  | 4.262 | 1.217 | 0.29 |
| sync_1M | n/a | n/a | n/a |  | 44.152 | 10.493 | 0.24 |
| inc_100k | n/a | n/a | n/a |  | 3.313 | 0.808 | 0.24 |
| inc_1M | n/a | n/a | n/a |  | 3.662 | 0.978 | 0.27 |
| chain_3 | n/a | n/a | n/a |  | 5.066 | 2.505 | 0.49 |
| chain_3_engine | n/a | n/a | n/a |  | n/a | 2.510 |  |

## Regressions

None above the threshold.
