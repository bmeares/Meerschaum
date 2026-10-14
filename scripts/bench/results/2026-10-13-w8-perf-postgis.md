# Sync benchmark: w8-perf-postgis (2026-10-13)

- v4: meerschaum 4.1.4 (reference venv)
- v5: `mrsm` release build (Meerschaum v5.0.0-a1), façade at dac87ac1 + working tree
- Python 3.14.8, Linux x86_64, 40 CPUs
- repeats: 3 (1 extra warm-up per case up to 100k; 1M uses at most 2); the table shows the median seconds
- `py` columns are timed around the call in-process; `cli` columns are whole-process wall time (startup included)
- `x` columns: v5 time / v4 time for the same style (`py`, `cli`). Below 1.00 is faster; **bold** marks a regression (> 10% slower)

## postgis

| case | v4py | v5py | x | v4cli | v5cli | x |
|---|---|---|---|---|---|---|
| sync_10k | 1.470 | 1.130 | 0.77 | n/a | n/a |  |
| sync_100k | 3.660 | 2.341 | 0.64 | n/a | n/a |  |
| sync_1M | 46.432 | 11.951 | 0.26 | n/a | n/a |  |
| inc_100k | 2.683 | 1.998 | 0.74 | n/a | n/a |  |
| inc_1M | 2.863 | 2.205 | 0.77 | n/a | n/a |  |
| filter_existing_100k | 1.796 | 0.634 | 0.35 | n/a | n/a |  |
| get_data_1M | 6.115 | 1.686 | 0.28 | n/a | n/a |  |

## Regressions

None above the threshold.
