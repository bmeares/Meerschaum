# Python

Install the package, then import the SDK:

```bash
python -m pip install meerschaum
```

```python
import meerschaum as mrsm
```

The package includes the native engine and `mrsm` command. Plain dictionaries work without a DataFrame library. Install `meerschaum[polars]` or `meerschaum[pandas]` when you want those frames.

| Next | What you build |
|---|---|
| [Pipes](pipes.md) | Sync and read local rows |
| [Pipelines](pipelines.md) | Save, plan, and run a workflow |
| [Plugins](plugins.md) | Fetch from a custom source |

[Browse the complete Python API](https://docs.meerschaum.io), including signatures and module documentation.
