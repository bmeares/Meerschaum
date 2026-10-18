# A v4-style plugin that does a plain `import pandas` (v4 activated the shared mrsm venv for plugins).
__version__ = '1.0.0'
required = ['tabulate']


def fetch(pipe, **kw):
    import pandas as pd
    return pd.DataFrame([{'ts': '2024-03-01', 'k': 'a', 'v': 1.0}])
