# Pipes in Python

A Pipe stores a named data stream on an instance. This example uses the built-in local SQLite instance and supplies its own rows:

```python
import meerschaum as mrsm

pipe = mrsm.Pipe(
    'plugin:manual', 'readings', instance='sql:local',
    columns={'id': 'id'},
)
result = pipe.sync([{'id': 1, 'temperature': 21.5}])
if not result.success:
    raise RuntimeError(result.message)

print(result.inserted, result.updated)
print(pipe.get_data(as_docs=True))
```

Sync the same row again to update it without adding a duplicate. Supply `columns={'datetime': 'ts', 'id': 'id'}` for a timestamped stream.

## Read a window

`begin` is inclusive and `end` is exclusive:

```python
rows = pipe.get_data(as_docs=True, params={'id': [1, 2]}, limit=10)
print(pipe.get_rowcount())
```

For a Pipe with a datetime column:

```python
rows = pipe.get_data(as_docs=True, begin='2026-01-01', end='2026-02-01')
```

## Find existing Pipes

```python
pipes = mrsm.get_pipes(instance='sql:local', tags=['daily'], as_list=True)
for pipe in pipes:
    print(pipe)
```

See [Pipe parameters](../../reference/pipes/parameters.md), [reading data](../../reference/pipes/reading.md), and the full [Pipe API](https://docs.meerschaum.io/meerschaum.html#Pipe).
