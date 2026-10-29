# Pipelines in Python

A Pipeline selects Pipes and runs their dependencies in order. Once your Pipes are registered, save a workflow on their instance:

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('sql:local', 'fahrenheit', instance='sql:local')
pipeline = mrsm.Pipeline(pipe, parents=True, instance='sql:local')
print(pipeline.plan())

success, message = pipeline.save('climate')
if not success:
    raise RuntimeError(message)
run = pipeline.sync()
print(run.run_id, run.success, run.inserted, run.updated)
```

The [first Pipeline tutorial](../../get-started/pipelines.md) creates the source and transformation used here.

## Load and schedule

```python
pipeline = mrsm.Pipeline('climate', instance='sql:local')
print(pipeline.get_runs())
job = pipeline.schedule('every 10 minutes')
```

A saved definition keeps member references and execution settings. A Job supplies a schedule. Deleting a Pipeline definition keeps its member Pipes and their data.

See [Pipeline operations](../../reference/pipelines/index.md) and the full [Python API](https://docs.meerschaum.io).
