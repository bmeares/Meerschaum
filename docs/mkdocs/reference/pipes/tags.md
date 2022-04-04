# 🏷️ Tags

<link rel="stylesheet" type="text/css" href="/assets/css/grid.css" />

You can define custom groupings of pipes with tags. Consider the example below:

<div class="grid-container center">

  <div class="grid-child">

    <img src="/assets/screenshots/tags_constructor.png"/>

  </div>

  <div class="grid-child">

    <img src="/assets/screenshots/tags_select.png"/>

  </div>

</div>


Although both pipes have different connectors and metrics, they share the tag `baz`, so they can be selected together with `--tags baz`.

This seems like a useful feature, especially if you have dozens of pipes! But how can we assign these tags?

## ✍️ Writing Tags

Like in the above example, you can define tags in the [Pipe constructor](https://docs.meerschaum.io/Pipe/):

```python
>>> import meerschaum as mrsm
>>> mrsm.Pipe(
...   "sql:foo", "bar",
...   tags=['tag1', 'tag2'],
... )
>>>
```

### Tags Live in [Parameters](/reference/pipes/#parameters)

To edit your tags interactively, just define a list under the `tags` key from `edit pipes`:

```bash
mrsm edit pipes -c sql:foo -m bar
```

```yaml
###################################################
# Edit the parameters for the Pipe 'sql_foo_bar'. #
###################################################

tags:
  - tag1
  - tag2
columns:
  datetime: date
  id: station_id
```

Finally, you can also add tags to an existing pipe by setting `.tags`:

```python
>>> import meerschaum as mrsm
>>> pipe = mrsm.get_pipes(as_list=True)[0]
>>> pipe.tags = ['tag1', 'tag2']
>>> pipe.edit() ### Persist the tags in the database.
```
