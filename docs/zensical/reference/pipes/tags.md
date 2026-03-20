# 🔖 Tags

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

## Selecting Pipes with Tags

The `--tags` syntax is simple yet powerful:

- Spaces are joined by `OR`.
- Commas are joined by `AND`.
- Underscores negate tags, overriding `OR` and `AND`.

You can read groups of tag pairs with commas. For example, the following will select pipes tagged as both `red` and `cat` or as `blue` and `dog`:

```bash
mrsm show pipes --tags red,cat blue,dog
```

Prefixing any tag with an underscore will negate it from the entire selection. The following selects all pipes tagged as `pet` except those tagged as `bird`:

```bash
mrsm show pipes --tags pet _bird
```

## Examining Tags

Like `show pipes`, the action `show tags` accepts the standard filter flags (`-c`, `-m`, `-l`, `-t`) and displays pipes grouped together by common tags (with repeats as necessary).

!!! tip ""
    Use positional arguments (instead of `--tags`) to limit the output of `show tags` to specific tags.

<img src="/assets/screenshots/show-tags-weather.png"/>

In the above example, we have four pipes:

- Greenville weather (`gvl`)
- Clemson weather (`clemson`)
- Atlanta weather (`atl`)
- Athens colors (`athens`)

Three tags are shared amongst these pipes:

- `ga` (Georgia)
- `sc` (South Carolina)
- `production`

From the screenshot, we can quickly tell that:

- `atl` and `athens` are tagged with `ga`.
- `clemson` and `gvl` are tagged with `sc`.
- `atl`, `clemson`, and `gvl` are tagged as `production`.

Even though we only specified the tags `sc` and `ga`, the mutual tag `production` was included in the output. This can be omitted by specifying the tags as positional arguments:

```bash
mrsm show tags sc ga
```


## Writing Tags

The quickest way to write tags is with the `tag pipes` action:

```bash
mrsm tag pipes production -c sql:main
```

Note that new tags must be specified as positional arguments ― the flag `--tags` is used for filtering.

## Removing Tags

You may also remove tags with the action `tag pipes` by prefixing tags with an underscore. In this example, we are removing the tag `production` from all pipes currently tagged as `production`.

```bash
mrsm tag pipes _production --tags production
```


## Tags in Python

You can define tags in the [Pipe constructor](https://docs.meerschaum.io/meerschaum.html#Pipe):

```python
>>> import meerschaum as mrsm
>>> mrsm.Pipe(
...   "sql:foo", "bar",
...   tags = ['tag1', 'tag2'],
... )
>>>
```

### Tags Live in [Parameters](/reference/pipes/#parameters)

To edit your tags interactively, just define a list under the `tags` key with `edit pipes`:

```bash
mrsm edit pipes -c sql:foo -m bar
```

```yaml
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
