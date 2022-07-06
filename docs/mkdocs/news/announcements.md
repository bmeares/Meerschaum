# 📢 Announcements

## At Long Last: v1.0.0 is here!
> *27 June 2022*

It's been almost 2 years since I started Meerschaum, and the project has come an incredibly long way in that time. And now we've arrived at the culmination of all that hard work ― I have officially published **v1.0.0: Mutable at Last!**

For a full breakdown of features, check out the [changelog](/news/changelog), but here are some of the highlights:

- **Mutable data**  
  Meerschaum now handles mutable data! When rows with existing `datetime` and/or `id` values are inserted, Meerschaum will issue an `UPDATE` or `MERGE` query to update your table.

  ```python
  >>> import meerschaum as mrsm
  >>> pipe = mrsm.Pipe('foo', 'bar', columns={'datetime': 'dt', 'id': 'id'})
  >>>
  >>> ### Insert the first row.
  >>> pipe.sync([{'dt': '2022-06-26', 'id': 1, 'value': 10}])
  >>>
  >>> ### Duplicate row, no change.
  >>> pipe.sync([{'dt': '2022-06-26', 'id': 1, 'value': 10}])
  >>>
  >>> ### Update the value columns of the first row.
  >>> pipe.sync([{'dt': '2022-06-26', 'id': 1, 'value': 100}])
  ```

- **Data Type Enforcement**  
  Data types are automatically inferred when a pipe is created, and all incoming dataframes will be cast to these types. You may also manually specify the data types for certain columns:

  ```yaml
  columns:
    datetime: timestamp_utc
    id: station_id
  dtypes:
    timestamp_utc: 'datetime64[ns]'
    station_id: 'Int64'
    station_name: 'object'

  ```

## Interviewed with Console!
> *11 April 2022*

I was recently featured in [Console #100](https://console.substack.com/p/console-100), the weekly open source newsletter! I am so grateful to have been interviewed about the development process behind Meerschaum — check out the article linked above to read the story of how Meerschaum got its start.

## New Video Series and Now Available for Consulting!
> *28 January 2022*

Hello, World! There's a lot to cover, so I'll cut straight to the point: I've just started a new video series [*Learning Meerschaum*](/tutorials), the show where I give viewers ways Meerschaum can make their lives as data analysts easier. [In the first episode](https://www.youtube.com/watch?v=cS9ZAG4INPk) (embedded below), I walk through building a SQL pipe from the remote database [Splitgraph](https://www.splitgraph.com/connect). We also take a quick look at Grafana and how easy it is to use the integrated Grafana service.

I'm also ready to make the announcement: I am now officially open for consulting projects! Yes, you read that right ― you can hire me, the author of Meerschaum, to help you build your data streams! My rates are flexible, so if you're interested in recruiting my help, head over to my [GitHub sponsors page](https://github.com/sponsors/bmeares) and let's get started!

<iframe width="560" height="315" src="https://www.youtube.com/embed/cS9ZAG4INPk" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
