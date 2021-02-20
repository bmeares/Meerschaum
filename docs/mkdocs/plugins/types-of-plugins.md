# Types of Plugins

Meerschaum plugins either **provide data** or **do actions**.

Data plugins retrieve and parse data, then pass it on to Meerschaum for later analysis. There are two types of data plugins: [**fetch**](#fetch-plugins) and [**sync**](#sync-plugins) plugins.

## Data Plugins

### Fetch Plugins

Fetch plugins are the most straightforward: they pull data from some data source and return a dictionary or Pandas DataFrame. For example, one common use-case for a fetch plugin is to parse JSON data from a web API.

!!! info ""
    The purpose of fetch plugins is to retrieve and parse data, then hand it off to Meerschaum for syncing.

In case you're thinking of writing your own plugin, I recommend getting started with fetch plugins, as they're the simplest way to getting your data into Meerschaum for analysis. Check out [Writing Plugins](/plugins/plugin-development/writing-plugins/) for an in-depth walk-through.

### Sync Plugins

Like fetch plugins, sync plugins define how to get and parse data. Sync plugins, however, override the built-in `sync` process and give you complete control over the syncing process. For example, you could get really fancy with multiprocessing, distributed computing, or creating additional pipes.

Ultimately, the goal of sync and fetch plugins is the same: retrieving data from an external source and handing it off to Meerschaum.

## Action Plugins

Action plugins add additional actions to Meerschaum, such as built-in actions like `sync`, `bootstrap`, and `show`. The sky is the limit for actions ― the action function serves as an entry point from `mrsm`.

For example, the `testing` plugin provides the `test` action, which is the command to execute Meerschaum's `pytest` tests.

An action plugin can provide multiple actions, and because plugins are loaded last, there is potential for overwriting built-in actions and greatly extending Meerschaum.

Actions are a blank slate, and I'm really excited to see the creativity the community comes up with!


