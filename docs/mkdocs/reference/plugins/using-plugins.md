# Installing and Using Plugins

Meerschaum gives you the ability to easily install and create plugins. Refer to the guide below on how to install, write, and publish plugins.

## Installing Plugins

To install plugins, run the `install plugins` command, followed by the names of the plugins you wish to install, for example:
```bash
install plugins noaa color
```

If you would like to install plugins from a private Meerschaum repository, you can specify from where you would like to download the plugin with the  `--repository` or  `-r` flag.  For example, to install the plugin   `example` from your private Meerschaum API `api:myapi`, you would execute:

```bash
install plugins example -r api:myapi
```

!!! info
    [Any Meerschaum API instance can act as a Meerschaum repository](/reference/connectors/#instances-and-repositories), and the default repository is the public `api.mrsm.io` repository. Follow the steps below to change your default repository to a private repository:

    1. Open your configuration file with `edit config`.
        ```
        mrsm edit config
        ```
    2. Change value of `meerschaum:default_repository` to the [Connector Keys](/reference/connectors) of your repository.
        ```yaml
        default_repository: api:myrepo
        ```

### Hosting Plugins
If you've [written](/tutorials/plugin-development/writing-plugins/) or installed plugins, you can host them on your own private repository. You will need a user login to your private repostitory to host plugins. Replace the values in angle brackets (`<>`) with your own values.

1. Create a user account on your [Meerschaum instance](/reference/connectors/#instances-and-repositories) with `register user <username>`.  
  *If you are connecting to an existing repository, skip to step 3.*

2. Start the API server with `start api`.

3. Register and upload the plugin to your private repository with `register plugin <plugin> --repo api:<label>`.  
  *If you're uploading to your own machine, use `api:local` as the repository.*

## Using Plugins
How you use a plugin depends on its [type](types-of-plugins): whether it's a [data](types-of-plugins/#data-plugins), [action](types-of-plugins/#action-plugins), or [API](/reference/plugins/types-of-plugins/#api-plugins) plugin, and sometimes plugins can be all three.

To use a data plugin (e.g. `noaa`), bootstrap a pipe and choose the the plugin as the [connector](/reference/connectors/#connectors) (e.g. `plugin:noaa`).

To use an action plugin, simply execute the new actions provided by the plugin. You can see the available actions with `show actions`.

To use an API plugin, launch the web API with `start api`.