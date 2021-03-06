# Installing and Using Plugins

Meerschaum gives you the ability to easily install and create plugins. Refer to the guide below on how to install, write, and publish plugins.

## Installing Plugins

To install plugins, run the `install plugins` command, followed by the names of the plugins you wish to install:
```bash
install plugins noaa
```

If you would like to install plugins from a private Meerschaum repository, you can specify from where you would like to download the plugin with the  `--repository` or  `-r` flag.  For example, to install the plugin   `example` from your private Meerschaum API `api:myapi`, you would execute:

```bash
install plugins example -r api:myapi
```

!!! info
    Any Meerschaum API instance can act as a Meerschaum repository, and the default repository is the public `api.mrsm.io` repository. Follow the  steps below to change your default repository to a private repository:

    1. Open your configuration file with `edit config`.
        ```
        mrsm edit config
        ```
    2. Change value of `meerschaum:default_repository` to the [Connector Keys](/reference/connectors) of your repository.
        ```yaml
        default_repository: api:myrepo
        ```

## Using Plugins
How you use a plugin depends on it's [type](types-of-plugins): whether it's a [data](types-of-plugins/#data-plugins) or an [action](types-of-plugins/#action-plugins) plugin, and sometimes plugins can be both.

To use a data plugin (e.g. `noaa`), bootstrap a pipe and 



## Publishing Plugins
TODO