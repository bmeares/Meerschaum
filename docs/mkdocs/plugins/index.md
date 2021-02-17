# Installing and Using Plugins

Meerschaum gives you the ability to easily install and create plugins. Refer to the guide below on how to install, write, and publish plugins.

## Installing Plugins

To install plugins, run the `install plugins` command:
```bash
mrsm install plugins testing
```

!!! info
    Any Meerschaum API instance can act as a Meerschaum repository, and the default repository is the public `mrsm.io` repository. Follow the below steps to change your default repository to a private repository:

    1. Open your configuration file with `edit config`.
        ```
        mrsm edit config
        ```
    2. Change value of `meerschaum:default_repository` to the [Connector Keys](#connector-keys) of your repository.
        ```yaml
        default_repository: api:myrepo
        ```

## Using Plugins
How you use a plugin depends on whether it's a data or an action plugin, and sometimes plugins are both.

## Publishing Plugins
TODO