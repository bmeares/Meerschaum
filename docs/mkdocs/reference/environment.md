# 🌳 Environment Variables

You can manage the behavior of Meerschaum and emulate multiple installations with environment variables.

The command `mrsm show environment` will print out your current set `MRSM_` variables. For example, running this command in the Web Console reveals how the configuration is managed to subprocesses by the web server.

![Meerschaum web console environment variables.](/assets/screenshots/web-console-environment.png)

## **`MRSM_ROOT_DIR`**

By default, your root Meerschaum directory is located in `~/.config/meerschaum` (Windows: `%APPDATA%\Meerschaum`). This is similar to your base `pip` environment behaves: with `pip`, you isolate packages in virtual environments, and with Meerschaum, you can create an isolated environment by specifying an alternative root directory. This will recreate your configuration files and virtual environments as if you were running a clean installation.

```bash
mkdir foo
MRSM_ROOT_DIR=foo \
  mrsm show environment
```

## **`MRSM_PLUGINS_DIR`**

Like `MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR` lets you isolate your plugins, e.g. if you wanted to manage your plugins in a version control system like `git`.

```bash
mkdir plugins
touch plugins/example.py
MRSM_PLUGINS_DIR=plugins \
  mrsm show plugins
```

### Multiple Plugins Directories

To allow you to group plugins together, Meerschaum supports a multiple plugins directories at once. Just set `MRSM_PLUGINS_DIR` to a JSON-encoded list of paths:

```bash
export MRSM_PLUGINS_DIR='[
    "./plugins",
    "./plugins_2"
]'
```

## **`MRSM_<TYPE>_<LABEL>`**

You can temporarily register new connectors in a variable in the form `MRSM_<TYPE>_<LABEL>`, where `<TYPE>` is either `SQL` or `API`, and `<LABEL>` is the label for the connector (converted to lower case). Check here for more information about [environment connectors](/reference/connectors/#-environment-connectors), but in a nutshell, set the variable to the [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) of your connector.

```bash
MRSM_SQL_FOO=sqlite:////tmp/temp.db\
MRSM_API_BAR=http://user:pass@localhost:8000 \
  mrsm show connectors
```

## **`MRSM_CONFIG`**

You may patch your existing configuration with `MRSM_CONFIG`. Simply set the variable to JSON or a simplified dictionary. Remember you may symlink to other keys in your configuration (see below).

```bash
MRSM_CONFIG='{"foo": {"bar": 123}}' \
  mrsm show config foo

MRSM_CONFIG='foo:123' \
  mrsm show config foo

MRSM_CONFIG='foo:MRSM{meerschaum:connectors:sql:main:password}' \
  mrsm show config foo
```

## **`MRSM_PATCH`**

The variable `MRSM_PATCH` behaves the same way as `MRSM_CONFIG`. The difference is that `MRSM_PATCH` is applied to your configuration after `MRSM_CONFIG`, so you may symlink to keys defined in `MRSM_CONFIG`.

```bash
MRSM_CONFIG='{"foo": "bar": 123}' \
MRSM_PATCH='baz:MRSM{foo:bar}' \
  mrsm show config baz
```