# 🌳 Environment Variables

You can manage the behavior of Meerschaum and emulate multiple installations with environment variables.

The command `mrsm show environment` will print out your current set `MRSM_` variables. For example, running this command in the Web Console reveals how the configuration is managed to subprocesses by the web server.

<img src="/assets/screenshots/web-console-environment.png" alt="Meerschaum web console environment variables.">

## Summary

All Meerschaum environment variables begin with the prefix `MRSM_`. The most commonly used variables are summarized below; each is described in detail later on this page.

| Variable | Purpose | Default |
|---|---|---|
| `MRSM_ROOT_DIR` | Root directory holding all config, plugins, venvs, jobs, and cache. | `~/.config/meerschaum` (Windows: `%APPDATA%\Meerschaum`) |
| `MRSM_CONFIG_DIR` | Directory for configuration files (isolate secrets from the root). | `$MRSM_ROOT_DIR/config` |
| `MRSM_PLUGINS_DIR` | Directory (or colon-separated list / JSON list) for plugins. | `$MRSM_ROOT_DIR/plugins` |
| `MRSM_VENVS_DIR` | Directory for virtual environments. | `$MRSM_ROOT_DIR/venvs` |
| `MRSM_WORK_DIR` | Base directory under which the root directory is created when `MRSM_ROOT_DIR` is unset. | current working directory |
| `MRSM_CONFIG` | Inline JSON or simplified-dictionary patch applied to the configuration. | _(unset)_ |
| `MRSM_PATCH` | A second patch applied *after* `MRSM_CONFIG` (may reference its keys). | _(unset)_ |
| `MRSM_<TYPE>_<LABEL>` | Define a connector by URI (e.g. `MRSM_SQL_MAIN`, `MRSM_API_MAIN`). | _(unset)_ |
| `MRSM_RUNTIME` | Marks a special runtime (e.g. `portable`, `docker`). | _(unset)_ |
| `MRSM_NOASK` / `MRSM_NONINTERACTIVE` | Disable interactive prompts (assume defaults). | _(unset)_ |

!!! tip "Inspecting your environment"
    Run `mrsm show environment` to list every currently set `MRSM_` variable. Run `mrsm show config` to inspect the resolved configuration after all patches and connectors have been applied.

## **`MRSM_ROOT_DIR`**

By default, your root Meerschaum directory is located in `~/.config/meerschaum` (Windows: `%APPDATA%\Meerschaum`). This is similar to how your base `pip` environment behaves: with `pip`, you isolate packages in virtual environments, and with Meerschaum, you can create an isolated environment by specifying an alternative root directory. This will recreate your configuration files and virtual environments as if you were running a clean installation.

```bash
mkdir foo
MRSM_ROOT_DIR=foo \
  mrsm show environment
```

## **`MRSM_CONFIG_DIR`**

Not to be confused with `MRSM_CONFIG`, setting `MRSM_CONFIG_DIR` allows you to designate a separate directory outside of the root to isolate your secrets and segment your configuration. If unset, the default directory is `$MRSM_ROOT_DIR/config`.

## **`MRSM_PLUGINS_DIR`**

Like `MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR` lets you isolate your plugins, e.g. if you wanted to manage your plugins in a version control system like `git`.

```bash
mkdir plugins
touch plugins/example.py

MRSM_PLUGINS_DIR=plugins \
  mrsm show plugins
```

### Multiple Plugins Directories

To allow you to group plugins together, Meerschaum supports loading multiple plugins directories at once. Simply separate the paths with a colon like you would for `$PATH`:

```bash
export MRSM_PLUGINS_DIR='./plugins:/another/plugins/path'
```

You could also set `MRSM_PLUGINS_DIR` to a JSON-encoded list of paths:

```bash
export MRSM_PLUGINS_DIR='[
    "./plugins",
    "/another/plugins/path"
]'
```

## **`MRSM_VENVS_DIR`**

Like `MRSM_PLUGINS_DIR`, you can designate a separate directory outside of the Meerschaum root to contain virtual environments. This is useful for sharing virtual environments between deployments as well as separating package data from user data (e.g. Kubernetes deployments).

```bash
MRSM_VENVS_DIR='venvs/' mrsm show plugins
```

## **`MRSM_WORK_DIR`**

`MRSM_WORK_DIR` designates the base working directory under which the root directory is located when `MRSM_ROOT_DIR` is not explicitly set. This is primarily useful for portable and containerized deployments where the process is launched from a known location.

## **`MRSM_<TYPE>_<LABEL>`**

You can temporarily register new connectors in a variable in the form `MRSM_<TYPE>_<LABEL>`, where `<TYPE>` is the connector type, and `<LABEL>` is the label for the connector (converted to lower case). Check here for more information about [environment connectors](/reference/connectors/#-environment-connectors), but in a nutshell, set the variable to the [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) of your connector.

```bash
MRSM_SQL_FOO=sqlite:////tmp/temp.db \
MRSM_API_BAR=http://user:pass@localhost:8000 \
  mrsm show connectors
```

Common examples are the default connectors `sql:main` and `api:main`:

```bash
MRSM_SQL_MAIN=postgresql://user:pass@localhost:5432/db \
MRSM_API_MAIN=https://user:pass@api.example.com \
  mrsm show connectors
```

You may also set the variable to a JSON object of connector attributes instead of a URI:

```bash
MRSM_SQL_FOO='{"flavor": "sqlite", "database": "/tmp/temp.db"}' \
  mrsm show connectors
```

!!! note "Reserved names"
    Names that match the reserved Meerschaum variables (e.g. `MRSM_ROOT_DIR`, `MRSM_CONFIG`) are *not* interpreted as connectors.

## **`MRSM_CONFIG`**

You may patch your existing configuration with `MRSM_CONFIG`. Simply set the variable to JSON or a simplified dictionary. Remember you may symlink to other keys in your configuration (see [`MRSM{}` config symlinks](#mrsm-config-symlinks) below).

```bash
MRSM_CONFIG='{"foo": {"bar": 123}}' \
  mrsm show config foo

MRSM_CONFIG='foo:123' \
  mrsm show config foo

MRSM_CONFIG='foo:MRSM{meerschaum:connectors:sql:main:password}' \
  mrsm show config foo
```

## **`MRSM_PATCH`**

The variable `MRSM_PATCH` behaves the same way as `MRSM_CONFIG`. The difference is that `MRSM_PATCH` is applied to your configuration *after* `MRSM_CONFIG`, so you may symlink to keys defined in `MRSM_CONFIG`.

```bash
MRSM_CONFIG='{"foo": {"bar": 123}}' \
MRSM_PATCH='baz:MRSM{foo:bar}' \
  mrsm show config baz
```

## **`MRSM_RUNTIME`**

`MRSM_RUNTIME` marks a special runtime context. For example, `MRSM_RUNTIME=portable` is set by the portable distribution so that Meerschaum can ensure `readline` is available, and `MRSM_RUNTIME=docker` is set inside the official Docker image. You typically do not need to set this yourself.

## **`MRSM_NOASK` / `MRSM_NONINTERACTIVE`**

Set either of these to disable interactive prompts. When set, Meerschaum assumes default answers (equivalent to passing `--noask` / `--yes`), which is useful for automation and CI pipelines.

```bash
MRSM_NOASK=1 mrsm bootstrap pipes
```

## Internal Variables

The following variables are set automatically by Meerschaum (e.g. for background jobs, the `systemd` executor, and the API server) and are documented here for completeness. You generally should not set them by hand.

| Variable | Purpose |
|---|---|
| `MRSM_SERVER_ID` | Identifier of the API server instance. |
| `MRSM_DAEMON_ID` | Identifier of the running daemon (background job). |
| `MRSM_JOB` | Marks that the current process is running as a job. |
| `MRSM_SYSTEMD_LOG_PATH` | Log path used by the `systemd` job executor. |
| `MRSM_SYSTEMD_STDIN_PATH` | Stdin FIFO path used by the `systemd` job executor. |
| `MRSM_SYSTEMD_RESULT_PATH` | Result path used by the `systemd` job executor. |
| `MRSM_SYSTEMD_DELETE_JOB` | Signals the `systemd` executor to delete the job on exit. |
| `MRSM_TEST_FLAVORS` | Comma-separated DB flavors used by the test suite. |

---

## `MRSM{}` Config Symlinks

Anywhere a configuration value (or pipe parameter) is read, you may reference another key in your configuration with the `MRSM{key1:key2:key3}` syntax. The reference is resolved to the target config value **at access time**, so a single source of truth (for example, a database password) can be reused across many keys.

The path inside the braces is a colon-separated sequence of config keys, equivalent to the arguments you would pass to `get_config()`. For example, `MRSM{meerschaum:connectors:sql:main:password}` resolves to `config.meerschaum.connectors.sql.main.password`.

Symlinks are resolved by `search_and_substitute_config()` in `meerschaum/config/_read_config.py` (exported from `meerschaum.config`).

### In configuration

```bash
# Reuse the SQL main password as a top-level config value.
MRSM_CONFIG='foo:MRSM{meerschaum:connectors:sql:main:password}' \
  mrsm show config foo
```

### In a connector

```yaml
# config/connectors.yaml
sql:
  reporting:
    flavor: postgresql
    host: localhost
    database: reporting
    username: MRSM{meerschaum:connectors:sql:main:username}
    password: MRSM{meerschaum:connectors:sql:main:password}
```

Now `sql:reporting` shares credentials with `sql:main` — change them in one place.

### In a pipe parameter

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('demo', 'cfg', parameters={
    'username': 'MRSM{meerschaum:connectors:sql:main:username}',
})
print(pipe.parameters['username'])  # resolved to the configured username at access time
```

!!! warning "`MRSM{}` is not `{{ Pipe() }}`"
    These are two distinct features:

    - **`MRSM{key1:key2:key3}`** references a value in your **configuration**.
    - **`{{ Pipe('ck', 'mk', 'lk') }}`** references another **pipe** (its target, columns, attributes, etc.) and is resolved when `Pipe.parameters` is accessed.

    See [Pipe Parameters → `{{ Pipe(...) }}` syntax](/reference/pipes/parameters/) for the pipe-reference feature.
