# 🎼 Meerschaum Compose

The [`compose` plugin](https://github.com/bmeares/compose) does the same for Meerschaum as [Docker Compose](https://docs.docker.com/engine/reference/commandline/compose/) did for Docker: with Meerschaum Compose, you can consolidate everything into a single YAML file ― that includes all of the pipes and configuration needed for your project!

The purpose of a Compose project is to stand up syncing jobs for your pipes ― one job per instance. Because the configuration is contained in the YAML file (e.g. custom connectors), Compose projects are useful for prototyping, collaboration, and consistency.

??? example "Example Compose File"
    This compose project demonstrates how to sync two pipes to a new database `awesome.db`:

    ```yaml
    sync:
      schedule: "every 30 seconds"
      pipes:
        - connector: "plugin:noaa"
          metric: "weather"
          location: "atlanta"
          parameters:
            noaa:
              stations:
                - "KATL"

        - connector: "sql:awesome"
          metric: "fahrenheit"
          target: "atl_temp"
          parameters:
            query: >
              SELECT
                timestamp,
                station,
                (("temperature (wmoUnit:degC)" * 1.8) + 32) AS fahrenheit
              FROM plugin_noaa_weather_atlanta
          columns:
            datetime: "timestamp"
            id: "station"

    plugins:
      - "noaa"

    config:
      meerschaum:
        instance: "sql:awesome"
        connectors:
          sql:
            awesome:
              database: "awesome.db"
              flavor: "sqlite"
    ```

## ⛺ Setup

Install the `compose` plugin from the public repository `api:mrsm`:

```bash
mrsm install plugin compose
```

From a new directory, create a file `mrsm-compose.yaml`. You can paste the example file above to get started.

```bash
mkdir awesome-sauce && \
  cd awesome-sauce && \
  vim mrsm-compose.yaml
```

!!! tip "Plugins directories"

    You may set multiple paths for `plugins_dir`. This is very useful if you want to group plugins together. A value of `null` will include the environment's plugins in your project.

    ```yaml
    plugins_dir:
      - "./plugins"
      - null
    ```

## 🪖 Commands

If you've used `docker-compose`, you'll catch on to Meerschaum Compose pretty quickly. Here's a quick summary:

Command | Description | Useful Flags
--|--|--
`compose up` | Bring up the syncing jobs (process per instance) | `-f`: Follow the logs once the jobs are running.
`compose down` | Take down the syncing jobs. | `-v`: Drop the pipes ("volumes").
`compose logs` | Follow the jobs' logs. | `--nopretty`: Print the logs files instead of following.
`compose ps` | Show the running status of background jobs.

For our example project `awesome-sauce`, let's bring up the syncing jobs:

```bash
mrsm compose up -f
```

??? tip "All other commands are executed as regular actions from within the project environment."

    ```bash
    ### Print out the environment variables set by the compose file.
    mrsm compose show environment

    ### Verify that custom connectors are able to be parsed.
    mrsm compose show connectors sql:awesome

    ### The default instance for this project is sql:awesome, so pipes will be fetched from there by default.
    mrsm compose show pipes
    mrsm compose show columns
    mrsm compose show rowcounts

    ### Jump into an interactive CLI.
    mrsm compose sql awesome
    ```

## 🎌 Flags

The `compose` plugin adds a few new custom flags. You can quickly view the available flags with `mrsm -h` or `mrsm show help`.

Flag | Description | Example
--|--|--
`-f` | Follow the logs when running `compose up`. | `mrsm compose up -f`
`-v`, `--volumes` | Delete pipes when running `compose down`. | `mrsm compose down -v`
`--dry` | For `compose up`, update the pipes' registrations but don't actually begin syncing. | `mrsm compose up --dry`
`--file`, `--compose-file` | Specify an alternate compose file (default: mrsm-compose.yaml). | `mrsm compose show connectors --file config-only.yaml`
`--env`, `--env-file` | Specify an alternate environment file (default: .env). | `mrsm compose show environment --env secrets.env`

## 🧬 Schema

The supported top-level keys in a Compose file are the following:

- **`sync`** (**required**)  
  Govern the behavior of the syncing process. See **The `sync` Key** section below for further details.
- **`root_dir`** (*optional*, default `./root/`)  
  A path to the root directory; see [`MRSM_ROOT_DIR`](/reference/environment/#mrsm_root_dir).
- **`plugins_dir`** (*optional*, default `./plugins/`)  
  Either a path or list of paths to the plugins directories. A value of `null` will include the current environment plugins directories in the project. See [`MRSM_PLUGINS_DIR`](/reference/environment/#mrsm_plugins_dir).
- **`plugins`** (*optional*)  
  A list of plugins expected to be in the plugins directory. Missing plugins will be [installed from `api:mrsm`](/reference/plugins/using-plugins/).  
  To install from a custom repository, append `@api:<label>` to the plugins' names or set the configuration variable `meerschaum:default_repository`.
- **`config`** (*optional*)  
  Configuration keys to be patched on top of your host configuration, see [`MRSM_CONFIG`](/reference/environment/#mrsm_config). Custom connectors should be defined here.
- **`environment`** (*optional*)  
  Additional environment variables to pass to subprocesses.

!!! tip "Accessing the host configuration"

    The Meerschaum Compose YAML file also supports Meerschaum symlinks. For example, to alias a new connector `sql:foo` to your host's `sql:main`:

    ```yaml
    config:
      meerschaum:
        sql:
          foo: MRSM{meerschaum:connectors:sql:main}
    ```

### The `sync` Key

Keys under the root key `sync` are the following:

- **`pipes`** (**required**)  
  The `pipes` key contains a list of [keyword arguments to build `mrsm.Pipe` objects](https://docs.meerschaum.io/index.html#meerschaum.Pipe), notably:
    - `connector` (**required**)
    - `metric` (**required**)
    - `location` (*optional*, default `null`)
    - `instance` (*optional*, defaults to `config:meerschaum:instance`)
    - `parameters` (*optional*)
    - `columns` (*optional*)  
      Alias for `parameters:columns`.
    - `target` (*optional*)  
      Alias for `parameters:target`.
    - `tags` (*optional*)  
      Alias for `parameters:tags`.
    - `dtypes` (*optional*)  
    Alias for `parameters:dtypes`.
- **`schedule`** (*optional*)  
  Define a regular interval for syncing processes by setting a [schedule](/reference/background-jobs/#schedules).  
  This corresponds to the flag `-s` / `--schedule`.
- **`min_seconds`** (*optional*, default `1`)  
  If a schedule is not set, pipes will be constantly synced and sleep `min_seconds` between laps.  
  This corresponds to the flags `--min-seconds` and `--loop`.
- **`timeout_seconds`** (*optional*)  
  If this value is set, long-running syncing processes which exceed this value will be terminated.  
  This corresponds to the flag `--timeout-seconds` / `--timeout`.
- **`args`** (*optional*)  
  This value may be a string or list of command-line arguments to append to the syncing command.  
  This option is available for specific edge case scenarios, such as when working with custom flags or specific intervals (i.e. `--begin` and `--end`).