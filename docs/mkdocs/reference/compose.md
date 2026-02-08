# 🎼 Meerschaum Compose

The [`compose` plugin](https://github.com/bmeares/compose) does the same for Meerschaum as [Docker Compose](https://docs.docker.com/engine/reference/commandline/compose/) does for Docker: with Meerschaum Compose, you can consolidate everything into a single YAML file ― that includes all of the pipes and configuration needed for your project!

With `mrsm compose up`, you can stand up syncing jobs for your pipes defined in the Compose project ― one job per instance. Because the configuration is contained in the YAML file (e.g. custom connectors), Compose projects are useful for prototyping, collaboration, and consistency.


!!! tip "Multiple Compose Files"
    For complicated projects, a common pattern is to include multiple Compose files and run them with `--file`:

    ```bash
    mrsm compose run --file mrsm-compose-00-extract.yaml + \
         compose run --file mrsm-compose-01-transform.yaml + \
         compose run --file mrsm-compose-02-load.yaml
    ```

    This pattern allows multiple projects to cleanly share root and plugins directories.

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
        query: |-
          SELECT
            timestamp,
            station,
            (("temperature (degC)" * 1.8) + 32) AS fahrenheit
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

!!! tip "Template Project"

    Want to skip the setup and work in a pre-configured environment? Create a new repository from the [Meerschaum Compose Project Template](https://github.com/bmeares/mrsm-compose-template).

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

!!! tip "Plugins Directories"

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
`compose init` | Initialize a new project and install dependencies.
`compose run` | Update and sync the pipes defined in the compose file. | `--debug`: Verbosity toggle. All flags are passed to `sync pipes`.
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

Below are the supported top-level keys in a Compose file. Note that all keys are optional.

- **`connectors`**  
  Define the available connectors for the project. Alias for `config.meerschaum.connectors`.
- **`instance`**  
  Connector keys for the default instance for the project. Alias for `config.meerschaum.instance`.
- **`pipes`**  
  List all of the pipes to be used in this project. See the **The `pipes` Key** section below.
- **`sync`**  
  Govern the behavior of the syncing process. See **The `sync` Key** section below.
- **`jobs`**  
  If provided, `compose up` will start the defined jobs. See **The `jobs` Key** below.
- **`project_name`** (default to directory name)  
  The tag given to all pipes in this project. Defaults to the current directory. If you're using multiple compose files, make sure each file has a unique project name.
- **`root_dir`** (default `./root/`)  
  A path to the root directory; see [`MRSM_ROOT_DIR`](/reference/environment/#mrsm_root_dir).
- **`plugins_dir`** (default `./plugins/`)  
  Either a path or list of paths to the plugins directories. A value of `null` will include the current environment plugins directories in the project. See [`MRSM_PLUGINS_DIR`](/reference/environment/#mrsm_plugins_dir).
- **`plugins`**  
  A list of plugins expected to be in the plugins directory. Missing plugins will be [installed from `api:mrsm`](/reference/plugins/using-plugins/).  
  To install from a custom repository, append `@api:<label>` to the plugins' names or set the configuration variable `meerschaum:default_repository`.
- **`config`**  
  Configuration keys to be patched on top of your host configuration, see [`MRSM_CONFIG`](/reference/environment/#mrsm_config).
- **`environment`**  
  Additional environment variables to pass to subprocesses.
- **`isolation`**  
  If `isolation` is set to `subprocess`, then `compose` will execute commands as subprocesses, ensuring the truest level of isolation. By default, commands are executed within the host Meerschaum process, with isolated configuration and plugins and root directories. This behavior may be temporarily enabled with the flag `--isolated`.
 - **`daemon`**  
   If `daemon` is `false`, then `compose` will not use a persistent CLI daemon but will instead execute all commands in-process (i.e. appends `--no-daemon` to each command). 

!!! tip "Accessing the host configuration"

    The Meerschaum Compose YAML file also supports Meerschaum symlinks. For example, to alias a new connector `sql:foo` to your host's `sql:main`:

    ```yaml
    config:
      meerschaum:
        sql:
          foo: MRSM{meerschaum:connectors:sql:main}
    ```

!!! tip "File Path and Environment Variables"
    Each Compose file substitutes the string values `{MRSM_ROOT_DIR}` and `{__file__}` with the corresponding file paths (root directory and compose file, respectively). Similarly, you may reference standard environment variables with `$`:

    ```yaml
    connectors:
      sql:
        foo:
          flavor: "sqlite"
          database: "{MRSM_ROOT_DIR}/foo.db"
        bar:
          uri: "sqlite:///{__file__}/../bar.db" 
        secret:
          flavor: "postgresql"
          username: "${PGUSER}"
          password: ${PGPASSWORD}
          port: $PGPORT
          database: "$PGDATABASE"
    ```

### The `connectors` Key

The `connectors` key (an alias for `config.meerschaum.connectors`) defines the connectors available to the project. Project-defined connectors may reference connectors from the host Meerschaum configuration by referencing the host keys (in braces). For example, below is how you would define `sql:main` within a project to be `sql:dev` from the host environment.

```yaml
connectors:
  sql:
    main: "{sql:dev}"
```

### The `pipes` Key

The `pipes` key contains a list of [keyword arguments to build `mrsm.Pipe` objects](https://docs.meerschaum.io/index.html#meerschaum.Pipe), notably:

- `connector` (required)
- `metric` (required)
- `location` (default `null`)
- `instance` (default to `config:meerschaum:instance`)
- `parameters`
- `columns` (alias for `parameters:columns`)
- `target` (alias for `parameters:target`)
- `tags` (alias for `parameters:tags`)
- `dtypes` (alias for `parameters:dtypes`)

### The `sync` Key

Keys under the parent key `sync` are the following:

- **`schedule`**  
  Define a regular interval for syncing processes by setting a [schedule](/reference/background-jobs/#schedules).  
  This corresponds to the flag `-s` / `--schedule`.
- **`min_seconds`** (default `1`)  
  If a schedule is not set, pipes will be constantly synced and sleep `min_seconds` between laps.  
  This corresponds to the flags `--min-seconds` and `--loop`.
- **`timeout_seconds`**  
  If this value is set, long-running syncing processes which exceed this value will be terminated.  
  This corresponds to the flag `--timeout-seconds` / `--timeout`.
- **`args`**  
  This value may be a string or list of command-line arguments to append to the syncing command.  
  This option is available for specific edge case scenarios, such as when working with custom flags or specific intervals (i.e. `--begin` and `--end`).


### The `jobs` Key

Keys under `jobs` are the names of jobs to be run with `compose up`. If defined, these jobs override the default syncing jobs.

!!! example "Example jobs"
    ```yaml
    jobs:
      sync: "sync pipes -s 'every 2 hours starting 00:30'"
      verify: "verify pipes -s 'daily starting 12:00 tomorrow'"
      date: "date -s 'every 10 seconds'"
      echo: "echo 'Hello, World!'"
    ```