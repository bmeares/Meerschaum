# 🎼 Meerschaum Compose

Meerschaum Compose does the same for Meerschaum as [Docker Compose](https://docs.docker.com/engine/reference/commandline/compose/) does for Docker: you can consolidate everything into a single YAML file, including all of the pipes and configuration needed for your project. Compose is built into Meerschaum 4.0 and later.

With `mrsm compose up`, you can stand up syncing jobs for your pipes defined in the Compose project, one job per instance. Because the configuration is contained in the YAML file (e.g. custom connectors), Compose projects are useful for prototyping, collaboration, and consistency.


!!! tip "Multiple Compose Files"
    For complicated projects, split the work into several compose files and tie them together with [`include:`](#multiple-projects-include-and-parent). The root file lists the others, and every `compose` command spans them:

    ```yaml
    # mrsm-compose.yaml
    project_name: acme
    include:
      - projects/
    ```

    You may also run each file on its own with `--file`, which lets multiple projects cleanly share root and plugins directories:

    ```bash
    mrsm compose run --file mrsm-compose-00-extract.yaml + \
         compose run --file mrsm-compose-01-transform.yaml + \
         compose run --file mrsm-compose-02-load.yaml
    ```

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

!!! note "Upgrading from the compose plugin"

    Compose has been built in since Meerschaum 4, and existing Compose files and commands do not need to change. To migrate:

    1. Upgrade Meerschaum. Compose is part of the core and needs no extra.
    2. Remove `mrsm install plugin compose` from project bootstrap scripts, then run `mrsm uninstall plugin compose`. Until it is removed, the built-in action takes precedence and warns once per process.
    3. Continue using the same YAML files, commands, and flags. Legacy Compose imports and registered `plugin:compose` pipes remain compatible.

    `compose down -v` deletes only resources whose project ownership is clear. Shared pipes are untagged, and ambiguous resources are preserved.

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
`compose up` | Bring up the syncing jobs (one job per instance; `systemd` services on a systemd host) | `-f`: Follow the logs once the jobs are running.
`compose down` | Take down the syncing jobs. | `-v`: Drop the pipes ("volumes").
`compose logs` | Follow the jobs' logs. | `--nopretty`: Print the logs files instead of following.
`compose ps` | Show the running status of background jobs.
`compose explain` | Print the defined pipes, whether they are registered, and a lint of the file. Nothing is created, installed or fetched. | `--project`: Limit the output to one project.

For our example project `awesome-sauce`, let's bring up the syncing jobs:

```bash
mrsm compose up -f
```

Running `mrsm compose` without a subaction opens the Meerschaum shell inside the project's environment. The project name is appended to the prompt and shown in the bottom toolbar so you can tell which project a command will run against:

```
 [ mrsm@sql:awesome | awesome-sauce ] ➤
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

The built-in `compose` action provides the following flags. You can quickly view the available flags with `mrsm -h` or `mrsm show help`.

Flag | Description | Example
--|--|--
`-f` | Follow the logs when running `compose up`. | `mrsm compose up -f`
`-v`, `--volumes` | Delete pipes when running `compose down`. | `mrsm compose down -v`
`--dry` | For `compose up`, update the pipes' registrations but don't actually begin syncing. | `mrsm compose up --dry`
`--file`, `--compose-file` | Specify an alternate compose file (default: mrsm-compose.yaml). | `mrsm compose show connectors --file config-only.yaml`
`--env`, `--env-file` | Specify an alternate environment file (default: .env). | `mrsm compose show environment --env secrets.env`
`--project` | Limit the command to one project of an [included tree](#multiple-projects-include-and-parent). | `mrsm compose up --project weather`
`--standalone` | Ignore the file's `parent:` and run it on its own. | `mrsm compose up --standalone`
`--no-stagger` | Start jobs without the [startup jitter and slots](/reference/background-jobs/). | `mrsm compose up --no-stagger`

The flags may come before or after the subaction, and `-f <file>.yaml` is accepted as a shorthand for `--file` (a bare `-f` still follows the logs): `mrsm compose --file other.yaml up` and `mrsm compose up -f other.yaml` are the same command.

## 🧬 Schema

Below are the supported top-level keys in a Compose file. Note that all keys are optional.

- **`connectors`**
  Define the available connectors for the project. Alias for `config.meerschaum.connectors`.
- **`instance`**
  Connector keys for the default instance for the project. Alias for `config.meerschaum.instance`.
- **`pipes`**
  List all of the pipes to be used in this project. See the **The `pipes` Key** section below.
- **`pipelines`**
  Define named, exact Pipe selections. Jobs reference these names; dependencies remain in Pipe parameters. See [Pipelines in Compose](pipelines/compose.md).
- **`sync`**
  Govern the behavior of the syncing process. See **The `sync` Key** section below.
- **`jobs`**
  If provided, `compose up` will start the defined jobs. See **The `jobs` Key** below.
- **`project_name`** (optional)  
  Tag applied to all pipes in this project. Defaults to the folder name for `mrsm-compose.yaml`/`mrsm-compose.yml`, or the filename stem for custom files (e.g., `metrics.yaml` → `metrics`). Every project in an `include:` tree must have a unique name.
- **`root_dir`** (default `./root/`)  
  Path to the root directory; see [`MRSM_ROOT_DIR`](/reference/environment/#mrsm_root_dir).
- **`plugins_dir`** (default `./plugins/`)  
  Path or list of paths to plugins directories (`null` includes the host's plugins directories). See [`MRSM_PLUGINS_DIR`](/reference/environment/#mrsm_plugins_dir).
- **`plugins`**  
  Plugins to ensure are installed (from `api:mrsm` by default, or append `@api:<label>` for a custom repository).
- **`config`**  
  Configuration overrides layered on top of host config ([`MRSM_CONFIG`](/reference/environment/#mrsm_config)). Compose jobs receive this via an owner-only file (`MRSM_CONFIG_FILE`) in the job directory instead of `/proc/<pid>/environ` or `systemd` unit files.
- **`environment`**
  Additional environment variables to pass to subprocesses.
- **`include`**
  Compose files, directories and globs to include. See [Multiple Projects](#multiple-projects-include-and-parent).
- **`parent`**
  The compose file or directory of the project which includes this one. See [Multiple Projects](#multiple-projects-include-and-parent).
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
  Define a regular interval for syncing processes by setting a [schedule](/reference/background-jobs/schedules/).
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

Define custom jobs under `jobs` to override the default per-instance syncing jobs (`compose up` registers pipes and starts these jobs; add `--presync` to run an initial sync first). Set `jobs: {}` to register pipes without starting any jobs.

!!! example "Example jobs"
    ```yaml
    jobs:
      sync: "sync pipes -s 'every 2 hours starting 00:30'"
      verify: "verify pipes -s 'daily starting 12:00 tomorrow'"
      date: "date -s 'every 10 seconds'"
      echo: "echo 'Hello, World!'"
    ```

- **Project ownership:** `compose up`, `start/stop/pause/restart/delete jobs` only touch jobs stamped by this project (refusing to clobber another project's job before making any changes).
- **`systemd` by default:** On systemd hosts, Compose starts jobs as user units (`mrsm-<project>.<job>.service`) so they survive reboots. Set `config: {meerschaum: {executor: local}}` to keep local daemons instead ([Executors](/reference/background-jobs/executors/)).

## 🧩 Multiple Projects: `include` and `parent`

Split large deployments into smaller Compose files tied together from a root file. Commands like `mrsm compose up`, `down`, `run`, `ps`, `logs`, and `explain` then span the entire tree.

```yaml
# mrsm-compose.yaml
project_name: acme
root_dir: ./root
plugins_dir: [./plugins]

include:
  - projects/                 # every *.yaml / *.yml, plus projects/*/mrsm-compose.yaml
  - extra/weather.yaml        # a single file
  - 'regions/*.yaml'          # a glob
  - path: local-overrides.yaml
    required: false           # missing optional files only warn

config:
  meerschaum:
    instance: "sql:acme"
```

```yaml
# projects/weather.yaml
project_name: weather
parent: ..                    # optional: run from the root project automatically
plugins: [noaa]
pipes:
  - connector: "plugin:noaa"
    metric: "weather"
    location: "atlanta"
```

### How overrides work

Each included project keeps its own `project_name`, `plugins_dir`, and relative `.env`, while the **root project wins** when values overlap:

| Key | Merge behavior |
|---|---|
| `config` / `environment` / `.env` | Deep-merged; root wins (each project keeps its own `stack.project_name`). |
| `pipes` | Matched by `(connector, metric, location, instance)`; root entry replaces the child's. |
| `plugins` | Matched by name; root spec (e.g., `noaa@api:custom`) wins. |
| `jobs` | Scoped to each project; override from the root with `<project>/<job>` (e.g., `weather/sync`). |

- **Execution order:** Depth-first (included projects before the root; `compose down` reverses order). Pass `--project <name>` to target one project.
- **`parent:`:** Lets you run `mrsm compose up` from a subproject directory while inheriting the root's config and overrides (pass `--standalone` to ignore `parent:`).
- **Startup gate:** Multi-job startups stagger automatically; pass `--no-stagger` to start immediately.

## 🔎 Explaining a project

`mrsm compose explain` prints defined pipes, registration status, parameter diffs, and lint findings—completely **read-only** (no directories created, no packages installed, no plugins downloaded):

```bash
mrsm compose explain
mrsm compose explain --project weather
```

It reports missing connectors, uninstalled plugins, unresolved `MRSM{...}` symlinks, broken `{{ Pipe() }}` references, and unreachable instances. `compose up` and `run` fail fast on missing connectors.

!!! tip "Generate a Compose file from existing pipes"
    Run `mrsm compose init -c <connector> -m <metric> -t <tag> -i <instance>` in an empty folder to scaffold a new `mrsm-compose.yaml` from matching registered pipes.
