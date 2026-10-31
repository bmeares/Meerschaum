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
- **`project_name`** (default derived from the file path)
  The tag given to all pipes in this project. Conventional `mrsm-compose.yaml` and `mrsm-compose.yml` files default to their containing directory's name. Other files default to their filename without the final `.yaml` or `.yml` suffix: `/projects/acme/metrics.yaml` becomes `metrics`. Each included file derives its own name. Explicit names (including effective environment values) take precedence. Set `project_name` explicitly to preserve an older directory-based identity for a custom-named file. Every project in a composition must have a unique name.
- **`root_dir`** (default `./root/`)
  A path to the root directory; see [`MRSM_ROOT_DIR`](/reference/environment/#mrsm_root_dir).
- **`plugins_dir`** (default `./plugins/`)
  Either a path or list of paths to the plugins directories. A value of `null` will include the current environment plugins directories in the project. See [`MRSM_PLUGINS_DIR`](/reference/environment/#mrsm_plugins_dir).
- **`plugins`**
  A list of plugins expected to be in the plugins directory. Missing plugins will be [installed from `api:mrsm`](/reference/plugins/repositories/#installing-plugins).
  To install from a custom repository, append `@api:<label>` to the plugins' names or set the configuration variable `meerschaum:default_repository`.
- **`config`**
  Configuration keys to be patched on top of your host configuration, see [`MRSM_CONFIG`](/reference/environment/#mrsm_config).
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

Keys under `jobs` are the names of jobs to be run with `compose up`. If defined, these jobs override the default syncing jobs: `compose up` registers the project's pipes and starts these jobs without syncing the pipes first (run `mrsm compose up --presync` to force a sync pass).

!!! example "Example jobs"
    ```yaml
    jobs:
      sync: "sync pipes -s 'every 2 hours starting 00:30'"
      verify: "verify pipes -s 'daily starting 12:00 tomorrow'"
      date: "date -s 'every 10 seconds'"
      echo: "echo 'Hello, World!'"
    ```

`compose up` only replaces jobs which belong to the project, and it checks every job it would start before it changes anything: if one of the names belongs to another project's job, `up` stops with the list of names before a pipe is registered or synced. A job started by Compose records its project's name, so it stays replaceable after you edit its command. Jobs started before that stamp existed are recognized two other ways: by the `-t <project_name>` tag Compose appends to job commands which set no tags of their own, and by running the exact command the project would start. A job under a configured name which proves none of these is refused rather than clobbered, with the caveat that two projects sharing a root directory and configuring the same job name with the same command cannot be told apart until each has started its own job.

On a host with `systemd`, Compose starts its jobs as `systemd` user services, whatever the project's `root_dir`, so they come back after a reboot without a unit of your own. A project's units are named after it (`mrsm-<project_name>.<job>.service`, for example `mrsm-weather.weather-sync-_sql:main_.service`) and carry the project's root, plugins directory and environment; `compose ps` and `show jobs` list only the current root's units. Jobs that earlier versions ran as `local` daemons move to `systemd` on the next `compose up`: Compose stops the old process first and keeps its logs. Set `meerschaum:executor: local` under the project's `config:` to keep its jobs as daemons. See [Executors](/reference/background-jobs/executors/).

Job actions run through Compose are scoped the same way, so several projects can share one root directory. `compose start jobs`, `stop jobs`, `pause jobs`, `restart jobs`, and `delete jobs` with no job names act only on this project's jobs (and do nothing if it has none). Naming a job stamped by another project is refused, since running it here would apply this project's config to it: start or stop it from its own project's compose file instead.

## 🧩 Multiple Projects: `include` and `parent`

A large deployment is easier to maintain as several small projects. A root compose file lists the others under `include:`, and `mrsm compose up`, `down`, `run`, `ps`, `logs`, `explain` and the rest then span the root and everything it includes.

```yaml
# mrsm-compose.yaml
project_name: acme
root_dir: ./root
plugins_dir: [./plugins]

include:
  - projects/                 # every *.yaml / *.yml in it, plus projects/*/mrsm-compose.yaml
  - extra/weather.yaml        # a file
  - 'regions/*.yaml'          # a glob
  - path: local-overrides.yaml
    required: false           # a missing optional file only warns

config:
  meerschaum:
    instance: "sql:acme"
```

```yaml
# projects/weather.yaml
project_name: weather
parent: ..                    # optional: run this file from the root's project
plugins: [noaa]
pipes:
  - connector: "plugin:noaa"
    metric: "weather"
    location: "atlanta"
```

**`include:` entries** are a path or `{path, required}`. A directory means every `*.yaml` / `*.yml` file in it plus `<dir>/*/mrsm-compose.yaml`. Paths are relative to the including file, includes are transitive, and a file reached twice loads once. A missing `required` path (the default) is an error, a missing optional path warns, and a glob that matches nothing is silent. An include cycle warns and skips that edge, and a file that fails to parse is reported (by `compose explain` as well) and skipped.

**Each included project keeps its identity.** It has its own `project_name` (job names and pipe tags are the same as if the file ran alone), its own `plugins_dir`, and its relative paths and `.env` resolve from its own directory. Its `root_dir` is the including project's unless the file sets one.

**The including project wins.** The root's values are layered over each included project's:

- `config` is deep-merged (the root's values win; every project keeps its own `stack.project_name`).
- `environment` and the root's `.env` variables are passed down and win. The root and plugins directories are not variables to pass down: they come through `root_dir` and `plugins_dir`.
- `pipes` are matched by `(connector, metric, location, instance)`. If the root defines the same pipe, the root's entry replaces the included one entirely (the pipe carries the root's tag).
- `plugins` are matched by name; the root's spec (for example `noaa@api:custom`) wins.
- `jobs` stay with their own project. The root may replace one with the key `<project>/<job>`, for example `weather/sync: "sync pipes -s 'every 2 hours'"`.
- A project searches its own `plugins_dir` first, then its ancestors' (never a sibling's).

`compose explain` lists every override as an informational finding.

**Order.** Operations run depth-first, the projects a file includes (in the order listed) before the file itself. `compose down` runs in reverse. `--project <name>` limits any command to one project of the tree.

**Jobs started from a root carry the label `MRSM__COMPOSE_ROOT`.** Running a command directly inside an included project that has no `parent:` warns (`managed by the root compose at ...; rerun from there or pass --force`) and leaves those jobs alone; `explain` and `show` still work.

**`parent:`** is an explicit path (a directory or a compose file, relative to the file that sets it; there is no search upward). Any subaction in a file with `parent:` runs as `mrsm compose <subaction> --project <this project>` from the parent: the parent's root directory, overrides, `.env` and plugins apply, and only this project is touched. If the parent does not include the project, it warns and treats the project as included. Parent chains are transitive (the topmost project wins per item) and a `parent:` cycle warns and stops at the last valid level. `--standalone` ignores the parent.

Jobs started together (`compose up` over several jobs or projects) go through the per-root [startup gate](/reference/background-jobs/), which staggers them; `--no-stagger` bypasses it.

## 🔎 Explaining a project

`mrsm compose explain` prints the pipes the file defines, whether each is registered on its instance, and the parameters that differ, in the same colors as the rest of Meerschaum. It is read-only: it does not create the root directory, write the config cache, install requirements or fetch plugins (it reports what `compose up` would install instead).

Every reference that does not resolve on this host is a finding, and `explain` keeps going:

- a connector or instance that is not defined (`connector 'mqtt:x' is not defined on this host (referenced by config.meerschaum.connectors.mqtt.x)`),
- a plugin that is not in the plugins directories and not listed under `plugins:`,
- an `MRSM{...}` symlink that does not resolve,
- a `{{ Pipe() }}` reference to a pipe that is not defined or registered,
- an instance that cannot be reached (the registrations are then reported as unknown).

`compose up`, `run` and `sync pipes` refuse to start on a missing connector, naming it and where it is referenced. Every compose error exits with a non-zero status.

`mrsm compose init -c <connector> -m <metric> -l <location> -t <tag> -i <instance>` in a directory with no project asks to add the matching registered pipes to the new `mrsm-compose.yaml`.
