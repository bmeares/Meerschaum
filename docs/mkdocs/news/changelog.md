---
hide:
  - toc
---

# Changelog and Release Notes
This changelog was not introduced until v0.2.12, so many of the critical releases of Meerschaum have already been published. I've backlogged previous releases but haven't added all notes for all the versions.

## 0.4.x Releases
This is the current release cycle, so future features will be updated below.

### v0.4.10
- **Fixed parsing issue with the Docker build.**  
  There is a strange edge case where multiple levels of JSON-encoding needed to be escaped, and this scenario has been accounted for.
- **Enforce `MRSM_CONFIG` and `MRSM_PATCH` in the Web Console actions.**  
  The Docker version of the API uses environment variables to manage instances, so this information is passed along to children threads.
- **Added the bottom toolbar to the interactive shell.**  
   The includes the current instance, repo, and connection status.
- **Delayed imports when changing instances.**  
   This postpones trying to connect to an instance until as late as possible.

### v0.4.1 — v0.4.7
- **Added features to the Web Console.**  
  Features such as the `Show Pipes` button and others were added to give the Web Console better functionality.
- **Migrated the Web Console to Bootstrap 5.**  
  Many components needed to be modified or rewritten, but ultimately the move to Bootstrap 5 is worth it in the long run.
- **Updated to work on Python 3.10.**  
  This included creating a standalone internal module for `cascadict` since the original project is no longer maintained.
- **Tighter security.**  
  Better enforcement of datetimes in `dateadd_str()` and denying users access to actions if the permissions setting does not allow non-admins to perform actions.
- **Bugfixes for broken dependencies.**  
  In addition to migrating to Bootstrap 5, components like `PyYAML` and `fastapi-login` changed their function signatures which broke things.

### v0.4.0
- **Allow for other plugins to be specified as dependencies.**  
  Other plugins from the same repository may be specified in the `required` list.
- **Added warnings for broken plugins.**  
  When plugins fail to be imported, warnings are thrown to help authors identify the problem.
- **Added registration to the Web Console.**  
  New users may create accounts by clicking the *No account?* link on the login page.
- **Added the `verify` action.**  
  For now, `verify packages` ensures that the installed dependencies meet the stated requirements for the installed version of Meerschaum.
- **Fixed "ghost" background jobs.**  
  Ensure that jobs are *actually* running before marking them as so.

## 0.3.x Releases
Version 0.3.0 introduced the web interface and added more robust SQL support for various flavors, including MSSQL and DuckDB.

### v0.3.12 — v0.3.19
- **Mostly small bugfixes.**  
  Docker-compose fixes, `params` in `get_pipe_rowcount()`, unique index names for pipes.
- **Added `newest` flag to `pipe.get_sync_time()`.**  
  Setting `newest=False` will return the oldest time instead of the newest.
- **Migrated `filter_existing` to a member of `Pipe`.**  
  Although the current implementation for APIConnectors offloads filtering to the SQLConnector, soon filtering will take place locally to save bandwidth.
- **Updated Docker base image.**  
  Bumped base image from Python 3.7 on Debian Buster Slim to Python 3.9 on Debian Bullseye Slim. Also removed ARM images for the sake of passing builds and reducing build times (e.g. DuckDB fails to compile with QEMU).
- **Improved DuckDB support.**  
  `sql:memory` is now the default in-memory DuckDB instance.

### v0.3.1 – v0.3.11
- **Improved Microsoft SQL Server support.**
- **Added plugins page to the dashboard.**  
  Although somewhat hidden away, the path `/dash/plugins` will show the plugins hosted on the API repository. If the user is logged in, the descriptions of plugins belonging to that user become editable.
- **Added locks to resolve race conditions with threading.**
- **Added `--params` when searching for data and backtracked data.**
- **Fixed the `--params` flag for API pipes.**
- **Added experimental multiplexed fetching feature**  
  To enable this feature, run `mrsm edit config system` and under the `experimental` section, set `fetch` to `true`.
- **Bugfixes and stability improvements**


### v0.3.0
- **Introduced the Web Interface.**  
  Added the Meerschaum Web Interface, an interactive dashboard for managing Meerschaum instances. Although not a total replacement for the Meerschaum Shell, the Web Interface allows multiple users to share connectors without needing to remote into the same machine.

- **Background jobs**  
  Actions may be run in the background with the `-d` or `--daemon` flags or with the action `start job`. To assign a name to a job, pass the flag `--name`.

- **Added `duckdb` as a database flavor**  
  The `duckdb` database flavor is a single file, similar to `sqlite`. Future releases may use `duckdb` as the cache store for local pipes' data.

- **Added `uninstall plugins` and `uninstall packages`.**  
  Plugins and virtual environment `pip` packages may now be removed via the `uninstall` command.

- **Delete plugin from repository**  
  The command `delete plugins` now deletes the archive file and database registration of the plugin on the remote repository. This does not uninstall plugins, so deleted plugins may be re-registered if they are still installed on the client.

- **Bound syncing with `--begin` and `--end`**  
  When performing a sync, you can specify `--begin` and `--end` to bound the search for retrieving data.

- **Bugfixes and improvements**  
  Small bugfixes like including the location `None` with other locations and improvements like only searching for plugin auto-complete suggestions when the search term is at least 1 character long.

## 0.2.x Releases
Version 0.2 improved greatly on 0.1, with a greater focus on the user experience, plugins, local performance, and a whole lot more. Read the release notes below for some of the highlights.

### v0.2.22
- **Critical bugfixes.**
  Version 0.2.22 fixes some critical bugs that went unnoticed in v0.2.21 and is another backport from the 0.3.x branch.

### v0.2.21
- **Bugfixes and performance improvements.**
  Improvements that were added to v0.3.0 release candidates were backported to the 0.2.x series prior to the release of v0.3.0. This release is essentially v0.3.0 with the Web Interface disabled.

### v0.2.20
- **Reformatted `show columns` to tables.**  
  The action `show columns` now displays tables rather than dictionaries.
- **SQLConnector bugfixes.**  
  The `debug` flag was breaking functionality of `SQLConnector` objects, but now connectors are more robust and thread safe.
- **Added `instance` as an alias to `mrsm_instance` when creating `Pipe` objects.**  
  For convenience, when building `Pipes`, `instance` may be used in place of `mrsm_instance`.

### v0.2.19
- **Added `show columns` action.**  
  The action `show columns` will now display a pipe's columns and data types.
- **`docker-compose` bugfix.**  
  When `docker-compose` is installed globally, skip using the virtual environment version.
- **Refactoring / linting**  
  A lot of code was cleaned up to conform with cleaner programming practices.

### v0.2.18
- **Added `login` action.**  
  To verify or correct login credentials for API instance, run the `login` action. The action will try to log in with your defined usernames and passwords, and if a connector is missing a username or password is incorrect, it will ask if you would like to try different login credentials, and upon success, it will ask if you would like to save the new credentials to the primary configuration file.

- **Critical bugfix.**  
  Fixed bug where `default` values were being copied over from the active shell `instance`. I finally found, deep in the code, the missing `.copy()`.

- **Reset `api:mrsm` to default repository.**  
  In my task to move everything to the preconfigured instance, I overstepped and made the default repository into the configured `instance`, which by default is a SQLConnector, so that broke things! In case you were affected by this change, you can simply reset the value of `default_repository` to `api:mrsm` (or your `api` server) to return to the desired behavior.

- **🧹 Housekeeping (refactoring)**.  
  I removed nearly all instances of declaring mutable types as optional values, as well as additional `typing` hints. There may still be some additional cleaning to do, but now the functions are neat and tidy!

### v0.2.17
- **Added CockroachDB as a supported database flavor.**  
  CockroachDB may be a data source or a Meerschaum backend. There may be some performance tuning to do, but for now, it is functional. For example, I may implement bulk insert for CockroachDB like what is done for PostgreSQL and TimescaleDB.
- **Only attempt to install readline once in Meerschaum portable.**  
  The first Meerschaum portable launch will attempt to install readline, but even in case of failure, it won't try to reinstall during subsequent launches or reloads.
- **Refactored SQLAlchemy configuration.**  
  Under `system:connectors:sql`, the key `create_engine` has been added to house all the `sqlalchemy` configuration settings. **WARNING:** You might need to run `delete config system` to refresh this portion of the config file in case any old settings break things.
- **Dependency conflict resolution.**
- **As always, more bugfixes :)**

### v0.2.16
- **Hypertable improvements and bugfixes.**  
  When syncing a new pipe, if an `id` column is specified, create partitions for the number of unique `id` values.
- **Only use `api:mrsm` for plugins, resort to default `instance` for everything else.**
- **Fix bug that mirrored changes to `main` under `default`.**

### v0.2.15
- **MySQL/MariaDB bugfixes.**
- **Added `aiomysql` as a driver dependency.**

### v0.2.14
- **Implemented `bootstrap pipes` action.**  
  The `bootstrap pipes` wizard helps guide new users through creating connectors and pipes.
- **Added `edit pipes definition` action.**  
  Adding the word `definition` to the `edit pipes` command will now open a `.sql` file for pipes with `sql` connectors.
- **Changed `api_instance` to symlink to `instance` by default.**
- **Registering users applies to instances, not repositories.**  
  The action `register users` now uses the value of `instance` instead of `default_repository`. For users to make accounts with `api.mrsm.io`, they will have to specify `-i api:mrsm`.

### v0.2.13
- **Fixed symlink handling for nesting dictionaries.**  
  For example, the environment variables for the API service now contain clean references to the `meerschaum` and `system` keys.
- **Added `MRSM_PATCH` environment variable.**  
  The `MRSM_PATCH` environment variable is treated the same as `MRSM_CONFIG` and is loaded after `MRSM_CONFIG` but before patch or permanent patch files. This allows the user to apply a patch on top of a symlinked reference. In the docker-compose configuration, `MRSM_PATCH` is used to change the `sql:main` hostname to `db`, and the entire `meerschaum` config file is loaded from `MRSM_CONFIG`.
- **Bugfixes, improved robustness.**  
  Per usual, many teeny bugs were squashed.

### v0.2.12
- **Improved symlink handling in the configuration dictionary.**  
  Symlinks are now stable and persistent but at this time cannot be chained together.
- **Improved config file syncing.**  
  Generated config files (e.g. Grafana data sources) may only be edited from the main `edit config` process.
- **Upgraded to PostgreSQL 13 TimescaleDB by default.**  
  This may break existing installs, but you can revert back to 12 with `edit config stack` and changing the string `latest-pg13-oss` under the `db` image to `latest-pg12-oss`.
- **Bugfixes.**  
  Like always, this release includes miscellaneous bugfixes.

### v0.2.11 (release notes before this point are back-logged)
- **API Chaining**  
  Set a Meerschaum API as a the parent source connector for a child Meerschaum API, as if it were a SQLConnector.

### v0.2.10
- **MRSM_CONFIG critical bugfix**  
  The environment variable MRSM_CONFIG is patched on top of your existing configuration. MRSM_PATH is also a patch that is added after MRSM_CONFIG.
### v0.2.9
- **API and SQL Chunking**  
  Syncing data via an APIConnector or SQLConnector uploads the dictionary or DataFrame in chunks (defaults to a chunksize of 900). When calling `read()` with a SQLConnector, a `chunk_hook` callable may be passed, and if `as_chunks` is `True`, a list of DataFrames will be returned. If `as_iterator` is `True`, a dataframe iterator will be returned.

### v0.2.8
- **API Chaining introduction**  
  Chaining is first released on v0.2.8, though it is finalized in 0.2.11.

### v0.2.7
- **Shell autocomplete bugfixes**

### v0.2.6
- **Miscellaneous bugfixes and dependency updates**

### v0.2.1 — v0.2.5
- **Shell improvements**  
  Stability, autosuggest, and more.
- **Virtual environments**  
  Isolate dependencies via virtual environments. The primary entrypoint for virtual environments is `meerschaum.utils.packages.attempt_import()`.

### v0.2.0
- **Plugins**  
  Introduced the plugin system, which allows users and developers to easily integrate any data source into Meerschaum. You can read more about plugins [here](/plugins).
- **Repositories**  
  Repositories are Meerschaum APIs that register and serve plugins. To register a plugin, you need a user login for that API instance.
- **Users**  
  A user account is required for most functions of the Meerschaum API (for security reasons). By default, user registration is disabled from the API side (but can be enabled with `edit config system` under `permissions`). You can register users on a direct SQL connection to a Meerschaum instance.
- **Updated shell design**  
  Added a new prompt, intro, and more shell design improvements.
- **SQLite improvements**  
  The connector `sql:local` may be used as as backend for cases such as when running on a low-powered device like a Raspberry Pi.

## 0.1.x Releases

Meerschaum's first point release focused on a lot, but mainly stability and improving important functionality, such as syncing.

## 0.0.x Releases

A lot was accomplished in the first 60 releases of Meerschaum. For the most part, the groundwork for core concepts like pipes, syncing, the config system, SQL and API connectors, bulk inserts, and more was laid.
