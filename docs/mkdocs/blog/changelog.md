# Changelog
This changelog was not introduced until v0.2.12, so many of the critical releases of Meerschaum have already been published.

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

