# Changelog
This changelog was not introduced until v0.2.12, so many of the critical releases of Meerschaum have already been published.

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

