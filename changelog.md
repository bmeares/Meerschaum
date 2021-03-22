# Changelog
This changelog was not introduced until v0.2.12, so many of the critical releases of Meerschaum has already been published.

### v0.2.12
- **Improved symlink handling in the configuration dictionary.** Symlinks are now stable and persistent but at this time cannot be chained together.
- **Improved config file syncing.** Generated config files (e.g. Grafana data sources) may only be edited from the main `edit config` process.
- **Upgraded to PostgreSQL 13 TimescaleDB by default.** This may break existing installs, but you can revert back to 12 with `edit config stack` and changing the string `latest-pg13-oss` under the `db` image to `latest-pg12-oss`.
- **Bugfixes.** Like always, this release includes miscellaneous bugfixes.

