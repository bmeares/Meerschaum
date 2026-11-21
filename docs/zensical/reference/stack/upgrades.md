# ⬆️ Upgrading the Stack

Pulling newer images (`mrsm stack pull` or `mrsm upgrade mrsm`) updates your containers, not the database extensions or PostgreSQL format inside your volume. Use these four guided commands to maintain your Stack database:

| Task | Command |
|---|---|
| Update extension versions in an existing database | `mrsm upgrade extensions` |
| Upgrade the Stack database to a newer PostgreSQL major | `mrsm upgrade postgres [MAJOR\|IMAGE\|rollback]` |
| Take an online backup | `mrsm backup postgres` |
| Verify and restore a backup into a new volume | `mrsm restore postgres [TIMESTAMP\|PATH\|rollback]` |

### Common flags

| Flag | Applies to | Meaning |
|---|---|---|
| `-y, --yes` | All | Approve prompts automatically (including image pulls and `pg_stat_statements` preload) |
| `-f, --force` | `upgrade`, `backup`, `restore` | Implies `--yes`; continues past connected clients or low disk space warnings |
| `--noask` | `upgrade` | Take each prompt's default without interactive input |
| `--debug` | `upgrade postgres`, `restore postgres` | Print Docker commands and keep the new volume on failure |
| `-i, --instance` | `upgrade extensions` | Target a specific PostgreSQL instance (default: configured instance) |

## Updating extensions

Docker runs init scripts only on an empty volume, so newer images leave existing databases on older extension versions (`mrsm stack up`, `mrsm start api`, and `mrsm doctor` warn when extensions lag).

```bash
mrsm upgrade extensions
mrsm upgrade extensions template1 postgres   # also update additional databases
```

The command previews installed vs. available versions and runs each update in a fresh session:

| Extension | Statement |
|---|---|
| `timescaledb` | `ALTER EXTENSION timescaledb UPDATE;` (first statement of a new session) |
| `postgis`, `postgis_raster`, `postgis_topology`, `postgis_sfcgal`, `postgis_tiger_geocoder` | `SELECT postgis_extensions_upgrade();` |
| Other (`timescaledb_toolkit`, `pg_stat_statements`, ...) | `ALTER EXTENSION "<name>" UPDATE;` |

On this root's `sql:main` Stack database (as superuser), `upgrade extensions` also disables TimescaleDB telemetry (`timescaledb.telemetry_level = 'off'`), optionally adds `pg_stat_statements` to `shared_preload_libraries` (apply with `mrsm stack restart db`), and restarts `api` and `grafana` after a TimescaleDB update. Restart long-running jobs afterward (`mrsm restart jobs`). If permissions are insufficient, the command prints the exact `psql -X -c ...` statement to run as a superuser.

## Upgrading PostgreSQL

PostgreSQL cannot read an older major's data directory. `mrsm upgrade postgres` runs a verified dump-and-restore into a new named volume (`<volume>_pg<major>`) and **never modifies or removes your old volume**.

```bash
mrsm upgrade postgres                                  # default stack image major
mrsm upgrade postgres 18                               # same image family (pg17 -> pg18, etc.)
mrsm upgrade postgres timescale/timescaledb-ha:pg18    # explicit target image
mrsm upgrade postgres rollback                         # revert to the old volume
```

Passing a major number maps standard image families (`timescale/timescaledb-ha:pg17`, `timescale/timescaledb:latest-pg16`, `postgres:17`, `postgis/postgis:17-3.5`); pass a full image name for pinned or custom tags.

1. **Preflight:** Checks `db`, verifies target image extension/TimescaleDB compatibility, checks disk space, and lists connected clients.
2. **Quiesce:** Stops dependent stack services (`api`, `grafana`), pauses TimescaleDB jobs, isolates `db` from Docker networks, and refuses external writers unless `--force`.
3. **Backup:** Dumps to `<root>/backups/postgres/<UTC timestamp>/`, verifies no post-snapshot writes occurred, and stops old `db`.
4. **Restore:** Initializes `<volume>_pg<major>` in a network-isolated container, restores roles and databases between `timescaledb_pre_restore()` / `timescaledb_post_restore()` at the dump's TimescaleDB version, carries over `ALTER SYSTEM` settings, and runs `ANALYZE`.
5. **Verify:** Confirms exact row counts and schema, upgrades extensions to the new image's versions, and re-verifies. Any mismatch aborts and restores the old stack.
6. **Switch:** Backs up `stack.yaml`, updates the `db` image and volume, regenerates Compose, and starts the stack.

**Downtime & rollback.** Dependent services are down from backup through switch; stop external writers (`mrsm stop jobs`) first. `stack up`/`start`/`restart` are blocked during the move. If interrupted, rerun or use `mrsm upgrade postgres rollback` to recover.

`mrsm upgrade postgres rollback` restores the pre-upgrade `stack.yaml` (saving your current copy as `stack.yaml.before-rollback-<timestamp>`) and restarts the old volume. **Post-upgrade writes remain in the new volume**; reconcile role passwords if changed since. Reclaim space once satisfied:

```bash
docker volume rm mrsm_meerschaum_db_data
rm -r ~/.config/meerschaum/backups/postgres/<timestamp>
```

Bind mounts, external tablespaces, standby servers, PostgreSQL < 13, patched configs (`MRSM_CONFIG`/`MRSM_PATCH`), and missing extensions/locales require the manual procedure below.

<details markdown="1">
<summary>Manual pg_dump / pg_restore procedure</summary>

Use the **new** PostgreSQL version's client tools after stopping all writers (`old` = source, `new` = target, `mydb` = database):

```bash
export PGUSER=mrsm PGPASSWORD=...    # superuser on both servers

# 1. Check old TimescaleDB version (e.g. 2.29.2).
psql -h old -d mydb -X -At -c "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"

# 2. Dump roles and database.
pg_dumpall -h old --globals-only -f globals.sql
pg_dump -h old -Fc --create -d mydb -f mydb.dump

# 3. Restore roles ("already exists" for the superuser is expected).
psql -h new -d postgres -X -f globals.sql

# 4. Recreate database and TimescaleDB at the SAME version.
pg_restore -l mydb.dump | awk '/^[0-9]+; [0-9]+ [0-9]+ (DATABASE - |DATABASE PROPERTIES - |COMMENT - DATABASE |ACL - DATABASE |SECURITY LABEL - DATABASE )/' > mydb-create.list
pg_restore -h new -d postgres --create --exit-on-error -L mydb-create.list mydb.dump
psql -h new -d mydb -X -c "CREATE EXTENSION timescaledb VERSION '2.29.2'"

# 5. Restore data between pre/post hooks.
pg_restore -l mydb.dump | grep -v -E '(EXTENSION - |COMMENT - EXTENSION )timescaledb *$|^[0-9]+; [0-9]+ [0-9]+ (DATABASE - |DATABASE PROPERTIES - |COMMENT - DATABASE |ACL - DATABASE |SECURITY LABEL - DATABASE )' > mydb.list
psql -h new -d mydb -X -c "SELECT timescaledb_pre_restore()"
pg_restore -h new -d mydb --exit-on-error -L mydb.list mydb.dump
psql -h new -d mydb -X -c "SELECT timescaledb_post_restore()"
psql -h new -d mydb -X -c "ANALYZE"

# 6. Update TimescaleDB as the first statement of a new session.
psql -h new -d mydb -X -c "ALTER EXTENSION timescaledb UPDATE"
```

Repeat steps 1, 2, and 4–6 per database, then verify row counts.

</details>

## Backing up and restoring

### `mrsm backup postgres`

Takes an **online** snapshot of all connectable non-template databases and roles with zero downtime:

```bash
mrsm backup postgres
```

Writes atomically to `<root>/backups/postgres/<UTC timestamp>/` (`0700`/`0600` permissions on Linux/macOS):

| File | Contents |
|---|---|
| `globals.sql` | Roles and password hashes (`pg_dumpall --globals-only`) |
| `NN-<database>.dump` | Custom-format dump (`pg_dump -Fc --create`, zstd when supported) |
| `manifest.json` | PostgreSQL version/image, extensions, SHA-256 checksums, row estimates |

`postgresql.conf`, `pg_hba.conf`, and `template1` customizations are not included. Schedule recurring backups as a [job](../background-jobs/index.md) and set retention in `mrsm edit config system` (never prunes upgrade/restore backups):

```bash
mrsm backup postgres -s 'daily' -d
```

```yaml
backups:
  postgres:
    keep: 7
```

### `mrsm restore postgres`

```bash
mrsm restore postgres                     # newest complete backup
mrsm restore postgres 20261018T120000Z    # by timestamp (including upgrade backups)
mrsm restore postgres /path/to/backup     # external backup directory
mrsm restore postgres rollback            # revert to previous volume
```

Restores verify SHA-256 checksums and image compatibility, restore into a **new volume**, check tables/hypertables/policies/extensions, and stop dependent services only for the final switch. **Writes made since the backup stay in the previous volume** (kept for `rollback`). Older-major backups restore into newer Stack images like an upgrade; newer-major backups into older images are refused.

!!! note "Automatic `PG_VERSION` check"

    `mrsm stack up` inspects `PG_VERSION` in existing named volumes and refuses startup on a major mismatch (`--force` bypasses). Pin the matching image in `mrsm edit config stack`, then run `mrsm upgrade postgres`.
