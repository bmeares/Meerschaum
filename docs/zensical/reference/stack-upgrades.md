# ⬆️ Upgrading the Stack

Pulling newer images (`mrsm stack pull`, or `mrsm upgrade mrsm`) updates the stack's software, but
not everything inside its database volume. This page covers what an upgrade leaves behind and the
commands that finish the job. See [the stack page](stack.md) for how the stack's database is set
up in the first place.

## Updating extensions

### What lags and why

Docker runs the database's setup scripts (`/docker-entrypoint-initdb.d`) only when the volume is
empty, so after a new image is pulled the database keeps the **extension versions it was created
with**. The image ships the new versions (and, for TimescaleDB, the older libraries too, so
nothing breaks), but the database keeps using the old ones until each extension is updated:

- **TimescaleDB** keeps loading its old library (fixes and features of the new release stay off).
- **PostGIS** (with `postgis_raster`, `postgis_topology`, ...) keeps its old SQL functions.
- **TimescaleDB Toolkit** and any other extension keep their old SQL objects.

Meerschaum tells you when this happens: `mrsm stack up` and `mrsm start api` print a one-line
hint when an extension of the instance is older than the server's version, and
`mrsm upgrade mrsm` offers the update after it brings the stack back up. `mrsm doctor` reports it
too.

The stack's own setup has also missed a few settings on stacks created before 5.0: the compose
file's `TIMESCALEDB_TELEMETRY: 'off'` never applied (the image's setup script that reads it was
hidden by the stack's mount), and `pg_stat_statements` was created without being added to
`shared_preload_libraries`, so its view cannot be read.

### The command

```bash
mrsm upgrade extensions
```

It reads the instance's database (`-i` picks another instance; the default is the configured
instance, usually `sql:main`), prints a plan and asks before changing anything:

```text
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Database   ┃ Extension           ┃ Installed ┃ Available ┃ Statement                                       ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ meerschaum │ timescaledb         │ 2.29.2    │ 2.30.2    │ ALTER EXTENSION timescaledb UPDATE;             │
├────────────┼─────────────────────┼───────────┼───────────┼─────────────────────────────────────────────────┤
│ meerschaum │ timescaledb_toolkit │ 1.22.0    │ 1.26.0    │ ALTER EXTENSION "timescaledb_toolkit" UPDATE;   │
└────────────┴─────────────────────┴───────────┴───────────┴─────────────────────────────────────────────────┘
```

| Flag / word | Meaning |
|---|---|
| `-i, --instance` | The PostgreSQL instance to update (e.g. `sql:main`). A non-PostgreSQL instance has nothing to update. |
| `<database> ...` | Other databases on the same server to update as well, e.g. `mrsm upgrade extensions template1 postgres`. Databases are never updated unless they are the instance's own or named here: a shared server may host other applications' databases. |
| `-y`, `-f` | Update without asking, including the `pg_stat_statements` preload on the stack's database (see below). |
| `--noask` | Accept the default answers: yes for the extensions, no for the `pg_stat_statements` preload (the statement is printed instead). |

### What it changes

Each update runs in a **new session of its own, as that session's first statement**:

| Extension | Statement |
|---|---|
| `timescaledb` | `ALTER EXTENSION timescaledb UPDATE;` (TimescaleDB refuses the update in a session that has already loaded the old library, so nothing else runs before it.) |
| `postgis`, `postgis_raster`, `postgis_topology`, `postgis_sfcgal`, `postgis_tiger_geocoder` | `SELECT postgis_extensions_upgrade();` (one call updates the family.) |
| anything else (`timescaledb_toolkit`, `pg_stat_statements`, ...) | `ALTER EXTENSION "<name>" UPDATE;` |

An extension the server cannot update (no update path from the installed version, e.g. a version
newer than the image's) is reported and skipped.

On **the stack's own database**, connected as a superuser, it also does the following. The
instance is the stack's database when it is `sql:main`, the stack was set up from this Meerschaum
root (its Compose origin labels identify this root's compose file), and the running `db`
container's initialized database and published or internal port match the connector. A read-only
check also matches PostgreSQL's system identifier and server address to that container, then
proves that the container can see a uniquely named live probe connection. This also separates
cloned clusters on different Docker hosts. A `sql:main` override to another localhost port or database is treated as an explicit instance:
extensions can still be updated there, while stack-specific settings and restarts are omitted.
The API and Grafana containers must also belong to this root before they can be restarted.

- applies the stack's `TIMESCALEDB_TELEMETRY: 'off'` the way the image's setup script does:
  `ALTER SYSTEM SET timescaledb.telemetry_level = 'off'`, `SELECT pg_reload_conf()`, and
  unscheduling TimescaleDB's telemetry job in each updated database;
- asks (a separate question, default no) whether to add `pg_stat_statements` to
  `shared_preload_libraries` with `ALTER SYSTEM`, keeping the libraries already listed. `-y` and
  `-f` add it without asking (so does `mrsm upgrade mrsm -y` when it runs the update); `--noask`
  keeps the default answer and prints the statement instead. It takes effect after the database
  restarts: `mrsm stack restart db`. Meerschaum never restarts the database for you.

The result counts the extensions updated, failed and skipped.

### Permissions

Updating an extension needs a superuser or the extension's owner (the stack's `sql:main` user is
a superuser). When an update is refused, the others still run and the result names the exact
statement to run as a superuser, connected to that database, e.g.:

```bash
psql -d meerschaum -X -c 'ALTER EXTENSION timescaledb UPDATE;'
```

(`-X` keeps `psql` from running a startup file first; TimescaleDB's update must be the session's
first statement.)

### Restarts

Sessions that loaded the old TimescaleDB library keep it until they reconnect. After a
TimescaleDB update on the stack's database, `mrsm upgrade extensions` restarts the stack's `api`
and `grafana` services if they are running (their pooled sessions hold the old library). Restart any other long-running process that uses the database (jobs,
API servers on other hosts) so it loads the new version:

```bash
mrsm restart jobs
```

## Upgrading PostgreSQL

The image of the stack's `db` service decides its PostgreSQL major. A stack whose `stack.yaml`
names the image keeps that major (stacks set up by Meerschaum 3.0 to 3.3 with a `stack.yaml` run
`timescale/timescaledb-ha:pg17`, new stacks get `pg18`). A new major cannot read an older major's
data directory, and the stack's images ship one PostgreSQL major each, so moving to a new major is
a dump and a restore. `mrsm upgrade postgres` does it, the way TimescaleDB requires, and never
touches the old data: you can go back at any time.

### The command

```bash
mrsm upgrade postgres                                  # to the default stack image's major
mrsm upgrade postgres 18                               # to a major, in the same image family
mrsm upgrade postgres timescale/timescaledb-ha:pg18    # to an image
mrsm upgrade postgres rollback                         # back to the old database
```

A major keeps the image family and variant of the current image:

| Current image | With `18` |
|---|---|
| `timescale/timescaledb-ha:pg17` (also `-oss`, `-all`) | `timescale/timescaledb-ha:pg18` |
| `timescale/timescaledb:latest-pg16` (also `-oss`) | `timescale/timescaledb:latest-pg18` |
| `postgres:17` (also `-alpine`, ...) | `postgres:18` |
| `postgis/postgis:17-3.5` | `postgis/postgis:18-3.5` |

Any other image, or a pinned tag (`pg17.5-ts2.20.3`, `postgres:17.6`), cannot be mapped: name the
target image, or follow [the manual procedure](#the-manual-procedure).

| Flag | Meaning |
|---|---|
| `-y, --yes` | Answer yes to the questions (pulling the image, starting the upgrade). |
| `-f, --force` | Like `--yes`, and go on although other clients are connected or disk space is low. |
| `--noask` | Take each question's default: nothing is pulled and the upgrade does not start. |
| `--debug` | Print the Docker commands; keep the new volume of a failed run for inspection. |

### What it does

Nothing changes until the plan is shown and confirmed:

1. **Preflight** (read-only). The stack's `db` service must be running. The command reads its
   server, databases, extensions and data volume, pulls the target image (after asking), and
   starts a throwaway container of it to check that it can create every extension, and every
   database's exact TimescaleDB version. It checks the free space for the backup and for Docker's
   volumes, and lists what may write to the database: the stack's services that depend on `db`
   (`api`, `grafana`, ...), other connected clients and the Meerschaum jobs running in this root.
2. **Quiesce.** The services that depend on `db` are stopped; `db` runs through the dump. If any other
   client is still connected, the upgrade stops here (unless `--force`): what it would write
   after the dump would not be in the new database. The old database is disconnected from its
   Docker networks before dumping, blocking new external TCP clients through the final checks
   and its stop; its loopback and Unix socket remain available to the dump tools. Network aliases
   and declared static addresses are recorded before disconnecting, and restored after it stops
   or during recovery. TimescaleDB's background jobs are paused until the old server restarts.
   Stop writers running inside the database container or using its Unix socket too: these
   administrative connections are outside the external-client fence.
3. **A full backup first**, exactly as [`mrsm backup postgres`](#backing-up-and-restoring) takes
   it, into `<root>/backups/postgres/<UTC timestamp>/`, with the new image's client tools (as
   PostgreSQL recommends) when they can reach the old server, and with the exact row count of
   every table recorded in each dump's own snapshot. After all dumps, connected clients, per-table
   write counters and structure are checked again. Any write after a snapshot aborts the upgrade,
   including an update that leaves the row count unchanged. Then the old database stops, before
   the restore begins. Stop external writers before starting; `--force` permits connected clients
   but does not bypass detected changes. Host/shared network namespaces and unsupported custom
   network attachments cannot be fenced; without `--force` they are refused. With `--force` the
   command's warnings explicitly describe the risk of concurrent writes being lost. Enabled logical
   replication subscriptions and unhandled background workers are also refused unless forced.
   A source running as a standby is always refused.
4. **Restore into a new volume.** A new volume, `<volume>_pg<major>` (e.g.
   `mrsm_meerschaum_db_data_pg18`, or a run-specific name if that name is in use), is set up by
   the new image in a temporary container with the stack's environment, command and setup script; it has no network and no published port, so
   nothing else can write to it. Then, per database: TimescaleDB is created at **the dump's
   version** (TimescaleDB restores only into the same version), the roles and the data are
   restored between `timescaledb_pre_restore()` and `timescaledb_post_restore()` with
   `--exit-on-error` (the restore list skips only the extensions that already exist, so every
   remaining error is real), then the database's own settings and `ANALYZE`. The server settings
   made with `ALTER SYSTEM` (`postgresql.auto.conf`, e.g. TimescaleDB's telemetry level or
   `shared_preload_libraries`) are carried over, and the temporary server is restarted to prove it
   still starts with them. If the old server's TimescaleDB is too old for the new image, it is
   updated on the old server first (the plan says so).
5. **Verify.** The copy must match the recorded counts and structure exactly. Its extensions are
   then updated to the new image's versions (TimescaleDB as the first statement of its own
   session, PostGIS with `postgis_extensions_upgrade()`), then its structure is checked again.
   Any difference stops the upgrade: nothing is switched, the stopped services start again, and the new volume is removed.
6. **Switch.** With the old `db` already stopped, `stack.yaml` is copied byte for byte into the backup's
   directory, and only three things in it change: the `db` image, its data volume entry and that
   volume's declaration under `volumes:`. Block-style YAML keeps its comments and formatting;
   if a safe line edit is unavailable (for example flow-style YAML or anchors), the plan says
   that the config will be rewritten. The original remains in the backup. If there was no stack
   config file, the command creates one with the current full stack config, pinning its defaults.
   The compose file is regenerated, `db` starts on the new volume, and the services that were running start again.

The old volume is never removed and nothing writes to it but its own server (and, only when the
plan says so, the TimescaleDB update of step 4).

On Linux and macOS, Ctrl-C, SIGTERM and a closed terminal (SIGHUP) stop the running step and
put the old stack back. Further signals are ignored during cleanup. On Windows a second Ctrl-C
exits immediately. Run long upgrades in tmux or screen. A forced kill or power loss can interrupt
cleanup: the next upgrade or restore detects the run and offers to put it back first;
`mrsm upgrade postgres rollback` also recovers an interrupted run. An interrupted rollback is
completed on its requested old volume, keeping both database volumes. Backups only warn about
unfinished recovery. A command that changes the stack waits (up to two minutes) for a
`docker compose` command of the project that a killed run left running, before it reads the
stack or puts it back. A `restore postgres` that began with the stack's `db` stopped leaves it
stopped when it is put back (a stopped database is never started on a volume the plan promised not
to modify).

If the disk of `<root>/backups/postgres` fills up, a failed upgrade removes its own dump files so
that its end can still be recorded (the stack is put back either way, and the result says so);
free space and run it again.

### Downtime

The services that use the database (the API, Grafana) are down from the start of the backup to the
end of the switch. External database connections are unavailable from network isolation before
the dump through the switch; the database process itself stops at the end of the dump. The whole run takes about as long as one dump, one restore and two passes of row counts: a few seconds for a small
database, tens of minutes for a large one. Stop the Meerschaum jobs and other clients that write
to the database before you start (`mrsm stop jobs`), and start them again afterwards.

While an upgrade, a restore or a rollback moves the database, `mrsm stack up`, `mrsm stack start`
and `mrsm stack restart` of that stack are refused (a stopped old `db`, or a service that uses it,
would write to the database being copied, and those writes would not be in the copy). Wait for the
command to finish; `--force` bypasses the check. If the old `db` is started again anyway, the
command notices before it switches, stops with "The old database ran again", puts the stack back
and removes the new volume: nothing is lost, and you run it again once the clients are stopped. A
backup only reads the database and never holds these commands back.

### Rollback and cleaning up

`mrsm upgrade postgres rollback` (after its own plan and question) stops the stack's database and
the services that use it, puts `stack.yaml` back from the latest upgrade's (or restore's) copy
(the current one is kept next to it as `stack.yaml.before-rollback-<timestamp>`), and starts `db`
on the old volume with the old image again. **What was written to the database after the upgrade
is not in the old volume.** The new volume is kept.

The roles and their passwords come with the data. After a rollback (or a `restore postgres` of an
older backup) a password changed since then is the old one again, so the stack's services cannot
log in until the role and the stack config agree. The command tries the stack's own login when it
is done and says so when the server refuses it; set the role's password to the configured one
(`ALTER ROLE ... PASSWORD ...`) or put the old password back in the config. If you edited
`stack.yaml` after the upgrade (a port, a password, another service), the plan warns that the
rollback puts the file from before the upgrade back, and the result names the copy of your
current file: `stack.yaml.before-rollback-<timestamp>` next to the upgrade's backup.

When you are satisfied with the new database, free the space of the old one, with the names the
upgrade printed:

```bash
docker volume rm mrsm_meerschaum_db_data                  # the old volume
rm -r ~/.config/meerschaum/backups/postgres/<timestamp>   # its backup (Linux; `<root>/backups/...`)
```

After a rollback, the new volume is kept too. A later upgrade chooses another unused name; it
never reuses or removes an existing volume automatically.

### When it refuses

- The stack's `db` is not running (`mrsm stack up -d db` first), or its container runs another
  image than the stack config says. When its image cannot start on the data (the stack config's
  image moved to a newer major while the data stayed, e.g. a stack without `stack.yaml` that
  follows a new default image), the command reads the data's major from the volume and says
  which image to pin first (`mrsm edit config stack`), so that the data runs again and can then be
  upgraded.
- The stack config is patched by `MRSM_CONFIG`, `MRSM_PATCH` or a patch directory (the edit of
  `stack.yaml` would not take effect), or an override file changes the `db` image or volume.
- The data is in a bind mount, not a named volume; or the server has tablespaces outside its data
  directory; or a database is a template or does not allow connections.
- An extension, or a database's TimescaleDB version, cannot be created by the new image.
- A database's encoding or locale cannot be recreated by the new image; additional named
  volumes, mounts inside a data directory or over a socket directory, or volume `driver` /
  `driver_opts` would require a manual migration.
- Another root uses the same Compose project: the container's Compose labels must identify this
  root's stack file. Commands lock both the root and the project on the Docker daemon.
- PostgreSQL older than 13.

The settings in the old volume's `postgresql.conf` and `pg_hba.conf` are not copied (the new image
writes its own); settings made with `ALTER SYSTEM` are, except preloaded libraries the new image
does not have (a warning names them). File paths, socket / port settings, archiving, recovery and
replication settings are not carried to the temporary server; the plan and warnings identify
these. Bind mounts outside the data and socket directories are read-only in that server.

### The manual procedure

For a database that is not the stack's (another server, a bind mount, a custom image), the same
steps by hand: `pg_dump` from the old server, `pg_restore` into a new one of the new major, with
TimescaleDB at the same version on both sides. Use the client tools of the **new** version, and
stop every client that writes to the database first. With the old server at `old`, the new one at
`new`, and one database `mydb`:

```bash
export PGUSER=mrsm PGPASSWORD=...    # a superuser of both servers

# 1. The database's TimescaleDB version on the old server (here 2.29.2). The new server must
#    be able to create it: SELECT version FROM pg_available_extension_versions WHERE name = 'timescaledb'
psql -h old -d mydb -X -At -c "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"

# 2. The roles and the database.
pg_dumpall -h old --globals-only -f globals.sql
pg_dump -h old -Fc --create -d mydb -f mydb.dump

# 3. The roles ("role ... already exists" for the new server's own superuser is expected).
psql -h new -d postgres -X -f globals.sql

# 4. The database, and TimescaleDB at the SAME version as in the old database.
# Use the dump's DATABASE entries to keep its owner, encoding, locale and database settings.
pg_restore -l mydb.dump | awk '/^[0-9]+; [0-9]+ [0-9]+ (DATABASE - |DATABASE PROPERTIES - |COMMENT - DATABASE |ACL - DATABASE |SECURITY LABEL - DATABASE )/' > mydb-create.list
pg_restore -h new -d postgres --create --exit-on-error -L mydb-create.list mydb.dump
psql -h new -d mydb -X -c "CREATE EXTENSION timescaledb VERSION '2.29.2'"

# 5. Everything but the extension that now exists, between the pre- and post-restore calls.
pg_restore -l mydb.dump | grep -v -E '(EXTENSION - |COMMENT - EXTENSION )timescaledb *$|^[0-9]+; [0-9]+ [0-9]+ (DATABASE - |DATABASE PROPERTIES - |COMMENT - DATABASE |ACL - DATABASE |SECURITY LABEL - DATABASE )' > mydb.list
psql -h new -d mydb -X -c "SELECT timescaledb_pre_restore()"
pg_restore -h new -d mydb --exit-on-error -L mydb.list mydb.dump
psql -h new -d mydb -X -c "SELECT timescaledb_post_restore()"
psql -h new -d mydb -X -c "ANALYZE"

# 6. TimescaleDB's update to the new server's version: the first statement of its own session.
psql -h new -d mydb -X -c "ALTER EXTENSION timescaledb UPDATE"
```

Then compare the row counts and database settings on both servers. Repeat steps 1, 2 and 4 to 6
for each database. In a Docker setup, run these commands in a container of the new image that can
reach both servers, and point the service at the new server's volume when it matches.

## Backing up and restoring

### `mrsm backup postgres`

An **online** backup of the stack's database: nothing is stopped or restarted, and clients keep
writing. Each database is dumped from one consistent snapshot (`pg_dump -Fc` of a snapshot that a
session exports for it, compressed with zstd when the server's `pg_dump` supports it), together
with the roles (`pg_dumpall --globals-only`), and the database's structure and a row estimate are
recorded in the same snapshot.

```bash
mrsm backup postgres
```

Each backup is a directory `<root>/backups/postgres/<UTC timestamp>/`:

| File | What it is |
|---|---|
| `globals.sql` | The roles (`pg_dumpall --globals-only`; it holds their password hashes). |
| `NN-<database>.dump` | Each database (`pg_dump -Fc --create`; readable with `pg_restore`). |
| `manifest.json` | The PostgreSQL version and image, each database's extensions and their versions (TimescaleDB's above all: a restore needs it), each file's size and SHA-256, and row estimates. |

The files are written to `<timestamp>.partial/` and the directory is renamed once every dump has
been read back with `pg_restore --list`; a failed backup removes it. The directories are readable
by you only on Linux and macOS (0700 directories, 0600 files); Windows uses the directory's
inherited access controls. The free disk space is checked first (`--force`
goes on anyway). The command prints the path, the sizes and the time it took.

Backups contain the roles and each connectable, non-template database. They do not copy
`postgresql.conf`, `pg_hba.conf`, or customizations of `template1`; keep those separately.
Settings made with `ALTER SYSTEM` are recorded, subject to the restore exclusions above.

**Scheduled backups.** The command asks nothing, so it runs as a [job](background-jobs.md):

```bash
mrsm backup postgres -s 'daily' -d
```

**Retention.** Every backup is kept unless you set how many to keep, e.g. with
`mrsm edit config system`:

```yaml
backups:
  postgres:
    keep: 7
```

After each successful backup, the oldest complete backups beyond that number are removed. Only
directories whose manifest says they are complete backups are removed: never an upgrade's or a
restore's directory, nor anything else in `backups/postgres/`.

### `mrsm restore postgres`

```bash
mrsm restore postgres                     # the newest complete backup
mrsm restore postgres 20261018T120000Z    # a backup by its timestamp (an upgrade's backup works too)
mrsm restore postgres /path/to/backup     # a backup directory elsewhere
mrsm restore postgres rollback            # back to the database before the restore
```

The restore never overwrites the current database. Every file is first checked against the size
and SHA-256 in the backup's manifest (damage is refused before anything changes;
these checksums do not authenticate the backup or protect against someone editing both files
and manifest), and a throwaway container of the stack's image checks that it can read every dump and
create every extension and TimescaleDB version. After the plan and its question (`--yes` answers
it), the backup is restored the way an upgrade restores its dump: into a NEW volume, in a temporary
container without network, verified against the manifest (each database's tables, hypertables,
compression settings, chunks, policies and extensions), and only then is the stack's `db` switched
to it (`stack.yaml` is copied first; only the `db` volume changes). The services that use the
database are stopped only for the switch, and the database keeps running until then.

**Everything written since the backup is not in the restored database**: it stays in the previous
volume, which is kept. `mrsm restore postgres rollback` (or `mrsm upgrade postgres rollback`)
switches back to it; remove it with `docker volume rm` once you no longer need it.

A backup of an older PostgreSQL major is restored into the stack's (newer) image the way an upgrade
does it; a backup of a newer major than the image is refused (set an image of that major for `db`
first).

Before `mrsm stack up`, a PostgreSQL image whose major can be identified is checked against
`PG_VERSION` in existing named volumes through a read-only mount, without pulling an image.
A mismatch refuses startup and explains how to pin the old image and run `mrsm upgrade postgres`.
A database already running its configured image skips the check. `mrsm stack up --force` bypasses
it; Compose does not receive that flag. `mrsm stack start` and `mrsm stack restart` start the
containers that exist with the image they were created from, so they skip this check; they, like
`stack up`, wait for an upgrade, restore or rollback that is moving the database (see
[Downtime](#downtime)). Unknown image families and tags such as `latest` cannot
be checked this way.
