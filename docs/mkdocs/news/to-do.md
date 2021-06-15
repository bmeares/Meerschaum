# To-Do / Wish List

Below are some ongoing tasks I have planned for Meerschaum. This is not an exhaustive list, and if you would like to contribute a feature idea or point out a bug, please [start a discussion](https://github.com/bmeares/Meerschaum/discussions/categories/ideas) or [open an issue](https://github.com/bmeares/Meerschaum/issues) on the [GitHub repository](https://github.com/bmeares/Meerschaum).

## 📝 General
- **Documentation**
    * [ ] Tutorials
    * [ ] FAQs
    * [ ] How-To's
    * [ ] Reference concepts
- **Videos**
    * [ ] Tutorial series
    * [ ] Usage and demonstration
- **Tests**
    * [ ] More coverage
    * [x] Syncing
- **Add more databases to supported flavors**
    * *Relational databases*
        * [x] CockroachDB
        * [x] MSSQL
    * *NoSQL databases*
        * [ ] InfluxDB

## 🐞 Bugs
- [x] Add locks to connectors to avoid concurrency issues.
- [x] Disable `prompt_toolkit` newlines in shell prompt.
- [x] ~~`parameters` column in the `pipes` table is a string, not JSON.~~
- [x] ~~`instance` command does not work after reloading when closing a config file~~
- [x] ~~Inconsistent web console colors (e.g. `show connectors` vs `show config`)~~
- [x] ~~`--name` flag is broken when spawning jobs~~
- [x] ~~Reload plugins when installing updates.~~
- [x] ~~When upgrading plugins, only install plugin if updates are available.~~
- [x] ~~Remove `Literal` import from `typing` for compatibility with Python 3.7.~~
- [x] ~~`default` values are populated from the active instance.~~
- [x] ~~Microsoft SQL autocommit breaks fetching values from `sqlalchemy`~~

## ✨ Features
- **Syncing**

    - [ ] **New syncing algorithm**  
      I have been brainstorming a better way to detect differences between source and cache when syncing, so a future release of Meerschaum will be able to detect changes in past data.
    - [ ] **Local Pipe HD5 caching**  
      When requesting data via `pipe.get_data()`, cache data locally and sync any changes. I am investigating using `duckdb` as a local cache database.
    - [ ] **Rewrite API data to paginate downstream**  
      When syncing upstream, Meerschaum defaults to sending `POST` requests for chunks. The chunking logic is mostly there, so I need to implement the same process in reverse.

- **Web Interface**
    - [x] **Login html page**  
      Request an OAuth2 token via a pretty web page.
    - [ ] **Meerschaum JS**  
      Interact with a Meerschaum API via web requests in Javascript.
    - [x] **Meerschaum web dashboard**  
      Interact with a Meerschaum API via a web interface.

- **Diagnostics**
    - [x] **Logging system**  
      Emit log messages to a more universal bus, similar to Splunk / Logstash.
    - [ ] **Diagnostic Grafana dashboards**  
      Ship pre-configured diagnostic dashboards.
    - [ ] **Monitoring daemon**  
      Handle logging and other child processes with a persistent Meerschaum daemon.

- **Job management**  
    - [x] **Run in the background with `-d` / `--daemon` flag**
    - [ ] **Save and restart jobs**  
      Like with `pm2`, add the ability to save the current state of running jobs to be started on system startup.
    - [x] **Show jobs**  
      The action`show jobs` will display running and stopped jobs.
    - [x] **Show logs**  
      Display jobs' logs with `show logs`.
    - [x] **Start job**  
      The action `start job` can spawn a new job (like with `-d`) or restart a stopped job.
    - [x] **Stop job**  
      Cancel running jobs.
    - [x] **Delete jobs**
      Remove jobs with `delete jobs`.
    - [ ] **Bootstrap job**  
    Guide the user through defining and running jobs.

- **Plugins**
    - [ ] **Reuse packages across virtual environments**  
      In an attempt to save space, if a package is already installed in another virtual environment and satisfies the requirements for a plugin, attempt to use that version instead of installing a new version.

    - [x] **API Plugins**  

      Add the decorator `@api_plugin` to defer API plugin initialization (lazy loading).

- **Other System Features**
    - [x] **Daemonize any process**  
      Allow any Meerschaum action to run in the background.

## 🔨 Refactoring
- [ ] Consolidate SQL-specific functions to one package to make adding flavors easier.
- [x] Add `typing` hinting to the Python package API.
- [ ] Migrate `meerschaum.actions.shell` to `meerschaum._internal.shell`.
