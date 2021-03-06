# Meerschaum To-Do List

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
    * *NoSQL databases*
        * [ ] InfluxDB

## 🐞 Bugs
- [ ] `parameters` column in the `pipes` table is a string, not JSON.
- [ ] Reload plugins when installing updates.
- [ ] When upgrading plugins, only install plugin if updates are available.
- [x] ~~Remove `Literal` import from `typing` for compatibility with Python 3.7.~~
- [x] ~~`default` values are populated from the active instance.~~

## ✨ Features
- **Syncing**
    - [ ] **New syncing algorithm**  
      I have been brainstorming a better way to detect differnces between source and cache when syncing, so a future release of Meerschaum will be able to detect changes in past data.
    - [ ] **Local Pipe HD5 caching**  
      When requesting data via `pipe.get_data()`, cache data locally and sync any changes.
    - [ ] **Rewrite API data to paginate downstream**  
      When syncing upstream, Meerschaum defaults to sending `POST` requests for chunks. The chunking logic is mostly there, so I need to implement the same process in reverse.
- **Web Interface**
    - [ ] **Login html page**  
      Request an OAUTH token via a pretty web page.
    - [ ] **Meerschaum JS**  
      Interact with a Meerschaum API via web requests in Javascript.
    - [ ] **Meerschaum web dashboard**  
      Interact with a Meerschaum API via a web interface.
- **Plugins**
    - [ ] **Reuse packages across virtual environments**  
      In an attempt to save space, if a package is already installed in another virtual environment and satisfies the requirements for a plugin, attempt to use that version instead of installing a new version.

## 🔨 Refactoring
- [ ] Consolidate SQL-specific functions to one package to make adding flavors easier.
- [x] Add `typing` hinting to the Python package API.
