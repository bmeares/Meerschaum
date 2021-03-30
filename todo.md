## 📝 General
- Documentation
- More tests
- Add CockroachDB and others to supported flavors

## 🐞 Bugs
- Reloading configuration copies 'sql:main' attributes into default.
- `parameters` column in the `pipes` table is a string, not JSON.
- `show users` needs to default to the instance connector, not repository.

## ✨ Features
- Reuse packages across virtual environments
- Login html page
- Meerschaum JS
- Rewrite API data to paginate downstream
- Local Pipe HD5 caching

## 🔨 Refactoring
- Consolidate SQL-specific functions to one packages to make adding flavors easier.
