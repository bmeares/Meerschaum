"""Generate crates/meerschaum-cli/src/native/packages_table.rs from _packages.py."""
import importlib.util
import pathlib
import sys

# Load `_packages.py` by path: it only needs `typing`, so no `meerschaum` import (and no built
# extension) is needed, and `cargo test` can check the generated file cheaply.
_path = pathlib.Path(__file__).resolve().parents[2] / 'meerschaum' / 'utils' / 'packages' / '_packages.py'
_spec = importlib.util.spec_from_file_location('_mrsm_packages_table', _path)
_module = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _module
_spec.loader.exec_module(_module)
packages, all_packages = _module.packages, _module.all_packages

def lit(s):
    return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'

out = []
out.append('''//! The dependency groups of `mrsm show packages` / `upgrade packages` / `verify packages`:
//! `packages` and `all_packages` of `meerschaum/utils/packages/_packages.py`, as data.
//!
//! @generated from the working tree's `_packages.py` (every v4 group plus the v5 extras groups,
//! in insertion order, `{import_name: install_name}`); regenerate with
//!
//! ```text
//! python3 scripts/rust/gen_packages_table.py > crates/meerschaum-cli/src/native/packages_table.rs
//! ```
//!
//! `scripts/check_packaging.py` keeps `_packages.py`, `pyproject.toml` and `extras.rs` in step;
//! this table is the same data for the groups that are not extras. `tests/generated_tables.rs`
//! fails when this file differs from the generator's output.

/// One group: `(import_name, install_name)` pairs in definition order.
pub type Group = &'static [(&'static str, &'static str)];

/// `packages`: every group in definition order.
#[rustfmt::skip]
pub const GROUPS: &[(&str, Group)] = &[''')
for g, d in packages.items():
    out.append(f'    ({lit(g)}, &[')
    for k, v in d.items():
        out.append(f'        ({lit(k)}, {lit(v)}),')
    out.append('    ]),')
out.append('];')
out.append('')
out.append('/// `all_packages`: the union of the groups (a later group wins on a shared import name).')
out.append('#[rustfmt::skip]')
out.append('pub const ALL_PACKAGES: Group = &[')
for k, v in all_packages.items():
    out.append(f'    ({lit(k)}, {lit(v)}),')
out.append('];')
out.append('''
/// `packages[group]`.
pub fn group(name: &str) -> Option<Group> {
    GROUPS.iter().find(|(g, _)| *g == name).map(|(_, d)| *d)
}
''')
sys.stdout.write('\n'.join(out))
