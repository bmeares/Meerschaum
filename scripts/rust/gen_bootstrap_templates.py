"""Generate crates/meerschaum-cli/src/native/bootstrap_templates.json from
`meerschaum/plugins/bootstrap.py` (the text `bootstrap plugins` writes, before `str.format`).

    python3 scripts/rust/gen_bootstrap_templates.py \
      > crates/meerschaum-cli/src/native/bootstrap_templates.json

The three tables are read with `ast.literal_eval` from the source text, so nothing from
`meerschaum` is imported (no built extension needed); `tests/generated_tables.rs` fails when
the JSON differs from this script's output.
"""
import ast
import json
import pathlib
import sys

SOURCE = pathlib.Path(__file__).resolve().parents[2] / 'meerschaum' / 'plugins' / 'bootstrap.py'
NAMES = ('FEATURE_CHOICES', 'FEATURE_LINES', 'IMPORTS_LINES')

values = {}
for node in ast.parse(SOURCE.read_text(encoding='utf-8')).body:
    if isinstance(node, ast.Assign):
        target = node.targets[0]
    elif isinstance(node, ast.AnnAssign):
        target = node.target
    else:
        continue
    name = getattr(target, 'id', None)
    if name in NAMES:
        values[name] = ast.literal_eval(node.value)
missing = [name for name in NAMES if name not in values]
if missing:
    sys.exit(f"{SOURCE}: no literal assignment for {', '.join(missing)}")

json.dump(
    {
        'feature_choices': [[key, text] for key, text in values['FEATURE_CHOICES'].items()],
        'imports_lines': values['IMPORTS_LINES'],
        'feature_lines': values['FEATURE_LINES'],
    },
    sys.stdout,
    indent=2,
    ensure_ascii=False,
)
sys.stdout.write('\n')
