def test_get_console_is_cached():
    from meerschaum.utils.formatting import get_console

    assert get_console() is get_console()


def test_formatting_without_rich_prints_plain_text(tmp_path):
    """`rich` is not a dependency of the base wheel: the helpers print plain text without it."""
    import os
    import subprocess
    import sys
    code = """
import sys
for name in ('rich', 'rich.text', 'rich.console', 'rich.table', 'rich.box', 'rich.ansi',
             'rich.pretty', 'rich.columns', 'rich.panel', 'rich.tree', 'asciitree'):
    sys.modules[name] = None
import meerschaum as mrsm
from meerschaum.utils.formatting import (
    print_options, pprint, colored, fill_ansi, pipe_repr, print_tuple, get_console,
)
from meerschaum.utils.formatting._pipes import pprint_pipes, pprint_pipe_columns
from meerschaum.utils.warnings import warn, info
assert get_console() is None
print_options(['alpha', 'beta'] * 8, name='things')
print_options(['alpha', 'beta'], name='things', number_options=True)
conn = mrsm.get_connector('sql', 'norich', flavor='sqlite', database=sys.argv[1])
pipe = mrsm.Pipe('norich', 'probe', instance=conn, columns={'id': 'id'})
assert pipe.sync([{'id': 1, 'v': 'a'}])[0]
pprint_pipes({'norich': {'probe': {None: pipe}}})
pprint_pipe_columns(pipe)
assert colored('x', style='bold') == 'x'
assert pipe_repr(pipe, ansi=False).startswith("Pipe('norich', 'probe'")
pprint({'a': 1})
print_tuple((True, 'done'))
warn('careful', stack=False)
print('NORICH_OK')
"""
    env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONPATH': os.getcwd(),
    })
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    completed = subprocess.run(
        [sys.executable, '-c', code, str(tmp_path / 'norich.db')],
        env=env, capture_output=True, text=True, timeout=180,
    )
    assert 'NORICH_OK' in completed.stdout, completed.stdout[-1500:] + completed.stderr[-3000:]
    assert 'alpha' in completed.stdout and 'norich' in completed.stdout
