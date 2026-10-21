"""The Python entrypoint must retain native actions' dropdown suggestions."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize('action, expected', [
    (['edit', 'config'], 'stack'),
    (['bootstrap', 'pipes', '-i', 'sql:local'], 'sql:local'),
])
def test_python_entry_routes_suggestions_to_the_terminal(tmp_path, action, expected):
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(Path(__file__).resolve().parent.parent),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        'EDITOR': 'true',
        'VISUAL': 'true',
    })
    script = '''
import sys
import meerschaum as mrsm
import meerschaum.utils.prompt as prompts
sys.stdin.isatty = lambda: True
sys.stdout.isatty = lambda: True
seen = []
def read(question, items, prefill):
    seen.extend(items)
    raise KeyboardInterrupt
prompts._read_suggested = read
mrsm.entry(ACTION)
assert EXPECTED in seen, repr(seen)
'''.replace('ACTION', repr(action)).replace('EXPECTED', repr(expected))
    result = subprocess.run(
        [sys.executable, '-c', script], env=env,
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_suggested_prompt_opens_menu_and_matches_middle(monkeypatch):
    import prompt_toolkit
    from prompt_toolkit.document import Document
    from prompt_toolkit.completion import CompleteEvent
    from meerschaum.utils.prompt import _read_suggested

    calls = {}
    def read(message, **kwargs):
        calls.update(kwargs)
        return 'sql:main'

    monkeypatch.setattr(prompt_toolkit, 'prompt', read)
    assert _read_suggested('Connector: ', ['sql:main', 'api:main']) == 'sql:main'
    assert calls['complete_while_typing']
    assert callable(calls['pre_run'])
    completions = list(calls['completer'].get_completions(Document('MAIN'), CompleteEvent()))
    assert [item.text for item in completions] == ['sql:main', 'api:main']
