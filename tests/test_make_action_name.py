#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`@make_action(name='weather wait')` (rc15 Q23) registers a multi-word action explicitly: it runs
as `mrsm weather wait [more words]` exactly like the underscore action `weather_wait`, and the
plugin manifest lists its words (`words`) so help and completion can show `wait` under `weather`.
"""

import json
import os
import subprocess
import sys
import textwrap

from tests._plugin_cli import REPO, marked_line, run_mrsm

_PLUGIN = '''
from meerschaum.plugins import make_action


@make_action(name='rc15d wait')
def wait_for_things(action=None, **kw):
    """Wait for the things."""
    print('ARGS', action)
    return True, 'waited'


@make_action('rc15d status report')
def status(action=None, **kw):
    """Report the status."""
    print('ARGS', action)
    return True, 'status ok'


@make_action
def rc15d_plain(action=None, **kw):
    print('ARGS', action)
    return True, 'plain ok'


@make_action(name='rc15d keep-word')
def keep(action=None, **kw):
    print('ARGS', action)
    return True, 'keep ok'


def rc15d(action=None, **kw):
    """The plugin's own action."""
    print('ARGS', action)
    return True, 'root ok'
'''


def test_named_actions_run_like_underscore_actions(tmp_path):
    plugins = {'rc15d.py': _PLUGIN}
    result = run_mrsm(tmp_path, plugins, ['rc15d', 'wait', 'extra', 'words'])
    assert result.returncode == 0, result.stdout + result.stderr
    assert marked_line(result, 'ARGS') == "ARGS ['extra', 'words']"

    result = run_mrsm(tmp_path, plugins, ['rc15d', 'status', 'report'])
    assert result.returncode == 0, result.stdout + result.stderr
    assert marked_line(result, 'ARGS') == 'ARGS []'

    result = run_mrsm(tmp_path, plugins, ['rc15d', 'keep-word', 'x'])
    assert result.returncode == 0, result.stdout + result.stderr
    assert marked_line(result, 'ARGS') == "ARGS ['x']"


def test_names_that_cannot_be_typed_are_refused():
    import pytest
    from meerschaum.plugins import make_action
    for name in ('', '   ', 'rc15d -x', 'rc15d wait_for'):
        with pytest.raises(ValueError):
            make_action(name=name)


def test_the_manifest_lists_the_words_of_each_action(tmp_path):
    plugins_dir = tmp_path / 'plugins'
    root_dir = tmp_path / 'root'
    plugins_dir.mkdir()
    root_dir.mkdir()
    (plugins_dir / 'rc15d.py').write_text(_PLUGIN, encoding='utf-8')
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root_dir),
        'MRSM_PLUGINS_DIR': str(plugins_dir),
        'MRSM_NO_AUTO_INSTALL': '1',
    })
    code = textwrap.dedent('''
        import json
        from meerschaum.worker._manifest import build_manifest
        from meerschaum.actions import actions
        manifest = build_manifest()
        out = {
            action['name']: {'words': action['words'], 'doc': action['doc']}
            for action in manifest['plugins']['rc15d']['actions']
        }
        out['__function_name__'] = actions['rc15d_wait'].__name__
        print('JSON:' + json.dumps(out))
    ''')
    result = subprocess.run(
        [sys.executable, '-c', code], env=env, cwd=str(tmp_path),
        capture_output=True, text=True, timeout=180,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith('JSON:'))
    out = json.loads(line[len('JSON:'):])
    assert out['rc15d_wait'] == {'words': ['rc15d', 'wait'], 'doc': 'Wait for the things.'}
    assert out['rc15d_status_report']['words'] == ['rc15d', 'status', 'report']
    assert out['rc15d_plain']['words'] == ['rc15d', 'plain']
    assert out['rc15d_keep-word']['words'] == ['rc15d', 'keep-word']
    assert out['rc15d']['words'] == ['rc15d']
    ### Registered under the underscore name the dispatcher strips from `action`.
    assert out['__function_name__'] == 'rc15d_wait'
