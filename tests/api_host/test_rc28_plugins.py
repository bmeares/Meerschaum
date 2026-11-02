#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
RC28 Plugins page, in process on the sandbox's SQLite instance (no server, no browser): New
plugin writes `bootstrap plugins`'s files through the extension, an installed plugin's files are
edited in place (inside the plugin, atomically, refused after a change on disk), both gated like
the web terminal, and the Language column shows the languages' logos.
"""

import json
import os
import pathlib
import shutil
import tempfile

import pytest

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401

#: The host configuration of a server that serves the web terminal.
WEBTERM_ON = {'mrsm_instance': 'sql:apihosttest', 'webterm': True}


def _json(result) -> dict:
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


@pytest.fixture
def plugins_dir(sandbox):
    """A fresh plugins directory per test (the sandbox's root is shared)."""
    root, _ = sandbox
    plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-rc28-plugins-', dir=root.parent))
    try:
        yield root, plugins
    finally:
        shutil.rmtree(plugins, ignore_errors=True)


def _run(root, plugins, code, host_config=None, extra_env=None):
    env = {'MRSM_API_HOST_CONFIG': json.dumps(host_config or WEBTERM_ON)}
    env.update(extra_env or {})
    return run_python(root, plugins, SIGN_IN + code, extra_env=env)


def test_new_plugin_writes_bootstrap_plugins_files(plugins_dir):
    """
    New plugin creates every language and packaging `bootstrap plugins` offers, with its files
    (the extension's scaffold, the same text the wizard writes), and lands on the file to edit;
    names and collisions are checked as the wizard checks them.
    """
    root, plugins = plugins_dir
    out = _json(_run(root, plugins, '''
import json, pathlib
from meerschaum import _core
from meerschaum.api.dash import plugins as mp
admin = sign_in('rc28-admin', user_type='admin')
choices = mp.new_plugin_choices('demo')
created = {}
for name, language, packaging in [('py_file', 'Python', 'file'), ('py_pkg', 'Python', 'package'),
                                  ('rs_file', 'Rust', 'file'), ('rs-pkg', 'Rust', 'package')]:
    created[name] = mp.create_new_plugin(name, language, packaging, ['fetch', 'action'], 'sing song',
                                         'ts', None, admin)
plugins = pathlib.Path(choices['directories'][0])
files = sorted(str(p.relative_to(plugins)) for p in plugins.rglob('*') if p.is_file())
refused = {
    'taken': mp.create_new_plugin('py_file', 'Rust', 'package', ['fetch'], '', '', None, admin),
    'taken_other_language': mp.create_new_plugin('rs_file', 'Python', 'file', ['fetch'], '', '', None, admin),
    'traversal': mp.create_new_plugin('../escape', 'Python', 'file', ['fetch'], '', '', None, admin),
    'reserved': mp.create_new_plugin('mrsm', 'Python', 'file', ['fetch'], '', '', None, admin),
    'empty': mp.create_new_plugin('  ', 'Python', 'file', ['fetch'], '', '', None, admin),
    'rust_name': mp.create_new_plugin('2nd', 'Rust', 'file', ['fetch'], '', '', None, admin),
    'no_features': mp.create_new_plugin('nofeat', 'Python', 'file', [], '', '', None, admin),
    'no_action': mp.create_new_plugin('noaction', 'Python', 'file', ['action'], ' ', '', None, admin),
    'elsewhere': mp.create_new_plugin('elsewhere', 'Python', 'file', ['fetch'], '', '', '/tmp', admin),
    'language': mp.create_new_plugin('cobol', 'COBOL', 'file', ['fetch'], '', '', None, admin),
}
print('JSON:' + json.dumps({
    'languages': choices['languages'],
    'features': [key for key, _, _ in choices['features']],
    'labels': [label for _, label, _ in choices['features']],
    'packaging': choices['packaging'],
    'created': created,
    'files': files,
    'py_file': (plugins / 'py_file.py').read_text(),
    'refused': refused,
    'after': sorted(str(p.relative_to(plugins)) for p in plugins.rglob('*') if p.is_file()),
}))
'''))
    assert out['languages'] == ['Python', 'Rust']
    assert out['features'] == ['fetch', 'connector', 'instance-connector', 'action', 'api', 'web']
    assert out['labels'][0] == 'Fetch data' and 'Instance connector' in out['labels']
    assert out['packaging']['Rust'] == ['Single file (demo.rs)', 'Package (demo/Cargo.toml)']
    for name, (ok, message, url) in out['created'].items():
        assert ok, (name, message)
        assert message.startswith('Successfully created '), message
    assert out['created']['py_file'][2] == '/dash/plugins/installed/py_file?file=py_file.py&edit=1&created=1'
    assert out['created']['py_pkg'][2] == '/dash/plugins/installed/py_pkg?file=__init__.py&edit=1&created=1'
    assert out['created']['rs-pkg'][2] == '/dash/plugins/installed/rs-pkg?file=src%2Flib.rs&edit=1&created=1'
    assert out['files'] == [
        'py_file.py', 'py_pkg/__init__.py', 'rs-pkg/Cargo.toml', 'rs-pkg/plugin.toml',
        'rs-pkg/src/lib.rs', 'rs_file.rs',
    ]
    assert 'def sing_song(' in out['py_file'] and '"ts"' in out['py_file']
    refused = out['refused']
    assert refused['taken'][:2] == [False, "Plugin 'py_file' already exists."]
    assert refused['taken_other_language'][:2] == [False, "Plugin 'rs_file' already exists."]
    assert refused['traversal'][0] is False and 'Invalid plugin name' in refused['traversal'][1]
    assert refused['reserved'][:2] == [False, "Invalid plugin name: 'mrsm'"]
    assert refused['empty'][:2] == [False, 'Enter the name of your new plugin.']
    assert refused['rust_name'][1].startswith('Rust plugin names must start with a letter')
    assert refused['no_features'][:2] == [False, 'Please pick a valid choice.']
    assert refused['no_action'][:2] == [False, 'Please enter an action.']
    assert 'is not one of the plugins directories' in refused['elsewhere'][1]
    assert refused['language'][1].startswith("Unknown language 'COBOL'")
    ### Nothing was written or overwritten by the refusals.
    assert out['after'] == out['files']


def test_authoring_is_gated_like_the_web_terminal(plugins_dir):
    """
    Creating and editing need what the terminal needs: a served terminal, an administrator (read
    fresh) who may use it; and a server that accepts plugins. The Installed table's circled plus
    and the source view's Edit are only rendered for those administrators.
    """
    root, plugins = plugins_dir
    code = '''
import json
from meerschaum.api.dash import plugin_sources as ps, plugins as mp
admin = sign_in('rc28-gate-admin', user_type='admin')
operator = sign_in('rc28-gate-operator', role='operator')
viewer = sign_in('rc28-gate-viewer', role='viewer')
print('JSON:' + json.dumps({
    'admin': ps.authoring_refusal(admin),
    'operator': ps.authoring_refusal(operator),
    'viewer': ps.authoring_refusal(viewer),
    'anonymous': ps.authoring_refusal({}),
    'create_as_operator': mp.create_new_plugin('nope', 'Python', 'file', ['fetch'], '', '', None, operator),
}))
'''
    served = _json(_run(root, plugins, code))
    assert served['admin'] is None
    for who in ('operator', 'viewer', 'anonymous'):
        assert served[who] == 'Only administrators can create and edit plugins.', who
    assert served['create_as_operator'][0] is False
    assert not list(plugins.iterdir())

    off = 'Creating and editing plugins is turned off on this server.'
    no_webterm = _json(_run(root, plugins, code, host_config={'mrsm_instance': 'sql:apihosttest'}))
    assert no_webterm['admin'] == off
    no_auth = _json(_run(root, plugins, code, host_config={
        'mrsm_instance': 'sql:apihosttest', 'no_auth': True, 'webterm': False,
    }))
    assert no_auth['admin'] == off
    no_uploads = _json(_run(root, plugins, code, extra_env={
        'MRSM_CONFIG': json.dumps({'api': {'permissions': {'registration': {'plugins': False}}}}),
    }))
    assert no_uploads['admin'] == off


def test_saving_stays_inside_the_plugin_and_refuses_changed_files(plugins_dir):
    """
    Save writes only a file the source view lists, inside that plugin's own directory, through a
    temporary file renamed over it (a new inode, the old permissions, nothing left behind), and
    refuses a file that changed on disk since it was opened, links that leave the plugin, and
    non-authors.
    """
    root, plugins = plugins_dir
    package = plugins / 'rc28_pkg'
    (package / 'helpers').mkdir(parents=True)
    (package / '__init__.py').write_text('"""A package."""\n\ndef fetch(pipe, **kwargs):\n    return []\n')
    (package / 'helpers' / 'util.py').write_text('VALUE = 1\n')
    os.chmod(package / 'helpers' / 'util.py', 0o640)
    outside = root.parent / f'rc28-outside-{os.getpid()}.py'
    outside.write_text('SECRET = True\n')
    (package / 'linked.py').symlink_to(outside)
    (package / 'linked_dir').symlink_to(root.parent, target_is_directory=True)
    (plugins / 'rc28_single.py').write_text('def fetch(pipe, **kwargs):\n    return []\n')
    (plugins / 'rc28_link.py').symlink_to(outside)
    try:
        out = _json(_run(root, plugins, '''
import json, os, pathlib
from meerschaum.api.dash import plugin_sources as ps
admin = sign_in('rc28-save-admin', user_type='admin')
operator = sign_in('rc28-save-operator', role='operator')
package = pathlib.Path(os.environ['MRSM_PLUGINS_DIR']) / 'rc28_pkg'
util = package / 'helpers' / 'util.py'
listed = ps.source_files('installed', 'rc28_pkg', admin)
text, digest = ps.read_source_with_digest('installed', 'rc28_pkg', 'helpers/util.py', admin)
inode = util.stat().st_ino
saved = ps.save_source('rc28_pkg', 'helpers/util.py', 'VALUE = 2\\n', digest, admin)
after = {'text': util.read_text(), 'mode': oct(util.stat().st_mode & 0o777),
         'new_inode': util.stat().st_ino != inode,
         'leftovers': sorted(p.name for p in util.parent.iterdir())}
stale = ps.save_source('rc28_pkg', 'helpers/util.py', 'VALUE = 3\\n', digest, admin)
_, fresh = ps.read_source_with_digest('installed', 'rc28_pkg', 'helpers/util.py', admin)
util.write_text('VALUE = 4\\n')
changed = ps.save_source('rc28_pkg', 'helpers/util.py', 'VALUE = 5\\n', fresh, admin)
single_text, single_digest = ps.read_source_with_digest('installed', 'rc28_single', 'rc28_single.py', admin)
out = {
    'listed': listed,
    'saved': saved[:2],
    'after': after,
    'stale': stale[:2],
    'changed': changed[:2],
    'kept': util.read_text(),
    'single': ps.save_source('rc28_single', 'rc28_single.py', single_text + '# more\\n', single_digest, admin)[:2],
    'operator': ps.save_source('rc28_pkg', 'helpers/util.py', 'X = 1\\n', ps.source_digest(b'VALUE = 4\\n'), operator)[:2],
    'traversal': ps.save_source('rc28_pkg', '../rc28_single.py', 'X = 1\\n', fresh, admin)[:2],
    'absolute': ps.save_source('rc28_pkg', '/etc/passwd', 'X = 1\\n', fresh, admin)[:2],
    'link': ps.save_source('rc28_pkg', 'linked.py', 'X = 1\\n', fresh, admin)[:2],
    'link_dir': ps.save_source('rc28_pkg', 'linked_dir/x.py', 'X = 1\\n', fresh, admin)[:2],
    'unlisted': ps.save_source('rc28_pkg', 'helpers/new.py', 'X = 1\\n', fresh, admin)[:2],
    'linked_plugin': ps.save_source('rc28_link', 'rc28_link.py', 'X = 1\\n', fresh, admin)[:2],
    'too_big': ps.save_source('rc28_pkg', 'helpers/util.py', 'x' * (ps.MAX_FILE_BYTES + 1), ps.source_digest(b'VALUE = 4\\n'), admin)[:2],
    'nul': ps.save_source('rc28_pkg', 'helpers/util.py', 'a\\0b', ps.source_digest(b'VALUE = 4\\n'), admin)[:2],
    'no_digest': ps.save_source('rc28_pkg', 'helpers/util.py', 'X = 1\\n', None, admin)[:2],
    'final': util.read_text(),
}
print('JSON:' + json.dumps(out))
'''))
        assert out['listed'] == ['__init__.py', 'helpers/util.py']
        assert out['saved'] == [True, 'Saved helpers/util.py.']
        assert out['after'] == {
            'text': 'VALUE = 2\n', 'mode': '0o640', 'new_inode': True, 'leftovers': ['util.py'],
        }
        changed_message = (
            'helpers/util.py changed on disk after it was opened, so it was not saved. '
            'Cancel to load the current file.'
        )
        assert out['stale'] == [False, changed_message]
        assert out['changed'] == [False, changed_message]
        assert out['kept'] == 'VALUE = 4\n'
        assert out['single'] == [True, 'Saved rc28_single.py.']
        assert out['operator'] == [False, 'Only administrators can create and edit plugins.']
        for key in ('traversal', 'absolute', 'link', 'link_dir', 'unlisted', 'linked_plugin',
                    'too_big', 'nul', 'no_digest'):
            assert out[key][0] is False, (key, out[key])
        assert 'not available' in out['unlisted'][1] and 'not available' in out['link'][1]
        assert 'outside the installed plugin directories' in out['linked_plugin'][1]
        assert '256 KiB' in out['too_big'][1]
        assert out['final'] == 'VALUE = 4\n'
        assert outside.read_text() == 'SECRET = True\n'
    finally:
        outside.unlink(missing_ok=True)


def _fire_code() -> str:
    return '''
import json
from meerschaum.api.dash import build_dash_app, dash_app
build_dash_app()
dash_app._setup_server()
client = dash_app.server.test_client()
endpoint = next(str(rule) for rule in dash_app.server.url_map.iter_rules()
                if str(rule).endswith('/_dash-update-component'))

def callback(fragment):
    key = next(key for key in dash_app.callback_map if fragment in key)
    return key, dash_app.callback_map[key]

def outputs_of(key):
    parts = key.strip('.').split('...')
    out = []
    for part in parts:
        ident, prop = part.rsplit('.', 1)
        out.append({'id': json.loads(ident) if ident.startswith('{') else ident, 'property': prop})
    return out if key.startswith('..') else out[0]

def fire(fragment, inputs, state):
    key, _ = callback(fragment)
    changed = inputs[0][0] if isinstance(inputs[0], list) else inputs[0]
    ident = changed['id']
    prop_id = (json.dumps(ident, sort_keys=True, separators=(',', ':')) if isinstance(ident, dict) else ident)
    response = client.post(endpoint, json={
        'output': key, 'outputs': outputs_of(key), 'inputs': inputs, 'state': state,
        'changedPropIds': [prop_id + '.' + changed['property']],
    })
    return response.status_code, response.get_json(silent=True) or {}
'''


def test_callbacks_refuse_non_authors_and_save_for_authors(plugins_dir):
    """
    Edit, Save and Create re-check the session on the server: a non-administrator's forged
    requests change nothing, an administrator's Save writes the file and closes the editor.
    """
    root, plugins = plugins_dir
    (plugins / 'rc28_cb.py').write_text('VALUE = 1\n')
    out = _json(_run(root, plugins, _fire_code() + '''
from meerschaum.api.dash import plugin_sources as ps
admin = sign_in('rc28-cb-admin', user_type='admin')
operator = sign_in('rc28-cb-operator', role='operator')
_, digest = ps.read_source_with_digest('installed', 'rc28_cb', 'rc28_cb.py', admin)

def source_action(action, session, text='VALUE = 2\\n', editing=True):
    return fire('plugin-source-context.data', [[{
        'id': {'type': 'plugin-source-action', 'action': action}, 'property': 'n_clicks', 'value': 1,
    }]], [
        {'id': 'plugin-source-file', 'property': 'value', 'value': 'rc28_cb.py'},
        {'id': 'plugin-source-context', 'property': 'data', 'value': {'kind': 'installed', 'name': 'rc28_cb'}},
        {'id': 'plugin-source-data', 'property': 'data',
         'value': {'path': 'rc28_cb.py', 'digest': digest, 'editing': editing}},
        {'id': 'plugin-source-editor', 'property': 'value', 'value': text},
        {'id': 'session-store', 'property': 'data', 'value': session},
    ])

def create(session):
    return fire('plugins-navigate.data', [{'id': 'plugins-new-create', 'property': 'n_clicks', 'value': 1}], [
        {'id': 'plugins-new-name', 'property': 'value', 'value': 'rc28_made'},
        {'id': 'plugins-new-language', 'property': 'value', 'value': 'Python'},
        {'id': 'plugins-new-packaging', 'property': 'value', 'value': 'file'},
        {'id': 'plugins-new-features', 'property': 'value', 'value': ['fetch']},
        {'id': 'plugins-new-action', 'property': 'value', 'value': ''},
        {'id': 'plugins-new-dt', 'property': 'value', 'value': ''},
        {'id': 'plugins-new-directory', 'property': 'value', 'value': None},
        {'id': 'session-store', 'property': 'data', 'value': session},
    ])

def render(session, editing=None):
    return fire('plugin-source-content.children', [
        {'id': 'plugin-source-file', 'property': 'value', 'value': 'rc28_cb.py'},
        {'id': 'plugin-source-context', 'property': 'data',
         'value': {'kind': 'installed', 'name': 'rc28_cb', 'edit': editing}},
    ], [{'id': 'session-store', 'property': 'data', 'value': session}])

import pathlib, os
path = pathlib.Path(os.environ['MRSM_PLUGINS_DIR']) / 'rc28_cb.py'
out = {
    'operator_edit': source_action('edit', operator),
    'operator_save': source_action('save', operator),
    'operator_create': create(operator),
    'after_operator': path.read_text(),
    'made_by_operator': (path.parent / 'rc28_made.py').exists(),
    'admin_render': json.dumps(render(admin)),
    'admin_editor': json.dumps(render(admin, {'path': 'rc28_cb.py', 'nonce': 'x'})),
    'operator_editor': json.dumps(render(operator, {'path': 'rc28_cb.py', 'nonce': 'x'})),
    'admin_edit': source_action('edit', admin),
    'admin_save': source_action('save', admin),
    'after_admin': path.read_text(),
    'admin_create': create(admin),
}
print('JSON:' + json.dumps(out))
'''))
    refusal = 'Only administrators can create and edit plugins.'
    status, body = out['operator_edit']
    assert status == 200 and 'plugin-source-context' not in body['response']
    assert refusal in json.dumps(body['response']['plugin-source-alert'])
    assert refusal in json.dumps(out['operator_save'][1])
    assert refusal in json.dumps(out['operator_create'][1])
    assert out['after_operator'] == 'VALUE = 1\n' and not out['made_by_operator']
    ### The source view: Edit for the administrator, the editor only for them.
    assert '"action": "edit"' in out['admin_render'] and 'plugin-source-editor' not in out['admin_render']
    assert 'plugin-source-editor' in out['admin_editor'] and '"action": "save"' in out['admin_editor']
    assert 'plugin-source-editor' not in out['operator_editor']
    assert '"action": "edit"' not in out['operator_editor']
    status, body = out['admin_edit']
    assert body['response']['plugin-source-context']['data']['edit']['path'] == 'rc28_cb.py'
    status, body = out['admin_save']
    assert body['response']['plugin-source-context']['data'] == {
        'kind': 'installed', 'name': 'rc28_cb', 'edit': None,
    }
    assert 'Saved rc28_cb.py.' in json.dumps(body['response']['plugin-source-alert'])
    assert out['after_admin'] == 'VALUE = 2\n'
    status, body = out['admin_create']
    assert body['response']['plugins-navigate']['data'] == (
        '/dash/plugins/installed/rc28_made?file=rc28_made.py&edit=1&created=1'
    )


def test_language_column_shows_the_languages_logos(plugins_dir):
    """The Installed table's Language column (was Kind) shows Font Awesome logos named for screen readers."""
    root, plugins = plugins_dir
    (plugins / 'rc28_lang_py.py').write_text('def fetch(pipe, **kwargs):\n    return []\n')
    (plugins / 'rc28_lang_rs.rs').write_text('fn setup() -> (bool, String) { (true, "ok".into()) }\n')
    out = _json(_run(root, plugins, '''
import json
from meerschaum.api.dash import plugins as mp
plugins = mp.get_installed_plugins(check_packages=False)
table = mp.build_installed_plugins_table(plugins, admin=True).to_plotly_json()
print('JSON:' + json.dumps(table, default=lambda o: o.to_plotly_json()))
'''))
    text = json.dumps(out)
    assert '"Language"' in text and '"Kind"' not in text
    assert 'fa-brands fa-python' in text and 'fa-brands fa-rust' in text
    assert '"aria-label": "Python"' in text and '"aria-label": "Rust"' in text
    assert '"title": "Python"' in text and '"title": "Rust"' in text
    assert 'visually-hidden' in text
    assert 'Rust (single file)' not in text


def test_the_portable_save_path_keeps_the_same_rules(plugins_dir):
    """
    Where Python cannot open directories relative to a descriptor (Windows), Save checks each
    component for links, stays under the root and still refuses a changed file.
    """
    root, plugins = plugins_dir
    package = plugins / 'rc28_portable'
    package.mkdir()
    (package / '__init__.py').write_text('VALUE = 1\n')
    (package / 'link').symlink_to(root.parent, target_is_directory=True)
    out = _json(_run(root, plugins, '''
import json, os, pathlib
from meerschaum.api.dash import plugin_sources as ps
plugins = pathlib.Path(os.environ['MRSM_PLUGINS_DIR'])
digest = ps.source_digest(b'VALUE = 1\\n')
def attempt(parts, data, expected):
    try:
        ps._replace_checked_path(plugins, parts, data, expected)
        return 'ok'
    except ps.SourceChanged:
        return 'changed'
    except (OSError, ValueError) as e:
        return type(e).__name__ + ': ' + str(e)
out = {
    'saved': attempt(('rc28_portable', '__init__.py'), b'VALUE = 2\\n', digest),
    'text': (plugins / 'rc28_portable' / '__init__.py').read_text(),
    'stale': attempt(('rc28_portable', '__init__.py'), b'VALUE = 3\\n', digest),
    'link': attempt(('rc28_portable', 'link', 'x.py'), b'X = 1\\n', digest),
    'leftovers': sorted(p.name for p in (plugins / 'rc28_portable').iterdir()),
}
print('JSON:' + json.dumps(out))
'''))
    assert out['saved'] == 'ok' and out['text'] == 'VALUE = 2\n'
    assert out['stale'] == 'changed'
    assert out['link'].startswith('ValueError: Linked source paths')
    assert out['leftovers'] == ['__init__.py', 'link']
