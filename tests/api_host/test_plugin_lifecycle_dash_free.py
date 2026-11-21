#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash-free plugin lifecycle & rights tests (`meerschaum.api.dash._plugins`):
verifies `PluginRights`, safe path/symlink/archive-bomb rejection, template creation,
source read/CAS save, description updates, team `claim`/`release` reconciliation via
`reconcile_plugin_run`, and zero AST import cycles — all without importing Dash.
"""

from __future__ import annotations

import ast
import collections
import json
import pathlib
import shutil
import tempfile

import pytest

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401

WEBTERM_ON = {'mrsm_instance': 'sql:apihosttest', 'webterm': True}


def _json(result) -> dict:
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


@pytest.fixture
def plugins_dir(sandbox):
    root, _ = sandbox
    plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-dash-free-plugins-', dir=root.parent))
    try:
        yield root, plugins
    finally:
        shutil.rmtree(plugins, ignore_errors=True)


def _run(root, plugins, code, host_config=None, extra_env=None):
    env = {'MRSM_API_HOST_CONFIG': json.dumps(host_config or WEBTERM_ON)}
    env.update(extra_env or {})
    return run_python(root, plugins, SIGN_IN + code, extra_env=env)


def test_plugin_rights_matrix_without_dash(plugins_dir):
    """`PluginRights` consolidates visibility, management, source, authoring, and publish policy without Dash."""
    root, plugins = plugins_dir
    out = _json(_run(root, plugins, '''
import json, sys
from meerschaum.api.dash import access
from meerschaum.api.dash._plugins import (
    AUTHORING_ADMINS,
    AUTHORING_OFF,
    MANAGE_REFUSAL,
    PluginRights,
    resolve_rights,
)

admin = access.make_viewer('df-admin', admin=True)
writer = access.make_viewer(
    'df-writer', role='editor', teams={'builders': 'member'},
    grants={'plugins': ['mine'], 'plugins_write': True},
)
walled_writer = access.make_viewer(
    'df-walled-writer', role='editor', teams={'island': 'member'},
    grants={'isolated': True, 'plugins': ['mine'], 'plugins_write': True},
)
member = access.make_viewer(
    'df-member', role='editor', teams={'readers': 'member'},
    grants={'plugins': ['mine']},
)
anon = access.ANONYMOUS

r_admin = resolve_rights(admin, webterm_enabled=True, registration_enabled=True, console_read_only=False)
r_admin_nowebterm = resolve_rights(admin, webterm_enabled=False, registration_enabled=True, console_read_only=False)
r_writer = resolve_rights(writer, webterm_enabled=False, registration_enabled=True, console_read_only=False)
r_writer_secure = resolve_rights(writer, webterm_enabled=True, registration_enabled=True, console_read_only=True)
r_writer_regoff = resolve_rights(writer, webterm_enabled=True, registration_enabled=False, console_read_only=False)
r_walled = resolve_rights(walled_writer, webterm_enabled=True, registration_enabled=True, console_read_only=False)
r_member = resolve_rights(member, webterm_enabled=True, registration_enabled=True, console_read_only=False)
r_anon = resolve_rights(anon, webterm_enabled=True, registration_enabled=True, console_read_only=False)

installed = {'mine', 'foreign'}
out = {
    'dash_loaded': any(m == 'dash' or m.startswith('dash.') or m.startswith('dash_bootstrap_components')
                       for m in sys.modules),
    'is_dataclass': isinstance(r_admin, PluginRights),
    'admin_author': r_admin.can_author('foreign'),
    'admin_nowebterm_refusal': r_admin_nowebterm.authoring_refusal(),
    'writer_nowebterm_author': r_writer.can_author('mine'),
    'writer_foreign_refusal': r_writer.authoring_refusal('foreign'),
    'writer_secure_refusal': r_writer_secure.authoring_refusal(),
    'writer_regoff_refusal': r_writer_regoff.authoring_refusal(),
    'writer_install_new': r_writer.can_install('fresh', installed),
    'writer_install_foreign': r_writer.can_install('foreign', installed),
    'writer_publish_mine': r_writer.can_publish('mine'),
    'writer_publish_foreign': r_writer.can_publish('foreign'),
    'writer_read_mine': r_writer.can_read_source('installed', 'mine'),
    'writer_read_foreign': r_writer.can_read_source('installed', 'foreign'),
    'walled_visible': r_walled.visible_names(['mine', 'foreign', 'fresh']),
    'walled_action_foreign': r_walled.action_refusal('install', 'foreign', installed),
    'member_action': r_member.action_refusal('install', 'mine', installed),
    'anon_see': r_anon.can_see('mine'),
    'anon_publish': r_anon.can_publish('mine'),
}
print('JSON:' + json.dumps(out))
'''))
    assert out['dash_loaded'] is False
    assert out['is_dataclass'] is True
    assert out['admin_author'] is True
    assert out['admin_nowebterm_refusal'] == 'Creating and editing plugins is turned off on this server.'
    assert out['writer_nowebterm_author'] is True
    assert out['writer_foreign_refusal'] == "Plugin 'foreign' does not belong to your team."
    assert out['writer_secure_refusal'] == 'Only administrators and team plugin managers can create and edit plugins.'
    assert out['writer_regoff_refusal'] == 'Creating and editing plugins is turned off on this server.'
    assert out['writer_install_new'] is True
    assert out['writer_install_foreign'] is False
    assert out['writer_publish_mine'] is True
    assert out['writer_publish_foreign'] is False
    assert out['writer_read_mine'] is True
    assert out['writer_read_foreign'] is False
    assert out['walled_visible'] == ['mine']
    assert out['walled_action_foreign'] == "Plugin 'foreign' is not available."
    assert out['member_action'] == 'Only administrators and team plugin managers can manage plugins.'
    assert out['anon_see'] is False
    assert out['anon_publish'] is False


def test_safe_paths_symlinks_and_archives_without_dash(plugins_dir):
    """Safe path, symlink `O_NOFOLLOW`, and zip/tar bomb checks run cleanly without Dash."""
    root, plugins = plugins_dir
    out = _json(_run(root, plugins, '''
import io, json, os, pathlib, stat, sys, tarfile, zipfile
from meerschaum.api.dash._plugins import (
    MAX_PATH_BYTES,
    MAX_PATH_PARTS,
    UploadError,
    checked_upload_path,
    is_safe_plugin_name,
    plan_upload,
    read_file_no_follow,
    relative_source_path,
    replace_file_cas,
    source_digest,
    valid_plugin_name,
    write_plugin,
)

dest = pathlib.Path(os.environ['MRSM_PLUGINS_DIR'])
outside = dest.parent / 'df-secret.py'
outside.write_text('SECRET = 1\\n')
pkg = dest / 'safe_pkg'
pkg.mkdir()
(pkg / '__init__.py').write_text('VALUE = 1\\n')
(pkg / 'link.py').symlink_to(outside)

def fails(fn, exc=(ValueError, OSError)):
    try:
        fn()
        return False
    except exc:
        return True

# Build a gzip bomb tar archive and a symlink zip archive in memory.
tar_buf = io.BytesIO()
with tarfile.open(fileobj=tar_buf, mode='w:gz') as tf:
    info = tarfile.TarInfo('bomb/__init__.py')
    payload = b'\\0' * (256 * 1024)
    info.size = len(payload)
    tf.addfile(info, io.BytesIO(payload))

zip_buf = io.BytesIO()
with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.writestr('zpkg/__init__.py', b'X = 1\\n')
    zi = zipfile.ZipInfo('zpkg/sym.py')
    zi.external_attr = (stat.S_IFLNK | 0o777) << 16
    zf.writestr(zi, '/etc/passwd')

out = {
    'dash_loaded': 'dash' in sys.modules,
    'safe_names': [is_safe_plugin_name('demo_1-a'), is_safe_plugin_name('../x'), is_safe_plugin_name('')],
    'valid_names': [valid_plugin_name('demo'), valid_plugin_name('_hidden'), valid_plugin_name('mrsm')],
    'skip_pycache': checked_upload_path('pkg/__pycache__/a.pyc') is None,
    'reject_long_source': fails(lambda: relative_source_path('a/' + 'x' * MAX_PATH_BYTES + '.py')),
    'reject_deep_upload': fails(lambda: checked_upload_path('/'.join(['d'] * (MAX_PATH_PARTS + 1))), UploadError),
    'reject_symlink_read': fails(lambda: read_file_no_follow(dest, 'safe_pkg/link.py')),
    'reject_symlink_cas': fails(lambda: replace_file_cas(dest, 'safe_pkg/link.py', b'NEW', source_digest(b'SECRET = 1\\n'))),
    'reject_tar_bomb': fails(lambda: plan_upload([('bomb.tar.gz', tar_buf.getvalue())], limit=16 * 1024), UploadError),
    'reject_zip_symlink': fails(lambda: plan_upload([('z.zip', zip_buf.getvalue())]), UploadError),
    'outside_intact': outside.read_text(),
}
outside.unlink(missing_ok=True)
print('JSON:' + json.dumps(out))
'''))
    assert out['dash_loaded'] is False
    assert out['safe_names'] == [True, False, False]
    assert out['valid_names'] == [True, False, False]
    assert out['skip_pycache'] is True
    assert out['reject_long_source'] is True
    assert out['reject_deep_upload'] is True
    assert out['reject_symlink_read'] is True
    assert out['reject_symlink_cas'] is True
    assert out['reject_tar_bomb'] is True
    assert out['reject_zip_symlink'] is True
    assert out['outside_intact'] == 'SECRET = 1\n'


def test_lifecycle_template_source_description_and_reconcile_without_dash(plugins_dir):
    """
    End-to-end Dash-free lifecycle against the temp plugins dir + SQLite sandbox:
    template creation, source read & CAS save, repository description update,
    and `reconcile_plugin_run` team claim/release.
    """
    root, plugins = plugins_dir
    out = _json(_run(root, plugins, '''
import json, os, pathlib, sys
from types import SimpleNamespace

class _BlockDash:
    """Simulate an environment without the optional `dash` extra installed."""
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'dash' or fullname.startswith(('dash.', 'dash_bootstrap_components', 'dash_ace')):
            raise ModuleNotFoundError(f"No module named {fullname!r} (blocked by Dash-free test)")
        return None

sys.meta_path.insert(0, _BlockDash())

import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.core import Plugin, User
from meerschaum.api.dash import access
from meerschaum.api.dash._plugins import (
    create_new_plugin,
    read_source_with_digest,
    reconcile_plugin_run,
    save_source,
    update_plugin_description,
)

conn = get_api_connector()
owner = sign_in('df-owner', user_type='admin')
writer = sign_in('df-team-writer', role='editor', teams={'df-builders': 'member'})
member = sign_in('df-team-member', role='editor', teams={'df-readers': 'member'})

team = mrsm.Team('df-builders', instance=conn)
team.attributes.update({'plugins_write': True})
assert team.edit()[0]
access.forget()

# 1. Template creation + automatic team claim
created = create_new_plugin('df_demo', 'Python', 'file', ['fetch'], '', 'ts', None, writer)
team_after_create = list(mrsm.Team('df-builders', instance=conn).attributes.get('plugins') or [])

# 2. Source read + digest CAS save + stale-digest refusal
text, digest = read_source_with_digest('installed', 'df_demo', 'df_demo.py', writer)
saved_ok = save_source('df_demo', 'df_demo.py', text + '# edited\\n', digest, writer)
saved_stale = save_source('df_demo', 'df_demo.py', text + '# stale\\n', digest, writer)

# 3. Repository description update (owner succeeds, non-owner refused)
plugin = Plugin('df_pub', user_id=conn.get_user_id(User('df-owner', instance=conn)))
assert conn.register_plugin(plugin)[0]
desc_refused = update_plugin_description('df_pub', 'Hijacked', writer)
desc_ok = update_plugin_description('df_pub', 'Updated Dash-free description.', owner)
stored_desc = (conn.get_plugin_attributes(Plugin('df_pub')) or {}).get('description')

# 4. Team claim / release reconciliation via reconcile_plugin_run (without Dash!)
def make_job(owner_name, plugin_name, action, status='stopped', result=(True, 'Success')):
    verb = 'uninstall' if action == 'uninstall' else 'install'
    return SimpleNamespace(
        name='execute-df0001',
        exists=lambda: True,
        status=status,
        result=result,
        sysargs=[verb, 'plugins', plugin_name, '--yes'],
        daemon=SimpleNamespace(properties={'mrsm_execution': {
            'origin': 'console_plugins',
            'owner': owner_name,
            'plugin': plugin_name,
            'action': action,
        }}),
    )

who_writer = access.resolve_viewer(writer, fresh=True)
who_member = access.resolve_viewer(member, fresh=True)

rec_running = reconcile_plugin_run(
    'execute-df0001', who_writer,
    job_loader=lambda _name: make_job('df-team-writer', 'from_repo', 'install', status='running'),
)
rec_denied = reconcile_plugin_run(
    'execute-df0001', who_member,
    job_loader=lambda _name: make_job('df-team-member', 'from_repo', 'install'),
)
rec_failed = reconcile_plugin_run(
    'execute-df0001', who_writer,
    job_loader=lambda _name: make_job('df-team-writer', 'broken_repo', 'install', result=(False, 'Boom')),
)
rec_install = reconcile_plugin_run(
    'execute-df0001', who_writer,
    job_loader=lambda _name: make_job('df-team-writer', 'from_repo', 'install'),
)
team_after_install = list(mrsm.Team('df-builders', instance=conn).attributes.get('plugins') or [])

rec_uninstall = reconcile_plugin_run(
    'execute-df0001', access.resolve_viewer(writer, fresh=True),
    job_loader=lambda _name: make_job('df-team-writer', 'from_repo', 'uninstall'),
)
team_after_uninstall = list(mrsm.Team('df-builders', instance=conn).attributes.get('plugins') or [])

out = {
    'dash_loaded': any(m == 'dash' or m.startswith('dash.') or m.startswith('dash_bootstrap_components')
                       for m in sys.modules),
    'created': [created[0], created[2]],
    'team_after_create': team_after_create,
    'saved_ok': [saved_ok[0], saved_ok[1], saved_ok[2] is not None],
    'saved_stale': [saved_stale[0], saved_stale[1]],
    'desc_refused': list(desc_refused),
    'desc_ok': list(desc_ok),
    'stored_desc': stored_desc,
    'rec_running': rec_running,
    'rec_denied': rec_denied,
    'rec_failed': rec_failed,
    'rec_install': rec_install,
    'team_after_install': team_after_install,
    'rec_uninstall': rec_uninstall,
    'team_after_uninstall': team_after_uninstall,
}
print('JSON:' + json.dumps(out))
'''))
    assert out['dash_loaded'] is False
    assert out['created'][0] is True
    assert out['created'][1].startswith('/dash/plugins/installed/df_demo?')
    assert out['team_after_create'] == ['df_demo']
    assert out['saved_ok'] == [True, 'Saved df_demo.py.', True]
    assert out['saved_stale'][0] is False and 'changed on disk' in out['saved_stale'][1]
    assert out['desc_refused'][0] is False and 'insufficient permissions' in out['desc_refused'][1]
    assert out['desc_ok'] == [True, 'Successfully updated description.']
    assert out['stored_desc'] == 'Updated Dash-free description.'
    assert out['rec_running'] == {'status': 'running', 'job_status': 'running'}
    assert out['rec_denied'] == {'status': 'denied'}
    assert out['rec_failed']['status'] == 'done' and out['rec_failed']['success'] is False
    assert out['rec_install']['status'] == 'done' and out['rec_install']['success'] is True
    assert out['rec_install']['recorded'] is True
    assert out['team_after_install'] == ['df_demo', 'from_repo']
    assert out['rec_uninstall']['status'] == 'done' and out['rec_uninstall']['success'] is True
    assert out['rec_uninstall']['recorded'] is True
    assert out['team_after_uninstall'] == ['df_demo']


def test_plugin_cluster_import_graph_is_strictly_acyclic():
    """
    AST import graph across the entire plugin cluster (both top-level and function-local
    imports) has strictly 0 cycles.
    """
    dash_root = pathlib.Path(__file__).resolve().parents[2] / 'meerschaum' / 'api' / 'dash'
    cluster_files = [
        *sorted((dash_root / '_plugins').glob('*.py')),
        dash_root / 'plugins.py',
        dash_root / 'plugin_sources.py',
        dash_root / 'plugin_upload.py',
        dash_root / 'plugin_publish.py',
        dash_root / 'pages' / 'plugins.py',
        dash_root / 'callbacks' / 'plugins.py',
    ]

    def mod_name(path: pathlib.Path) -> str:
        rel = path.relative_to(dash_root.parents[2]).with_suffix('')
        parts = list(rel.parts)
        if parts[-1] == '__init__':
            parts = parts[:-1]
        return '.'.join(parts)

    mods = {mod_name(p): p for p in cluster_files}
    graph = collections.defaultdict(set)
    for mod, path in mods.items():
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in mods and alias.name != mod:
                        graph[mod].add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ''
                if node.level > 0:
                    pkg = mod.split('.')[:-node.level]
                    base = '.'.join(pkg + ([base] if base else []))
                if base in mods and base != mod:
                    graph[mod].add(base)
                for alias in node.names:
                    cand = f'{base}.{alias.name}' if base else alias.name
                    if cand in mods and cand != mod:
                        graph[mod].add(cand)

    index = 0
    indices, lowlink, stack, on_stack, cycles = {}, {}, [], set(), []

    def strongconnect(v: str):
        nonlocal index
        indices[v] = lowlink[v] = index
        index += 1
        stack.append(v)
        on_stack.add(v)
        for w in sorted(graph.get(v, ())):
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in on_stack:
                lowlink[v] = min(lowlink[v], indices[w])
        if lowlink[v] == indices[v]:
            scc = []
            while True:
                w = stack.pop()
                on_stack.remove(w)
                scc.append(w)
                if w == v:
                    break
            if len(scc) > 1:
                cycles.append(sorted(scc))

    for mod in sorted(mods):
        if mod not in indices:
            strongconnect(mod)

    assert cycles == [], f"Unexpected import cycles in plugin cluster: {cycles}"
