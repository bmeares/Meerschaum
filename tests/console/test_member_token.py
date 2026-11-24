"""A member registers a scoped key on the API instance and uses it against an owned pipe."""

import json
import os

import pytest

from tests._ports import service_port
from tests.console.conftest import PASSWORD, USERNAME, Server, Tab
from tests.console.test_credential_interop import login, request

pytestmark = pytest.mark.console


def test_member_token_lifecycle(browser, record_property):
    from tests._binary import skip_or_fail
    record_property('revision', os.environ.get('GITHUB_SHA', 'working-tree'))
    if 'valkey' not in json.loads(os.environ.get('MRSM_TEST_PORTS', '{}')):
        skip_or_fail('The member token lifecycle needs an owned Valkey test port.')
    instance = Server('member-token', config={
        'meerschaum': {'instance': 'sql:other'},
        'api': {'cache': {'connector': 'valkey:session'},
                'permissions': {'instances': {'allow_multiple_instances': True,
                                              'allowed_instance_keys': ['sql:w7console', 'sql:team']}}},
        'system': {'experimental': {'valkey_session_cache': True}},
    }, args=['-i', 'sql:w7console', '--production', '--private'], native=False)
    instance.env.update({
        'MRSM_SQL_OTHER': f"sqlite:///{instance.root / 'default.db'}",
        'MRSM_SQL_TEAM': json.dumps({'flavor': 'sqlite', 'database': str(instance.root / 'team.db'),
                                     'teams': ['Readers']}),
        'MRSM_VALKEY_SESSION': f"valkey://127.0.0.1:{service_port('valkey')}/0",
    })
    (instance.plugins / 'cache_probe.py').write_text("""
from meerschaum.plugins import api_plugin

@api_plugin
def routes(app):
    @app.get('/cache-probe')
    def cache_probe():
        from meerschaum.api import get_api_connector
        cache = get_api_connector()._cache_connector
        return str(cache) == 'valkey:session' and cache.test_connection()
""")
    context = browser.new_context()
    try:
        ### Create the untagged pipe before the instance is reserved for the team.
        reserved = instance.env['MRSM_SQL_TEAM']
        definition = json.loads(reserved)
        del definition['teams']
        instance.env['MRSM_SQL_TEAM'] = json.dumps(definition)
        prior = instance.python("""
import meerschaum as mrsm
pipe = mrsm.Pipe('sql:team', 'legacy', instance='sql:team', columns={'id': 'id'})
assert pipe.sync([{'id': 7}])[0]
""")
        assert prior.returncode == 0, prior.stderr[-2500:]
        instance.env['MRSM_SQL_TEAM'] = reserved
        instance.start()
        seed = instance.python("""
import meerschaum as mrsm
from meerschaum.core import User, Token
conn = mrsm.get_connector('sql:w7console')
assert mrsm.get_connector('valkey:session').test_connection()
user = User('member', 'member-password', instance=conn, attributes={'role': 'viewer'})
assert conn.register_user(user)[0]
team = mrsm.Team('Readers', instance=conn, members=['member'],
                 attributes={'isolated': True, 'instance': 'sql:team'})
assert team.register()[0]
pipe = mrsm.Pipe('sql:team', 'sample', instance='sql:team',
                 parameters={'access': {'owner': 'member', 'team': 'Readers'}}, columns={'id': 'id'})
assert pipe.sync([{'id': 1, 'value': 42}])[0]
assert Token('seeded', user=user, instance=conn, scopes=['pipes:read']).register()[0]
""")
        assert seed.returncode == 0, seed.stderr[-2500:]
        page = context.new_page()
        tab = Tab(page, instance.base_url)
        tab.login('member', 'member-password', path='/dash/tokens')
        page.get_by_text('seeded', exact=True).first.wait_for()
        page.click('#tokens-create-button')
        page.fill('#tokens-name-input', 'read-one')
        page.uncheck('#tokens-toggle-scopes-switch')
        page.click('#tokens-deselect-scopes-button')
        page.locator('#tokens-scopes-checklist').get_by_text('pipes:read', exact=True).click()
        page.click('#tokens-register-button')
        table = page.locator('#tokens-register-table')
        key = table.locator('tr').filter(has_text='API Key').locator('pre').inner_text()
        token_id = table.locator('#token-id-pre').inner_text()
        assert table.locator('tr').filter(has_text='Scopes').locator('td').last.inner_text() == 'pipes:read'
        check = instance.python(f"""
import meerschaum as mrsm
conn = mrsm.get_connector('sql:w7console')
token = conn.get_token({token_id!r})
assert token.user.username == 'member' and token.scopes == ['pipes:read']
""")
        assert check.returncode == 0, check.stderr[-2500:]
        path = '/pipes/sql:team/sample/None/docs?instance_keys=sql:team'
        assert request(instance, path, key) == (200, [{'id': 1, 'value': 42}])
        assert request(instance, '/cache-probe', login(instance)) == (200, True)
        assert request(instance, '/pipes/sql:team/legacy/None/docs?instance_keys=sql:team', key) == (200, [{'id': 7}])
        page.click('#tokens-register-copy-button')
        page.click('#tokens-close-register-output-modal-button')
        row = page.locator('tr').filter(has_text='read-one')
        row.get_by_title('Manage token').click()
        page.locator('.popover').get_by_text('Invalidate', exact=True).click()
        page.locator('.modal.show').get_by_role('button', name='Invalidate', exact=True).click()
        page.wait_for_selector('.modal.show', state='detached')
        page.reload()
        page.get_by_text('read-one', exact=True).first.wait_for()
        assert request(instance, path, key)[0] == 401
        admin = browser.new_context()
        try:
            admin_tab = Tab(admin.new_page(), instance.base_url)
            admin_tab.login(USERNAME, PASSWORD, path='/dash/pipes')
            select = admin_tab.page.locator('#pages-team-select')
            assert select.locator('option:checked').inner_text() == 'All teams (Admin)'
            with admin_tab.page.expect_navigation():
                select.select_option(label='Everyone')
            admin_tab.page.reload()
            assert select.locator('option:checked').inner_text() == 'Everyone'
        finally:
            admin.close()
    finally:
        context.close()
        instance.stop()
