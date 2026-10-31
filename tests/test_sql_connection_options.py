"""Connection settings survive both raw SQL and the native engine's source registry."""

import os
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest
import meerschaum as mrsm
from tests.connectors import conns, get_flavors


FLAVORS = [f for f in get_flavors() if f in ('timescaledb', 'timescaledb_ha', 'postgis')]


@pytest.mark.parametrize('flavor', FLAVORS)
@pytest.mark.parametrize('form', ['mapping', 'query_string', 'uri'])
def test_postgres_connection_settings_reach_sql_and_pipeline_sources(flavor, form, tmp_path):
    if os.environ.get('MRSM_TEST_ISOLATED') != '1':
        from tests._binary import skip_or_fail
        skip_or_fail('Live connection checks require an isolated test database.')
    base = conns[flavor]
    label = 'connection_options_' + uuid.uuid4().hex[:12]
    settings = '-c default_transaction_read_only=on -c statement_timeout=1500'
    if form == 'uri':
        parts = urlsplit(base.URI)
        query = dict(parse_qsl(parts.query))
        query.update(options=settings, application_name=label)
        uri = urlunsplit((*parts[:3], urlencode(query), parts.fragment))
        reader = mrsm.get_connector('sql', label, uri=uri, protected=True)
    else:
        attrs = base.parse_uri(base.URI)
        attrs = {k: v for k, v in attrs.items()
                 if k in ('host', 'port', 'username', 'password', 'database', 'flavor')}
        options = {'options': settings}
        reader = mrsm.get_connector(
            'sql', label, **attrs, options=(options if form == 'mapping' else urlencode(options)),
            application_name=label, pool_size=1, max_overflow=0, protected=True,
        )
    assert reader.value('SHOW default_transaction_read_only') == 'on'
    assert reader.value('SHOW statement_timeout') == '1500ms'
    assert reader.value('SHOW application_name') == label
    target = mrsm.get_connector('sql', label + '_target', flavor='sqlite',
                                database=str(tmp_path / 'options.db'))
    pipe = mrsm.Pipe(str(reader), 'settings', instance=target, columns={'primary': 'id'},
                    parameters={'fetch': {'definition': """
                        SELECT 1 AS id,
                            current_setting('default_transaction_read_only') AS read_only,
                            current_setting('statement_timeout') AS statement_timeout,
                            current_setting('application_name') AS application_name
                    """}})
    try:
        assert pipe.register()[0]
        result = mrsm.Pipeline(pipe, instance=target).sync(workers=1)
        assert result.success, result.message
        row = pipe.get_data(as_docs=True)[0]
        assert row['read_only'] == 'on'
        assert row['statement_timeout'] == '1500ms'
        assert row['application_name'] == label
    finally:
        mrsm._core.flush_sync_statuses(timeout=5.0)
        pipe.delete()
