"""An explicitly protected destination stays protected when the engine rebuilds it."""

import uuid

import meerschaum as mrsm
from meerschaum.connectors import SQLConnector


def test_pipeline_preserves_an_ad_hoc_destinations_write_protection(tmp_path):
    label = 'protected_pipeline_' + uuid.uuid4().hex[:10]
    database = str(tmp_path / 'destination.db')
    writable = mrsm.get_connector('sql', label, flavor='sqlite', database=database)
    source = mrsm.get_connector('sql', label + '_source', flavor='sqlite',
                                database=str(tmp_path / 'source.db'))
    parameters = {'fetch': {'definition': 'SELECT 1 AS id'}}
    original = mrsm.Pipe(str(source), 'protected', instance=writable,
                        columns={'primary': 'id'}, parameters=parameters)
    assert original.register()[0]
    protected = SQLConnector(label=label, flavor='sqlite', database=database, protected=True)
    member = mrsm.Pipe(str(source), 'protected', instance=protected,
                      columns={'primary': 'id'}, parameters=parameters)
    try:
        try:
            result = mrsm.Pipeline(member, instance=protected).sync(workers=1)
        except RuntimeError as error:
            message = str(error)
        else:
            assert not result.success, result.message
            message = '\n'.join(outcome.get('result', {}).get('message', '')
                                for outcome in result.outcomes)
        assert 'on protected instance' in message.lower(), message
        assert not original.exists() or original.get_rowcount() == 0
    finally:
        mrsm._core.flush_sync_statuses(timeout=5.0)
        original.delete()
