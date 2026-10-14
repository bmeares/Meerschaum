"""
Wave 9 (task U): under `MRSM_CORE=rust`, parameters holding a cross-pipe `{{ Pipe() }}`
template are memoized (PARITY.md row 39) instead of re-resolving on every access.

The memo is keyed on the pipe's `_core.Pipe` generation and on this process's metadata writes
(`register`, `edit`, `delete`, and the Rust instance's writes via `_core.pipe_metadata_epoch()`),
and it expires after `pipes:attributes:local_cache_timeout_seconds`.
"""

import uuid

import pytest

import meerschaum as mrsm
from meerschaum._core import _gate

pytestmark = pytest.mark.skipif(_gate.mode() != _gate.RUST, reason="rust-mode façade")


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', 'w9umemo_' + uuid.uuid4().hex,
        flavor='sqlite', database=str(tmp_path / 'w9umemo.db'),
    )


def _resolutions(pipe) -> float:
    """The time of the last resolution of `pipe`'s cross-pipe templates."""
    return pipe.__dict__.get('_resolved_parameters_ts')


def _child(conn, note_attr: str = "columns['datetime']"):
    return mrsm.Pipe(
        'plugin:p', 'child', instance=conn,
        parameters={
            'note': (
                "{{ Pipe('plugin:p', 'par', instance='" + str(conn) + "')."
                + note_attr + " }}"
            ),
        },
    )


def test_cross_pipe_template_is_memoized_between_writes(conn):
    parent = mrsm.Pipe('plugin:p', 'par', instance=conn, columns={'datetime': 'dt'})
    assert parent.register()[0]
    child = _child(conn)

    assert child.get_parameters()['note'] == 'dt'
    first = _resolutions(child)
    assert first is not None
    ### Repeated accesses (every `.columns`, `.dtypes`, `.target`, ...) reuse the resolution.
    for _ in range(5):
        assert child.parameters['note'] == 'dt'
        assert child.get_parameters()['note'] == 'dt'
    assert _resolutions(child) == first

    ### A metadata write re-resolves on the next access.
    parent.columns = {'datetime': 'ts'}
    assert parent.edit()[0]
    assert child.get_parameters()['note'] == 'ts'
    second = _resolutions(child)
    assert second is not None and second != first

    ### A Rust-side metadata write (any `_core.SqlConnector` edit) also invalidates it.
    from meerschaum import _core
    epoch = _core.pipe_metadata_epoch()
    other = mrsm.Pipe('plugin:p', 'other', instance=conn, columns={'datetime': 'x'})
    assert other.register()[0]
    assert _core.pipe_metadata_epoch() > epoch
    assert child.get_parameters()['note'] == 'ts'
    assert _resolutions(child) != second

    ### The raw parameters never take the resolved text.
    assert '{{' in child._attributes['parameters']['note']
    other.delete()
    parent.delete()


def test_cross_pipe_template_memo_expires(conn):
    parent = mrsm.Pipe('plugin:p', 'par', instance=conn, columns={'datetime': 'dt'})
    assert parent.register()[0]
    child = _child(conn)
    assert child.get_parameters()['note'] == 'dt'
    first = _resolutions(child)

    ### Another process edits the parent: this process has no write to key on.
    timeout = mrsm.get_config('pipes', 'attributes', 'local_cache_timeout_seconds')
    child.__dict__['_resolved_parameters_ts'] = first - timeout - 1
    assert child.get_parameters()['note'] == 'dt'
    assert _resolutions(child) > first - timeout - 1
    parent.delete()


def test_own_mutation_invalidates_cross_pipe_memo(conn):
    parent = mrsm.Pipe(
        'plugin:p', 'par', instance=conn, columns={'datetime': 'dt', 'id': 'pid'},
    )
    assert parent.register()[0]
    child = _child(conn)
    assert child.get_parameters()['note'] == 'dt'
    child.parameters['note'] = (
        "{{ Pipe('plugin:p', 'par', instance='" + str(conn) + "').columns['id'] }}"
    )
    assert child.get_parameters()['note'] == 'pid'
    parent.delete()
