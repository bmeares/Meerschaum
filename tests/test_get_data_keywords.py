#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`Pipe.get_data(**kw)` warns once per call about keywords nothing reads, with a did-you-mean
(rc15, Q11): `start` used to be ignored silently and every row came back. Keywords the SQL
instance reads, the `Pipe.sync()` options `copy_to()` forwards, and the keywords a custom
instance connector's `get_pipe_data()` names (or any, with `**kwargs`) never warn. Nothing raises.
Also `Pipe(hypercore=...)` (B7), which was documented but not accepted.
"""

import os
import re
import uuid
import warnings

import pytest
import meerschaum as mrsm
from meerschaum.core.Pipe._keywords import (
    warn_unknown_backtrack_keywords,
    warn_unknown_get_data_keywords,
)

from tests._worker_sqlite import worker_sqlite

_ANSI = re.compile(r'\x1b\[[0-9;]*m')


def _keyword_warnings(caught):
    """The recorded unknown-keyword warnings (other libraries may warn too)."""
    return [
        warning
        for warning in caught
        if 'unknown keyword' in ' '.join(_ANSI.sub('', str(warning.message)).split())
    ]


def _messages(caught):
    """The unknown-keyword warning texts without ANSI styling or line wrapping."""
    return [
        ' '.join(_ANSI.sub('', str(warning.message)).split())
        for warning in _keyword_warnings(caught)
    ]


@pytest.fixture
def pipe():
    conn = worker_sqlite('rc15_get_data_keywords')
    pipe = mrsm.Pipe(
        'rc15', 'kw_' + uuid.uuid4().hex[:8], instance=conn,
        columns={'datetime': 'ts', 'id': 'id'},
    )
    success, message = pipe.sync([
        {'ts': '2024-01-01', 'id': 1},
        {'ts': '2024-01-02', 'id': 2},
    ])
    assert success, message
    yield pipe
    pipe.drop()
    pipe.delete()


def test_an_unknown_keyword_warns_with_a_suggestion_and_is_ignored(pipe):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = pipe.get_data(start='2024-01-02')
    ### Ignored, as before: every row comes back.
    assert len(df) == 2
    messages = _messages(caught)
    assert len(messages) == 1, messages
    assert "get_data() ignored the unknown keyword argument 'start' (did you mean 'begin'?)" in messages[0]
    assert str(pipe) in messages[0]
    ### The warning points at the caller's line, not at Meerschaum's internals.
    assert os.path.basename(_keyword_warnings(caught)[0].filename) == os.path.basename(__file__)


def test_several_unknown_keywords_warn_once_and_mention_params(pipe):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        pipe.get_data(where={'id': 1}, begn='2024-01-01', zzz=1)
    messages = _messages(caught)
    assert len(messages) == 1, messages
    message = messages[0]
    assert "'where' (did you mean 'params'?)" in message
    assert "'begn' (did you mean 'begin'?)" in message
    assert "'zzz'." in message or "and 'zzz'" in message
    assert 'params={' in message

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        docs = pipe.get_docs(filters={'id': 1})
    assert len(docs) == 2
    messages = _messages(caught)
    assert len(messages) == 1 and ".get_docs() ignored" in messages[0], messages
    assert "'filters' (did you mean 'params'?)" in messages[0]
    assert os.path.basename(_keyword_warnings(caught)[0].filename) == os.path.basename(__file__)


def test_known_keywords_do_not_warn(pipe):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        pipe.get_data(begin='2024-01-01', end='2024-01-03', params={'id': [1, 2]}, limit=5)
        pipe.get_data(begin_add_minutes=-5, end_add_minutes=5, skip_existing_cols_check=True)
        ### The `Pipe.sync()` options `copy_to()` passes through, and v4's SQL read options.
        pipe.get_data(workers=2, chunksize=100, check_existing=True, sort_datetimes=True)
        pipe.get_doc(order='desc')
        pipe.get_value('id', params={'id': 2})
    assert _messages(caught) == []


class _Fake:
    def __init__(self, connector):
        self.instance_connector = connector

    def __str__(self):
        return "Pipe('fake', 'keywords')"


def test_custom_instance_connectors_may_read_their_own_keywords():
    class Named:
        type = 'rc15named'

        def get_pipe_data(self, pipe, select_columns=None, rc15_marker=None, debug=False):
            return None

    class AnyKeyword:
        type = 'rc15any'

        def get_pipe_data(self, pipe, **kwargs):
            return None

    class Native:
        type = 'sql'

        def get_pipe_data(self, pipe, **kwargs):
            return None

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        warn_unknown_get_data_keywords(_Fake(Named()), {'rc15_marker': 'x'})
        warn_unknown_get_data_keywords(_Fake(AnyKeyword()), {'anything': 1, 'start': 2})
    assert _messages(caught) == []

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        warn_unknown_get_data_keywords(_Fake(Named()), {'rc15_marker': 'x', 'stop': 1})
        ### The Rust core serves `sql:` itself: a connector's own `**kwargs` does not count.
        warn_unknown_get_data_keywords(_Fake(Native()), {'columns': ['a']})
    messages = _messages(caught)
    assert len(messages) == 2, messages
    assert "'stop' (did you mean 'end'?)" in messages[0] and 'rc15_marker' not in messages[0]
    assert "'columns' (did you mean 'select_columns'?)" in messages[1]


def test_get_backtrack_data_warns_about_unknown_keywords_too(pipe):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        everything = pipe.get_backtrack_data(backtrack_minutes=10_000)
        ignored = pipe.get_backtrack_data(minutes=10_000, start='2024-01-02')
    ### Ignored, as before: the same rows come back.
    assert len(ignored) == len(everything) == 2
    messages = _messages(caught)
    assert len(messages) == 1, messages
    assert (
        ".get_backtrack_data() ignored the unknown keyword arguments "
        "'minutes' (did you mean 'backtrack_minutes'?) and 'start' (did you mean 'begin'?)."
    ) in messages[0]
    assert os.path.basename(_keyword_warnings(caught)[0].filename) == os.path.basename(__file__)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        ### The `get_data()` options it reads from `**kw`, and the SQL instance's own.
        docs = pipe.get_backtrack_data(
            backtrack_minutes=10_000, end='2024-01-03', select_columns=['id'], order='asc',
            as_docs=True, begin_add_minutes=0, omit_columns=None, add_missing_columns=False,
        )
        pipe.get_backtrack_data(as_polars=False, workers=2, silent=True)
    assert [doc['id'] for doc in docs] == [1, 2]
    assert _messages(caught) == []

    ### A suggestion is only made for a parameter `get_backtrack_data()` has.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        pipe.get_backtrack_data(iterator=True)
    messages = _messages(caught)
    assert len(messages) == 1 and "'iterator'." in messages[0], messages
    assert 'as_iterator' not in messages[0]


def test_backtrack_reads_the_custom_connector_method_the_worker_calls():
    class OwnBacktrack:
        type = 'rc15own'

        def get_backtrack_data(self, pipe, begin=None, backtrack_minutes=None, rc15_mark=None):
            return None

        def get_pipe_data(self, pipe, rc15_other=None):
            return None

    class DataOnly:
        type = 'rc15data'

        def get_pipe_data(self, pipe, rc15_other=None):
            return None

    class AnyKeyword:
        type = 'rc15anybacktrack'

        def get_backtrack_data(self, pipe, **kwargs):
            return None

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        warn_unknown_backtrack_keywords(_Fake(OwnBacktrack()), {'rc15_mark': 1})
        warn_unknown_backtrack_keywords(_Fake(DataOnly()), {'rc15_other': 1})
        warn_unknown_backtrack_keywords(_Fake(AnyKeyword()), {'anything': 1})
    assert _messages(caught) == []

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        ### The worker calls the connector's own `get_backtrack_data()`, not `get_pipe_data()`.
        warn_unknown_backtrack_keywords(_Fake(OwnBacktrack()), {'rc15_other': 1})
    messages = _messages(caught)
    assert len(messages) == 1, messages
    assert "Pipe('fake', 'keywords').get_backtrack_data() ignored" in messages[0]
    assert "'rc15_other'" in messages[0]


def test_the_check_never_raises():
    class Broken:
        @property
        def instance_connector(self):
            raise RuntimeError('cannot parse')

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        warn_unknown_get_data_keywords(Broken(), {'start': 1})
        warn_unknown_get_data_keywords(object(), {})
        warn_unknown_backtrack_keywords(Broken(), {'start': 1})
        warn_unknown_backtrack_keywords(object(), {})
    assert _messages(caught) == []

    ### Even when warnings are errors, a failing check stays quiet.
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        warn_unknown_get_data_keywords(Broken(), {'start': 1})
        warn_unknown_backtrack_keywords(Broken(), {'start': 1})


def test_warning_filters_can_make_the_warning_an_error(pipe):
    ### `-W error` (or `simplefilter('error')`) raises it like any other warning, before the read.
    def text(raised):
        return ' '.join(_ANSI.sub('', str(raised.value)).split())

    with warnings.catch_warnings():
        warnings.simplefilter('error', UserWarning)
        with pytest.raises(UserWarning) as raised:
            pipe.get_data(start='2024-01-02')
        assert "get_data() ignored the unknown keyword argument 'start'" in text(raised)
        with pytest.raises(UserWarning) as raised:
            pipe.get_backtrack_data(minutes=10)
        assert "get_backtrack_data() ignored the unknown keyword argument 'minutes'" in text(raised)
        assert len(pipe.get_data(begin='2024-01-02')) == 1


def test_hypercore_is_a_constructor_argument():
    def build(**kwargs):
        return mrsm.Pipe('a', 'b', instance='sql:memory', temporary=True, **kwargs)

    assert build(hypercore=False).parameters['hypercore'] is False
    assert build(hypercore=True).parameters['hypercore'] is True
    assert 'hypercore' not in build().parameters
    assert build(parameters={'hypercore': False}).parameters['hypercore'] is False
