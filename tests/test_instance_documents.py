"""Metadata listings and custom instance document reads work with either DataFrame library."""

import uuid

import pytest

import meerschaum as mrsm
from meerschaum.connectors.instance import _pipes, _plugins, _tokens
from tests._worker_sqlite import worker_sqlite
from tests.test_tokens import _user_and_token


class FrameInstance:
    """A custom frame-reading instance backed by the test's SQLite metadata tables."""

    IS_INSTANCE = True
    get_pipe_docs = _pipes.get_pipe_docs
    get_plugins = _plugins.get_plugins
    get_tokens = _tokens.get_tokens

    def __init__(self, backend, library):
        self.backend = backend
        self.library = library

    def __str__(self):
        return str(self.backend)

    def __getattr__(self, name):
        return getattr(self.backend, name)

    def get_pipe_data(self, pipe, **kwargs):
        return pipe.backend.get_data(as_polars=self.library == 'polars', **kwargs)

    def get_tokens_pipe(self):
        return DocumentPipe(self, self.backend.get_tokens_pipe())

    def get_plugins_pipe(self):
        return DocumentPipe(self, self.backend.get_plugins_pipe())


class DocumentPipe:
    """Use the instance's generic document fallback rather than its backend's docs shortcut."""

    def __init__(self, instance, backend):
        self.instance = instance
        self.backend = backend

    def get_docs(self, **kwargs):
        return self.instance.get_pipe_docs(self, **kwargs)


@pytest.mark.parametrize('library', ['polars', 'pandas'])
def test_metadata_listings_accept_sqlite_and_custom_frame_instances(monkeypatch, library):
    monkeypatch.setenv('MRSM_DATAFRAME', library)
    conn = worker_sqlite('metadata_docs_' + uuid.uuid4().hex[:8])
    user, token = _user_and_token(conn, ['pipes:read'])
    name = 'docs_plugin_' + uuid.uuid4().hex[:8]
    plugin = mrsm.Plugin(name, version='1.2.3', user_id=conn.get_user_id(user))
    assert conn.register_plugin(plugin)[0]
    try:
        # Exercise the generic plugin helper on a real SQL instance, as well as the native path.
        assert conn.get_plugins(search_term=name) == [name]
        assert _plugins.get_plugins(conn, search_term=name) == [name]
        assert [str(item.id) for item in conn.get_tokens(user=user)] == [str(token.id)]

        custom = FrameInstance(conn, library)
        assert custom.get_plugins(user_id=conn.get_user_id(user), search_term=name) == [name]
        listed = custom.get_tokens(user=user, ids=[token.id])
        assert len(listed) == 1
        assert str(listed[0].id) == str(token.id)
        assert listed[0].scopes == ['pipes:read']
        assert listed[0].creation is not None and listed[0].expiration is not None
        assert custom.get_plugins(search_term='absent_plugin') == []
        assert custom.get_tokens(user=user, ids=[uuid.uuid4()]) == []
    finally:
        conn.delete_plugin(plugin)
        token.delete()
        conn.delete_user(user)
