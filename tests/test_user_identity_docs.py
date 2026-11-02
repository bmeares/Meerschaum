"""
Identity reads (a user ID's username, through the instance connector's users pipe) read
documents, never a DataFrame: an API host installed without Polars or pandas still resolves
whom a credential belongs to (its owner-capped plugin routes answered `503` instead).
"""

import meerschaum as mrsm
from meerschaum.core import User


def test_identity_reads_need_no_dataframe_library(tmp_path, monkeypatch):
    instance = mrsm.get_connector(
        'sql', 'identity_docs', uri=f"sqlite:///{tmp_path / 'identity.db'}",
    )
    assert instance.register_user(User('docs-owner', 'probe-password', instance=instance))[0]
    user_id = instance.get_user_id(User('docs-owner', instance=instance))
    assert user_id is not None

    pipe_class = type(instance.get_users_pipe())
    real_get_data = pipe_class.get_data

    def documents_only(self, *args, **kwargs):
        if not kwargs.get('as_docs'):
            raise ImportError('No DataFrame library is installed here (simulated).')
        return real_get_data(self, *args, **kwargs)

    monkeypatch.setattr(pipe_class, 'get_data', documents_only)
    assert instance.get_username(user_id) == 'docs-owner'
    # Exact matches only: an unknown ID and no ID name nobody.
    unknown = int(user_id) + 1000 if str(user_id).isdigit() else '00000000-0000-0000-0000-000000000000'
    assert instance.get_username(unknown) is None
    assert instance.get_username(None) is None
