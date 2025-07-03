#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the high level tokens instance methods.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.core import Token
from meerschaum.core.User import hash_password


def get_tokens_pipe(self) -> mrsm.Pipe:
    """
    Return the internal pipe for tokens management.
    """
    users_pipe = self.get_users_pipe()
    user_id_dtype = users_pipe.dtypes.get('user_id', 'uuid')

    return mrsm.Pipe(
        'mrsm', 'tokens',
        instance=self,
        target='mrsm_tokens',
        temporary=True,
        static=True,
        autotime=True,
        null_indices=True,
        columns={
            'primary': 'token_id',
            'user_id': 'user_id',
        },
        dtypes={
            'created_at': 'datetime64[ns, UTC]',
            'expires_at': 'datetime64[ns, UTC]',
            'is_valid': 'bool',
            'token_id': 'uuid',
            'token_label': 'string',
            'user_id': user_id_dtype,
            'scopes': 'json',
            'token_hash': 'string',
        },
    )


def register_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Register the new token to the tokens table.
    """
    tokens_pipe = self.get_tokens_pipe()
    user_id = self.get_user_id(token.user) if token.user is not None else None
    doc = {
        'token_id': uuid.uuid4(),
        'user_id': user_id,
        'created_at': datetime.now(timezone.utc),
        'expires_at': token.expiration,
        'token_label': token.label,
        'is_valid': token.is_valid,
        'scopes': list(token.scopes) if token.scopes else None,
        'token_hash': hash_password(token.secret),
    }
    sync_success, sync_msg = tokens_pipe.sync([doc], check_existing=False, debug=debug)
    if not sync_success:
        return False, f"Failed to register token:\n{sync_msg}"
    return True, "Success"
