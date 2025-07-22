#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the high level tokens instance methods.
"""

from __future__ import annotations

from typing import List, Union, Optional, Dict
import uuid
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.core import Token, User
from meerschaum.core.User import hash_password
from meerschaum._internal.static import STATIC_CONFIG


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
            'datetime': 'creation',
            'primary': 'id',
        },
        indices={
            'unique': 'label',
            'user_id': 'user_id',
        },
        dtypes={
            'id': 'uuid',
            'creation': 'datetime',
            'expiration': 'datetime',
            'is_valid': 'bool',
            'label': 'string',
            'user_id': user_id_dtype,
            'scopes': 'json',
            'secret_hash': 'string',
        },
    )


def register_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Register the new token to the tokens table.
    """
    token_id, token_secret = token.generate_credentials()
    tokens_pipe = self.get_tokens_pipe()
    user_id = self.get_user_id(token.user) if token.user is not None else None
    if user_id is None:
        raise ValueError("Cannot register a token without a user.")

    doc = {
        'id': token_id,
        'user_id': user_id,
        'creation': datetime.now(timezone.utc),
        'expiration': token.expiration,
        'label': token.label,
        'is_valid': token.is_valid,
        'scopes': list(token.scopes) if token.scopes else [],
        'secret_hash': hash_password(
            str(token_secret),
            rounds=STATIC_CONFIG['tokens']['hash_rounds']
        ),
    }
    sync_success, sync_msg = tokens_pipe.sync([doc], check_existing=False, debug=debug)
    if not sync_success:
        return False, f"Failed to register token:\n{sync_msg}"
    return True, "Success"


def edit_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Persist the token's in-memory state to the tokens pipe.
    """
    if not token.id:
        return False, "Token ID is not set."

    if not token.exists(debug=debug):
        return False, f"Token {token.id} does not exist."

    if not token.creation:
        token_model = self.get_token_model(token.id)
        token.creation = token_model.creation

    tokens_pipe = self.get_tokens_pipe()
    doc = {
        'id': token.id,
        'creation': token.creation,
        'expiration': token.expiration,
        'label': token.label,
        'is_valid': token.is_valid,
        'scopes': list(token.scopes) if token.scopes else [],
    }
    sync_success, sync_msg = tokens_pipe.sync([doc], debug=debug)
    if not sync_success:
        return False, f"Failed to edit token '{token.id}':\n{sync_msg}"

    return True, "Success"


def invalidate_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Set `is_valid` to `False` for the given token.
    """
    if not token.id:
        return False, "Token ID is not set."

    if not token.exists(debug=debug):
        return False, f"Token {token.id} does not exist."

    if not token.creation:
        token_model = self.get_token_model(token.id)
        token.creation = token_model.creation

    token.is_valid = False
    tokens_pipe = self.get_tokens_pipe()
    doc = {
        'id': token.id,
        'creation': token.creation,
        'is_valid': False,
    }
    sync_success, sync_msg = tokens_pipe.sync([doc], debug=debug)
    if not sync_success:
        return False, f"Failed to invalidate token '{token.id}':\n{sync_msg}"

    return True, "Success"


def delete_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Delete the given token from the tokens table.
    """
    if not token.id:
        return False, "Token ID is not set."

    if not token.exists(debug=debug):
        return False, f"Token {token.id} does not exist."

    if not token.creation:
        token_model = self.get_token_model(token.id)
        token.creation = token_model.creation

    token.is_valid = False
    tokens_pipe = self.get_tokens_pipe()
    clear_success, clear_msg = tokens_pipe.clear(params={'id': token.id}, debug=debug)
    if not clear_success:
        return False, f"Failed to delete token '{token.id}':\n{clear_msg}"

    return True, "Success"


def get_tokens(
    self,
    user: Optional[User] = None,
    labels: Optional[List[str]] = None,
    ids: Optional[List[uuid.UUID]] = None,
    debug: bool = False,
) -> List[Token]:
    """
    Return a list of `Token` objects.
    """
    tokens_pipe = self.get_tokens_pipe()
    user_id = (
        self.get_user_id(user, debug=debug)
        if user is not None
        else None
    )
    user_type = self.get_user_type(user, debug=debug) if user is not None else None
    params = (
        {
            'user_id': (
                user_id
                if user_type != 'admin'
                else [user_id, None]
            )
        }
        if user_id is not None
        else {}
    )
    if labels:
        params['label'] = labels
    if ids:
        params['id'] = ids
        
    tokens_df = tokens_pipe.get_data(params=params, debug=debug)
    if tokens_df is None:
        return []

    tokens_docs = tokens_df.to_dict(orient='records')
    return [
        Token(
            instance=self,
            **token_doc
        )
        for token_doc in reversed(tokens_docs)
    ]


def get_token(self, token_id: Union[uuid.UUID, str], debug: bool = False) -> Union[Token, None]:
    """
    Return the `Token` from its ID.
    """
    from meerschaum.utils.misc import is_uuid
    if isinstance(token_id, str):
        if is_uuid(token_id):
            token_id = uuid.UUID(token_id)
        else:
            raise ValueError("Invalid token ID.")
    token_model = self.get_token_model(token_id)
    if token_model is None:
        return None
    return Token(**dict(token_model))


def get_token_model(self, token_id: Union[uuid.UUID, Token], debug: bool = False) -> 'Union[TokenModel, None]':
    """
    Return a token's model from the instance.
    """
    from meerschaum.models import TokenModel
    if isinstance(token_id, Token):
        token_id = Token.id
    if not token_id:
        raise ValueError("Invalid token ID.")
    tokens_pipe = self.get_tokens_pipe()
    doc = tokens_pipe.get_doc(
        params={'id': token_id},
        debug=debug,
    )
    if doc is None:
        return None
    return TokenModel(**doc)


def get_token_secret_hash(self, token_id: Union[uuid.UUID, Token], debug: bool = False) -> Union[str, None]:
    """
    Return the secret hash for a given token.
    """
    if isinstance(token_id, Token):
        token_id = token_id.id
    if not token_id:
        raise ValueError("Invalid token ID.")
    tokens_pipe = self.get_tokens_pipe()
    return tokens_pipe.get_value('secret_hash', params={'id': token_id}, debug=debug)


def get_token_user_id(self, token_id: Union[uuid.UUID, Token], debug: bool = False) -> Union[int, str, uuid.UUID, None]:
    """
    Return a token's user_id.
    """
    if isinstance(token_id, Token):
        token_id = token_id.id
    if not token_id:
        raise ValueError("Invalid token ID.")

    tokens_pipe = self.get_tokens_pipe()
    return tokens_pipe.get_value('user_id', params={'id': token_id}, debug=debug)


def get_token_scopes(self, token_id: Union[uuid.UUID, Token], debug: bool = False) -> List[str]:
    """
    Return the scopes for a token.
    """
    if isinstance(token_id, Token):
        token_id = token_id.id
    if not token_id:
        raise ValueError("Invalid token ID.")

    tokens_pipe = self.get_tokens_pipe()
    return tokens_pipe.get_value('scopes', params={'id': token_id}, debug=debug) or []


def token_exists(self, token_id: Union[uuid.UUID, Token], debug: bool = False) -> bool:
    """
    Return `True` if a token exists in the tokens pipe.
    """
    if isinstance(token_id, Token):
        token_id = token_id.id
    if not token_id:
        raise ValueError("Invalid token ID.")

    tokens_pipe = self.get_tokens_pipe()
    return tokens_pipe.get_value('creation', params={'id': token_id}, debug=debug) is not None
