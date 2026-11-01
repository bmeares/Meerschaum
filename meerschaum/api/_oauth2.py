#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define JWT authorization here.
"""

import os
import base64
import functools
import inspect
from typing import Dict, List, Optional, Union

from meerschaum.api import endpoints, CHECK_UPDATE, _IMPORT_KW, no_auth, debug
from meerschaum.api._tokens import optional_token, get_token_from_authorization
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.packages import attempt_import
from meerschaum.core import User, Token

fastapi, starlette = attempt_import(
    'fastapi', 'starlette', lazy=False, check_update=CHECK_UPDATE, **_IMPORT_KW,
)
fastapi_responses = attempt_import(
    'fastapi.responses', lazy=False, check_update=CHECK_UPDATE, **_IMPORT_KW,
)
jwt = attempt_import(
    'jwt', lazy=False, check_update=CHECK_UPDATE, **_IMPORT_KW,
)
if jwt is None:
    raise ImportError(
        "`meerschaum.api` needs `PyJWT`: install the `api` extra "
        "(`pip install \"meerschaum[api]\"`)."
    )
### v4 validated with `python-jose`; PyJWT validates the same HS256 tokens.
from fastapi import Depends, HTTPException, Request
from starlette import status


class CustomOAuth2PasswordRequestForm:
    def __init__(
        self,
        grant_type: str = fastapi.Form(None, pattern="password|client_credentials"),
        username: Optional[str] = fastapi.Form(None),
        password: Optional[str] = fastapi.Form(None),
        ### An omitted `scope` means the server decides (RFC 6749 § 3.3):
        ### the login route then grants the principal's allowed scopes.
        scope: str = fastapi.Form(''),
        client_id: Optional[str] = fastapi.Form(None),
        client_secret: Optional[str] = fastapi.Form(None),
        authorization: Optional[str] = fastapi.Header(None),
    ):
        self.grant_type = grant_type
        self.username = username
        self.password = password
        self.scope = scope
        self.client_id = client_id
        self.client_secret = client_secret

        if (
            not username
            and not password
            and not self.client_id
            and not self.client_secret
            and authorization
        ):
            try:
                scheme, credentials = authorization.split()
                if credentials.startswith('mrsm-key:'):
                    credentials = credentials[len('mrsm-key:'):]
                if scheme.lower() in ('basic', 'bearer'):
                    decoded_credentials = base64.b64decode(credentials).decode('utf-8')
                    _client_id, _client_secret = decoded_credentials.split(':', 1)
                    self.client_id = _client_id
                    self.client_secret = _client_secret
                    self.grant_type = 'client_credentials'
            except ValueError:
                pass


async def optional_user(request: Request) -> Optional[User]:
    """
    FastAPI dependency that returns a User if logged in, otherwise None.
    """
    if no_auth:
        return None
    try:
        return await manager(request)
    except HTTPException:
        return None


async def load_user_or_token(
    request: Request,
    users: bool = True,
    tokens: bool = True,
) -> Union[User, Token, None]:
    """
    Load the current user or token.
    """
    authorization = request.headers.get('authorization', request.headers.get('Authorization', None))
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
        )

    authorization = authorization.replace('Basic ', '').replace('Bearer ', '')
    if not authorization.startswith('mrsm-key:'):
        if not users:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Users not authenticated for this endpoint.",
            )

        return await manager(request)

    if not tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tokens not authenticated for this endpoint.",
        )

    return get_token_from_authorization(authorization)


def get_granted_scopes(
    request: Request,
    user_or_token: Union[User, Token, None],
) -> List[str]:
    """
    Return the scopes an authenticated principal holds: what its credential grants, capped by
    its owner's current permissions, by the Rust server's rule
    (`meerschaum.core.access.credential_permissions`).

    API keys are resolved against the instance on every request, so a revoked or edited token
    takes effect immediately. A JWT carries its own scopes; one minted from an API token keeps
    no more than the token holds now. A token whose owner no longer exists is refused (401), and
    a store that cannot be read refuses the request (503), never a wider grant.

    Parameters
    ----------
    request: Request
        The incoming request, for its `Authorization` header.

    user_or_token: Union[User, Token, None]
        The principal resolved by `load_user_or_token()`.

    Returns
    -------
    A list of scope strings. `'*'` holds everything.
    """
    if no_auth:
        return ['*']

    if not user_or_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
            headers={"WWW-Authenticate": "Basic"},
        )

    authorization = request.headers.get(
        'authorization', request.headers.get('Authorization', None)
    )
    if not authorization:
        # This should be caught by `load_user_or_token` but we can be safe.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
        )

    if 'mrsm-key:' in authorization:
        granted = _read_or_unavailable(lambda: user_or_token.get_scopes(refresh=True, debug=debug))
        return _owner_capped(user_or_token, granted)

    scheme, _, token_str = authorization.partition(' ')
    if not token_str or scheme.lower() != 'bearer':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unsupported authentication scheme.",
        )

    try:
        payload = decode_jwt(token_str)
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    claimed = _claimed_scopes(payload.get('scopes', []))
    if isinstance(user_or_token, Token):
        current = _read_or_unavailable(lambda: user_or_token.get_scopes(refresh=True, debug=debug))
        from meerschaum.core import access
        claimed = access.narrow_to_token_scopes(claimed, current or [])
    return _owner_capped(user_or_token, claimed)


def _claimed_scopes(claim) -> List[str]:
    """
    A JWT's `scopes` claim as a list: a string claim (an instance quirk) holds the permissions
    it contains, as on the Rust server.
    """
    if isinstance(claim, str):
        from meerschaum.core import access
        held = [scope for scope in access.ALL_PERMISSIONS if scope in claim]
        if '*' in claim:
            held.append('*')
        return held
    if isinstance(claim, (list, tuple)):
        return [str(scope) for scope in claim]
    return []


def _read_or_unavailable(read):
    """
    Run an instance read; a failure refuses the request (503), never a guess. The reason is
    logged on the server only (a driver's error can name its connection or query).
    """
    try:
        return read()
    except HTTPException:
        raise
    except Exception as e:
        from meerschaum.utils.warnings import warn
        warn(f"Refused a request: the caller's permissions could not be read: {e}", stack=False)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="The caller's permissions cannot be read right now.",
        )


def _owner_capped(user_or_token: Union[User, Token], granted: List[str]) -> List[str]:
    """
    Cap a credential's scopes by its owner's current permissions (the Rust server's
    `access_of`): a user's own, or a token owner's. An unowned token keeps its own scopes.
    """
    from meerschaum.core import access
    from meerschaum.api import get_api_connector
    conn = get_api_connector()
    if isinstance(user_or_token, Token):
        user_id = getattr(user_or_token, '_user_id', None)
        if user_id is None:
            return list(granted or [])
        username = _read_or_unavailable(lambda: conn.get_username(user_id))
        if not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked token.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        user = User(str(username), instance=conn)
    else:
        user = user_or_token
    user_type = _read_or_unavailable(lambda: conn.get_user_type(user))
    attributes = _read_or_unavailable(lambda: conn.get_user_attributes(user))
    return access.credential_permissions(
        user_type,
        attributes if isinstance(attributes, dict) else None,
        list(granted or []),
        access.default_role(),
    )


async def CurrentScopes(request: Request) -> List[str]:
    """
    Dependency which returns the caller's granted scopes without requiring any
    particular one.

    Used by endpoints which multiplex many operations behind a single route (the
    MCP server), where the required scope depends on which operation was
    requested. Every other endpoint should use `ScopedAuth` instead.

    `load_user_or_token` is awaited directly rather than declared as a
    sub-dependency so that the `no_auth` check happens first: it raises a 401 on
    a missing `Authorization` header, which would otherwise fire even when the
    server is running without authentication.

    The resolved principal is stashed on `request.state.user_or_token` so a route
    which needs it (the MCP endpoint, for its permission checks) doesn't have to
    resolve the token a second time.
    """
    request.state.user_or_token = None
    if no_auth:
        return ['*']

    user_or_token = await load_user_or_token(request)
    request.state.user_or_token = user_or_token
    return get_granted_scopes(request, user_or_token)


def ScopedAuth(scopes: List[str], aliases: Optional[Dict[str, str]] = None):
    """
    Dependency factory for authenticating with either a user session or a scoped token.

    Parameters
    ----------
    scopes: List[str]
        The scopes which the caller must hold (a literal `*` holds all of them).

    aliases: Optional[Dict[str, str]], default None
        Map a required scope to a legacy spelling which is accepted in its place
        when the token explicitly holds it. The 403 still names the current scope.
    """
    aliases = aliases or {}
    async def _authenticate(
        request: Request,
    ) -> Union[User, Token, None]:
        ### Checked before resolving a principal: `load_user_or_token` raises a
        ### 401 on a missing `Authorization` header, which would defeat
        ### `--no-auth` entirely if it ran as a sub-dependency.
        if no_auth:
            return None

        user_or_token = await load_user_or_token(request)
        current_scopes = get_granted_scopes(request, user_or_token)

        if '*' in current_scopes:
            return user_or_token

        for scope in scopes:
            legacy_scope = aliases.get(scope, None)
            if scope not in current_scopes and (
                legacy_scope is None or legacy_scope not in current_scopes
            ):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Missing required scope: '{scope}'",
                )

        return user_or_token
    return _authenticate


from meerschaum.api._login import LoginManager


def decode_jwt(token: str) -> dict:
    """
    Validate a JWT signed with the API secret (HS256) and return its claims.
    Raises `jwt.PyJWTError` when the signature, expiry or format is invalid.
    """
    return jwt.decode(token, SECRET, algorithms=['HS256'])


def generate_secret_key() -> bytes:
    """
    Read or generate the secret keyfile.
    """
    import meerschaum.config.paths as paths
    if not paths.API_SECRET_KEY_PATH.exists():
        secret_key = os.urandom(24).hex()
        with open(paths.API_SECRET_KEY_PATH, 'w+', encoding='utf-8') as f:
            f.write(secret_key)
    else:
        with open(paths.API_SECRET_KEY_PATH, 'r', encoding='utf-8') as f:
            secret_key = f.read()

    return secret_key.encode('utf-8')


SECRET = generate_secret_key()
manager = LoginManager(
    SECRET,
    token_url=endpoints['login'],
    scopes=STATIC_CONFIG['tokens']['scopes'],
)

#: The claim that binds a user's JWT (and a console session's `credential`) to the user's
#: current password: a deleted and re-registered username, or a changed password, ends every
#: credential issued before. The Rust server's `meerschaum_server::auth::CREDENTIAL_CLAIM`.
CREDENTIAL_CLAIM: str = 'mrsm_credential'


def credential_fingerprint(password_hash: str) -> str:
    """
    Return the credential fingerprint of a password hash: `HMAC-SHA256(secret,
    "mrsm-credential\\0" + hash)`, its first 16 bytes in unpadded base64url (the Rust server's
    `credential_fingerprint`).
    """
    import hmac
    import hashlib
    digest = hmac.new(
        SECRET,
        b'mrsm-credential\x00' + str(password_hash).encode('utf-8'),
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(digest[:16]).rstrip(b'=').decode('ascii')


def user_credential(username: str) -> Optional[str]:
    """
    Return the current credential fingerprint of `username` (`None`: no such user or no stored
    password). Raises when the instance cannot be read.
    """
    from meerschaum.api import get_api_connector
    conn = get_api_connector()
    password_hash = conn.get_user_password_hash(User(str(username), instance=conn))
    return credential_fingerprint(password_hash) if password_hash else None


def credential_matches(username: str, credential: Optional[str]) -> bool:
    """
    Return whether `credential` is `username`'s current one. A missing credential (issued before
    5.0 bound credentials to their user) never matches. Raises when the instance cannot be read.
    """
    import hmac
    if not credential or not isinstance(credential, str):
        return False
    current = user_credential(username)
    return current is not None and hmac.compare_digest(current, credential)


#: The claim naming the kind of principal a JWT's `sub` is, set by its issuer: `user` (a
#: username, whatever it looks like) or `token` (an API token's ID). A JWT without it is refused,
#: so a username that looks like a token's ID never selects that token. The Rust server's
#: `meerschaum_server::auth::PRINCIPAL_CLAIM`.
PRINCIPAL_CLAIM: str = 'mrsm_principal'
PRINCIPAL_KINDS = ('user', 'token')


def _issuer_claims(data: Dict) -> Dict:
    """
    `manager.claims_for`: the principal kind of a JWT naming a `sub` (`user` unless the caller
    says `token`, as v4's `create_access_token(data={'sub': username})` made user tokens), and a
    user's current credential.
    """
    sub = data.get('sub', None)
    if not sub:
        return {}
    kind = data.get(PRINCIPAL_CLAIM, None) or 'user'
    if kind not in PRINCIPAL_KINDS:
        raise ValueError(f"Invalid principal kind '{kind}' (expected 'user' or 'token').")
    claims = {PRINCIPAL_CLAIM: kind}
    if kind == 'user' and CREDENTIAL_CLAIM not in data:
        credential = user_credential(str(sub))
        if credential:
            claims[CREDENTIAL_CLAIM] = credential
    return claims


def principal_matches(payload: Dict, principal) -> bool:
    """
    Whether `principal` is what a JWT's payload names: the kind its issuer set, the same subject,
    and for a user their current credential. Raises when the instance cannot be read.
    """
    kind = payload.get(PRINCIPAL_CLAIM, None)
    sub = str(payload.get('sub', None))
    if kind == 'user':
        return (
            isinstance(principal, User)
            and principal.username == sub
            and credential_matches(principal.username, payload.get(CREDENTIAL_CLAIM, None))
        )
    if kind == 'token':
        return isinstance(principal, Token) and str(getattr(principal, 'id', None)) == sub
    return False


manager.claims_for = _issuer_claims
manager.payload_check = principal_matches
manager.principal_claim = PRINCIPAL_CLAIM
