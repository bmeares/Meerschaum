"""Safe public caller projection for an exact private extension request.

This is not authentication and never accepts a client header, bearer, cookie or module name.
The native Operation admission must already have selected and checked the ASGI scope value.
The Main API continues to authorize each actual data callback independently.
"""

from __future__ import annotations

SCOPE_KEY = 'mrsm.extension.caller'


def extension_principal(request, session_only=False):
    """Return (handled, public principal, scopes) inside the exact native boundary only."""
    import json
    import os
    if os.environ.get('MRSM_API_EXECUTION') != '1':
        return False, None, []
    scope = getattr(request, 'scope', None)
    if not isinstance(scope, dict) or SCOPE_KEY not in scope:
        return False, None, []
    from meerschaum.worker._plugin_invoke import _require_execution
    _require_execution()
    caller = scope[SCOPE_KEY]
    if not isinstance(caller, dict) or set(caller) != {'principal', 'scopes', 'console_session'}:
        raise PermissionError('Invalid public extension caller context.')
    if len(json.dumps(caller, ensure_ascii=False).encode('utf-8')) > 8192:
        raise PermissionError('The public extension caller exceeds its bound.')
    principal = caller['principal']
    scopes = caller['scopes']
    if not isinstance(principal, dict) or not isinstance(scopes, list) or not all(isinstance(scope, str) for scope in scopes):
        raise PermissionError('Invalid public extension caller projection.')
    if not isinstance(caller['console_session'], bool) or len(scopes) > 128 or len(scopes) != len(set(scopes)):
        raise PermissionError('Invalid public extension scope projection.')
    from meerschaum.core.access import ALL_PERMISSIONS
    if any(scope not in ALL_PERMISSIONS for scope in scopes):
        raise PermissionError('Unknown public extension permission.')
    kind = principal.get('kind')
    if kind == 'token':
        if set(principal) != {'kind', 'token_id', 'label', 'user_id', 'expiration'} or caller['console_session']:
            raise PermissionError('Invalid public extension token projection.')
        if session_only:
            return True, None, list(scopes)
        import uuid
        from datetime import datetime
        from meerschaum.core import Token
        admitted_scopes = tuple(scopes)

        class ExtensionToken(Token):
            def get_scopes(self, refresh=False, debug=False):
                return list(admitted_scopes)

            @property
            def permissions(self):
                return list(admitted_scopes)

        token = ExtensionToken(
            id=uuid.UUID(principal['token_id']), label=principal['label'],
            user_id=principal['user_id'], scopes=list(scopes),
            expiration=datetime.fromisoformat(principal['expiration'].replace('Z', '+00:00')) if principal['expiration'] else None,
            secret=None, secret_hash=None,
        )
        return True, token, list(scopes)
    if kind != 'user' or set(principal) != {'kind', 'username', 'user_id', 'user_type'}:
        raise PermissionError('Authority-bearing fields cannot enter the public caller context.')
    if session_only and caller['console_session'] is not True:
        return True, None, list(scopes)
    username, user_id, user_type = principal['username'], principal['user_id'], principal['user_type']
    if not isinstance(username, str) or not username or len(username) > 256:
        raise PermissionError('Invalid public extension username.')
    if isinstance(user_id, bool) or not isinstance(user_id, (str, int)) or (user_type is not None and not isinstance(user_type, str)):
        raise PermissionError('Invalid registered extension subject.')
    from meerschaum.core.User import User
    admitted_scopes = tuple(scopes)

    class ExtensionUser(User):
        """Request-local public data; all transport authorization stays in Main."""
        def get_scopes(self, refresh=False, debug=False):
            # Refreshing an ordinary user's global role must not add rights removed by
            # the original token, execution parent or Team boundary of this request.
            return list(admitted_scopes)

        def get_attributes(self, refresh=False, debug=False):
            return {'scopes': list(admitted_scopes)}

    user = ExtensionUser(
        username, password=None, user_id=user_id, type=user_type,
        attributes={'scopes': list(admitted_scopes)},
    )
    return True, user, list(scopes)


def extension_session_user(request):
    handled, user, _ = extension_principal(request, session_only=True)
    return handled, user


def extension_granted_scopes(request, user_or_token):
    handled, projected, scopes = extension_principal(request)
    if not handled:
        return False, []
    from meerschaum.core import User, Token
    if isinstance(projected, User):
        same = isinstance(user_or_token, User) and projected.username == user_or_token.username and projected.user_id == user_or_token.user_id
    else:
        same = isinstance(projected, Token) and isinstance(user_or_token, Token) and str(projected.id) == str(user_or_token.id)
    if not same:
        raise PermissionError('The private dependency principal differs from its admitted caller.')
    return True, scopes
