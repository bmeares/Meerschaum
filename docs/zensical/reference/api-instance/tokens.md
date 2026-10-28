# 🪙 Tokens

Meerschaum tokens are long-lived API keys that grant programmatic access to a Meerschaum instance. They are a secure way to authenticate for services like IoT devices or CI/CD pipelines without using your username and password.

Tokens are tied to a specific user and have a defined set of permissions, called **scopes**.

When you create a token, you receive three key pieces of information: a Client ID, a Client Secret, and an API Key.

!!! warning "Write it down!"
    The Client Secret is **only shown once** upon token creation, so be sure to copy it somewhere safe when registering tokens.

- **Client ID:** A public, unique identifier (`UUID`)
- **Client Secret:** A private secret (treat this like a password)
- **API Key:** To be used in the `Authorization` header (contains Client ID and Secret within).

Additionally, tokens have the following attributes:

- **Label:** A human-readable name to help you identify the token
- **Expiration:** An optional timestamp after which the token will become invalid  
  Set the `expiration` to `None` to prevent a token from expiring (use at your own risk!)
- **Scopes:** A list of permissions that define what actions the token can perform
- **User:** The user account that owns the token

---

## ➕ Registering

You can create new tokens via the action `register token`:

```bash
mrsm register token
```

This will prompt you for the owner of the token and print the Client ID, Client Secret and API Key. Registering to an `api` instance will assign the token the connector's configured user.

## 🌐 Web UI

The easiest way to can manage tokens is on the [Web Console](/reference/api-instance/web-console). Open the user menu (your username, at the top right) and choose **Tokens**. From here, you can:

-   Create new tokens with **New token**.
-   Edit, invalidate, or delete existing tokens from the context menu (`⠇`).

When you create a new token, a form prompts you for a label, expiration date, and scopes. The **Grant all scopes, including custom and future ones** switch (the scope `*`) is on by default for an administrator or a holder of `*` (for anyone else the form starts from your own scopes and the switch is off); turn it off to pick built-in scopes from a checklist and to type custom (plugin) scopes. The edit form of a token that holds `*` shows the switch on, and saving it keeps `*`. After registration, you **must** copy the generated credentials, as the secret will not be shown again.

<img src="/assets/screenshots/web-console-tokens.png" alt="Web Console tokens page">

---
## 🐍 Python API

In addition to the CLI and Web UI, you can manage tokens directly on [instance connectors](/reference/connectors/instance-connectors/).

### Creating a Token

To create a token, instantiate a `meerschaum.core.Token` object and call its `register()` method.

??? example "Creating a new token"
    ```python
    import meerschaum as mrsm
    from meerschaum.core import Token, User

    # Tokens must be associated with a user.
    user = User('myuser', instance='sql:main')

    # The token will be managed by the 'sql:main' instance connector.
    token = Token(
        label='my-iot-device',
        user=user,
        instance='sql:main',
    )

    # The register() method generates the credentials and saves the token.
    success, msg = token.register()
    if not success:
        print(msg)
    else:
        print(f"Successfully registered token '{token.label}'")
        print(f"  Client ID: {token.id}")
        print(f"  Client Secret: {token.secret}")
        print(f"  API Key: {token.get_api_key()}")
    ```

### Fetching Tokens

You can retrieve a list of all tokens for a user from an instance connector.

```python
import meerschaum as mrsm

conn = mrsm.get_connector('sql:main')
user = mrsm.core.User('myuser', instance=conn)

# Admins can see all tokens, including those without a user.
# Regular users only see their own tokens.
tokens = conn.get_tokens(user=user)

for token in tokens:
    print(token)
```

### Editing a Token

You can modify a token's label, expiration date, and scopes.

```python
from datetime import datetime, timedelta, timezone

# Get the first token from the list.
token = tokens[0]

# Extend the expiration by 30 days.
if token.expiration:
    token.expiration += timedelta(days=30)
else:
    token.expiration = datetime.now(timezone.utc) + timedelta(days=30)

# Limit the token's scopes.
token.scopes = ['read:pipes', 'write:pipes']

success, msg = token.edit()
```

### Invalidating and Deleting Tokens

If a token is compromised, you can invalidate it immediately. Invalidated tokens cannot be used but remains registered; deleting a token removes it permanently.

```python
# Invalidate the token.
success, msg = token.invalidate()

# Delete the token permanently.
success, msg = token.delete()
```

---

## ⚙️ Authorization

There are two ways to use a token to authenticate with the API:

1. Post the **Client ID** and **Client Secret** to `/login`.  
  The standard OAuth flow now accepts `client_id` and `client_secret` (in addition to `username` and `password`). This returns short-lived bearer tokens.
2. Set the **API Key** directly in the `Authorization` header.  
  For situations where the standard OAuth flow isn't feasible (e.g. some IoT workloads), you can directly embed the API Key in the `Authorization` header. The API Key wraps the Client ID and Secret and is authenticated on every request.

??? example "Authenticating with `curl`"

    ```bash
    # Replace <API_KEY> with your generated key.
    export API_KEY="mrsm-key:YTg1ZTI3NDEtZ..."

    curl -X GET http://localhost:8000/pipes \
      -H "Authorization: $API_KEY"
    ```

---

## 🎯 Scopes

A token (and a user, through the `scopes` attribute of their account) holds a list of scope strings, and every API route requires the scopes it names (`pipes:read`, `jobs:execute`, `jobs:stop`, ...). A request missing one is refused with `403` and `Missing required scope: '<scope>'`.

- **`*` grants all scopes, including custom and future scopes.** It is stored literally and checked on every request, so a `*` token keeps working when a plugin defines a new scope or a later release adds one. Use it for trusted automation. `edit tokens` and `edit users` keep a `*` you type as `*` (v4 replaced it with the list of built-in scopes), and so do `register tokens --scopes '*'`, `POST /tokens/register` with `"scopes": ["*"]` and the console's token form. When no scopes are given, a new token gets its owner's scopes, and a new user (`register users`) follows the instance's default role (see [Teams, Access and Locks](../actions/teams-and-access.md)).
- **A token carries at most its owner's scopes.** Registering or editing a token with a scope the owner does not hold, or with `*` when the owner is neither an administrator nor a holder of `*`, is refused with `403` (or a failure in `register tokens` / `edit tokens` and the console) naming each disallowed scope; nothing is trimmed. A token registered without scopes gets its owner's own, and editing a token keeps the scopes it already holds. The console's register form starts from your own scopes, and "Grant all scopes" is on by default only for an administrator or a holder of `*`.
- **Only the owner or an administrator may change a token.** Editing, invalidating or deleting another user's token answers `403`; only an administrator may make an invalidated token valid again; an edit that omits `scopes` or `is_valid` keeps them. `mrsm edit tokens` and the other token actions work on the instance directly with the operator's own access and are trusted; the rule is enforced by an `api:` instance's server.
- **Custom scopes are plain strings.** A plugin's `@api_plugin` routes may require any scope, for example `ScopedAuth(['myplugin:read'])`, and a token is granted it by listing it explicitly (`--scopes myplugin:read pipes:read`). Meerschaum does not validate scope names, so a typo grants nothing.
- **Stopping a job needs `jobs:execute` and `jobs:stop`.** v4 required a misspelled `josb:stop` that no one could hold, so only `*` tokens and administrators could stop jobs over the API. v5 requires `jobs:stop`, and still accepts a token that explicitly holds the legacy `josb:stop` in its place.
- **Running actions.** Remote actions need `actions:execute` and `jobs:execute`; creating jobs also needs `jobs:write`. Setting `api.permissions.actions.non_admin: false` (`start api --secure`) restricts these operations to administrators. Restricted keys follow the remote command limits even when their owner is an administrator. Pipe Sync and Verify controls use the Pipe's own permissions. See [who may run actions](/reference/api-instance/permissions/#who-may-run-actions).

```bash
mrsm register tokens --name automation --scopes '*'
mrsm register tokens --name plugin-reader --scopes myplugin:read pipes:read
```
