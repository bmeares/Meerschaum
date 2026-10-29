# Job Access

API-created jobs record the authenticated **owner** and that user's primary **team**. Requests cannot assign a different owner or team. Jobs created on the host through the CLI, Compose, systemd, or Python belong to Everyone.

| Job | Who may see its metadata and logs |
|---|---|
| Team job | Its owner, team members and managers, and administrators. |
| Everyone job | Anyone with the required read permissions. |
| Automatic console Run | Its owner, its team's managers, and administrators. |

Hidden jobs return `404` from listings, metadata, logs, streams, websockets, and MCP tools.

## Permissions

An operation requires both visibility and its permissions:

| Operation | Permissions |
|---|---|
| Read jobs and logs | `jobs:read`, `logs:read` |
| Start | `jobs:execute` |
| Stop or pause | `jobs:stop` or `jobs:pause` |
| Delete | `jobs:delete` |
| Create | `jobs:write`, `jobs:execute`, `actions:execute` |
| Answer prompts through the log websocket | `jobs:execute`, `actions:execute` |

`--secure`, or `api:permissions:actions:non_admin: false`, limits job mutations to full administrators. Other users can still read the jobs available to them.

??? info "Teams and nested jobs"

    A job started by another job inherits that job's owner and team. Renaming a team updates its jobs; deleting a team moves its jobs to Everyone. These changes affect the root where the team is managed. A job which still names a missing team remains visible only to its owner and administrators.

    Local CLI and Python access trust the host's files. They do not apply the API's caller filtering.

See [users, teams, and access](../actions/teams-and-access.md) for account administration.
