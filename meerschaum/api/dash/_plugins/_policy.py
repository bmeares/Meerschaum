#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash-free plugin access & authoring policy: `PluginRights` computed from a `Viewer`
(`meerschaum.api.dash.viewer`), consolidating visibility, management, installation, source
reading, authoring, publishing, and repository ownership checks in one acyclic module. A callback
resolves its `PluginRights` once (`rights_for_session`) and passes them down.
"""

from __future__ import annotations

from dataclasses import dataclass

from meerschaum.utils.typing import Any, List, Optional
import meerschaum.api.dash.viewer as viewer_model
from meerschaum.api.dash._plugins._fs import discover_plugin_files, is_safe_plugin_name

#: Why a viewer may not manage, author, or publish plugins.
MANAGE_REFUSAL: str = 'Only administrators and team plugin managers can manage plugins.'
NO_WRITING_TEAM: str = 'None of your teams may manage plugins.'
AUTHORING_OFF: str = 'Creating and editing plugins is turned off on this server.'
AUTHORING_ADMINS: str = 'Only administrators and team plugin managers can create and edit plugins.'
PUBLISH_REFUSAL: str = 'Only administrators and team plugin managers can publish installed plugins.'


def foreign_plugin_message(name: str) -> str:
    """Why a team's plugin manager may not touch an installed plugin of someone else's."""
    return f"Plugin '{name}' is already installed and does not belong to your team."


def _console_is_read_only() -> bool:
    """Whether non-administrators may not act from the console (`--secure`)."""
    try:
        from meerschaum.api.dash.viewer import secure_mode
        return bool(secure_mode())
    except Exception:
        return True


def _plugins_registration_enabled() -> bool:
    """Whether the server accepts plugins (`api:permissions:registration:plugins`)."""
    try:
        from meerschaum.config import get_config
        return get_config('api', 'permissions', 'registration', 'plugins', warn=False) is not False
    except Exception:
        return False


def _webterm_enabled() -> bool:
    """Whether the server serves the web terminal (the host configuration's `webterm`)."""
    try:
        from meerschaum.api.host import get_host_config
        return bool(get_host_config().get('webterm', False))
    except Exception:
        return False


def manages_everything(viewer: Any) -> bool:
    """Administrators (and everyone under `--no-auth`) manage every plugin."""
    return bool(viewer is not None and (viewer.no_auth or (viewer.signed_in and viewer.is_admin)))


def team_plugins(viewer: Any) -> set:
    """The plugins that belong to the viewer's teams (their teams' `plugins` lists)."""
    return {
        str(name)
        for name in ((getattr(viewer, 'grants', None) or {}).get('plugins', None) or [])
    }


@dataclass(frozen=True)
class PluginRights:
    """
    Effective plugin rights for a resolved `Viewer` on this server.

    Computed once per fresh viewer resolution so visibility, install/manage/uninstall,
    source preview, authoring (create/upload/edit), and publishing share a single policy.
    """

    viewer: Any
    manages_everything: bool
    can_read: bool
    can_manage_any: bool
    team_plugins: frozenset[str]
    webterm_enabled: bool
    registration_enabled: bool
    console_read_only: bool

    def can_see(self, name: Any) -> bool:
        """Whether the viewer may see `name`."""
        if self.manages_everything:
            return True
        if self.viewer is None or not self.viewer.signed_in:
            return False
        return bool(self.viewer.can_use_plugin(str(name)))

    def visible_names(self, names: Any) -> List[str]:
        """The plugin names among `names` that the viewer may see, in order."""
        return [str(name) for name in (names or []) if self.can_see(name)]

    def can_manage(self, name: Any) -> bool:
        """Whether the viewer may reinstall, update, uninstall, edit, or publish `name`."""
        if self.manages_everything:
            return True
        return self.can_manage_any and str(name) in self.team_plugins

    def manageable_names(self, names: Any) -> List[str]:
        """The plugin names among `names` that the viewer may manage, in order."""
        return [str(name) for name in (names or []) if self.can_manage(name)]

    def can_install(self, name: Any, installed: Any) -> bool:
        """Whether the viewer may install repository plugin `name` given `installed` names."""
        if self.manages_everything:
            return True
        if not self.can_manage_any or not self.can_see(name):
            return False
        return str(name) not in set(installed or []) or self.can_manage(name)

    def action_refusal(
        self,
        action: str,
        name: Any,
        installed: Optional[Any] = None,
    ) -> Optional[str]:
        """Why the viewer may not run `action` on `name`, or `None` when allowed."""
        if self.manages_everything:
            return None
        if not self.can_manage_any:
            return MANAGE_REFUSAL
        name = str(name)
        if not self.can_see(name):
            return f"Plugin '{name}' is not available."
        if self.can_manage(name):
            return None
        if installed is None:
            installed = discover_plugin_files()
        if name in set(installed):
            return foreign_plugin_message(name)
        if action == 'install':
            return None
        return f"Plugin '{name}' does not belong to your team."

    def can_read_source(self, kind: str, name: Optional[str] = None) -> bool:
        """Whether the viewer may read repository or installed source files."""
        who = self.viewer
        if who is None:
            return False
        if who.no_auth:
            return True
        if not who.signed_in or not self.can_read:
            return False
        if kind == 'repository':
            return name is None or self.can_see(name)
        if kind != 'installed':
            return False
        # Installed code is server-private.
        if who.is_admin:
            return True
        return name is not None and self.can_see(name) and self.can_manage(name)

    def authoring_refusal(self, name: Optional[str] = None) -> Optional[str]:
        """
        Why the viewer may not create, upload, or edit plugins (`None` when allowed).
        Never allowed when registration is off or under `--no-auth`.
        """
        if not self.registration_enabled:
            return AUTHORING_OFF
        refused = AUTHORING_ADMINS if self.webterm_enabled else AUTHORING_OFF
        who = self.viewer
        if who is None or who.no_auth or not who.signed_in:
            return refused
        if who.is_admin:
            return None if (self.webterm_enabled and who.can('webterm:use')) else refused
        if not self.can_manage_any:
            return refused
        if name is not None and not self.can_manage(name):
            return f"Plugin '{name}' does not belong to your team."
        return None

    def can_author(self, name: Optional[str] = None) -> bool:
        """Whether the viewer may create plugins, or edit `name`."""
        return self.authoring_refusal(name) is None

    def can_publish(self, name: Optional[str] = None) -> bool:
        """Whether the viewer may publish `name` (or publish at all when `name is None`)."""
        who = self.viewer
        if who is None:
            return False
        if who.no_auth:
            return True
        if not who.signed_in or not self.can_read:
            return False
        if name is None:
            return self.can_manage_any
        return is_safe_plugin_name(name) and self.can_see(name) and self.can_manage(name)


def resolve_rights(
    viewer: Any,
    *,
    webterm_enabled: Optional[bool] = None,
    registration_enabled: Optional[bool] = None,
    console_read_only: Optional[bool] = None,
) -> PluginRights:
    """Compute `PluginRights` from an already-resolved `Viewer`."""
    all_power = manages_everything(viewer)
    read_only = _console_is_read_only() if console_read_only is None else bool(console_read_only)
    reg_on = (
        _plugins_registration_enabled()
        if registration_enabled is None
        else bool(registration_enabled)
    )
    term_on = _webterm_enabled() if webterm_enabled is None else bool(webterm_enabled)
    can_read = bool(
        viewer is not None
        and (viewer.no_auth or (viewer.signed_in and viewer.can('plugins:read')))
    )
    can_manage_any = bool(
        all_power
        or (
            viewer is not None
            and viewer.signed_in
            and can_read
            and viewer.can_write_plugins
            and not read_only
        )
    )
    return PluginRights(
        viewer=viewer,
        manages_everything=all_power,
        can_read=can_read,
        can_manage_any=can_manage_any,
        team_plugins=frozenset(team_plugins(viewer)),
        webterm_enabled=term_on,
        registration_enabled=reg_on,
        console_read_only=read_only,
    )


def rights_for_session(session: Any, *, fresh: bool = True) -> PluginRights:
    """Resolve the session's viewer (fresh by default) and return its `PluginRights`."""
    try:
        who = viewer_model.resolve_viewer(session, fresh=fresh)
    except Exception:
        who = None
    return resolve_rights(who)


########################################################################################
# Module-level policy functions (single source of truth).
########################################################################################

def can_see_plugin(viewer: Any, name: Any) -> bool:
    return resolve_rights(
        viewer, webterm_enabled=False, registration_enabled=False, console_read_only=False,
    ).can_see(name)


def visible_plugin_names(viewer: Any, names: Any) -> List[str]:
    return [str(name) for name in (names or []) if can_see_plugin(viewer, name)]


def may_manage_plugins(viewer: Any) -> bool:
    return resolve_rights(
        viewer, webterm_enabled=False, registration_enabled=False,
    ).can_manage_any


def may_manage_plugin(viewer: Any, name: Any) -> bool:
    return resolve_rights(
        viewer, webterm_enabled=False, registration_enabled=False,
    ).can_manage(name)


def manageable_plugin_names(viewer: Any, names: Any) -> List[str]:
    rights = resolve_rights(viewer, webterm_enabled=False, registration_enabled=False)
    return rights.manageable_names(names)


def may_install_plugin(viewer: Any, name: Any, installed: Any) -> bool:
    return resolve_rights(
        viewer, webterm_enabled=False, registration_enabled=False,
    ).can_install(name, installed)


def plugin_action_refusal(
    viewer: Any,
    action: str,
    name: Any,
    installed: Optional[Any] = None,
) -> Optional[str]:
    return resolve_rights(
        viewer, webterm_enabled=False, registration_enabled=False,
    ).action_refusal(action, name, installed=installed)


def is_plugin_owner(
    plugin_name: str,
    username: Optional[str],
    *,
    conn: Any = None,
    debug: bool = False,
) -> bool:
    """Whether `username` is the owner of the published plugin `plugin_name`."""
    from meerschaum.api import get_api_connector
    from meerschaum.core import Plugin

    if not username:
        return False
    api_conn = conn if conn is not None else get_api_connector()
    plugin_username = api_conn.get_plugin_username(Plugin(plugin_name), debug=debug)
    return username == plugin_username
