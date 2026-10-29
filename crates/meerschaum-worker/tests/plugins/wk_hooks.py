#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""Worker test plugin: hooks, actions, arguments, and host extensions for the manifest."""

__version__ = '2.1.0'

import os

from meerschaum.plugins import (
    pre_sync_hook, post_sync_hook, make_action, add_plugin_argument,
    api_plugin, dash_plugin, web_page,
)

required = ['wk_dependency>=0.1']

add_plugin_argument('--wk-flag', type=int, help="A worker test flag.")


@pre_sync_hook
def wk_pre(pipe, sync_timestamp=None, **kwargs):
    return True, f"pre {pipe.metric_key} {type(sync_timestamp).__name__}"


@post_sync_hook
def wk_post(pipe, success_tuple=None, sync_duration=None, sync_complete_timestamp=None):
    return True, (
        f"post {pipe.metric_key} {success_tuple[0]} {success_tuple[1]} "
        f"{type(sync_duration).__name__} {type(sync_complete_timestamp).__name__}"
    )


@post_sync_hook
def wk_post_broken(pipe, **kwargs):
    raise RuntimeError("synthetic hook failure")


@make_action
def wk_hello(action=None, wk_flag=None, **kwargs):
    """Say hello from a worker."""
    return True, f"hello {action} {wk_flag}"


def wk_hooks(action=None, **kwargs):
    """The plugin's own action (named after the module)."""
    return True, "module action"


def wk_hooks_sub(action=None, **kwargs):
    """A subaction."""
    return True, "sub"


@make_action
def wk_crash(**kwargs):
    """Kill the interpreter mid-call."""
    os._exit(3)


@make_action
def wk_sleep(**kwargs):
    """Sleep for a long time."""
    import time
    time.sleep(60)
    return True, "slept"


@api_plugin
def wk_api(app):
    pass


@dash_plugin
def wk_dash(dash_app):

    @web_page('wk/page', login_required=False, page_group='Worker')
    def wk_page():
        return None


@web_page('wk/top')
def wk_top():
    return None


@web_page('wk/html')
def wk_html():
    return '<p>page contract</p>'


try:
    from meerschaum.mcp import tool, resource

    @tool('wk_tool', "A worker test tool.", scopes=['pipes:read'], read_only=True)
    def wk_tool(**kw):
        return {'ok': True}

    @resource('wk_resource', "A worker test resource.", uri='mrsm://wk/resource')
    def wk_resource(**kw):
        return {}
except ValueError:
    ### Duplicate registration when the module is imported twice.
    pass
