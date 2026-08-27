#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utilities for managing defined pipes.
"""

from typing import List, Dict, Any, Union
import meerschaum as mrsm
from meerschaum.utils.warnings import warn, dprint


def get_defined_pipes(
    compose_config: Dict[str, Any],
    as_meta: bool = False,
    cache: bool = True,
    debug: bool = False,
) -> List[Union[mrsm.Pipe, Dict[str, Any]]]:
    """
    Return a list of the Pipes defined in `mrsm-compose.yaml`.

    Parameters
    ----------
    compose_config: Dict[str, Any]
        The Meerschaum compose configuration dictionary.

    as_meta: bool, default False
        If `True`, return a list of metadata (attributes)
        rather than `Pipe` objects.

    Returns
    -------
    A list of pipes (or metadata).
    """
    from meerschaum.compose.utils.stack import get_project_name
    from meerschaum.config import get_config
    import copy
    project_name = get_project_name(compose_config)
    default_instance = compose_config.get(
        'config',
        {}
    ).get(
        'meerschaum',
        {}
    ).get(
        'instance',
        get_config('meerschaum', 'instance')
    )
    sync_pipes_meta = compose_config.get('sync', {}).get('pipes', [])
    global_pipes_meta = compose_config.get('pipes', [])
    _pipes_meta = sync_pipes_meta + global_pipes_meta
    pipes_meta = []
    for _pipe_meta in _pipes_meta:
        pipe_meta = copy.deepcopy(_pipe_meta)
        if 'tags' not in pipe_meta:
            pipe_meta['tags'] = []
        pipe_meta['tags'].append(project_name)
        if not pipe_meta.get('instance', None):
            legacy_mrsm_instance = pipe_meta.get('mrsm_instance', None)
            if not legacy_mrsm_instance:
                pipe_meta['instance'] = default_instance
        if 'cache' not in pipe_meta:
            pipe_meta['cache'] = cache
        pipes_meta.append(pipe_meta)

    if debug:
        dprint("Compose: Pipes metadata:")
        mrsm.pprint(pipes_meta)

    if as_meta:
        return pipes_meta

    if debug:
        dprint("Building pipes from metadata...")

    pipes = [mrsm.Pipe(**pipe_meta) for pipe_meta in pipes_meta]

    if debug:
        dprint("Returning pipes...")

    return pipes


def build_custom_connectors(compose_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    This function constructs the custom connectors
    so that they are stored in the in-memory registry.
    """
    from meerschaum.connectors import types, custom_types
    custom_connectors_config = compose_config.get(
        'config',
        {}
    ).get(
        'meerschaum',
        {}
    ).get(
        'connectors',
        {}
    )
    if not custom_connectors_config:
        return {}

    custom_connectors = {}
    for typ, labels in custom_connectors_config.items():
        _load_plugins = typ not in types
        for label, connector_kwargs in labels.items():
            conn_keys = typ + ':' + label
            custom_connectors[conn_keys] = mrsm.get_connector(
                conn_keys,
                _load_plugins=_load_plugins,
                **connector_kwargs
            )

    return custom_connectors


def instance_pipes_from_pipes_list(pipes: List[mrsm.Pipe]) -> Dict[str, List[mrsm.Pipe]]:
    """
    Return a dictionary of pipes lists, grouping by instance connector keys.
    """
    from collections import defaultdict
    instance_pipes = defaultdict(lambda: [])
    for pipe in pipes:
        instance_pipes[str(pipe.instance_keys)].append(pipe)
    return instance_pipes


def build_parent_pipe(
    compose_config: Dict[str, Any],
) -> mrsm.Pipe:
    """
    Construct a `plugin:compose` pipe from the give project config.
    """
    from meerschaum.compose.utils.stack import get_project_name
    instance_keys = compose_config.get(
        'config',
        {}
    ).get(
        'meerschaum',
        {}
    ).get(
        'default_instance',
        None
    )
    _ = build_custom_connectors(compose_config)
    children_pipes_meta = get_defined_pipes(compose_config, as_meta=True)
    project_name = get_project_name(
        compose_config
    ).replace('-', '_').lstrip('_')
    return mrsm.Pipe(
        'plugin:compose', project_name,
        instance=instance_keys,
        parameters={
            'children': children_pipes_meta,
            'compose': {
                'project_name': project_name,
            },
        },
    )
