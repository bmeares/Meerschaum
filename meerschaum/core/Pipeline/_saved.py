"""Named pipeline selections stored as internal pipe registrations on their instance.

The registration holds metadata only; no data table is created. Internal ``mrsm`` pipes
are excluded from the Syncing Engine graph. Jobs resolve membership afresh for every run.
"""
from __future__ import annotations

import re
import meerschaum as mrsm


def validate_name(name):
    if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', name):
        raise ValueError('Use 1–128 letters, numbers, dots, underscores or hyphens, starting with a letter or number.')
    return name


def registration(name, instance):
    return mrsm.Pipe('mrsm', 'pipeline', validate_name(name), instance=instance)


def list_pipelines(instance=None):
    """Read saved selections, keyed by name, from the instance registry."""
    pipes = mrsm.get_pipes(connector_keys=['mrsm'], metric_keys=['pipeline'],
                           instance=instance, as_list=True)
    return {pipe.location_key: dict(pipe.parameters['pipeline']) for pipe in pipes
            if isinstance(pipe.parameters.get('pipeline'), dict)}


def save_pipeline(name, targets, instance=None, parents=None, children=None):
    """Save exact pipe membership, avoiding Cartesian products of the three keys."""
    from meerschaum.core.Pipeline import _pipe_ref, _pipe_from_ref
    from meerschaum.core.Pipeline._engine import get_depth
    name = validate_name(name)
    refs = []
    for target in targets:
        if isinstance(target, mrsm.Pipe):
            ref = _pipe_ref(target)
        elif isinstance(target, (tuple, list)) and 2 <= len(target) <= 4:
            keys = list(target)
            if len(keys) == 2:
                keys.append(None)
            ref = _pipe_ref(mrsm.Pipe(*keys[:3], instance=(keys[3] if len(keys) == 4 else instance)))
        elif isinstance(target, str):
            ref = target
        else:
            raise ValueError(f'Invalid pipeline target: {target!r}')
        refs.append(ref)
    refs = sorted(set(refs))
    if not refs:
        raise ValueError('Select at least one pipe before saving a pipeline.')
    for ref in refs:
        if not isinstance(ref, str) or len(ref.split('|', 3)) != 4:
            raise ValueError(f'Invalid pipe reference: {ref!r}')
        pipe = _pipe_from_ref(ref)
        if pipe.connector_keys == 'mrsm':
            raise ValueError('Internal metadata pipes cannot be pipeline members.')
    definition = {'name': name, 'targets': refs, 'parents': get_depth(parents),
                  'children': get_depth(children)}
    pipe = registration(name, instance)
    exists = pipe.get_id() is not None
    pipe.parameters = {'pipeline': definition}
    success, message = pipe.edit(interactive=False) if exists else pipe.register()
    if not success:
        raise ValueError(message)
    return definition


def delete_pipeline(name, instance=None):
    """Remove the saved selection only; its member pipes remain registered."""
    return registration(name, instance).delete()


def load_pipeline(name, instance=None):
    """Build a runnable named Pipeline from its saved membership."""
    from meerschaum.core.Pipeline import Pipeline, _pipe_from_ref
    pipe = registration(name, instance)
    if pipe.get_id() is None:
        raise ValueError(f"Pipeline '{name}' does not exist on '{pipe.instance_keys}'.")
    definition = pipe.parameters.get('pipeline') or {}
    return Pipeline([_pipe_from_ref(ref) for ref in definition['targets']],
                    instance=pipe.instance_connector, name=name,
                    parents=definition.get('parents'), children=definition.get('children'))


def job_arguments(name, instance, schedule, workers=None, retries=0, force=False):
    """Arguments of an ordinary Job that resolves the saved pipeline each time it runs."""
    validate_name(name)
    if not schedule or not str(schedule).strip():
        raise ValueError('Enter a schedule, for example "every 5 minutes".')
    args = ['sync', 'pipeline', name, '-i', str(instance), '--schedule', str(schedule),
            '--retries', str(int(retries))]
    if int(retries) < 0:
        raise ValueError('Retries cannot be negative.')
    if workers is not None:
        if int(workers) < 1:
            raise ValueError('Workers must be positive.')
        args += ['--workers', str(int(workers))]
    if force:
        args += ['--force']
    return args


def initialize_pipelines(graph, instance=None):
    """Give pre-existing connected components saved names once on each instance.

    A separate marker records that discovery completed, including an empty graph. Deleting
    the last saved pipeline therefore does not rediscover and resurrect it on page reload.
    """
    marker = mrsm.Pipe('mrsm', 'pipeline_registry', instance=instance)
    definitions = list_pipelines(instance)
    if marker.get_id() is not None:
        return definitions
    if not definitions:
        graph = graph() if callable(graph) else graph
        nodes = {node['pipe_ref']: node for node in graph.get('nodes', [])
                 if node['pipe_ref'].split('|', 3)[1] != 'mrsm'}
        neighbours = {ref: set() for ref in nodes}
        children = set()
        for edge in graph.get('edges', []):
            parent, child = edge['parent'], edge['child']
            if parent in nodes and child in nodes:
                neighbours[parent].add(child)
                neighbours[child].add(parent)
                children.add(child)
        remaining = set(nodes)
        while remaining:
            seed = min(remaining)
            component, pending = set(), [seed]
            while pending:
                ref = pending.pop()
                if ref in component:
                    continue
                component.add(ref)
                pending.extend(neighbours[ref] - component)
            remaining -= component
            roots = sorted(component - children) or sorted(component)
            metric = roots[0].split('|', 3)[2]
            base = re.sub(r'[^A-Za-z0-9_.-]+', '-', metric).strip('._-')[:110] or 'pipeline'
            name, ordinal = base, 2
            while name in definitions:
                name = f'{base}-{ordinal}'
                ordinal += 1
            definitions[name] = save_pipeline(name, sorted(component), instance)
    marker.parameters = {'pipeline_registry': {'version': 1}}
    success, message = marker.register()
    if not success:
        raise ValueError(message)
    return definitions


def derivative_job_settings(job_name, arguments, existing_names):
    """Copy a pipeline Job's options into a distinct name, preserving its history."""
    if arguments[:2] != ['sync', 'pipeline']:
        raise ValueError('This Job does not sync a saved pipeline.')
    def option(flag, default=None):
        if flag not in arguments:
            return default
        index = arguments.index(flag)
        if index + 1 >= len(arguments):
            raise ValueError(f'Missing value for {flag}.')
        return arguments[index + 1]
    base, ordinal = job_name[:120], 2
    name = f'{base}-{ordinal}'
    while name in existing_names:
        ordinal += 1
        name = f'{base}-{ordinal}'
    return {
        'name': name, 'schedule': option('--schedule', option('-s', '')),
        'workers': int(option('--workers', 4)), 'retries': int(option('--retries', 0)),
        'force': '--force' in arguments,
    }
