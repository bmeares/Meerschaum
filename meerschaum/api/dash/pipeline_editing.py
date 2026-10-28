"""Validate staged shared Pipe facts before the workspace makes any edits."""
from copy import deepcopy
import json
import meerschaum as mrsm
from meerschaum.api.dash import engine


def make_pipe(ref, parameters=None):
    instance, connector, metric, location = engine.split_ref(ref)
    return mrsm.Pipe(connector, metric, location, instance=instance, parameters=parameters)


def pipe_ref(pipe):
    return '|'.join((str(pipe.instance_keys), pipe.connector_keys, pipe.metric_key,
                     pipe.location_key if pipe.location_key is not None else 'None'))


def fingerprint(parameters):
    return json.dumps(parameters, sort_keys=True, default=str)


def dependencies(ref, parameters):
    """Extract real explicit declarations and inferred templates using the native parser."""
    from meerschaum.utils.pipes import get_pipe_from_value
    from meerschaum import _core
    pipe = make_pipe(ref, parameters)
    edges = set()
    for plural, singular in (('parents', 'parent'), ('children', 'child')):
        values = parameters.get(plural, parameters.get(singular)) or []
        if isinstance(values, (str, dict)):
            values = [values]
        if not isinstance(values, list):
            raise ValueError(f'{ref}: {plural} must contain Pipe references.')
        for value in values:
            other = get_pipe_from_value(value, _pipe=pipe)
            if other is None:
                raise ValueError(f'{ref}: invalid {plural} reference {value!r}.')
            edge = (pipe_ref(other), ref) if plural == 'parents' else (ref, pipe_ref(other))
            if edge[0] != edge[1]:
                edges.add(edge)
    def walk(value):
        if isinstance(value, str):
            for parent in _core.extract_pipe_references(value, pipe.instance_keys, pipe.connector_keys,
                                                        pipe.metric_key, pipe.location_key):
                if parent != ref:
                    edges.add((parent, ref))
        elif isinstance(value, dict):
            for key, child in value.items():
                # Parameter inheritance is not a data dependency.
                if key not in ('reference', 'references', 'parents', 'parent', 'children', 'child'):
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    if mrsm.get_config('system', 'engine', 'implicit_edges', warn=False) is not False:
        walk(parameters)
    return edges


def validate_edits(edits, roots):
    """Re-read baselines, resolve affected graph, and reject cycles without persisting edits."""
    pending = list(dict.fromkeys(list(roots) + list(edits)))
    parameters_by_ref, edges = {}, set()
    while pending:
        ref = pending.pop()
        if ref in parameters_by_ref:
            continue
        pipe = make_pipe(ref)
        if pipe.get_id() is None:
            raise ValueError(f'Pipe is missing: {ref}')
        actual = deepcopy(dict(pipe.get_parameters(apply_symlinks=False)))
        edit = edits.get(ref)
        if edit and fingerprint(actual) != edit['baseline']:
            raise ValueError(f'Pipe parameters changed while editing: {ref}. Reload before staging again.')
        parameters = edit['parameters'] if edit else actual
        parameters_by_ref[ref] = parameters
        new_edges = dependencies(ref, parameters)
        edges.update(new_edges)
        pending.extend(endpoint for edge in new_edges for endpoint in edge if endpoint not in parameters_by_ref)
    children = {}
    for parent, child in edges:
        children.setdefault(parent, set()).add(child)
    visiting, visited = [], set()
    def visit(ref):
        if ref in visiting:
            raise ValueError('Dependency cycle: ' + ' → '.join(visiting[visiting.index(ref):] + [ref]))
        if ref in visited:
            return
        visiting.append(ref)
        for child in children.get(ref, ()):
            visit(child)
        visiting.pop()
        visited.add(ref)
    for ref in parameters_by_ref:
        visit(ref)
    return edges


def apply_edits(edits, roots, viewer=None):
    """
    All validation precedes writes; each cross-instance result remains explicit. Each edit is
    the console's checked write (`pipe_writes.save_parameters`: the viewer's edit rights on the
    pipe, its locks, SQL definitions).
    """
    from meerschaum.api.dash import access, pipe_writes
    viewer = viewer if viewer is not None else access.current_viewer()
    for ref in edits:
        if not access.ref_is_visible(viewer, ref):
            raise ValueError(f'Pipe is missing: {ref}')
    validate_edits(edits, roots)
    results = []
    for ref, edit in edits.items():
        try:
            pipe = make_pipe(ref)
            from meerschaum.core.Pipeline._saved import _mutation_lock
            with _mutation_lock(pipe.instance_connector, pipe):
                if fingerprint(dict(pipe.get_parameters(apply_symlinks=False))) != edit['baseline']:
                    raise ValueError('Parameters changed since validation.')
                success, message = pipe_writes.save_parameters(
                    pipe, deepcopy(edit['parameters']), viewer=viewer,
                )
            results.append((ref, bool(success), str(message)))
        except Exception as exc:
            results.append((ref, False, str(exc)))
    return results



def raw_query(parameters):
    fetch = parameters.get('fetch') or {}
    return (fetch.get('definition') or next((parameters[key] for key in ('definition', 'query', 'sql')
                                           if parameters.get(key)), ''))


def set_raw_query(parameters, query):
    if 'definition' in (parameters.get('fetch') or {}):
        parameters['fetch']['definition'] = query
    else:
        key = next((key for key in ('definition', 'query', 'sql') if key in parameters), 'sql')
        parameters[key] = query
