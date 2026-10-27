"""Registered Pipeline selections and explicitly approved membership-tag analysis.

Metadata uses native Pipe operations. The same schema is consumed by the standalone CLI;
reading a definition or discovering dependencies never writes tags or registrations.
"""
from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import inspect
import hashlib
import json
import re
import uuid
import meerschaum as mrsm

_leases = ContextVar('pipeline_mutation_leases', default={})
EXECUTION_DEFAULTS = frozenset((
    'begin', 'end', 'workers', 'retries', 'min_seconds', 'chunksize',
    'check_existing', 'enforce_dtypes', 'force', 'params', 'yes',
))


def validate_execution_defaults(defaults):
    if not isinstance(defaults, dict):
        raise ValueError('Pipeline execution defaults must be an object.')
    unknown = set(defaults) - EXECUTION_DEFAULTS
    if unknown:
        raise ValueError('Unsupported Pipeline execution defaults: ' + ', '.join(sorted(unknown)))
    return deepcopy(defaults)



@contextmanager
def _mutation_lock(instance, pipe=None):
    """Backend leases serialize CLI/SDK revisions and shared member tag ownership."""
    from meerschaum.core.Pipeline import _pipe_ref
    from meerschaum.core.Pipeline._engine import build_request, get_core
    if pipe is None:
        pipe = mrsm.Pipe('mrsm', 'pipeline_registry', instance=instance)
    reference = _pipe_ref(pipe)
    active = _leases.get()
    if reference in active:
        active[reference].check()
        yield
        return
    request = build_request(pipe.instance_connector)
    request['targets'] = []
    lease = get_core().pipeline_mutation_lock(json.dumps(request), reference)
    token = _leases.set({**active, reference: lease})
    try:
        yield
    finally:
        _leases.reset(token)
        lease.close()


def _locked_registry(function):
    signature = inspect.signature(function)
    @wraps(function)
    def locked(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        instance = bound.arguments.get('instance')
        if instance is None and 'proposal' in bound.arguments:
            instance = bound.arguments['proposal']['registry']
        with _mutation_lock(instance):
            return function(*args, **kwargs)
    return locked


def validate_name(name):
    if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', name):
        raise ValueError('Use 1–128 letters, numbers, dots, underscores or hyphens, starting with a letter or number.')
    return name


def registration(name, instance=None):
    return mrsm.Pipe('mrsm', 'pipeline', validate_name(name), instance=instance)


def _normalize(definition, name, registry):
    result = deepcopy(definition)
    result.update(name=name, registry=str(registry), schema_version=2)
    result.setdefault('id', f'legacy:{registry}:{name}')
    result.setdefault('revision', 0)
    result.setdefault('description', '')
    result.setdefault('defaults', {})
    result.setdefault('layout', {})
    result.setdefault('tag_associations', [])
    return result


def list_pipelines(instance=None):
    """Read registered definitions; legacy metadata is normalized without writing it."""
    pipes = mrsm.get_pipes(connector_keys=['mrsm'], metric_keys=['pipeline'],
                           instance=instance, as_list=True)
    definitions = {}
    for pipe in pipes:
        definition = pipe.get_parameters(apply_symlinks=False).get('pipeline')
        if isinstance(definition, dict):
            definitions[pipe.location_key] = _normalize(
                dict(definition), pipe.location_key, pipe.instance_keys,
            )
    return definitions


def _read(name, instance=None):
    pipe = registration(name, instance)
    if pipe.get_id() is None:
        raise ValueError(f"Pipeline '{name}' does not exist on '{pipe.instance_keys}'.")
    return _normalize(dict(pipe.get_parameters(apply_symlinks=False)['pipeline']), name, pipe.instance_keys)


def _write(definition, instance=None, create=False):
    for lease in _leases.get().values():
        lease.check()
    pipe = registration(definition['name'], instance or definition['registry'])
    pipe.parameters = {'pipeline': definition}
    success, message = pipe.register() if create else pipe.edit(interactive=False)
    if not success:
        raise ValueError(message)
    return definition


def _refs(targets, instance):
    from meerschaum.core.Pipeline import _pipe_ref, _pipe_from_ref
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
        if len(ref.split('|', 3)) != 4 or any(not key for key in ref.split('|', 3)[:3]):
            raise ValueError(f'Invalid pipe reference: {ref!r}')
        if _pipe_from_ref(ref).connector_keys == 'mrsm':
            raise ValueError('Internal metadata pipes cannot be pipeline members.')
        refs.append(ref)
    return sorted(set(refs))


@_locked_registry
def save_pipeline(name, targets, instance=None, parents=None, children=None, *,
                  expected_revision=None, description=None, defaults=None, layout=None, create=False):
    """Save exact membership; optional expected_revision rejects a stale editor.

    This does not apply membership tags. Analyze and approve those separately.
    """
    from meerschaum.core.Pipeline._engine import get_depth
    name = validate_name(name)
    pipe = registration(name, instance)
    exists = pipe.get_id() is not None
    if create and exists:
        raise ValueError(f"Pipeline '{name}' already exists.")
    old = _read(name, instance) if exists else {}
    if expected_revision is not None and old.get('revision', 0) != expected_revision:
        raise ValueError('Pipeline revision changed; reload the definition before saving.')
    refs = _refs(targets, pipe.instance_keys)
    definition = {**old, 'schema_version': 2, 'name': name, 'registry': pipe.instance_keys,
                  'id': old.get('id', str(uuid.uuid4())), 'revision': old.get('revision', 0) + 1,
                  'targets': refs, 'parents': get_depth(parents), 'children': get_depth(children),
                  'description': description if description is not None else old.get('description', ''),
                  'defaults': validate_execution_defaults(defaults if defaults is not None else old.get('defaults', {})),
                  'tag_associations': deepcopy(old.get('tag_associations', [])),
                  'layout': deepcopy(layout if layout is not None else old.get('layout', {}))}
    return _write(definition, instance, create=not exists)


def referencing_jobs(name, instance=None):
    """Find scheduled Jobs, resolving each chained command's own registry."""
    from meerschaum.jobs import get_jobs
    from meerschaum._internal.arguments import (
        parse_arguments, split_chained_sysargs, split_pipeline_sysargs,
    )
    pipe = registration(name, instance)
    registry = pipe.instance_keys
    found = []
    if registry.startswith('api:'):
        attributes = pipe.instance_connector.get_pipe_attributes(pipe)
        if not isinstance(attributes, dict) or 'pipeline_referencing_jobs' not in attributes:
            raise ValueError('The API instance could not confirm Pipeline schedule references.')
        found.extend(f'{registry}:{job}' for job in attributes['pipeline_referencing_jobs'])
    for job_name, job in get_jobs(include_hidden=True).items():
        args = list(job.sysargs)
        if not any(flag.split('=', 1)[0] in ('--schedule', '-s', '--loop') for flag in args):
            continue
        commands, global_args = split_pipeline_sysargs(args)
        inherited = parse_arguments(global_args).get('mrsm_instance') if global_args else None
        for step in split_chained_sysargs(commands):
            parsed = parse_arguments(step)
            action = parsed.get('action', [])
            if action[:2] not in (['sync', 'pipeline'], ['sync', 'pipelines']):
                continue
            configured = parsed.get('mrsm_instance') or inherited
            if name in action[2:] and (configured is None or configured == registry):
                found.append(job_name)
    return sorted(set(found))


def _tag_state(pipe):
    params = deepcopy(dict(pipe.get_parameters(apply_symlinks=False, refresh=True)))
    return params, list(params.get('tags') or []), deepcopy(params.get('_pipeline_tags') or {})


def _change_tag(pipe, tag, owner, add):
    with _mutation_lock(pipe.instance_connector, pipe):
        return _change_tag_locked(pipe, tag, owner, add)


def _change_tag_locked(pipe, tag, owner, add):
    params, tags, provenance = _tag_state(pipe)
    association = provenance.get(tag)
    if add:
        association = association or {'preexisting': tag in tags, 'owners': []}
        association['owners'] = sorted(set(association.get('owners', [])) | {owner})
        if tag not in tags:
            tags.append(tag)
        provenance[tag] = association
    elif association and owner in association.get('owners', []):
        association['owners'] = [other for other in association['owners'] if other != owner]
        if association['owners']:
            provenance[tag] = association
        else:
            if not association.get('preexisting', False):
                tags = [value for value in tags if value != tag]
            provenance.pop(tag, None)
    else:
        return True, 'No owned association to remove.'
    params['tags'] = tags
    if provenance:
        params['_pipeline_tags'] = provenance
    else:
        params.pop('_pipeline_tags', None)
    pipe.parameters = params
    for lease in _leases.get().values():
        lease.check()
    return pipe.edit(interactive=False)


def analyze_pipeline(name, instance=None):
    """Resolve the native graph and preview tag edits, without any writes."""
    from meerschaum.core.Pipeline import _pipe_from_ref
    definition = _read(name, instance)
    proposal = {'name': name, 'registry': definition['registry'], 'definition': definition,
                'revision': definition['revision'], 'resolved': [], 'changes': [], 'shared': {},
                'errors': []}
    resolved = set()
    try:
        pipeline = load_pipeline(name, instance)
        plan = pipeline.plan()
        resolved = {node.pipe_ref for node in plan.nodes}
        proposal['resolved'] = [{'pipe_ref': ref, 'reason': 'explicit member' if ref in definition['targets'] else 'configured traversal'}
                                for ref in sorted(resolved)]
        # Missing references are planning warnings in the general graph; reconciliation is
        # stricter because an incomplete traversal cannot safely remove owned associations.
        proposal['errors'].extend(
            str(warning) for warning in plan.warnings
            if 'crosses an integer/datetime axis; assuming' not in str(warning)
        )
        missing = set(definition['targets']) - resolved
        if missing:
            proposal['errors'].append('Unresolved members: ' + ', '.join(sorted(missing)))
    except Exception as exc:
        proposal['errors'].append(str(exc))
    tag = f'pipeline-{name}'
    expected = {(ref, tag) for ref in resolved}
    previous = {(a['pipe_ref'], a['tag']) for a in definition['tag_associations']}
    snapshots = {}
    for ref, candidate in sorted(expected | previous):
        try:
            pipe = _pipe_from_ref(ref)
            if pipe.get_id() is None:
                raise ValueError(f'Missing registered Pipe: {ref}')
            _, tags, provenance = _tag_state(pipe)
            snapshots[f'{ref}\n{candidate}'] = {'tags': tags, 'provenance': provenance}
            for known_tag, association in provenance.items():
                for owner in association.get('owners', []):
                    if owner != definition['id']:
                        proposal['shared'].setdefault(ref, []).append(f'{known_tag} (owner {owner})')
            owners = (provenance.get(candidate) or {}).get('owners', [])
            if (ref, candidate) in expected:
                if candidate not in tags or definition['id'] not in owners:
                    proposal['changes'].append({'pipe_ref': ref, 'tag': candidate, 'action': 'add'})
            elif definition['id'] in owners:
                proposal['changes'].append({'pipe_ref': ref, 'tag': candidate, 'action': 'remove'})
        except Exception as exc:
            proposal['errors'].append(str(exc))
    for other_name, other in list_pipelines(instance).items():
        if other['id'] == definition['id']:
            continue
        for ref in sorted(resolved.intersection(other['targets'])):
            proposal['shared'].setdefault(ref, []).append(other_name)
    proposal['shared'] = {ref: sorted(set(owners)) for ref, owners in proposal['shared'].items()}
    proposal['shared_scope'] = 'Known registered memberships and tag owners; not an exhaustive inventory of every instance.'
    if proposal['errors']:
        proposal['changes'] = [change for change in proposal['changes'] if change['action'] != 'remove']
    fingerprint_data = {'definition': definition, 'resolved': sorted(resolved), 'snapshots': snapshots,
                        'errors': proposal['errors']}
    proposal['fingerprint'] = hashlib.sha256(json.dumps(fingerprint_data, sort_keys=True,
                                                       default=str).encode()).hexdigest()
    return proposal


@_locked_registry
def apply_pipeline_analysis(proposal, instance=None):
    """Apply an explicitly approved proposal; reject changes to its inputs since preview."""
    from meerschaum.core.Pipeline import _pipe_from_ref
    instance = instance or proposal['registry']
    current = analyze_pipeline(proposal['name'], instance)
    if current['fingerprint'] != proposal.get('fingerprint'):
        return {'success': False, 'stale': True, 'message': 'Analysis changed; review and approve the refreshed proposal.',
                'proposal': current, 'results': []}
    if current['errors']:
        return {'success': False, 'stale': False, 'message': 'Resolve analysis errors before applying tags.', 'results': []}
    definition = current['definition']
    # Persist recovery bookkeeping before writes so interruption cannot orphan ownership.
    associations = {(a['pipe_ref'], a['tag']) for a in definition['tag_associations']}
    associations.update((row['pipe_ref'], row['tag']) for row in current['changes'])
    definition['tag_associations'] = [{'pipe_ref': ref, 'tag': tag} for ref, tag in sorted(associations)]
    _write(definition, instance)
    results = []
    for row in current['changes']:
        try:
            success, message = _change_tag(_pipe_from_ref(row['pipe_ref']), row['tag'],
                                           definition['id'], row['action'] == 'add')
        except Exception as exc:
            success, message = False, str(exc)
        results.append({'pipe_ref': row['pipe_ref'], 'success': success, 'message': message})
        if success and row['action'] == 'remove':
            associations.discard((row['pipe_ref'], row['tag']))
    latest = _read(definition['name'], instance)
    if latest['revision'] != definition['revision']:
        return {'success': False, 'stale': True, 'message': 'Definition changed while applying tags; analyze again.', 'results': results}
    latest['tag_associations'] = [{'pipe_ref': ref, 'tag': tag} for ref, tag in sorted(associations)]
    _write(latest, instance)
    success = all(row['success'] for row in results)
    return {'success': success, 'stale': False, 'message': 'Membership tags updated.' if success else 'Some tag writes failed; analyze again to retry.', 'results': results}


def _remove_owned_tags(definition):
    from meerschaum.core.Pipeline import _pipe_from_ref
    errors = []
    for association in definition['tag_associations']:
        ref, tag = association['pipe_ref'], association['tag']
        try:
            pipe = _pipe_from_ref(ref)
            if pipe.get_id() is None:
                continue
            success, message = _change_tag(pipe, tag, definition['id'], False)
            if not success:
                errors.append(f'{ref}: {message}')
        except Exception as exc:
            errors.append(f'{ref}: {exc}')
    return errors


@_locked_registry
def delete_pipeline(name, instance=None):
    """Delete only the definition and its owned tags; refuse referencing schedules."""
    if registration(name, instance).get_id() is None:
        return False, f"Pipeline '{name}' does not exist."
    jobs = referencing_jobs(name, instance)
    if jobs:
        return False, 'Remove or reassign scheduled Jobs first: ' + ', '.join(jobs)
    definition = _read(name, instance)
    errors = _remove_owned_tags(definition)
    if errors:
        return False, 'Tag cleanup incomplete; definition retained for retry. ' + '; '.join(errors)
    return registration(name, instance).delete()


@_locked_registry
def rename_pipeline(name, new_name, instance=None, expected_revision=None):
    validate_name(new_name)
    old = _read(name, instance)
    if expected_revision is not None and old['revision'] != expected_revision:
        raise ValueError('Pipeline revision changed; reload before renaming.')
    if name == new_name:
        return old
    if registration(new_name, instance).get_id() is not None:
        raise ValueError(f"Pipeline '{new_name}' already exists.")
    jobs = referencing_jobs(name, instance)
    if jobs:
        raise ValueError('Remove or reassign scheduled Jobs first: ' + ', '.join(jobs))
    errors = _remove_owned_tags(old)
    if errors:
        raise ValueError('Tag cleanup incomplete; original definition retained. ' + '; '.join(errors))
    new = {**old, 'name': new_name, 'revision': old['revision'] + 1, 'tag_associations': []}
    _write(new, instance, create=True)
    success, message = registration(name, instance).delete()
    if not success:
        raise ValueError(f'New definition saved, but old definition could not be removed: {message}')
    return new


@_locked_registry
def copy_pipeline(name, new_name, instance=None):
    old = _read(name, instance)
    return save_pipeline(new_name, old['targets'], instance, old.get('parents'), old.get('children'),
                         description=old.get('description'), defaults=old.get('defaults'), layout=old.get('layout'), create=True)


def load_pipeline(name, instance=None):
    """The saved Pipeline `name` (`mrsm.Pipeline(name, instance=instance)`)."""
    from meerschaum.core.Pipeline import Pipeline
    definition = _read(name, instance)
    return Pipeline._from_definitions(
        [definition], registration(name, instance).instance_connector,
    )


def initialize_pipelines(graph=None, instance=None):
    """Compatibility name: read registrations without auto-saving connected components."""
    return list_pipelines(instance)


def job_arguments(name, instance, schedule, workers=None, retries=0, force=False):
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


def derivative_job_settings(job_name, arguments, existing_names):
    """Copy a pipeline Job's options into a distinct name, preserving its history."""
    if arguments[:2] not in (['sync', 'pipeline'], ['sync', 'pipelines']):
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
