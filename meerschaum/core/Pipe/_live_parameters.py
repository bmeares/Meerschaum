#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Live views over a pipe's parameters (design §4.4, "What the façade must preserve").

v4's `Pipe.parameters` returns a resolved *copy*, so `pipe.parameters['x'] = 1` followed by
`pipe.edit()` silently loses the change. Under `MRSM_CORE=rust` the property returns a `LiveDict`
instead: a real `dict` subclass (so `isinstance`, `json.dumps`, `**` unpacking and equality keep
working) whose values are the *resolved* parameters (references, `{{ Pipe() }}` and `MRSM{}`
substituted, as v4 reads them) and whose mutating methods write through to the pipe's *raw*
(unresolved) parameters, the ones `Pipe.edit()` persists. Nested dicts and lists are live too, so
`pipe.parameters['fetch']['backtrack_minutes'] = 5` persists.

Under `MRSM_CORE=rust` the raw parameters themselves are owned by the pipe's `_core.Pipe`
(`RawDict` / `RawList` / `Attrs` below): every mutation of `pipe._attributes['parameters']`, nested
or not, is pushed into the Rust raw map, so what `edit()` persists is what Rust holds.

Only the touched keys are written: a value that is merely read keeps its raw template in the raw
parameters. The view itself is a snapshot taken when `pipe.parameters` is read; a held reference
stays write-through, but does not observe later changes made through other references.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

__all__ = [
    'LiveDict', 'LiveList', 'live_view', 'plain', 'RawDict', 'RawList', 'Attrs', 'adopt_attributes',
]

_MISSING = object()
_MEMO_KEYS = ('_resolved_parameters_raw', '_resolved_parameters', '_resolved_parameters_symlinks')


def plain(obj: Any) -> Any:
    """Return `obj` with every live container replaced by a plain, detached dict or list."""
    if isinstance(obj, dict):
        return {k: plain(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [plain(v) for v in obj]
    return obj


def _wrap(parent: Any, key: Any, value: Any) -> Any:
    if isinstance(value, dict):
        return LiveDict(value, _parent=parent, _key=key)
    if isinstance(value, list):
        return LiveList(value, _parent=parent, _key=key)
    return value


def live_view(pipe: Any, resolved: Any) -> 'LiveDict':
    """Wrap the resolved parameters of `pipe` in a write-through root `LiveDict`."""
    return LiveDict(resolved, _pipe=pipe)


class _Plainable:
    """Copies and pickles of a live container are plain dicts and lists."""

    def __deepcopy__(self, memo: Any) -> Any:
        return deepcopy(plain(self), memo)

    def __copy__(self) -> Any:
        return type(plain(self))(self)

    def __reduce__(self) -> Any:
        return (type(plain(self)), (plain(self),))


class _Node(_Plainable):
    """Shared plumbing: where this container lives in the raw parameters."""

    _pipe: Any
    _parent: Any
    _key: Any

    def _root_pipe(self) -> Any:
        node = self
        while node._parent is not None:
            node = node._parent
        return node._pipe

    def _raw_node(self) -> Any:
        """
        Return the raw container this view mirrors, creating it when only a reference or a
        template supplied it (a missing list is seeded from this view so indices agree).
        """
        parent = self._parent
        if parent is None:
            pipe = self._pipe
            attrs = pipe.attributes
            raw = attrs.get('parameters', None)
            if not isinstance(raw, dict):
                pipe._attributes['parameters'] = {}
                raw = pipe._attributes['parameters']
            return raw

        raw_parent = parent._raw_node()
        if isinstance(raw_parent, list):
            return raw_parent[self._key]
        raw = raw_parent.get(self._key, _MISSING)
        want = dict if isinstance(self, dict) else list
        if not isinstance(raw, want):
            raw_parent[self._key] = {} if want is dict else plain(self)
            raw = raw_parent[self._key]
        return raw

    def _changed(self) -> None:
        pipe = self._root_pipe()
        for key in _MEMO_KEYS:
            pipe.__dict__.pop(key, None)
        pipe.__dict__.pop('_target', None)


class LiveDict(_Node, dict):
    """A dict of resolved values whose mutations write through to the raw parameters."""

    def __init__(self, data: Any = (), *, _pipe: Any = None, _parent: Any = None, _key: Any = None):
        dict.__init__(self)
        self._pipe = _pipe
        self._parent = _parent
        self._key = _key
        for k, v in dict(data).items():
            dict.__setitem__(self, k, _wrap(self, k, v))

    def __setitem__(self, key: Any, value: Any) -> None:
        value = plain(value)
        self._raw_node()[key] = deepcopy(value)
        dict.__setitem__(self, key, _wrap(self, key, value))
        self._changed()

    def __delitem__(self, key: Any) -> None:
        dict.__delitem__(self, key)
        self._raw_node().pop(key, None)
        self._changed()

    def update(self, *args: Any, **kwargs: Any) -> None:
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def __ior__(self, other: Any) -> 'LiveDict':
        self.update(other)
        return self

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key not in self:
            self[key] = default
        return dict.__getitem__(self, key)

    def pop(self, key: Any, *default: Any) -> Any:
        if key not in self:
            if default:
                return default[0]
            raise KeyError(key)
        value = plain(dict.__getitem__(self, key))
        del self[key]
        return value

    def popitem(self) -> Any:
        key = next(reversed(self))
        return key, self.pop(key)

    def clear(self) -> None:
        for key in list(self):
            del self[key]

    def copy(self) -> dict:
        return dict(self)


class LiveList(_Node, list):
    """A list of resolved values whose mutations replay on the raw list."""

    def __init__(self, data: Any = (), *, _pipe: Any = None, _parent: Any = None, _key: Any = None):
        list.__init__(self)
        self._pipe = _pipe
        self._parent = _parent
        self._key = _key
        list.extend(self, (_wrap(self, i, v) for i, v in enumerate(data)))

    def _rewrap(self) -> None:
        for i, v in enumerate(self):
            if not isinstance(v, (LiveDict, LiveList)) or v._parent is not self or v._key != i:
                list.__setitem__(self, i, _wrap(self, i, plain(v)))

    def _replay(self, name: str, *args: Any) -> Any:
        raw = self._raw_node()
        raw_args = tuple(plain(a) for a in args)
        result = getattr(raw, name)(*raw_args)
        local = getattr(list, name)(self, *raw_args)
        self._rewrap()
        self._changed()
        return result if local is None else local

    def append(self, item: Any) -> None:
        self._replay('append', item)

    def extend(self, items: Any) -> None:
        self._replay('extend', list(items))

    def insert(self, index: int, item: Any) -> None:
        self._replay('insert', index, item)

    def remove(self, item: Any) -> None:
        self._replay('remove', item)

    def pop(self, index: int = -1) -> Any:
        return plain(self._replay('pop', index))

    def clear(self) -> None:
        self._replay('clear')

    def reverse(self) -> None:
        self._replay('reverse')

    def sort(self, *, key: Any = None, reverse: bool = False) -> None:
        raw = self._raw_node()
        raw.sort(key=key, reverse=reverse)
        list.sort(self, key=key, reverse=reverse)
        self._rewrap()
        self._changed()

    def __setitem__(self, index: Any, value: Any) -> None:
        self._replay('__setitem__', index, value)

    def __delitem__(self, index: Any) -> None:
        self._replay('__delitem__', index)

    def __iadd__(self, items: Any) -> 'LiveList':
        self.extend(items)
        return self

    def __imul__(self, n: int) -> 'LiveList':
        self._replay('__imul__', n)
        return self

    def copy(self) -> list:
        return list(self)


# ---------------------------------------------------------------------------------------------
# Raw parameters owned by `_core.Pipe`

def _raw_wrap(parent: Any, key: Any, value: Any) -> Any:
    if isinstance(value, dict):
        return RawDict(value, _parent=parent, _key=key)
    if isinstance(value, list):
        return RawList(value, _parent=parent, _key=key)
    return value


class _RawNode(_Plainable):
    _owner: Any
    _parent: Any
    _key: Any

    def _notify(self) -> None:
        node = self
        while node._parent is not None:
            node = node._parent
        node._sync()

    def _sync(self) -> None:
        """Root only: drop the resolution memo and push the raw map into `_core.Pipe`."""
        pipe = self._owner
        for key in _MEMO_KEYS:
            pipe.__dict__.pop(key, None)
        pipe.__dict__.pop('_target', None)
        core = pipe.__dict__.get('_core_pipe', None)
        if core is not None:
            core.set_raw_parameters(plain(self))


class RawDict(_RawNode, dict):
    """A raw parameters dict whose every mutation, nested ones included, reaches `_core.Pipe`."""

    def __init__(self, data: Any = (), *, _owner: Any = None, _parent: Any = None, _key: Any = None):
        dict.__init__(self)
        self._owner = _owner
        self._parent = _parent
        self._key = _key
        for k, v in dict(data).items():
            dict.__setitem__(self, k, _raw_wrap(self, k, plain(v)))

    def __setitem__(self, key: Any, value: Any) -> None:
        dict.__setitem__(self, key, _raw_wrap(self, key, plain(value)))
        self._notify()

    def __delitem__(self, key: Any) -> None:
        dict.__delitem__(self, key)
        self._notify()

    def update(self, *args: Any, **kwargs: Any) -> None:
        for k, v in dict(*args, **kwargs).items():
            dict.__setitem__(self, k, _raw_wrap(self, k, plain(v)))
        self._notify()

    def __ior__(self, other: Any) -> 'RawDict':
        self.update(other)
        return self

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key not in self:
            self[key] = default
        return dict.__getitem__(self, key)

    def pop(self, key: Any, *default: Any) -> Any:
        if key not in self:
            if default:
                return default[0]
            raise KeyError(key)
        value = plain(dict.pop(self, key))
        self._notify()
        return value

    def popitem(self) -> Any:
        key = next(reversed(self))
        return key, self.pop(key)

    def clear(self) -> None:
        dict.clear(self)
        self._notify()

    def copy(self) -> dict:
        return dict(self)


class RawList(_RawNode, list):
    """A raw parameters list whose mutations reach `_core.Pipe`."""

    def __init__(self, data: Any = (), *, _owner: Any = None, _parent: Any = None, _key: Any = None):
        list.__init__(self)
        self._owner = _owner
        self._parent = _parent
        self._key = _key
        list.extend(self, (_raw_wrap(self, i, plain(v)) for i, v in enumerate(data)))

    def _after(self) -> None:
        for i, v in enumerate(self):
            if not isinstance(v, (RawDict, RawList)) or v._parent is not self or v._key != i:
                list.__setitem__(self, i, _raw_wrap(self, i, plain(v)))
        self._notify()

    def append(self, item: Any) -> None:
        list.append(self, item)
        self._after()

    def extend(self, items: Any) -> None:
        list.extend(self, items)
        self._after()

    def insert(self, index: int, item: Any) -> None:
        list.insert(self, index, item)
        self._after()

    def remove(self, item: Any) -> None:
        list.remove(self, item)
        self._after()

    def pop(self, index: int = -1) -> Any:
        value = plain(list.pop(self, index))
        self._after()
        return value

    def clear(self) -> None:
        list.clear(self)
        self._after()

    def reverse(self) -> None:
        list.reverse(self)
        self._after()

    def sort(self, *, key: Any = None, reverse: bool = False) -> None:
        list.sort(self, key=key, reverse=reverse)
        self._after()

    def __setitem__(self, index: Any, value: Any) -> None:
        list.__setitem__(self, index, value)
        self._after()

    def __delitem__(self, index: Any) -> None:
        list.__delitem__(self, index)
        self._after()

    def __iadd__(self, items: Any) -> 'RawList':
        self.extend(items)
        return self

    def __imul__(self, n: int) -> 'RawList':
        list.__imul__(self, n)
        self._after()
        return self

    def copy(self) -> list:
        return list(self)


class Attrs(_Plainable, dict):
    """
    `Pipe._attributes` under `MRSM_CORE=rust`: assigning `'parameters'` adopts the value as the
    `RawDict` backed by the pipe's `_core.Pipe`.
    """

    def __init__(self, pipe: Any, data: Any = ()):
        dict.__init__(self)
        self._pipe = pipe
        for k, v in dict(data).items():
            self[k] = v

    def __setitem__(self, key: Any, value: Any) -> None:
        if key == 'parameters':
            if isinstance(value, RawDict) and value._owner is self._pipe and value._parent is None:
                pass
            else:
                value = RawDict(plain(value) if isinstance(value, dict) else {}, _owner=self._pipe)
            dict.__setitem__(self, key, value)
            value._sync()
            return
        dict.__setitem__(self, key, value)

    def update(self, *args: Any, **kwargs: Any) -> None:
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key not in self:
            self[key] = default
        return dict.__getitem__(self, key)

    def copy(self) -> dict:
        return dict(self)


def adopt_attributes(pipe: Any, attributes: Any) -> Any:
    """Return `attributes` as an `Attrs` whose parameters are backed by `pipe._core_pipe`."""
    if isinstance(attributes, Attrs) and attributes._pipe is pipe:
        return attributes
    if not isinstance(attributes, dict):
        return attributes
    return Attrs(pipe, attributes)
