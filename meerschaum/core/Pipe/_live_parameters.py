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
    'new_raw_root',
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

_MISSING_PATH = object()


def _raw_wrap(core: Any, path: tuple, value: Any, gen: int) -> Any:
    if isinstance(value, dict):
        node = RawDict(_core=core, _path=path)
        node._load(value, gen)
        return node
    if isinstance(value, list):
        node = RawList(_core=core, _path=path)
        node._load(value, gen)
        return node
    return value


class _RawNode(_Plainable):
    """
    A read-through cache of one node of the raw parameters, which live in `_core.Pipe`.

    The node knows its path in the Rust map. Every write goes to Rust (`set_path` / `del_path`)
    first; every read compares the node's generation to the core's mutation counter and reloads
    the node from Rust when they differ, so a node reflects writes made through any other view
    (and through `Pipe.update_parameters`, the setters, a metadata load...). Reloading keeps the
    identity of child containers whose path still holds a container of the same kind.

    Nodes are addressed by path: a list index shifts when elements are inserted before it.
    """

    _core: Any
    _path: tuple
    _gen: int

    def _fresh(self) -> None:
        core = self._core
        if self._gen != core.generation:
            self._reload()

    def _reload(self) -> None:
        core = self._core
        try:
            gen, value = core.snapshot(self._path)
        except KeyError:
            ### The node was removed or replaced by another kind: it is detached, keep its data.
            self._gen = core.generation
            return
        self._load(value, gen)

    def _sync_gen(self) -> None:
        self._gen = self._core.generation


class RawDict(_RawNode, dict):
    """A raw parameters dict: reads come from `_core.Pipe`, every mutation goes to it."""

    def __init__(self, data: Any = (), *, _core: Any = None, _path: tuple = (), _gen: int = 0):
        dict.__init__(self)
        self._core = _core
        self._path = tuple(_path)
        self._gen = _gen
        if data:
            for k, v in dict(data).items():
                dict.__setitem__(self, k, plain(v))

    def _load(self, value: Any, gen: int) -> None:
        old = dict(dict.items(self))
        dict.clear(self)
        core = self._core
        base = self._path
        for k, v in value.items():
            child = old.get(k)
            if isinstance(v, dict) and isinstance(child, RawDict) and child._path == base + (k,):
                child._load(v, gen)
                dict.__setitem__(self, k, child)
            elif isinstance(v, list) and isinstance(child, RawList) and child._path == base + (k,):
                child._load(v, gen)
                dict.__setitem__(self, k, child)
            else:
                dict.__setitem__(self, k, _raw_wrap(core, base + (k,), v, gen))
        self._gen = gen

    # -- reads --------------------------------------------------------------------------------
    def __getitem__(self, key: Any) -> Any:
        self._fresh()
        return dict.__getitem__(self, key)

    def get(self, key: Any, default: Any = None) -> Any:
        self._fresh()
        return dict.get(self, key, default)

    def __contains__(self, key: Any) -> bool:
        self._fresh()
        return dict.__contains__(self, key)

    def __iter__(self) -> Any:
        self._fresh()
        return dict.__iter__(self)

    def __reversed__(self) -> Any:
        self._fresh()
        return dict.__reversed__(self)

    def __len__(self) -> int:
        self._fresh()
        return dict.__len__(self)

    def keys(self) -> Any:
        self._fresh()
        return dict.keys(self)

    def values(self) -> Any:
        self._fresh()
        return dict.values(self)

    def items(self) -> Any:
        self._fresh()
        return dict.items(self)

    def __eq__(self, other: Any) -> bool:
        self._fresh()
        return dict.__eq__(self, other)

    def __ne__(self, other: Any) -> bool:
        self._fresh()
        return dict.__ne__(self, other)

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        self._fresh()
        return dict.__repr__(self)

    def __or__(self, other: Any) -> dict:
        self._fresh()
        return dict(self) | other

    def __ror__(self, other: Any) -> dict:
        self._fresh()
        return other | dict(self)

    def copy(self) -> dict:
        self._fresh()
        return dict(dict.items(self))

    # -- writes -------------------------------------------------------------------------------
    def __setitem__(self, key: Any, value: Any) -> None:
        self._fresh()
        value = plain(value)
        core = self._core
        core.set_path(self._path + (key,), value)
        self._gen = core.generation
        dict.__setitem__(self, key, _raw_wrap(core, self._path + (key,), value, self._gen))

    def __delitem__(self, key: Any) -> None:
        self._fresh()
        if not dict.__contains__(self, key):
            raise KeyError(key)
        core = self._core
        core.del_path(self._path + (key,))
        self._gen = core.generation
        dict.__delitem__(self, key)

    def _replace_all(self, data: dict) -> None:
        core = self._core
        if self._path:
            core.set_path(self._path, data)
        else:
            core.set_raw_parameters(data)
        self._load(data, core.generation)

    def update(self, *args: Any, **kwargs: Any) -> None:
        self._fresh()
        merged = plain(self)
        merged.update(plain(dict(*args, **kwargs)))
        self._replace_all(merged)

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
        value = plain(dict.__getitem__(self, key))
        del self[key]
        return value

    def popitem(self) -> Any:
        self._fresh()
        if not dict.__len__(self):
            raise KeyError('popitem(): dictionary is empty')
        key = next(reversed(dict.keys(self)))
        return key, self.pop(key)

    def clear(self) -> None:
        self._fresh()
        self._replace_all({})


class RawList(_RawNode, list):
    """A raw parameters list: reads come from `_core.Pipe`, every mutation goes to it."""

    def __init__(self, data: Any = (), *, _core: Any = None, _path: tuple = (), _gen: int = 0):
        list.__init__(self)
        self._core = _core
        self._path = tuple(_path)
        self._gen = _gen
        if data:
            list.extend(self, (plain(v) for v in data))

    def _load(self, value: Any, gen: int) -> None:
        old = list(list.__iter__(self))
        list.clear(self)
        core = self._core
        base = self._path
        for i, v in enumerate(value):
            child = old[i] if i < len(old) else None
            if isinstance(v, dict) and isinstance(child, RawDict) and child._path == base + (i,):
                child._load(v, gen)
                list.append(self, child)
            elif isinstance(v, list) and isinstance(child, RawList) and child._path == base + (i,):
                child._load(v, gen)
                list.append(self, child)
            else:
                list.append(self, _raw_wrap(core, base + (i,), v, gen))
        self._gen = gen

    # -- reads --------------------------------------------------------------------------------
    def __getitem__(self, index: Any) -> Any:
        self._fresh()
        return list.__getitem__(self, index)

    def __iter__(self) -> Any:
        self._fresh()
        return list.__iter__(self)

    def __reversed__(self) -> Any:
        self._fresh()
        return list.__reversed__(self)

    def __len__(self) -> int:
        self._fresh()
        return list.__len__(self)

    def __contains__(self, item: Any) -> bool:
        self._fresh()
        return list.__contains__(self, item)

    def index(self, *args: Any) -> int:
        self._fresh()
        return list.index(self, *args)

    def count(self, item: Any) -> int:
        self._fresh()
        return list.count(self, item)

    def __eq__(self, other: Any) -> bool:
        self._fresh()
        return list.__eq__(self, other)

    def __ne__(self, other: Any) -> bool:
        self._fresh()
        return list.__ne__(self, other)

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        self._fresh()
        return list.__repr__(self)

    def __add__(self, other: Any) -> list:
        self._fresh()
        return list(list.__iter__(self)) + list(other)

    def __radd__(self, other: Any) -> list:
        self._fresh()
        return list(other) + list(list.__iter__(self))

    def __mul__(self, n: int) -> list:
        self._fresh()
        return list(list.__iter__(self)) * n

    __rmul__ = __mul__

    def copy(self) -> list:
        self._fresh()
        return list(list.__iter__(self))

    # -- writes -------------------------------------------------------------------------------
    def _apply(self, name: str, *args: Any) -> Any:
        """Replay a `list` method on a plain copy, store the result in Rust, reload the node."""
        self._fresh()
        items = plain(list(list.__iter__(self)))
        result = getattr(list, name)(items, *(plain(a) for a in args))
        core = self._core
        core.set_path(self._path, items)
        self._load(items, core.generation)
        return plain(result)

    def append(self, item: Any) -> None:
        self._apply('append', item)

    def extend(self, items: Any) -> None:
        self._apply('extend', list(items))

    def insert(self, index: int, item: Any) -> None:
        self._apply('insert', index, item)

    def remove(self, item: Any) -> None:
        self._apply('remove', item)

    def pop(self, index: int = -1) -> Any:
        return self._apply('pop', index)

    def clear(self) -> None:
        self._apply('clear')

    def reverse(self) -> None:
        self._apply('reverse')

    def sort(self, *, key: Any = None, reverse: bool = False) -> None:
        self._fresh()
        items = plain(list(list.__iter__(self)))
        items.sort(key=key, reverse=reverse)
        core = self._core
        core.set_path(self._path, items)
        self._load(items, core.generation)

    def __setitem__(self, index: Any, value: Any) -> None:
        self._apply('__setitem__', index, value)

    def __delitem__(self, index: Any) -> None:
        self._apply('__delitem__', index)

    def __iadd__(self, items: Any) -> 'RawList':
        self.extend(items)
        return self

    def __imul__(self, n: int) -> 'RawList':
        self._apply('__imul__', n)
        return self


class Attrs(_Plainable, dict):
    """
    `Pipe._attributes` under `MRSM_CORE=rust`. The `'parameters'` entry is always the pipe's one
    root `RawDict`; assigning it stores the value in `_core.Pipe` (the root, a read-through
    view of the Rust map, keeps its identity).
    """

    def __init__(self, pipe: Any, data: Any = (), root: Any = None):
        dict.__init__(self)
        self._pipe = pipe
        self._root = root
        for k, v in dict(data).items():
            self[k] = v

    def __setitem__(self, key: Any, value: Any) -> None:
        if key == 'parameters':
            root = self._root
            if value is not root:
                root._replace_all(plain(value) if isinstance(value, dict) else {})
            dict.__setitem__(self, key, root)
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
        return dict(dict.items(self))


def adopt_attributes(pipe: Any, attributes: Any) -> Any:
    """Return `attributes` as an `Attrs` whose parameters are backed by `pipe._core_pipe`."""
    if isinstance(attributes, Attrs) and attributes._pipe is pipe:
        return attributes
    if not isinstance(attributes, dict):
        return attributes
    return Attrs(pipe, attributes, pipe._raw_root)


def new_raw_root(core: Any) -> RawDict:
    """The root `RawDict` over `core` (a `_core.Pipe`), loaded from its current raw parameters."""
    root = RawDict(_core=core)
    root._reload()
    return root
