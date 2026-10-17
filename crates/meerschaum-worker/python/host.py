"""Host extensions to the worker protocol, embedded in the Rust binary.

The installed Python SDK still owns connector operations. These operations handle
CLI-only dispatch, output capture, and completion without editing the SDK tree.
"""
import contextlib
import io
import json
import sys

# Worker diagnostics must never share an MCP host's protocol stdout.
sys.stdout = sys.stderr

from meerschaum.worker import main
from meerschaum.worker import _ops

# This interpreter is a worker: while it imports a plugin, no worker is started (a plugin whose
# import syncs a `plugin:` pipe or calls `mrsm.entry()` would recurse without end).
try:
    import meerschaum.plugins as _plugins
    from meerschaum import _core
    _plugins._IN_WORKER = True
    _core.set_in_worker(True)
except Exception:
    pass


class Console(io.TextIOBase):
    def __init__(self, conn, call_id, stream):
        self.conn, self.call_id, self.stream = conn, call_id, stream
    def write(self, text):
        if text:
            self.conn.send({'t': 'chunk', 'id': self.call_id, 'kind': 'text',
                            'value': json.dumps({'stream': self.stream, 'text': text})})
        return len(text)
    def flush(self):
        pass
    def isatty(self):
        return sys.stdin.isatty()


def captured(handler):
    def run(conn, call_id, args, frames):
        streaming = args.get('stream_output', False)
        stdout = Console(conn, call_id, 'stdout') if streaming else io.StringIO()
        stderr = Console(conn, call_id, 'stderr') if streaming else io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            value, result_frames = handler(conn, call_id, args, frames)
        return {'result': value,
                'stdout': '' if streaming else stdout.getvalue(),
                'stderr': '' if streaming else stderr.getvalue()}, result_frames
    return run


def plugin_action(conn, call_id, args, frames):
    from meerschaum import Pipe
    from meerschaum.core.Pipe._sync import InferFetch
    original = Pipe.sync
    def sync(pipe, df=InferFetch, **kwargs):
        return _ops._sync_callback(conn, pipe, df, kwargs)
    Pipe.sync = sync
    try:
        return _ops.op_action(conn, call_id, args, frames)
    finally:
        Pipe.sync = original


def complete(conn, call_id, args, frames):
    from meerschaum.plugins import load_plugins
    from meerschaum.actions import get_completer
    from meerschaum._internal.arguments import parse_line
    load_plugins(skip_if_loaded=True)
    function = get_completer(args['action'])
    if function is None:
        return None, []
    line = args.get('line') or ''
    kwargs = parse_line(line)
    kwargs['action'] = (kwargs.get('action') or [])[1:]
    text = line.rsplit(' ', 1)[-1]
    kwargs.update(text=text, line=line, begin_index=len(line) - len(text), end_index=len(line))
    return list(function(**kwargs)), []


def parse_plugin_args(conn, call_id, args, frames):
    from meerschaum.plugins import load_plugins
    from meerschaum._internal.arguments import parse_arguments
    load_plugins(skip_if_loaded=True)
    kwargs = parse_arguments(args.get('sysargs') or [])
    error = kwargs.get('_argparse_exception')
    if error:
        raise _ops.BadRequest(str(error))
    return {key: kwargs[key] for key in args.get('destinations', []) if key in kwargs}, []


original_manifest = _ops.HANDLERS['build_manifest']


def manifest(conn, call_id, args, frames):
    value, result_frames = original_manifest(conn, call_id, args, frames)
    from meerschaum.actions import get_completer
    value['schema_version'] = 3
    for plugin in value.get('plugins', {}).values():
        for action in plugin.get('actions', []):
            action['has_completer'] = get_completer(action['name']) is not None
    return value, result_frames


_ops.HANDLERS.update({
    'host_action': captured(plugin_action),
    'complete': captured(complete),
    'parse_plugin_args': parse_plugin_args,
    'build_manifest': manifest,
})
sys.exit(main())
