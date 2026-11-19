def _mrsm_sandbox():
    """
    Restrict an isolated team's worker to its filesystem sandbox (5.1) before anything of the
    `meerschaum` package, a plugin or user code is imported: run after `_mrsm_guard()`, before
    the host. A no-op unless `MRSM_TEAM_SANDBOX` names the team's rules. The package's
    `meerschaum/_internal/team_sandbox.py` (standard library only) is loaded by its path and
    applies them through the extension; its report goes into the handshake. A package without
    that file reports the sandbox as not enforced, which the Rust host judges by mode.
    """
    import os
    import sys
    if not os.environ.get('MRSM_TEAM_SANDBOX'):
        return
    socket = None
    if '--socket' in sys.argv:
        index = sys.argv.index('--socket')
        socket = sys.argv[index + 1] if index + 1 < len(sys.argv) else None
    if socket and socket.startswith('fd:'):
        ### 5.1: an inherited channel; nothing to connect to.
        socket = None
    try:
        import importlib.util
        spec = importlib.util.find_spec('meerschaum')
        path = os.path.join(os.path.dirname(spec.origin), '_internal', 'team_sandbox.py')
        boot_spec = importlib.util.spec_from_file_location('_mrsm_team_sandbox_boot', path)
        module = importlib.util.module_from_spec(boot_spec)
        boot_spec.loader.exec_module(module)
        module.boot(socket=socket)
    except Exception as e:
        sys._mrsm_team_sandbox = {
            'mode': 'require',
            'enforced': False,
            'reason': (
                "this interpreter's `meerschaum` package cannot apply the team sandbox "
                f"(install the release of the server): {e}"
            ),
        }
