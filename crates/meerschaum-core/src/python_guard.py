def _mrsm_guard():
    """
    Run before anything of the `meerschaum` Python package is imported, by the process the Rust
    program starts (the worker host, the API host). It finds the package WITHOUT importing it
    and refuses (exit 113, `REFUSED_EXIT_CODE`: not a code Python, a shell or uvicorn uses) a copy this program cannot work with: another major version (a stale
    v4 in the user site), a copy that looks like v4 (no `worker` module, no readable version) or
    another worker protocol. Importing v4 would run its start-up, which installs packages
    whatever `MRSM_NO_AUTO_INSTALL` says. Another release of the same major version, with the
    same protocol, starts; one warning names both versions (`MRSM_GUARD_WARN`, set for one
    start per program).

    Environment: `MRSM_WORKER_EXPECT_VERSION` (PEP 440) and `MRSM_WORKER_EXPECT_PROTOCOL`.
    """
    import os
    import re
    import sys
    # Read the warning switch once and drop it: it must not leak to processes this one starts
    # (a plugin that runs `mrsm` would warn from every worker it starts).
    warn = os.environ.pop('MRSM_GUARD_WARN', None)
    # How to retry after fixing the install: a server cannot be told to `reload` from the CLI.
    retry = os.environ.pop('MRSM_GUARD_RETRY_HINT', None) or (
        'Then run `mrsm reload` (or restart this program); the worker is also asked again '
        'after a delay (10 s, doubling to 5 minutes).'
    )
    expected = os.environ.get('MRSM_WORKER_EXPECT_VERSION')
    if not expected:
        return
    expected_protocol = os.environ.get('MRSM_WORKER_EXPECT_PROTOCOL')
    try:
        import importlib.util
        spec = importlib.util.find_spec('meerschaum')
        if spec is None or not spec.origin or spec.origin == 'namespace':
            return
        directory = os.path.dirname(spec.origin)
    except Exception:
        return

    def read(*parts):
        try:
            with open(os.path.join(directory, *parts)) as f:
                return f.read()
        except Exception:
            return None

    def major(version):
        m = re.match(r'\s*(\d+)', version or '')
        return int(m.group(1)) if m else None

    text = read('config', '_version.py')
    m = re.search(r"__version__\s*=\s*['\"]([^'\"]+)", text) if text else None
    found = m.group(1) if m else None
    worker_text = read('worker', '__init__.py')
    m = re.search(r'PROTOCOL_VERSION\s*(?::\s*int\s*)?=\s*(\d+)', worker_text) if worker_text else None
    protocol = m.group(1) if m else None

    problem = None
    if found is not None and major(found) is not None and major(found) != major(expected):
        problem = "is another major version than this program"
    elif found is None and worker_text is None:
        problem = (
            "looks like Meerschaum 4 (it has no `worker` module and no readable version)"
        )
    elif protocol is not None and expected_protocol and protocol != expected_protocol:
        problem = (
            f"speaks worker protocol {protocol}, this program speaks {expected_protocol}"
        )
    python = sys.executable or 'python3'
    if problem is not None:
        copies = []
        for entry in sys.path:
            try:
                candidate = os.path.join(entry or os.getcwd(), 'meerschaum')
                if os.path.isfile(os.path.join(candidate, '__init__.py')) and candidate not in copies:
                    copies.append(candidate)
            except Exception:
                pass
        if len(copies) > 1:
            advice = (
                f"Another copy is installed behind it: uninstall this one ({directory}; "
                f"`{python} -m pip uninstall meerschaum` removes copies one at a time) until the "
                "right one is the one found first."
            )
        else:
            advice = (
                f"Install the matching release (`{python} -m pip install 'meerschaum=={expected}'`), "
                f"or set MRSM_PYTHON to an interpreter that has it."
            )
        print(
            f"The Python package `meerschaum` {found or '(unknown version)'} at {directory} "
            f"cannot be used by this program ({expected}): it {problem}. Nothing was imported or "
            f"installed. {advice} {retry} `mrsm doctor` shows the details.",
            file=sys.stderr,
        )
        sys.exit(113)
    if found is not None and found != expected and warn:
        print(
            f"Warning: the Python package `meerschaum` is {found} (at {directory}) and this "
            f"program is {expected}. They work together while the worker protocol matches, "
            f"but align them: `{python} -m pip install 'meerschaum=={expected}'` (or upgrade "
            "the program).",
            file=sys.stderr,
        )
