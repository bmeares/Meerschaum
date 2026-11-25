"""Selection of Python entries for the plugins namespace."""
import json
import os

# The Rust host supplies the native implementations that opened successfully.
shadowed_plugins = frozenset(json.loads(os.environ.get('MRSM_SHADOWED_PYTHON_PLUGINS', '[]')))


def native_only(path):
    """Whether a path belongs only to the native loader."""
    return path.suffix == '.rs' or (
        (path / 'plugin.toml').is_file() and not (path / '__init__.py').is_file()
    )


def python_namespace_paths(directories):
    """List configured paths that may be exposed in the Python plugins package."""
    return [
        path.resolve()
        for directory in directories if directory.is_dir()
        for path in directory.iterdir()
        if not path.name.startswith('.') and path.name not in ('__pycache__', '__init__.py')
        and not native_only(path)
    ]
