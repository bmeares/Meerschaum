"""Private interpreter imports must never initialize or manage a named virtual environment."""


def test_private_import_preserves_lazy_modules_without_package_management(monkeypatch, tmp_path):
    import sys
    import meerschaum.utils.packages as packages

    monkeypatch.setenv('MRSM_API_EXECUTION', '1')
    def forbidden(*args, **kwargs):
        raise AssertionError('Private dependency import entered package management.')
    for name in (
        'Venv', 'is_installed', 'venv_contains_package', 'get_module_path',
        'pip_install', 'init_venv', '_record_import_request', 'need_update',
    ):
        monkeypatch.setattr(packages, name, forbidden)
    monkeypatch.syspath_prepend(str(tmp_path))
    module_name = '_mrsm_prepared_lazy_probe'
    eager_name = '_mrsm_prepared_eager_probe'
    (tmp_path / (module_name + '.py')).write_text('VALUE = 73\n', encoding='utf-8')
    (tmp_path / (eager_name + '.py')).write_text('VALUE = 29\n', encoding='utf-8')
    try:
        lazy = packages.attempt_import(module_name, lazy=True, install=True, check_update=True)
        assert module_name not in sys.modules, 'A lazy import executed before access.'
        assert lazy.VALUE == 73
        assert sys.modules[module_name].VALUE == 73
        eager, loaded = packages.attempt_import(eager_name, module_name, lazy=False)
        assert eager.VALUE == 29
        assert loaded is sys.modules[module_name]
        assert packages.attempt_import('_mrsm_prepared_missing_probe', warn=False) is None
        assert packages.attempt_import('_mrsm_prepared_missing_probe', lazy=False, warn=False) is None
    finally:
        sys.modules.pop(module_name, None)
        sys.modules.pop(eager_name, None)
        packages.__dict__.pop(module_name, None)


def test_ordinary_import_retains_existing_package_lookup(monkeypatch):
    import meerschaum.utils.packages as packages
    monkeypatch.delenv('MRSM_API_EXECUTION', raising=False)
    called = []
    def observed(*args, **kwargs):
        called.append(True)
        return True
    monkeypatch.setattr(packages, 'is_installed', observed)
    monkeypatch.setattr(packages, '_record_import_request', lambda *args, **kwargs: None)
    monkeypatch.setattr(packages, '_is_installed_first_check', {})
    import json
    assert packages.attempt_import('json', lazy=False, warn=False, install=False) is json
    assert called, 'Ordinary dependency import skipped its existing lookup policy.'
