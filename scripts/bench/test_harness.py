"""Safety and measurement contracts; no Meerschaum imports, servers or benchmark runs."""

import argparse
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import defs
import run as bench
import worker


class HarnessTests(unittest.TestCase):
    def setUp(self):
        cache = Path.home() / '.cache'
        cache.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='mrsm-bench-static-', dir=cache)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_children_have_scratch_defaults_and_no_inherited_connectors_or_tmux(self):
        inherited = {'MRSM_SQL_MAIN': 'unrelated', 'MRSM_CONFIG': 'unrelated',
                     'MRSM_ALLOW_DEFAULT_ROOT': '1', 'PYTHONPATH': 'unrelated',
                     'TMUX': 'user-server', 'TMUX_PANE': 'user-pane'}
        impl = bench.Impl('v4py', self.root, 'sqlite')
        with mock.patch.dict(os.environ, inherited, clear=True):
            env = impl.env()
        for key in ('MRSM_SQL_MAIN', 'MRSM_ALLOW_DEFAULT_ROOT', 'PYTHONPATH', 'TMUX', 'TMUX_PANE'):
            self.assertNotIn(key, env)
        self.assertEqual(json.loads(env['MRSM_CONFIG'])['meerschaum']['instance'], defs.DEST_INSTANCE)
        self.assertEqual(env['MRSM_NO_AUTO_INSTALL'], '1')
        self.assertTrue(Path(env['MRSM_VENVS_DIR']).is_relative_to(self.root))
        self.assertIn('-B', impl.py_cmd('identify'))
        self.assertIn('-E', impl.py_cmd('identify'))

    def test_wheel_console_and_standalone_commands_are_distinct(self):
        with mock.patch.object(bench, 'V5_PY', Path('/isolated/bin/python')):
            wheel = bench.Impl('v5cli', self.root, 'sqlite')
            binary = bench.Impl('v5bin', self.root, 'sqlite')
            self.assertEqual(wheel.cli_cmd('--version'), ['/isolated/bin/mrsm', '--version'])
            self.assertEqual(binary.cli_cmd('--version'), [str(bench.MRSM_BIN), '--version'])

    def test_one_million_rows_uses_all_requested_repeats(self):
        driver = bench.Bench.__new__(bench.Bench)
        driver.args = argparse.Namespace(repeats=3)
        self.assertEqual(driver.reps('1M'), (0, 3))
        self.assertEqual(driver.reps('100k'), (1, 3))

    def test_plan_does_not_launch_children_or_construct_the_runner(self):
        with mock.patch.object(bench.sys, 'argv', ['run.py', '--plan']), \
             mock.patch.object(bench.subprocess, 'run') as process, \
             mock.patch.object(bench, 'Bench') as runner, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(bench.main(), 0)
        process.assert_not_called()
        runner.assert_not_called()
        plan = json.loads(output.getvalue())
        self.assertIn('timescaledb', plan['backends'])
        self.assertIn('v5bin', plan['impls'])
        self.assertEqual(plan['repeats'], 3)

    def test_no_applicable_measurement_is_rejected_before_any_product_work(self):
        for args in (['--impls', 'v5bin', '--cases', 'filter_existing'],
                     ['--sizes', '10k', '--cases', 'get_data']):
            with mock.patch.object(bench.sys, 'argv', ['run.py', *args]), \
                 mock.patch.object(bench, 'Bench') as runner, \
                 contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                bench.main()
            self.assertEqual(raised.exception.code, 2)
            runner.assert_not_called()

    def test_nonzero_worker_exit_cannot_be_hidden_by_a_success_result(self):
        impl = bench.Impl('v4py', self.root, 'sqlite')
        proc = SimpleNamespace(stdout='RESULT {"ok": true, "seconds": 1}\n',
                               stderr='shutdown failed', returncode=1)
        with mock.patch.object(impl, 'run', return_value=proc):
            result = impl.worker('sync')
        self.assertFalse(result['ok'])
        self.assertIn('shutdown failed', result['error'])

    def test_console_identity_requires_record_entrypoint_and_selected_interpreter(self):
        script = self.root / 'mrsm'
        python = self.root / 'python'
        script.write_text(f'#!{python}\nfrom _meerschaum_mrsm import main\nmain()\n')
        digest = base64.urlsafe_b64encode(hashlib.sha256(script.read_bytes()).digest()).decode().rstrip('=')
        entry = SimpleNamespace(hash=SimpleNamespace(mode='sha256', value=digest))
        dist = SimpleNamespace(files=[entry], locate_file=lambda _entry: script,
                               entry_points=[SimpleNamespace(name='mrsm', group='console_scripts',
                                                             value='_meerschaum_mrsm:main')])
        self.assertTrue(worker.console_identity(dist, script, python))
        self.assertIsNone(worker.console_identity(dist, script, self.root / 'other-python'))
        script.write_text(script.read_text() + '# changed after install\n')
        self.assertIsNone(worker.console_identity(dist, script, python))
        dist.entry_points = []
        self.assertIsNone(worker.console_identity(dist, script, python))

    def _persisted_main(self, prior_failed=False, incompatible=False, label='merge'):
        versions = {'v4_version': '4.1.4', 'v5_version': '5.0.0rc1',
                    'v4_python': '3.14.1', 'v5_python': '3.14.1',
                    'v4_dependencies': {'psycopg': '3.3.6'},
                    'v5_dependencies': {'psycopg': '3.3.6'},
                    'v5_extension_sha256': 'a' * 64, 'v5_binary_sha256': 'b' * 64}
        prior_meta = dict(versions, label=label, date=bench.datetime.date.today().isoformat(),
                          repeats=3, sizes=['10k'], cases=['sync'], implementations=['v5bin'],
                          profiling=False, latency_iterations=200, complete=True,
                          backends=['sqlite'], notes=[], load_checks=[])
        if incompatible:
            prior_meta['v5_extension_sha256'] = 'c' * 64
        old = {'meta': prior_meta, 'results': [
            {'backend': 'sqlite', 'case': 'sync_10k', 'impl': 'v5bin',
             'median': None if prior_failed else 1.0, 'failed': int(prior_failed)}]}
        results_dir = self.root / 'results'
        results_dir.mkdir()
        previous = results_dir / f'{bench.datetime.date.today().isoformat()}-{label}.json'
        previous.write_text(json.dumps(old))
        measured = [{'backend': 'postgis', 'case': 'sync_10k', 'impl': 'v5bin',
                     'median': 1.0, 'failed': 0}]
        runner = SimpleNamespace(snapshot=self.root, notes=[], loads=[], databases=[],
                                 results=measured, run_backend=lambda _backend: None)
        args = ['run.py', '--label', label, '--backends', 'postgis', '--sizes', '10k',
                '--cases', 'sync', '--impls', 'v5bin', '--workdir', str(self.root / 'scratch')]
        with mock.patch.object(bench, 'HERE', self.root), \
             mock.patch.object(bench.sys, 'argv', args), \
             mock.patch.object(bench, 'Bench', return_value=runner), \
             mock.patch.object(bench, 'version_info', return_value=versions), \
             mock.patch.object(bench.signal, 'signal'), \
             contextlib.redirect_stdout(io.StringIO()):
            status = bench.main()
        paths = [p for p in results_dir.glob('*.json') if '-checkpoint-' not in p.name]
        return status, old, json.loads(previous.read_text()), [json.loads(p.read_text()) for p in paths]

    def _checkpoint_main(self, backends=('sqlite',), impls=('v5py',),
                         exit_code='no-exit', versions=None, previous=False):
        root = Path(tempfile.mkdtemp(prefix='checkpoint-', dir=self.root))
        versions = versions if versions is not None else {
            'v4_version': '4.1.4', 'v5_version': '5.0.0rc1',
            'v4_dependencies': {'psycopg': '3.3.6'},
            'v5_dependencies': {'psycopg': '3.3.6'},
            'v5_extension_sha256': 'a' * 64, 'harness_sha256': 'b' * 64,
        }
        calls = []
        args = argparse.Namespace(backends=list(backends), impls=list(impls),
                                  cases=['startup', 'sync'], sizes=['10k'])
        cells = bench.expected_cells(args)
        runner = SimpleNamespace(snapshot=root, notes=[], loads=[], results=[], databases=[
            SimpleNamespace(cleanup=lambda: calls.append('cleanup-first')),
            SimpleNamespace(cleanup=lambda: calls.append('cleanup-second')),
        ])

        def measure(stage):
            calls.append(stage)
            if stage == 'postgis' and exit_code != 'no-exit':
                raise SystemExit(exit_code)
            for backend, case, impl in sorted(cells):
                if backend == stage:
                    runner.results.append({'backend': backend, 'case': case, 'impl': impl,
                                           'seconds': [0.7, 1.0, 1.4], 'median': 1.0,
                                           'min': 0.7, 'failed': 0})

        runner.startup = lambda: measure('startup')
        runner.run_backend = measure
        original = {}
        if previous:
            directory = root / 'results'
            directory.mkdir()
            stem = directory / (bench.datetime.date.today().isoformat() + '-checkpoint-test')
            original = {bench.result_path(stem, '.json'):
                        b'{"meta": {"complete": true}, "results": []}',
                        bench.result_path(stem, '.md'): b'original completed report\n'}
            for path, content in original.items():
                path.write_bytes(content)
        argv = ['run.py', '--label', 'checkpoint-test', '--backends', ','.join(backends),
                '--sizes', '10k', '--cases', 'startup,sync', '--impls', ','.join(impls),
                '--workdir', str(root / 'scratch')]
        with mock.patch.object(bench, 'HERE', root), \
             mock.patch.object(bench.sys, 'argv', argv), \
             mock.patch.object(bench, 'Bench', return_value=runner), \
             mock.patch.object(bench, 'version_info', return_value=versions), \
             mock.patch.object(bench.signal, 'signal'), \
             mock.patch.object(bench.subprocess, 'run', side_effect=AssertionError('no children')), \
             contextlib.redirect_stdout(io.StringIO()):
            status = bench.main()
        documents = {p.name: json.loads(p.read_text()) for p in (root / 'results').glob('*.json')}
        checkpoints = [doc for name, doc in documents.items() if '-checkpoint-w17cbench_' in name]
        finals = [doc for name, doc in documents.items() if '-checkpoint-w17cbench_' not in name]
        return status, checkpoints, finals, calls, original

    def test_checkpoints_preserve_raw_samples_and_provenance_without_claiming_completion(self):
        status, checkpoints, finals, calls, _ = self._checkpoint_main()
        self.assertEqual(status, 0)
        self.assertEqual({doc['meta']['checkpoint_after'] for doc in checkpoints}, {'startup', 'sqlite'})
        for doc in checkpoints:
            self.assertFalse(doc['meta']['complete'])
            self.assertIn('final workload and cleanup are not accepted', str(doc['meta']['notes']))
            self.assertEqual(doc['meta']['v5_extension_sha256'], 'a' * 64)
            self.assertEqual(doc['meta']['harness_sha256'], 'b' * 64)
            self.assertTrue(all(row['seconds'] == [0.7, 1.0, 1.4] for row in doc['results']))
        startup = next(doc for doc in checkpoints if doc['meta']['checkpoint_after'] == 'startup')
        self.assertEqual({row['backend'] for row in startup['results']}, {'startup'})
        self.assertEqual(len(finals), 1)
        self.assertTrue(finals[0]['meta']['complete'])
        self.assertEqual(len(finals[0]['results']), 2)
        self.assertTrue(all(row['seconds'] == [0.7, 1.0, 1.4] for row in finals[0]['results']))
        self.assertEqual(calls, ['startup', 'sqlite', 'cleanup-second', 'cleanup-first'])

    def test_system_exit_retains_completed_checkpoints_and_samples_and_cleans_up(self):
        for code, expected in [('registration failed', 1), (None, 1), (0, 1), (143, 143)]:
            with self.subTest(code=code):
                status, checkpoints, finals, calls, _ = self._checkpoint_main(
                    backends=('sqlite', 'postgis'), exit_code=code)
                self.assertEqual(status, expected)
                self.assertEqual({doc['meta']['checkpoint_after'] for doc in checkpoints}, {'startup', 'sqlite'})
                self.assertEqual(len(finals), 1)
                self.assertFalse(finals[0]['meta']['complete'])
                self.assertIn('SystemExit', str(finals[0]['meta']['notes']))
                for doc in [*checkpoints, *finals]:
                    self.assertFalse(doc['meta']['complete'])
                    self.assertEqual(len(doc['results']), 1 if doc['meta'].get('checkpoint_after') == 'startup' else 2)
                    self.assertTrue(all(row['seconds'] == [0.7, 1.0, 1.4] for row in doc['results']))
                self.assertEqual(calls, ['startup', 'sqlite', 'postgis', 'cleanup-second', 'cleanup-first'])

    def test_early_exit_cannot_overwrite_completed_result_or_markdown(self):
        status, _checkpoints, finals, _calls, original = self._checkpoint_main(
            backends=('sqlite', 'postgis'), exit_code='registration failed', previous=True)
        self.assertEqual(status, 1)
        for path, content in original.items():
            self.assertEqual(path.read_bytes(), content)
        self.assertEqual(len(finals), 2)
        self.assertEqual(sum(doc['meta']['complete'] for doc in finals), 1)

    def test_postgres_prerequisites_fail_before_measurements(self):
        for backend, impls, v4, v5 in [
            ('postgis', ('v5py',), {}, {'psycopg': '3.3.6'}),
            ('timescaledb', ('v5bin',), {}, {'psycopg': '3.3.6'}),
            ('postgis', ('v5py',), {'psycopg': '3.3.6'}, {}),
        ]:
            with self.subTest(backend=backend, impls=impls, v4=v4):
                versions = {'v4_dependencies': v4, 'v5_dependencies': v5}
                status, checkpoints, finals, calls, _ = self._checkpoint_main(
                    backends=(backend,), impls=impls, versions=versions)
                self.assertEqual(status, 1)
                self.assertEqual(checkpoints, [])
                self.assertEqual(calls, ['cleanup-second', 'cleanup-first'])
                self.assertFalse(finals[0]['meta']['complete'])
                self.assertEqual(finals[0]['results'], [])
                self.assertIn('missing psycopg', str(finals[0]['meta']['notes']))

    def test_driver_guard_does_not_require_unused_environments(self):
        for backend, impls, versions in [
            ('sqlite', ('v5py',), {}),
            ('postgis', ('v4py',), {'v4_dependencies': {'psycopg': '3.3.6'}}),
        ]:
            with self.subTest(backend=backend, impls=impls):
                status, checkpoints, finals, calls, _ = self._checkpoint_main(
                    backends=(backend,), impls=impls, versions=versions)
                self.assertEqual(status, 0)
                self.assertEqual(len(checkpoints), 2)
                self.assertTrue(finals[0]['meta']['complete'])
                self.assertEqual(calls, ['startup', backend, 'cleanup-second', 'cleanup-first'])

    def test_persisted_merge_cannot_hide_a_prior_failure(self):
        status, original, previous, documents = self._persisted_main(prior_failed=True)
        self.assertEqual(status, 1)
        self.assertEqual(previous, original)
        new = next(doc for doc in documents if doc != original)
        self.assertFalse(new['meta']['complete'])
        self.assertTrue(any(row['failed'] for row in new['results']))

    def test_persisted_merge_rejects_a_different_native_artifact(self):
        status, original, previous, documents = self._persisted_main(incompatible=True)
        self.assertEqual(status, 1)
        self.assertEqual(previous, original)
        new = next(doc for doc in documents if doc != original)
        self.assertFalse(new['meta']['complete'])
        self.assertIn('v5_extension_sha256', str(new['meta']['notes']))

    def test_compatible_persisted_merge_keeps_both_backends(self):
        status, _original, current, documents = self._persisted_main()
        self.assertEqual(status, 0)
        self.assertTrue(current['meta']['complete'])
        self.assertEqual({row['backend'] for row in current['results']}, {'sqlite', 'postgis'})
        self.assertEqual(len(documents), 1)

    def test_dotted_labels_are_not_treated_as_filename_extensions(self):
        status, _original, _current, documents = self._persisted_main(label='rc1.confirm')
        self.assertEqual(status, 0)
        self.assertEqual(len(documents), 1)
        plain = self.root / 'results' / f'{bench.datetime.date.today().isoformat()}-rc1.json'
        self.assertFalse(plain.exists())

    def test_different_latency_sampling_counts_cannot_merge(self):
        meta = {'latency_iterations': 200}
        prior = {'meta': {'latency_iterations': 100, 'complete': True}, 'results': []}
        with self.assertRaisesRegex(ValueError, 'latency_iterations'):
            bench.merge_compatible(meta, [], prior)

    def test_database_collision_never_claims_or_drops_the_database(self):
        with mock.patch.object(defs, 'PG_DATABASE', 'w17cbench_collision'):
            db = bench.Db('postgis', self.root)
        with mock.patch.object(db, '_psql', side_effect=RuntimeError('already exists')) as psql:
            with self.assertRaises(RuntimeError):
                db.prepare()
            db.cleanup()
        self.assertEqual(psql.call_count, 1)
        self.assertFalse(db.created_database)

    def test_cleanup_only_removes_the_database_this_instance_created(self):
        with mock.patch.object(defs, 'PG_DATABASE', 'w17cbench_owned'):
            db = bench.Db('timescaledb', self.root)
        with mock.patch.object(db, '_psql', return_value='') as psql:
            db.cleanup()
            psql.assert_not_called()
            db.prepare()
            with mock.patch.object(defs, 'PG_DATABASE', 'w17cbench_someone_else'):
                db.cleanup()
            psql.assert_called_with('DROP DATABASE w17cbench_owned WITH (FORCE)', database='testdb')
            psql.reset_mock()
            db.cleanup()
            psql.assert_not_called()

    def test_ordinary_database_name_is_refused_before_any_connection(self):
        with mock.patch.object(defs, 'PG_DATABASE', 'ordinary'):
            db = bench.Db('postgis', self.root)
        with mock.patch.object(db, '_psql') as psql:
            with self.assertRaises(ValueError):
                db.prepare()
            psql.assert_not_called()

    def test_timescale_backend_selects_only_the_test_endpoint(self):
        env = defs.connector_env('timescaledb', str(self.root))
        for value in env.values():
            conn = json.loads(value)
            self.assertEqual(conn['flavor'], 'timescaledb')
            self.assertEqual(conn['port'], 5439)

    def test_wave_ten_comparison_maps_old_binary_name_and_ignores_non_case_metadata(self):
        old = json.loads((bench.HERE / 'results' / '2026-10-15-w10-hardening-cli.json').read_text())
        row = next(r for r in old['results'] if r['impl'] == 'v5cli' and r.get('median'))
        current = dict(row, impl='v5bin', median=row['median'] * 1.3)
        slow = bench.baseline_regressions([current])
        self.assertEqual(len(slow), 1)
        self.assertEqual(slow[0]['impl'], 'v5bin')
        self.assertAlmostEqual(slow[0]['ratio'], 1.3)

    def test_public_error_scrubbing_and_partial_failure_reporting(self):
        error = f'{self.root}/root/trace at postgresql://user:secret@localhost:5449/db'
        clean = bench.scrub_public({'error': error}, self.root)
        self.assertNotIn(str(self.root), clean['error'])
        self.assertNotIn('secret', clean['error'])
        meta = {'label': 'static', 'date': '2000-01-01', 'v4_version': '4.1.4',
                'v5_version': '5.0.0rc1', 'python': '3.14', 'os': 'Linux', 'cpus': 1,
                'repeats': 3, 'backends': ['sqlite']}
        result = {'backend': 'sqlite', 'case': 'sync_10k', 'impl': 'v5bin',
                  'median': 1.0, 'failed': 1, 'error': 'one run failed'}
        text = bench.render_markdown(meta, [result])
        self.assertIn('Failed runs', text)
        self.assertIn('one run failed', text)
        self.assertIn('v5cli (wheel)', text)
        table = [line for line in text.splitlines() if line.startswith('|')]
        self.assertEqual({line.count('|') for line in table}, {10})


class ThreeImplementationTests(unittest.TestCase):
    """The opt-in v3 adapter: commands, selection, coverage and reporting (no children run)."""

    def setUp(self):
        cache = Path.home() / '.cache'
        cache.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='mrsm-bench-static-v3-', dir=cache)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_v3_is_opt_in_and_does_not_change_the_default_selection(self):
        self.assertEqual(defs.IMPLS, ('v4py', 'v4cli', 'v5py', 'v5cli', 'v5bin'))
        self.assertEqual(defs.V3_IMPLS, ('v3py', 'v3cli'))
        self.assertTrue(set(defs.V3_IMPLS) <= set(defs.ALL_IMPLS))
        self.assertIn('v3py', defs.PY_IMPLS)
        self.assertIn('v3cli', defs.CLI_IMPLS)

    def test_v3_commands_use_the_v3_interpreter_in_isolated_mode(self):
        with mock.patch.object(bench, 'V3_PY', Path('/isolated-v3/bin/python')), \
             mock.patch.object(bench, 'V5_PY', Path('/isolated-v5/bin/python')), \
             mock.patch.dict(os.environ, {'MRSM_SQL_MAIN': 'x', 'TMUX': 'y', 'PYTHONPATH': 'z'},
                             clear=True):
            py = bench.Impl('v3py', self.root, 'sqlite')
            cli = bench.Impl('v3cli', self.root, 'sqlite')
            self.assertTrue(py.is_py)
            self.assertFalse(cli.is_py)
            self.assertEqual(py.py_cmd('identify')[:4], ['/isolated-v3/bin/python', '-B', '-s', '-E'])
            self.assertEqual(
                cli.cli_cmd('--version'),
                ['/isolated-v3/bin/python', '-B', '-s', '-E', '-m', 'meerschaum', '--version'],
            )
            env = cli.env()
        self.assertEqual(env['MRSM_PYTHON'], '/isolated-v3/bin/python')
        self.assertEqual(env['UV_PYTHON'], '/isolated-v3/bin/python')
        for key in ('MRSM_SQL_MAIN', 'TMUX', 'PYTHONPATH'):
            self.assertNotIn(key, env)
        self.assertTrue(Path(env['MRSM_ROOT_DIR']).is_relative_to(self.root))
        self.assertTrue(Path(env['MRSM_VENVS_DIR']).is_relative_to(self.root))
        self.assertEqual(env['MRSM_NO_AUTO_INSTALL'], '1')

    def test_v4_and_v5_commands_are_unchanged_by_the_v3_adapter(self):
        with mock.patch.object(bench, 'V3_PY', Path('/isolated-v3/bin/python')), \
             mock.patch.object(bench, 'REF_PY', Path('/ref/bin/python')), \
             mock.patch.object(bench, 'V5_PY', Path('/isolated-v5/bin/python')):
            self.assertEqual(bench.Impl('v4py', self.root, 'sqlite').py_cmd('x')[:4],
                             ['/ref/bin/python', '-B', '-s', '-E'])
            self.assertEqual(bench.Impl('v4cli', self.root, 'sqlite').cli_cmd('x')[-3:],
                             ['-m', 'meerschaum', 'x'])
            self.assertEqual(bench.Impl('v5py', self.root, 'sqlite').py_cmd('x')[:3],
                             ['/isolated-v5/bin/python', '-B', '-s'])
            self.assertNotIn('-E', bench.Impl('v5py', self.root, 'sqlite').py_cmd('x')[:4])

    def test_missing_v3_environment_is_reported_not_faked(self):
        with mock.patch.object(bench, 'V3_PY', self.root / 'absent' / 'python'):
            for name in defs.V3_IMPLS:
                self.assertIn('--v3-python', bench.Impl(name, self.root, 'sqlite').available())

    def test_expected_cells_cover_v3_new_cases_and_startup_only_selection(self):
        args = argparse.Namespace(
            backends=['sqlite'], impls=['v3py', 'v3cli', 'v5py'], sizes=['100k'],
            cases=['startup', 'sync', 'resync', 'get_data_100k'], startup_only=['v3cli'],
        )
        cells = bench.expected_cells(args)
        self.assertIn(('startup', 'startup_import', 'v3py'), cells)
        self.assertIn(('startup', 'startup_version', 'v3cli'), cells)
        self.assertIn(('sqlite', 'resync_100k', 'v3py'), cells)
        self.assertIn(('sqlite', 'get_data_100k', 'v5py'), cells)
        self.assertNotIn(('sqlite', 'get_data_100k', 'v3cli'), cells)
        self.assertEqual({c for c in cells if c[2] == 'v3cli'},
                         {('startup', 'startup_version', 'v3cli'),
                          ('startup', 'startup_show_version', 'v3cli')})

    def test_startup_only_must_be_selected_and_needs_the_startup_case(self):
        for extra in (['--impls', 'v5py', '--startup-only-impls', 'v3cli'],
                      ['--impls', 'v3cli', '--cases', 'sync', '--startup-only-impls', 'v3cli']):
            with mock.patch.object(bench.sys, 'argv', ['run.py', *extra]), \
                 mock.patch.object(bench, 'Bench') as runner, \
                 contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                bench.main()
            self.assertEqual(raised.exception.code, 2)
            runner.assert_not_called()

    def test_v3_selection_is_accepted_by_the_plan(self):
        argv = ['run.py', '--plan', '--impls', 'v3py,v3cli,v4py,v5py', '--cases', 'sync,resync,startup',
                '--startup-only-impls', 'v3cli']
        with mock.patch.object(bench.sys, 'argv', argv), \
             mock.patch.object(bench.subprocess, 'run') as process, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(bench.main(), 0)
        process.assert_not_called()
        self.assertIn('v3py', json.loads(output.getvalue())['impls'])

    def test_different_v3_artifacts_cannot_merge(self):
        meta = {'v3_version': '3.5.2'}
        prior = {'meta': {'v3_version': '3.5.1', 'complete': True}, 'results': []}
        with self.assertRaisesRegex(ValueError, 'v3_version'):
            bench.merge_compatible(meta, [], prior)

    def test_report_with_v3_has_v3_columns_ratios_and_na_for_missing_cases(self):
        meta = {'label': 'static', 'date': '2000-01-01', 'v3_version': '3.5.2', 'v4_version': '4.1.4',
                'v5_version': '5.0.0rc2', 'python': '3.14', 'os': 'Linux', 'cpus': 1, 'repeats': 3,
                'backends': ['sqlite'], 'implementations': ['v3py', 'v4py', 'v5py'],
                'hardware': {'cpu_model': 'Test CPU', 'logical_cpus': 8, 'ram_gib': 16}}

        def row(case, impl, median):
            return {'backend': 'sqlite', 'case': case, 'impl': impl, 'median': median, 'failed': 0}

        results = [row('sync_10k', 'v3py', 4.0), row('sync_10k', 'v4py', 2.0),
                   row('sync_10k', 'v5py', 1.0), row('resync_100k', 'v5py', 1.0),
                   row('resync_100k', 'v3py', 0.5)]
        text = bench.render_markdown(meta, results)
        self.assertIn('- v3: meerschaum 3.5.2', text)
        self.assertIn('Test CPU, 8 logical CPUs, 16 GiB RAM', text)
        self.assertIn('| case | v3py | v4py | v5py | v5py/v3py | v5py/v4py |', text)
        self.assertIn('| sync_10k | 4.000 | 2.000 | 1.000 | 0.25 | 0.50 |', text)
        self.assertIn('| resync_100k | 0.500 | n/a | 1.000 | **2.00** |  |', text)
        self.assertIn('- sqlite `resync_100k`: v5py is 2.00x v3py', text)

    def test_fresh_reference_root_is_initialized_before_measurement_and_v5_is_untouched(self):
        with mock.patch.object(bench, 'V3_PY', Path('/isolated-v3/bin/python')):
            v3 = bench.Impl('v3py', self.root, 'sqlite')
            outcomes = [SimpleNamespace(returncode=1, stdout='', stderr='RecursionError'),
                        SimpleNamespace(returncode=0, stdout='', stderr=''),
                        SimpleNamespace(returncode=0, stdout='', stderr=''),
                        SimpleNamespace(returncode=0, stdout='', stderr='')]
            with mock.patch.object(v3, 'run', side_effect=outcomes) as run:
                v3.bootstrap()
            self.assertEqual(run.call_count, 3)
            self.assertEqual(run.call_args[0][0][-2:], ['-c', 'import meerschaum'])
            stuck = SimpleNamespace(returncode=0, stdout='', stderr='warning')
            with mock.patch.object(v3, 'run', return_value=stuck), self.assertRaises(RuntimeError):
                v3.bootstrap()
            v5 = bench.Impl('v5py', self.root, 'sqlite')
            with mock.patch.object(v5, 'run') as run:
                v5.bootstrap()
            run.assert_not_called()

    def test_hardware_summary_has_no_host_identifier(self):
        info = bench.hardware_summary()
        self.assertEqual(set(info) - {'logical_cpus', 'cpu_model', 'ram_gib'}, set())


if __name__ == '__main__':
    unittest.main()
