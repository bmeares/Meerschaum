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


if __name__ == '__main__':
    unittest.main()
