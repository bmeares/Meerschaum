"""Exercise daemonized listen shutdown with a background subscription thread."""
import importlib
from pathlib import Path
import sys
import threading
import time
import os
import json
import uuid

import pytest
import meerschaum as mrsm


def _listen_target(thread_kind):
    from meerschaum._internal.entry import entry_without_daemon
    from meerschaum.utils.threading import STOP_EVENT, Thread
    sync = importlib.import_module('meerschaum.actions.sync')
    sync.get_pipes = lambda **kwargs: {}
    def subscribe(**kwargs):
        cls = Thread if thread_kind == 'core' else threading.Thread
        cls(target=lambda: STOP_EVENT.wait(), daemon=True).start()
        return {mrsm.Pipe('test', 'subscription', temporary=True): (True, 'Subscribed.')}
    sync._pipes_lap = subscribe
    print(f'LISTEN_SOURCE={mrsm.__file__}; PYTHON={sys.version}', flush=True)
    return entry_without_daemon(['sync', 'pipes', '--listen', '--nopretty', '--debug'])


@pytest.mark.parametrize('thread_kind', ['stdlib', 'core'])
@pytest.mark.parametrize('stop_method', ['quit', 'kill'])
def test_daemon_listen_quit(thread_kind, stop_method):
    from meerschaum.utils.daemon import Daemon
    daemon = Daemon(target=_listen_target, target_args=[thread_kind],
                    properties={'logs': {'stdin': False}})
    try:
        ok, msg = daemon.run()
        assert ok, msg
        deadline = time.monotonic() + 20
        logs = ''
        while time.monotonic() < deadline:
            logs += daemon.rotating_log.read()
            if 'Listening for new data' in logs:
                break
            time.sleep(0.05)
        assert 'Listening for new data' in logs, logs
        assert str(Path(mrsm.__file__).resolve()) in logs
        ok, msg = getattr(daemon, stop_method)(timeout=5)
        assert ok, msg
        logs += daemon.rotating_log.read()
        print(logs)
        assert 'release unlocked lock' not in logs, logs
    finally:
        if daemon.status != 'stopped':
            daemon.kill(timeout=3)
        daemon.cleanup()


def _failing_action():
    raise RuntimeError('test action failure')


def _failing_action_target():
    from meerschaum._internal.entry import _do_action_wrapper
    return _do_action_wrapper(_failing_action, None, debug=False)


def test_daemon_action_logs_traceback_without_debug():
    from meerschaum.utils.daemon import Daemon
    daemon = Daemon(target=_failing_action_target, properties={'logs': {'stdin': False}})
    try:
        ok, msg = daemon.run()
        assert ok, msg
        deadline = time.monotonic() + 15
        result = None
        while time.monotonic() < deadline:
            result = (daemon.read_properties() or {}).get('result')
            if result is not None:
                break
            time.sleep(0.05)
        assert result is not None and result[0] is False
        assert '--debug' not in result[1]
        logs = daemon.rotating_log.read()
        assert 'Traceback (most recent call last)' in logs
        assert '_failing_action' in logs and 'test action failure' in logs
    finally:
        if daemon.status != 'stopped':
            daemon.kill(timeout=3)
        daemon.cleanup()


def test_foreground_action_keeps_concise_error(monkeypatch, capsys):
    from meerschaum._internal.static import STATIC_CONFIG
    monkeypatch.delenv(STATIC_CONFIG['environment']['daemon_id'], raising=False)
    result = _failing_action_target()
    assert result[0] is False and '--debug' in result[1]
    assert 'Traceback' not in capsys.readouterr().err


def _mqtt_listen_target(metric):
    from meerschaum._internal.entry import entry_without_daemon
    assert mrsm.Plugin('mqtt-connector').module is not None
    print(f'LISTEN_SOURCE={mrsm.__file__}; PYTHON={sys.version}', flush=True)
    return entry_without_daemon(['sync', 'pipes', '-c', 'mqtt:release_test', '-m', metric,
                                 '-i', 'sql:release_test', '--listen', '--nopretty', '--debug'])


@pytest.mark.skipif(not os.environ.get('MRSM_TEST_MQTT_PORT'), reason='Isolated MQTT broker required.')
@pytest.mark.parametrize('stop_method', ['quit', 'kill'])
@pytest.mark.parametrize('repeat', range(3))
def test_real_mqtt_listen_shutdown(stop_method, repeat):
    from meerschaum.utils.daemon import Daemon
    from paho.mqtt.publish import single
    assert mrsm.Plugin('mqtt-connector').module is not None
    conn = mrsm.get_connector('sql:release_test')
    assert conn.flavor == 'sqlite'
    metric = 'listen_' + uuid.uuid4().hex
    topic = 'release_test/' + metric
    pipe = mrsm.Pipe('mqtt:release_test', metric, instance=conn, cache=False,
                     columns={'id': 'sequence'}, parameters={'fetch': {'topic': topic}})
    ok, msg = pipe.register()
    assert ok, msg
    daemon = Daemon(target=_mqtt_listen_target, target_args=[metric],
                    properties={'logs': {'stdin': False}})
    stopped = threading.Event()
    publisher = None
    def publish():
        sequence = 0
        while not stopped.wait(0.02):
            single(topic, json.dumps({'sequence': sequence}), hostname='127.0.0.1',
                   port=int(os.environ['MRSM_TEST_MQTT_PORT']))
            sequence += 1
    logs = ''
    try:
        ok, msg = daemon.run()
        assert ok, msg
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            logs += daemon.rotating_log.read()
            if 'Listening for new data' in logs:
                break
            time.sleep(0.05)
        assert 'Listening for new data' in logs, logs
        publisher = threading.Thread(target=publish, daemon=True)
        publisher.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and pipe.get_rowcount() < 1:
            time.sleep(0.05)
        assert pipe.get_rowcount() >= 1, logs + daemon.rotating_log.read()
        ok, msg = getattr(daemon, stop_method)(timeout=5)
        logs += daemon.rotating_log.read()
        print(logs)
        assert ok, msg
        assert 'release unlocked lock' not in logs, logs
    finally:
        stopped.set()
        if publisher:
            publisher.join(timeout=5)
        if daemon.status != 'stopped':
            daemon.kill(timeout=3)
        daemon.cleanup()
        pipe.delete()
