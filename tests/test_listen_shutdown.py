"""A `sync pipes --listen` job shuts down cleanly when it is quit or killed."""
import threading
import time
import os
import json
import uuid

import pytest
import meerschaum as mrsm


def _binary():
    from meerschaum._internal.binary import find_binary
    return find_binary()


@pytest.mark.skipif(not os.environ.get('MRSM_TEST_MQTT_PORT'), reason='Isolated MQTT broker required.')
@pytest.mark.skipif(_binary() is None, reason='The `mrsm` binary is required to run jobs.')
@pytest.mark.parametrize('stop_method', ['quit', 'kill'])
@pytest.mark.parametrize('repeat', range(3))
def test_real_mqtt_listen_shutdown(stop_method, repeat):
    from meerschaum.jobs import Job
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
    job = Job(
        'listen-' + uuid.uuid4().hex[:8],
        ['sync', 'pipes', '-c', 'mqtt:release_test', '-m', metric, '-i', 'sql:release_test',
         '--listen', '--nopretty', '--debug'],
        executor_keys='local',
    )
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
        ok, msg = job.start()
        assert ok, msg
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            logs = job.get_logs() or ''
            if 'Listening for new data' in logs:
                break
            time.sleep(0.05)
        assert 'Listening for new data' in logs, logs
        publisher = threading.Thread(target=publish, daemon=True)
        publisher.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and pipe.get_rowcount() < 1:
            time.sleep(0.05)
        assert pipe.get_rowcount() >= 1, job.get_logs()
        if stop_method == 'quit':
            ok, msg = job.stop(timeout_seconds=5)
        else:
            ok, msg = job.daemon.kill(timeout=5)
        logs = job.get_logs() or ''
        print(logs)
        assert ok, msg
        assert 'release unlocked lock' not in logs, logs
    finally:
        stopped.set()
        if publisher:
            publisher.join(timeout=5)
        if job.status != 'stopped':
            job.daemon.kill(timeout=3)
        job.delete()
        pipe.delete()
