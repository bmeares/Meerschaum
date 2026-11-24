"""Exercise the installed Python worker's Windows named pipe using its real wire protocol."""

from concurrent.futures import ThreadPoolExecutor
from multiprocessing.connection import Listener
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest


def exercise_pipe():
    from meerschaum.worker._frames import encode_message
    from meerschaum.worker._server import Connection, _connect

    frame = bytes(range(256)) * 8193
    request = {'t': 'call', 'id': 1, 'op': 'ping'}
    response = {'t': 'result', 'id': 1, 'value': 'pong'}
    with Listener(family='AF_PIPE') as listener, ThreadPoolExecutor(max_workers=2) as threads:
        with _connect(listener.address) as stream, listener.accept() as host:
            connection = Connection(stream)
            reader = threads.submit(connection.reader)
            host.send_bytes(encode_message(request, [frame]))
            message, frames = connection.calls.get(timeout=5)
            assert message == {**request, 'frames': 1} and frames == [frame]

            # The worker's reader waits for the next call while its dispatcher replies.
            writer = threads.submit(connection.send, response, [frame])
            expected = encode_message(response, [frame])
            received = bytearray()
            while len(received) < len(expected):
                assert host.poll(5), 'The worker could not reply during its pending read.'
                received.extend(host.recv_bytes())
            writer.result(timeout=5)
            assert received == expected

            host.close()
            reader.result(timeout=5)
            assert connection.closed.is_set()
            assert stream.read(5) == b''


class TestWindowsWorkerPipe(unittest.TestCase):
    @unittest.skipUnless(os.name == 'nt', 'Windows named-pipe transport')
    def test_reply_during_read_and_disconnect(self):
        python_flags = ['-I'] if sys.flags.isolated else []
        with tempfile.TemporaryDirectory() as root:
            plugins = Path(root) / 'plugins'
            plugins.mkdir()
            env = {**os.environ, 'MRSM_ROOT_DIR': root, 'MRSM_PLUGINS_DIR': str(plugins),
                   'MRSM_NO_AUTO_INSTALL': '1'}
            proc = subprocess.Popen(
                [sys.executable, *python_flags, str(Path(__file__).resolve()), 'exercise'],
                env=env,
            )
            try:
                self.assertEqual(proc.wait(timeout=35), 0)
            finally:
                if proc.poll() is None:
                    # Terminate the venv launcher and the interpreter blocked on the pipe.
                    subprocess.run(['taskkill', '/PID', str(proc.pid), '/T', '/F'], timeout=15)
                    proc.wait(timeout=15)


if __name__ == '__main__':
    if sys.argv[1:] == ['exercise']:
        exercise_pipe()
    else:
        unittest.main()
