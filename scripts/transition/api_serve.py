"""Start a `mrsm start api` server for a probe (random high port) and tear it down. Used by the harness."""
import os, signal, socket, subprocess, time, urllib.request


def free_port(lo=59100, hi=59900):
    import random
    for _ in range(100):
        p = random.randint(lo, hi)
        with socket.socket() as s:
            if s.connect_ex(('127.0.0.1', p)) != 0:
                return p
    raise RuntimeError('no free port')


class Server:
    def __init__(self, cmd, env, cwd, log, extra=()):
        self.port = free_port()
        self.cmd = cmd + ['start', 'api', '--host', '127.0.0.1', '--port', str(self.port), *extra]
        self.env, self.cwd, self.log = dict(env), cwd, log
        # v4 starts a separate webterm process on `webterm.port` (default 8765, a port other tools use): give it its own
        import json
        cfg = json.loads(self.env.get('MRSM_CONFIG') or '{}')
        cfg.setdefault('webterm', {})['port'] = free_port(59901, 59990)
        self.env['MRSM_CONFIG'] = json.dumps(cfg)
        self.p = None

    def __enter__(self):
        self.f = open(self.log, 'wb')
        self.p = subprocess.Popen(self.cmd, env=self.env, cwd=str(self.cwd), stdout=self.f, stderr=subprocess.STDOUT,
                                  stdin=subprocess.DEVNULL, start_new_session=True)
        t0 = time.time()
        while time.time() - t0 < 120:
            if self.p.poll() is not None:
                raise RuntimeError(f"server exited rc={self.p.returncode}; see {self.log}")
            try:
                urllib.request.urlopen(f'http://127.0.0.1:{self.port}/', timeout=2)
                return self
            except urllib.error.HTTPError:
                return self
            except Exception:
                time.sleep(0.5)
        raise RuntimeError('server did not start')

    def __exit__(self, *a):
        try:
            os.killpg(self.p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self.p.wait(timeout=15)
        except Exception:
            try:
                os.killpg(self.p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.f.close()
