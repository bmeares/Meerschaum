# A v4-style plugin used by the transition harness (synthetic).
__version__ = '1.0.0'
required = ['tabulate']

import os
import datetime
import meerschaum as mrsm
from meerschaum.plugins import make_action, add_plugin_argument, pre_sync_hook, post_sync_hook, api_plugin


def _log(msg):
    p = os.environ.get('TRN_LOG')
    if p:
        with open(p, 'a') as f:
            f.write(f"{os.getpid()} {msg}\n")


def fetch(pipe, begin=None, end=None, debug=False, **kw):
    from meerschaum.utils.packages import attempt_import
    pd = attempt_import('pandas')
    import tabulate
    n = int(os.environ.get('TRN_FETCH_N', '5'))
    base = datetime.datetime(2024, 3, 1)
    rows = [
        {'ts': base + datetime.timedelta(hours=i), 'sensor': f's{i % 2}', 'reading': i * 0.5}
        for i in range(n)
    ]
    _log(f"fetch n={n} tabulate={tabulate.__file__}")
    return pd.DataFrame(rows)


@make_action
def trn_hello(action=None, trn_flag=None, **kw):
    """Say hello (harness action)."""
    _log(f"action flag={trn_flag}")
    return True, f"hello flag={trn_flag}"


add_plugin_argument('--trn-flag', type=str, help='A harness flag.')


@pre_sync_hook
def _pre(pipe, **kw):
    _log(f"pre_sync {pipe.keys()}")


@post_sync_hook
def _post(pipe, success_tuple, **kw):
    _log(f"post_sync {pipe.keys()} {success_tuple[0]}")


@api_plugin
def init(app):
    @app.get('/trn/ping')
    def ping():
        return {'pong': True}
