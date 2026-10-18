"""Probe an API server with stdlib only. usage: api_probe.py <base_url> <spec_tokens.json> <flavor>"""
import base64, json, sys, urllib.request, urllib.parse, urllib.error

base = sys.argv[1].rstrip('/')
tokens = json.loads(open(sys.argv[2]).read())
flavor = sys.argv[3]
users = tokens['users'][flavor]
toks = tokens['tokens'][flavor]
inst = f"sql:trn_{flavor}"
rows = []


def call(method, path, headers=None, body=None, form=None, label=None):
    url = base + path
    data = None
    h = dict(headers or {})
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        h['Content-Type'] = 'application/x-www-form-urlencoded'
    elif body is not None:
        data = json.dumps(body).encode()
        h['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            code, txt = r.status, r.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        code, txt = e.code, e.read().decode('utf-8', 'replace')
    except Exception as e:
        code, txt = -1, repr(e)
    try:
        js = json.loads(txt)
    except Exception:
        js = None
    rows.append({'label': label or f"{method} {path}", 'code': code, 'json': js, 'text': txt[:300]})
    return code, js, txt


def bearer(t):
    return {'Authorization': f'Bearer {t}'}


# --- JWT login -------------------------------------------------------------------------------------
c, js, _ = call('POST', '/login', form={'username': users['admin']['username'], 'password': users['admin']['password']}, label='login admin')
admin_jwt = (js or {}).get('access_token')
c, js, _ = call('POST', '/login', form={'username': users['bob']['username'], 'password': users['bob']['password']}, label='login bob')
bob_jwt = (js or {}).get('access_token')
call('POST', '/login', form={'username': users['bob']['username'], 'password': 'wrong'}, label='login bob wrong password')
call('GET', '/users/me', headers=bearer(admin_jwt), label='GET /users/me (admin jwt)')
call('GET', '/users/me', headers=bearer(bob_jwt), label='GET /users/me (bob jwt)')
call('GET', '/pipes', headers=bearer(admin_jwt), label='GET /pipes (admin jwt)')
call('GET', '/pipes', label='GET /pipes (no auth)')
call('GET', '/version', label='GET /version (no auth)')
call('GET', '/info', headers=bearer(admin_jwt), label='GET /info')
call('GET', '/users', headers=bearer(admin_jwt), label='GET /users (admin jwt)')
call('GET', '/users', headers=bearer(bob_jwt), label='GET /users (bob jwt)')
# --- tokens (API keys) ---------------------------------------------------------------------------------
admin_key, bob_key = toks['trn-admin-all']['api_key'], toks['trn-bob-ro']['api_key']
call('GET', '/pipes', headers=bearer(admin_key), label='GET /pipes (admin api key)')
call('GET', '/pipes', headers=bearer(bob_key), label='GET /pipes (bob read-only api key)')
call('GET', '/users', headers=bearer(admin_key), label='GET /users (admin api key)')
call('GET', '/users', headers=bearer(bob_key), label='GET /users (bob read-only key: no users scope)')
call('GET', '/pipes/plugin_trn_plug/readings/None/data?instance_keys=' + urllib.parse.quote(inst), headers=bearer(bob_key), label='GET pipe data (bob key)')
call('GET', f'/pipes/{inst}/kitchen/None/rowcount?instance_keys=' + urllib.parse.quote(inst), headers=bearer(bob_key), label='GET pipe rowcount (bob key)')
# register a pipe: bob (read-only) must be refused, admin allowed
reg = f'/pipes/plugin:trn_plug/apicreated/None/register?instance_keys={urllib.parse.quote(inst)}'
call('POST', reg, headers=bearer(bob_key), body={'columns': {'datetime': 'ts'}}, label='POST register pipe (bob read-only key)')
call('POST', reg, headers=bearer(admin_key), body={'columns': {'datetime': 'ts'}}, label='POST register pipe (admin key)')
call('DELETE', f'/pipes/plugin:trn_plug/apicreated/None/delete?instance_keys={urllib.parse.quote(inst)}', headers=bearer(bob_key), label='DELETE pipe (bob read-only key)')
call('DELETE', f'/pipes/plugin:trn_plug/apicreated/None/delete?instance_keys={urllib.parse.quote(inst)}', headers=bearer(admin_key), label='DELETE pipe (admin key)')
call('GET', '/tokens', headers=bearer(admin_jwt), label='GET /tokens (admin)')
call('GET', '/plugins', headers=bearer(admin_jwt), label='GET /plugins')
call('GET', '/jobs', headers=bearer(admin_jwt), label='GET /jobs')
call('GET', '/connectors', headers=bearer(admin_jwt), label='GET /connectors')
call('GET', '/trn/ping', label='GET /trn/ping (api plugin route)')
c, js, _ = call('GET', '/openapi.json', label='GET /openapi.json')
paths = sorted((js or {}).get('paths', {}).keys())
print(json.dumps({'rows': rows, 'paths': paths}, default=str))
