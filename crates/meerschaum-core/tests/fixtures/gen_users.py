#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate `users.json`: passlib-format hashes from the Python `hash_password` (fixed and random
salts, several round counts), `verify_password` results for edge cases, `valid_username`
results, and a token API key.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_users.py

The reverse direction (Rust-produced hashes verified by Python) runs live in
`tests/users_parity.rs`, which calls `verify.py` next to this file.
"""

import json
import pathlib
import sys
import uuid

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import _stubs  # noqa: E402

_stubs.isolate()

from meerschaum.core.User._User import hash_password, verify_password  # noqa: E402
from meerschaum.connectors.sql._users import valid_username  # noqa: E402
from meerschaum.core.Token._Token import Token  # noqa: E402

PASSWORDS = ['hunter2', '', 'pässwörd ✓', 'a' * 100, ' spaces ', 'emoji 🐍']


def main():
    hashes = []
    for i, password in enumerate(PASSWORDS):
        salt = bytes(range(i, i + 16))
        for rounds in (1, 1000, 29000):
            hashes.append({'password': password, 'hash': hash_password(password, salt=salt, rounds=rounds)})
        # Fixed salts throughout: the fixture must regenerate byte-identically from the
        # parity reference. Random-salt behavior is covered by the Rust side's own tests.
        hashes.append({'password': password, 'hash': hash_password(password, salt=salt[::-1], rounds=5000)})
    hashes.append({'password': 'default-rounds', 'hash': hash_password('default-rounds', salt=b'fixed-salt-16byt')})
    hashes.append({'password': 'token-secret', 'hash': hash_password('token-secret', salt=b'token-salt-16byt', rounds=100_000)})

    good = hash_password('pw', salt=b'0123456789abcdef', rounds=10)
    edge = {
        'standard_alphabet': good.replace('.', '+'),
        'corrupt_parts': '$pbkdf2-sha256$10$abc',
        'bad_rounds': good.replace('$10$', '$ten$'),
        'whitespace_rounds': good.replace('$10$', '$ 10 $'),
        'bad_base64_len': good[:-1] + good[-1] + 'A',
        'junk_chars': good.replace('$10$', '$10$**'),
        'sha512': '$pbkdf2-sha512$10$' + good.split('$')[3] + '$' + good.split('$')[4],
    }
    edge_results = {}
    for name, h in edge.items():
        try:
            edge_results[name] = {'hash': h, 'ok': verify_password('pw', h)}
        except Exception as e:
            edge_results[name] = {'hash': h, 'error': type(e).__name__}

    usernames = {}
    for name in ['alice', 'a.b-c_d@e', '', 'x' * 61, 'bad name', 'ünïcode', 'semi;colon']:
        usernames[name] = list(valid_username(name))[0]

    token = Token(label='device', instance='sql:main')
    token.id = uuid.UUID('12345678-1234-5678-1234-567812345678')
    token.secret = 'abcDEF123'
    with open(HERE / 'users.json', 'w', encoding='utf-8') as f:
        json.dump({
            'hashes': hashes,
            'edge_cases': edge_results,
            'valid_username': usernames,
            'api_key': {'id': str(token.id), 'secret': token.secret, 'key': token.get_api_key()},
        }, f, indent=2, ensure_ascii=False)
        f.write('\n')


if __name__ == '__main__':
    main()
