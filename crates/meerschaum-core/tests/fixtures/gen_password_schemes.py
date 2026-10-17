#!/usr/bin/env python3
"""Generate one-round legacy password hashes through the isolated released-v4 oracle.

Run: scripts/parity/run.sh crates/meerschaum-core/tests/fixtures/gen_password_schemes.py
"""

import base64
import hashlib
import json
import os
from pathlib import Path

if not os.environ.get('MRSM_PARITY_REFERENCE'):
    raise RuntimeError('Run this generator with scripts/parity/run.sh.')

import meerschaum
from meerschaum.core.User import verify_password

assert meerschaum.__version__ == os.environ['MRSM_PARITY_REFERENCE']

ALGORITHMS = (
    'blake2b', 'blake2s', 'md5', 'md5-sha1', 'ripemd160', 'sha1', 'sha224', 'sha256',
    'sha384', 'sha3_224', 'sha3_256', 'sha3_384', 'sha3_512', 'sha512', 'sha512_224',
    'sha512_256', 'sm3',
    'blake2b512', 'blake2s256', 'ripemd', 'rmd160',
    '1.2.840.113549.2.5', '1.3.14.3.2.26', '1.3.36.3.2.1', '1.2.156.10197.1.401',
    '2.16.840.1.101.3.4.2.1', '2.16.840.1.101.3.4.2.2', '2.16.840.1.101.3.4.2.3',
    '2.16.840.1.101.3.4.2.4', '2.16.840.1.101.3.4.2.5', '2.16.840.1.101.3.4.2.6',
    '2.16.840.1.101.3.4.2.7', '2.16.840.1.101.3.4.2.8', '2.16.840.1.101.3.4.2.9',
    '2.16.840.1.101.3.4.2.10', '1.3.6.1.4.1.1722.12.2.1.16', '1.3.6.1.4.1.1722.12.2.2.8',
)


def main():
    cases = []
    for algorithm in ALGORITHMS:
        checksum = hashlib.pbkdf2_hmac(algorithm, b'p', b'a', 1)
        encoded = base64.b64encode(checksum).rstrip(b'=').replace(b'+', b'.').decode()
        value = f'$pbkdf2-{algorithm}$1$YQ${encoded}'
        cases.append({
            'algorithm': algorithm, 'hash': value, 'valid': verify_password('p', value),
        })
    output = Path(__file__).with_name('password_schemes.json')
    output.write_text(json.dumps({'password': 'p', 'cases': cases}, indent=2) + '\n')


if __name__ == '__main__':
    main()
