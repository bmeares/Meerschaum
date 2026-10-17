"""Passwords use Rust in every façade mode and retain released-v4 hash vectors."""

import json
import ctypes
from pathlib import Path

import pytest


FIXTURES = Path(__file__).resolve().parents[1] / 'crates/meerschaum-core/tests/fixtures/users.json'
SCHEMES = FIXTURES.with_name('password_schemes.json')


@pytest.mark.parametrize('mode', ['python', 'rust'])
def test_password_facade_uses_native_with_reference_hashes(mode, monkeypatch):
    import base64
    import hashlib
    import _hashlib
    from meerschaum import _core
    from meerschaum._core import _gate
    from meerschaum.core.User import hash_password, verify_password

    if _core.load() is None:
        pytest.skip('requires the native extension')

    def forbidden(*args, **kw):
        pytest.fail('password façade reached the deleted Python hashing implementation')

    monkeypatch.setattr(hashlib, 'pbkdf2_hmac', forbidden)
    monkeypatch.setattr(_hashlib, 'new', forbidden)
    long_bits = ctypes.sizeof(ctypes.c_long) * 8
    long_max = 2 ** (long_bits - 1) - 1
    previous = (_gate._MODE, _gate._FEATURES)
    try:
        _gate.set_mode(mode)
        for vector in json.loads(FIXTURES.read_text())['hashes']:
            expected = vector['hash']
            _, _, rounds, salt, _ = expected.split('$')
            salt = base64.b64decode(salt.replace('.', '+') + '=' * (-len(salt) % 4))
            assert hash_password(vector['password'], salt=salt, rounds=int(rounds)) == expected
            assert verify_password(vector['password'], expected)
            assert not verify_password(vector['password'] + 'wrong', expected)
        assert not verify_password(None, expected)
        assert not verify_password('password', None)
        assert hash_password(None, salt=b'fixed', rounds=1) == hash_password('', salt=b'fixed', rounds=1)
        with pytest.raises(ValueError, match='iteration value'):
            hash_password('password', rounds=0)
        with pytest.raises(ValueError, match='iteration value'):
            hash_password('password', rounds=-1)
        message = (
            'Python int too large to convert to C long'
            if 2 ** 31 > long_max else 'iteration value is too great'
        )
        with pytest.raises(OverflowError, match=message):
            hash_password('password', rounds=2 ** 31)
        for malformed in (
            'invalid', '$pbkdf2-sha256$bad$YQ$Yg', '$pbkdf2-sha256$1$A$Yg',
        ):
            with pytest.warns(UserWarning, match='Failed to extract context'):
                assert not verify_password('password', malformed)
        assert verify_password('p', '$pbkdf2-sha1$1$YQ$Fjdb0.tvFFKt9C1qqYPG1X541e0')
        for case in json.loads(SCHEMES.read_text())['cases']:
            assert verify_password('p', case['hash']) is case['valid'], case['algorithm']
            assert not verify_password('wrong', case['hash']), case['algorithm']
        for rounds in ('2147483648', '4294967296', '999999999999999999999999'):
            message = (
                'Python int too large to convert to C long'
                if int(rounds) > long_max else 'iteration value is too great'
            )
            with pytest.raises(OverflowError, match=message):
                verify_password('p', f'$pbkdf2-sha256${rounds}$YQ$Yg')
        for rounds in (2 ** 100, -(2 ** 100)):
            with pytest.raises(OverflowError, match='Python int too large to convert to C long'):
                hash_password('p', salt=b'a', rounds=rounds)
            with pytest.raises(OverflowError, match='Python int too large to convert to C long'):
                verify_password('p', f'$pbkdf2-sha256${rounds}$YQ$Yg')
    finally:
        _gate._MODE, _gate._FEATURES = previous


@pytest.mark.parametrize('mode', ['python', 'rust'])
@pytest.mark.parametrize('algorithm', ['BOGUS', '', 'shake_128', 'shake_256', 'bad\0hash'])
def test_password_digest_errors_match_runtime(mode, algorithm, monkeypatch):
    """Released v4 propagates hashlib errors, including its argument validation order."""
    import hashlib
    from meerschaum import _core
    from meerschaum._core import _gate
    from meerschaum.core.User import verify_password

    if _core.load() is None:
        pytest.skip('requires the native extension')
    expected = []
    for rounds in (0, -1, 1, 2 ** 31, 2 ** 100, -(2 ** 100)):
        try:
            hashlib.pbkdf2_hmac(algorithm, b'p', b'a', rounds)
        except Exception as exc:
            expected.append((rounds, type(exc), exc.args))
        else:
            pytest.fail(f'expected invalid digest or rounds: {algorithm!r}, {rounds}')

    def forbidden(*args, **kw):
        pytest.fail('digest error handling called Python PBKDF2')

    monkeypatch.setattr(hashlib, 'pbkdf2_hmac', forbidden)
    previous = (_gate._MODE, _gate._FEATURES)
    try:
        _gate.set_mode(mode)
        for rounds, exception_type, exception_args in expected:
            with pytest.raises(exception_type) as captured:
                verify_password('p', f'$pbkdf2-{algorithm}${rounds}$YQ$Yg')
            assert type(captured.value) is exception_type
            assert captured.value.args == exception_args
        # Context extraction precedes digest lookup and all iteration validation.
        for malformed in (
            f'$pbkdf2-{algorithm}$bad$YQ$Yg',
            f'$pbkdf2-{algorithm}${2 ** 100}$A$Yg',
        ):
            with pytest.warns(UserWarning, match='Failed to extract context'):
                assert not verify_password('p', malformed)
    finally:
        _gate._MODE, _gate._FEATURES = previous
