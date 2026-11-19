"""Both native socket mechanisms support the same confined extension profiles."""

import errno
import socket
import sys

import pytest

from meerschaum.worker._plugin_invoke import _require_execution
from meerschaum.worker._plugin_registry import _require_metadata


@pytest.mark.parametrize(
    'native_report',
    [
        {
            'abi': 9,
            'level': 'filesystem and sockets',
            'unix_sockets': 'landlock',
            'features': ['scope_signal', 'scope_abstract_unix', 'resolve_unix'],
        },
        {
            'abi': 7,
            'level': 'filesystem only',
            'unix_sockets': 'seccomp',
            'socket_probe': 'denied',
            'features': ['scope_signal', 'scope_abstract_unix'],
        },
    ],
    ids=['landlock-sockets', 'seccomp-sockets'],
)
def test_extension_profiles_accept_native_socket_boundaries(monkeypatch, native_report):
    monkeypatch.setattr(
        sys, '_mrsm_team_sandbox',
        {'enforced': True, 'canary': 'denied', **native_report},
        raising=False,
    )
    monkeypatch.setenv('MRSM_API_EXECUTION', '1')
    monkeypatch.delenv('MRSM_METADATA_EXECUTION', raising=False)
    _require_execution()

    network_checks = []

    def denied_socket(family, kind):
        network_checks.append((family, kind))
        raise OSError(errno.EAFNOSUPPORT, 'Metadata network is unavailable')

    monkeypatch.setattr(socket, 'socket', denied_socket)
    monkeypatch.setenv('MRSM_METADATA_EXECUTION', '1')
    _require_metadata()
    assert network_checks == [(socket.AF_INET, socket.SOCK_STREAM)]
