"""
`prompt()` inside a job announces its wait (`input.stdin.block`) where the job's supervisor
cannot see it (anything but Linux), and removes the marker after reading.
"""

import pathlib
import sys

import pytest

from meerschaum.utils import prompt as prompt_module


class _Daemon:
    def __init__(self, path: pathlib.Path):
        self.blocking_stdin_file_path = path / 'input.stdin.block'


def test_linux_leaves_the_marker_to_the_supervisor(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, 'platform', 'linux')
    assert prompt_module._announce_wait(_Daemon(tmp_path)) is None
    assert not (tmp_path / 'input.stdin.block').exists()


@pytest.mark.parametrize('platform', ['darwin', 'freebsd14'])
def test_elsewhere_the_prompt_announces_its_wait(tmp_path, monkeypatch, platform):
    monkeypatch.setattr(sys, 'platform', platform)
    marker = prompt_module._announce_wait(_Daemon(tmp_path))
    assert marker == tmp_path / 'input.stdin.block'
    assert marker.exists()


def test_no_daemon_or_unwritable_marker_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, 'platform', 'darwin')
    assert prompt_module._announce_wait(None) is None
    assert prompt_module._announce_wait(_Daemon(tmp_path / 'missing')) is None


def test_the_daemon_prompt_removes_the_marker_after_reading(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, 'platform', 'darwin')
    daemon = _Daemon(tmp_path)
    daemon.prompt_kwargs_file_path = tmp_path / 'prompt_kwargs.json'
    seen = []

    def fake_input():
        seen.append(daemon.blocking_stdin_file_path.exists())
        return 'answer'

    import meerschaum.utils.daemon as daemon_module
    monkeypatch.setattr(daemon_module, 'running_in_daemon', lambda: True)
    monkeypatch.setattr(daemon_module, 'get_current_daemon', lambda: daemon)
    monkeypatch.setattr('builtins.input', fake_input)
    assert prompt_module.prompt('Name?') == 'answer'
    assert seen == [True]
    assert not daemon.blocking_stdin_file_path.exists()
