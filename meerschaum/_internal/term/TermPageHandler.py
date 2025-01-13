#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the terminal page handler class.
"""

from __future__ import annotations
from typing import Any

import meerschaum as mrsm
from meerschaum.utils.warnings import warn
tornado_web = mrsm.attempt_import('tornado.web', lazy=False)
terminado = mrsm.attempt_import('terminado', lazy=False)

tmux_suffix = mrsm.get_config('system', 'webterm', 'tmux', 'session_suffix')


class TermPageHandler(tornado_web.RequestHandler):

    def get(self, term_name):
        term_manager = self.application.settings['term_manager']
        shell_command = [
            cmd.replace('MRSM_SESSION', term_name + tmux_suffix)
            for cmd in term_manager.shell_command
        ]
        terminal = term_manager.new_terminal(shell_command=shell_command)
        terminal.term_name = term_name
        term_manager.terminals[term_name] = terminal
        term_manager.start_reading(terminal)
        return self.render(
            "termpage.html",
            static=self.static_url,
            ws_url_path=f"/websocket/{term_name}",
        )


class CustomTermSocket(terminado.TermSocket):

    def open(self, url_component: Any = None) -> None:
        super(terminado.TermSocket, self).open(url_component)
        url_component = (
            url_component.decode("utf-8")
            if isinstance(url_component, bytes)
            else url_component
        )
        self.term_name = url_component
        shell_command = [
            cmd.replace('MRSM_SESSION', self.term_name + tmux_suffix)
            for cmd in self.term_manager.shell_command
        ]
        if self.term_name not in self.term_manager.terminals:
            self.terminal = self.term_manager.new_terminal(shell_command=shell_command)
            self.terminal.term_name = self.term_name
            self.term_manager.terminals[self.term_name] = self.terminal
            self.term_manager.start_reading(self.terminal)
        else:
            self.terminal = self.term_manager.terminals[self.term_name]
        self.terminal.clients.append(self)
        self.send_json_message(["setup", {}])
        buffered = ""
        preopen_buffer = self.terminal.read_buffer.copy()
        while True:
            if not preopen_buffer:
                break
            s = preopen_buffer.popleft()
            buffered += s
        if buffered:
            self.on_pty_read(buffered)
