#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the prompt_toolkit Completer base class.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional

from meerschaum.utils.packages import attempt_import
prompt_toolkit = attempt_import('prompt_toolkit', lazy=False, install=True)

from prompt_toolkit.completion import Completer, Completion
from meerschaum.actions.arguments import parse_line
from meerschaum.actions import get_shell, get_completer

class ShellCompleter(Completer):
    def get_completions(self, document, completer_event):
        """
        Bridge the built-in cmd completer with the prompt_toolkit completer system.
        """
        shell_actions = [a[3:] for a in dir(get_shell()) if a.startswith('do_')]
        yielded = []
        for i, a in enumerate(shell_actions):
            poss = get_shell().complete(document.text, i)
            if not poss:
                break
            yield Completion(poss, start_position=(-1 * len(poss)))
            yielded.append(poss)

        if yielded:
            return

        action = document.text.split(' ')[0]
        possibilities = []
        if action and f'complete_{action}' in get_shell().__dict__:
            possibilities = get_shell().__dict__[f'complete_{action}'](document.text.split(' ')[-1], document.text, 0, 0)

        if possibilities:
            for suggestion in possibilities:
                yield Completion(suggestion, start_position=(-1 * len(document.text.split(' ')[-1])))
            return

        yield Completion('')

