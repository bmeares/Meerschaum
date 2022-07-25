#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the prompt_toolkit Completer base class.
"""

from __future__ import annotations
from prompt_toolkit.completion import Completer, Completion
from meerschaum.utils.typing import Optional
from meerschaum.actions import get_shell, get_completer, get_main_action_name, get_action
from meerschaum.actions.arguments import parse_line

from meerschaum.utils.packages import attempt_import, ensure_readline
prompt_toolkit = attempt_import('prompt_toolkit', lazy=False, install=True)

class ShellCompleter(Completer):
    """
    Implement the `prompt_toolkit` Completer to use the built-in `complete_` methods
    and the `cmd` completer system.
    """
    def get_completions(self, document, complete_event):
        """
        Bridge the built-in cmd completer with the `prompt_toolkit` completer system.
        """
        shell_actions = [a[3:] for a in dir(get_shell()) if a.startswith('do_')]
        yielded = []
        ensure_readline()
        parts = document.text.split('-')
        ends_with_space = parts[0].endswith(' ')
        part_0_subbed_spaces = parts[0].replace(' ', '_')
        parsed_text = part_0_subbed_spaces + '-'.join(parts[1:])

        ### Index is the rank order (0 is closest match).
        ### Break when no results are returned.
        for i, a in enumerate(shell_actions):
            try:
                poss = get_shell().complete(parsed_text, i)
                if poss:
                    poss = poss.replace('_', ' ')
            ### Having issues with readline on portable Windows.
            except ModuleNotFoundError:
                poss = False
            if not poss:
                break
            yield Completion(poss, start_position=(-1 * len(poss)))
            yielded.append(poss)

        args = parse_line(document.text)
        action_function = get_action(args['action'])
        if action_function is None:
            return
        main_action_name = get_main_action_name(args['action'])

        ### If we haven't yet hit space, don't suggest subactions.
        if not parsed_text.replace(action_function.__name__.lstrip('_'), '').startswith('_'):
            return

        possibilities = []
        if f'complete_{main_action_name}' in dir(get_shell()):
            possibilities = getattr(
                get_shell(), f'complete_{main_action_name}'
            )(document.text.split(' ')[-1], document.text, 0, 0)

        if possibilities:
            for suggestion in possibilities:
                yield Completion(
                    suggestion, start_position=(-1 * len(document.text.split(' ')[-1]))
                )
            return

        if not yielded:
            yield Completion('')

