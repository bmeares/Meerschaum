#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement prompt_toolkit's AutoSuggest base class.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional

from meerschaum.utils.packages import attempt_import
prompt_toolkit_auto_suggest, prompt_toolkit_document, prompt_toolkit_buffer = (
    attempt_import('prompt_toolkit.auto_suggest'),
    attempt_import('prompt_toolkit.document'),
    attempt_import('prompt_toolkit.buffer'),
)
from meerschaum._internal.entry import get_shell

class ValidAutoSuggest(prompt_toolkit_auto_suggest.AutoSuggest):
    def get_suggestion(
        self,
        buffer: prompt_toolkit_buffer.Buffer,
        document: prompt_toolkit_document.Document,
    ) -> Optional[prompt_toolkit_auto_suggest.Suggestion]:
        """Only return valid commands from history."""
        history = buffer.history
        text = document.text.rsplit("\n", 1)[-1]
        if not text.strip():
            return None

        for string in reversed(list(history.get_strings())):
            for line in reversed(string.splitlines()):
                if line.startswith(text) and 'do_' + line.split(' ')[0] in dir(get_shell()):
                    return prompt_toolkit_auto_suggest.Suggestion(line[len(text) :])

