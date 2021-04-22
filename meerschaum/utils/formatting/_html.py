#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Format ANSI text to HTML.
"""

from meerschaum.utils.packages import attempt_import
ansi2html = attempt_import('ansi2html')
ansi2html.converter._html_template = """


"""


def ansi_to_html(ansi : str) -> str:
    """
    Convert a string with ANSI codes to a stylized HTML span.
    """
    converter = ansi2html.Ansi2HTMLConverter()

