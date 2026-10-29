#!/usr/bin/env python3
"""Normalize punctuation in generated API prose without changing source or code examples."""
from __future__ import annotations

import argparse
from html.parser import HTMLParser
from pathlib import Path
import re

DASH = re.compile(r'[ \t]*(?:[—―]|&mdash;|&#8212;|&#x2014;)[ \t]*', re.IGNORECASE)
PROTECTED = {'code', 'pre', 'script', 'style', 'textarea'}


class Prose(HTMLParser):
    """Locate original text spans, preserving every tag and protected payload byte."""

    def __init__(self, source: str):
        super().__init__(convert_charrefs=False)
        self.source = source
        self.lines = [0]
        for match in re.finditer('\n', source):
            self.lines.append(match.end())
        self.protected = []
        self.start = None
        self.end = 0
        self.edits = []

    def source_offset(self):
        line, column = self.getpos()
        return self.lines[line - 1] + column

    def flush(self):
        if self.start is not None:
            old = self.source[self.start:self.end]
            new = DASH.sub(', ', old)
            if new != old:
                self.edits.append((self.start, self.end, new))
        self.start = None

    def text(self, length):
        if self.protected:
            return
        if self.start is None:
            self.start = self.source_offset()
        self.end = self.source_offset() + length

    def handle_data(self, data):
        self.text(len(data))

    def handle_entityref(self, name):
        self.text(len(name) + 2)

    def handle_charref(self, name):
        self.text(len(name) + 3)

    def handle_starttag(self, tag, attrs):
        self.flush()
        if tag in PROTECTED:
            self.protected.append(tag)

    def handle_endtag(self, tag):
        self.flush()
        if self.protected and tag == self.protected[-1]:
            self.protected.pop()

    def handle_startendtag(self, tag, attrs):
        self.flush()

    def handle_comment(self, data):
        self.flush()

    def handle_decl(self, decl):
        self.flush()

    def handle_pi(self, data):
        self.flush()


def normalize_prose(source: str) -> str:
    """Return normalized HTML while keeping markup and executable/literal content intact."""
    parser = Prose(source)
    parser.feed(source)
    parser.close()
    parser.flush()
    for start, end, replacement in reversed(parser.edits):
        source = source[:start] + replacement + source[end:]
    return source


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    count = 0
    for page in sorted(args.directory.rglob('*.html')):
        if page.is_symlink():
            continue
        old = page.read_text(encoding='utf-8')
        new = normalize_prose(old)
        if new != old:
            page.write_text(new, encoding='utf-8')
            count += 1
    print(f'Normalized generated prose in {count} HTML pages.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
