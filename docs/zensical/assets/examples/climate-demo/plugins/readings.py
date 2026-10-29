"""A tiny local source for the Meerschaum Compose tutorial."""


def fetch(pipe, **kwargs):
    return [
        {'id': 1, 'ts': '2026-01-01 12:00:00', 'temperature': 21.5},
        {'id': 2, 'ts': '2026-01-01 13:00:00', 'temperature': 22.0},
    ]
