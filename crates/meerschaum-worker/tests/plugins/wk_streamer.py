#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Worker test plugin: the v4 streaming pattern. `sync()` starts a background "subscription" and
returns immediately; each message callback calls `pipe.sync()`.
"""

__version__ = '1.0.0'

import threading
import time
from datetime import datetime, timezone, timedelta

BASE = datetime(2024, 3, 1, tzinfo=timezone.utc)


def sync(pipe, **kw):
    """Subscribe and return; messages arrive on another thread."""
    count = int(pipe.location_key or 3)

    def on_messages():
        for i in range(count):
            time.sleep(0.05)
            pipe.sync([{'dt': BASE + timedelta(minutes=i), 'id': 1, 'val': i}])

    threading.Thread(target=on_messages, daemon=True).start()
    return True, "subscribed"
