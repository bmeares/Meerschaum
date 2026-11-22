"""`sync()` subscribes and returns; each message is synced from the client's thread."""

import functools
import json


def sync(self, pipe, **kwargs):
    topic = pipe.parameters['fetch']['topic']
    self.subscribe(topic, functools.partial(_on_message, pipe=pipe, sync_kwargs=kwargs))
    return True, f"Syncing {pipe} in a background thread."


def _on_message(payload, pipe, sync_kwargs):
    doc = json.loads(payload)
    success, message = pipe.sync([doc], **sync_kwargs)
    print(('Synced' if success else 'Failed to sync') + f" a message to {pipe}: {message}")
