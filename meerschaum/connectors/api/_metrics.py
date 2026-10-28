"""APIConnector performance reads use its existing native transport and current credentials."""


def get_engine_metrics(self, **kwargs):
    """Read bounded sync and Pipeline measurements visible to this credential."""
    from meerschaum.core.Pipeline._metrics import get_engine_metrics as read
    return read(instance=self, **kwargs)


def initialize_engine_metrics(self):
    """Initialize metrics storage on the served instance (full administrator only)."""
    from meerschaum.core.Pipeline._metrics import initialize_engine_metrics as initialize
    return initialize(instance=self)
