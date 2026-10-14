"""The connector class, assembled from imported methods (the mixin-via-import pattern)."""

from meerschaum.connectors import Connector, make_connector


@make_connector
class W9MQTTConnector(Connector):
    """Subscribe to topics on an MQTT 3.1.1 broker."""

    REQUIRED_ATTRIBUTES = ['host']

    from ._client import subscribe
    from ._sync import sync

    @property
    def port(self) -> int:
        return int(self.__dict__.get('port', 1883))
