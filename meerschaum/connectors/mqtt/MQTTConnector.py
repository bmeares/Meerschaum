#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Meerschaum Connector to connect to MQTT brokers.
"""

from meerschaum.connectors.Connector import Connector

class MQTTConnector(Connector):

    from ._subscribe import subscribe
    from ._fetch import fetch

    def __init__(
        self,
        label : str = 'main',
        debug : bool = False,
        **kw
    ):
        super().__init__('mqtt', label=label, **kw)

        self.verify_attributes({'host'})

        ### default port for MQTT is 1883
        if 'port' not in self.__dict__:
            self.port = 1883

        if 'keepalive' not in self.__dict__:
            self.keepalive = 60

        from meerschaum.utils.packages import attempt_import
        mqtt = attempt_import('paho.mqtt.client')

        ### default: 'tcp'. Can also be 'websockets'
        transport = 'tcp'
        if 'transport' in self.__dict__:
            transport = self.transport

        ### tell the broker to delete client information on disconnect
        clean_session = True
        if 'clean_session' in self.__dict__:
            clean_session = self.clean_session

        self.client = mqtt.Client(
            clean_session = clean_session,
            transport = transport,
        )

        ### if username and password provided, pass to client
        if 'username' in self.__dict__ and 'password' in self.__dict__:
            self.client.username_pw_set(username=self.username, password=self.password)

        ### keep a record of the last messages per topic in case we want to omit duplicate values
        self._last_msgs = dict()

    def __del__(self):
        self.client.disconnect()
