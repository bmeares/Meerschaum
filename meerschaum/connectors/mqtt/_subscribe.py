#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Subscribe to a MQTT topic.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any, Callable

def subscribe(
        self,
        topic : str = '#',
        callback : 'function' = None,
        skip_duplicates = True,
        forever : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Subscribe to an MQTT topic and execute a callback function.

    topic : str : '#'
        MQTT topic to subscribe to. Default is all available topics.
        (WARNING: this may have unexpected results!)

    callback : function
        Callback function must take only one string parameter.

    skip_duplicates : bool : True
        If True, only execute the callback function if the message value is different from the last
        one received.

    forever : bool : False
        If `forever` is True, block the main thread in a loop. Otherwise spin up a new thread
        for the duration of the main thread.
    """
    from meerschaum.utils.warnings import error, warn
    from meerschaum.utils.debug import dprint
    
    ### default callback action: debug print message
    def default_callback(msg : str):
        dprint(msg)

    if callback is None:
        callback = default_callback

    ### decode the payload and execute the callback function with the string as the only parameter
    def _parse_message(client, userdata, message):
        """
        Parse the payload (assuming it's a UTF-8 string. May add options to this later)
        and if skip_duplicates is True, check for a change in the payload.
        """
        execute_callback = True
        if skip_duplicates:
            ### check if the current message is different from the last
            if message.topic not in self._last_msgs:
                execute_callback = True
            else:
                execute_callback = (self._last_msgs[message.topic] != message.payload)
        self._last_msgs[message.topic] = message.payload
        if execute_callback:
            return callback(message.payload.decode('utf-8'))
        dprint("Message on topic " + f'"{topic}"' + " has not changed since the last message. Skipping...")

    def _subscribe_on_connect(client, userdata, flags, rc):
        """
        Subscribe to the topic when connecting (resubscribes in case of disconnect).
        """
        if rc > 0:
            warn(f"Received return code {rc} from '{self.host}' on topic '{topic}'.")
            if rc == 5:
                warn(f"Are the credentials for '{self}' correct?", stack=False)
        if debug:
            dprint("Subscribed to " + f'"{topic}"' + ". Starting network loop...")
        client.subscribe(topic)

    self.client.on_message = _parse_message
    self.client.on_connect = _subscribe_on_connect

    try:
        self.client.connect(self.host, self.port, self.keepalive)
    except Exception as e:
        error(
            "Failed to connect to MQTT broker " +
            '"{self.host}"' + " with connector: " + f"{self}"
        )

    ### start a new thread that fires callback when messages are received
    if not forever:
        self.client.loop_start()
    else:
        self.client.loop_forever()
