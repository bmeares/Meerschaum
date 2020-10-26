#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Subscribe to a MQTT stream
"""

def subscribe(
        self,
        topic : str = '#',
        callback : 'function' = None,
        forever : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Subscribe to a MQTT topic and execute a callback function.

    Callback function must take only one string parameter.

    If `forever` is True, block the main thread in a loop. Otherwise spin up a new thread
    for the duration of the main thread.
    """
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint
    
    ### default callback action: debug print message
    def default_callback(msg : str):
        dprint(msg)

    if callback is None:
        callback = default_callback

    ### decode the payload and execute the callback function with the string as the only parameter
    def _parse_message(client, userdata, message):
        return callback(message.payload.decode('utf-8'))

    self.client.on_message = _parse_message

    try:
        self.client.connect(self.host, self.port, self.keepalive)
    except:
        error(f"Failed to connect to MQTT broker \"{self.host}\" with connector: {self}")

    self.client.subscribe(topic)
    if debug: dprint(f"Subscribed to \"{topic}\". Starting network loop...")

    ### start a new thread that fires callback when messages are received
    if not forever:
        self.client.loop_start()
    else:
        self.client.loop_forever()
