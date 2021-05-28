#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2021 bmeares <bmeares@pop-os>
#
# Distributed under terms of the MIT license.

"""

"""

import Pyro4.utils.flame
Pyro4.config.SERIALIZER = 'pickle'
flame = Pyro4.utils.flame.connect('localhost:38765')

with flame.console() as console:
    console.interact()
