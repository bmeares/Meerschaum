#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2021 bmeares <bmeares@pop-os>
#
# Distributed under terms of the MIT license.

"""

"""

import meerschaum as mrsm
import pathlib, os, sys, time
from daemoniker import Daemonizer, SignalHandler1

with Daemonizer() as (is_setup, daemonizer):
    is_parent = daemonizer('pid.pid')

sighandler = SignalHandler1('pid.pid')
sighandler.start()

while True:
    print('inside daemon')
    time.sleep(1)
