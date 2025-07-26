#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import os

debug_str = os.environ.get('MRSM_DEBUG', None) or 'true'
debug = False if debug_str.lower() in ('false', '0') else True
