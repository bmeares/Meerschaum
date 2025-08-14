#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import os
import shutil
from meerschaum.config.paths import PIPES_CACHE_RESOURCES_PATH

debug_str = os.environ.get('MRSM_DEBUG', None) or 'true'
debug = False if debug_str.lower() in ('false', '0') else True

if 'test_root' in str(PIPES_CACHE_RESOURCES_PATH) and PIPES_CACHE_RESOURCES_PATH.exists():
    shutil.rmtree(PIPES_CACHE_RESOURCES_PATH)
