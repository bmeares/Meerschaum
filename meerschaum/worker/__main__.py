#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""`python -m meerschaum.worker --socket PATH`"""

import sys

from meerschaum.worker import main

if __name__ == '__main__':
    sys.exit(main())
