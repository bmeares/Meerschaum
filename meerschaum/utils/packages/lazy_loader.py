#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""A LazyLoader class."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from meerschaum.utils.typing import Optional
import importlib, types

class LazyLoader(types.ModuleType):
    """Lazily import a module, mainly to avoid pulling in large dependencies.
    
    `contrib`, and `ffmpeg` are examples of modules that are large and not always
    needed, and this allows them to only be loaded when they are used.
    """

    # The lint error here is incorrect.
    def __init__(
        self,
        local_name: str,
        parent_module_globals,
        name: str,
        **kw
    ):
        self._local_name = local_name
        self._parent_module_globals = parent_module_globals
        kw['lazy'] = False
        self._attempt_import_kw = kw
        self._module = None
        super(LazyLoader, self).__init__(name)

    def _load(self):
        """Load the module and insert it into the parent's globals."""
        if self._module is not None:
            return self._module

        from meerschaum.utils.packages import attempt_import
        self._module = attempt_import(self.__name__, **self._attempt_import_kw)
        self._parent_module_globals[self._local_name] = self._module

        # Update this object's dict so that if someone keeps a reference to the
        #   LazyLoader, lookups are efficient (__getattr__ is only called on lookups
        #   that fail).
        self.__dict__.update(self._module.__dict__)

        return self._module

    def __getattr__(self, item):
        module = self._load() if self._module is None else self._module
        return getattr(module, item)

    def __dir__(self):
        module = self._load() if self._module is None else self._module
        return dir(module)

