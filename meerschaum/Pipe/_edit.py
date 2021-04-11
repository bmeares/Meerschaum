#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Edit a Pipe's parameters here.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, SuccessTuple

def edit(
        self,
        patch : bool = False,
        interactive : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit a Pipe's configuration.
    If `patch` is True, update parameters by cascading rather than overwriting.
    """
    if not interactive:
        return self.instance_connector.edit_pipe(self, patch=patch, debug=debug, **kw)
    from meerschaum.config._paths import PIPES_CACHE_RESOURCES_PATH
    from meerschaum.utils.misc import edit_file
    import pathlib, os
    parameters_filename = str(self) + '.yaml'
    parameters_path = pathlib.Path(os.path.join(PIPES_CACHE_RESOURCES_PATH, parameters_filename))
    
    from meerschaum.utils.yaml import yaml

    edit_header = "#######################################"
    for i in range(len(str(self))):
        edit_header += "#"
    edit_header += "\n"
    edit_header += f"# Edit the parameters for the Pipe '{self}' #"
    edit_header += "\n#######################################"
    for i in range(len(str(self))):
        edit_header += "#"
    edit_header += "\n\n"

    from meerschaum.config import get_config
    parameters = dict(get_config('pipes', 'parameters', patch=True))
    from meerschaum.config._patch import apply_patch_to_config
    parameters = apply_patch_to_config(parameters, self.parameters)

    ### write parameters to yaml file
    with open(parameters_path, 'w+') as f:
        f.write(edit_header)
        yaml.dump(parameters, stream=f, sort_keys=False)

    ### only quit editing if yaml is valid
    editing = True
    while editing:
        edit_file(parameters_path)
        try:
            with open(parameters_path, 'r') as f:
                file_parameters = yaml.load(f.read())
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(f"Invalid format defined for '{self}':\n\n{e}")
            input(f"Press [Enter] to correct the configuration for '{self}': ")
        else:
            editing = False

    self.parameters = file_parameters

    if debug:
        from meerschaum.utils.formatting import pprint
        pprint(self.parameters)

    return self.instance_connector.edit_pipe(self, patch=patch, debug=debug, **kw)

def edit_definition(
        self,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit a pipe's definition file and update its configuration.
    NOTE: This function is interactive and should not be used in automated scripts!
    """
    if self.connector.type not in ('sql', 'api'):
        return self.edit(interactive=True, debug=debug, **kw)

    from meerschaum.utils.warnings import info
    from meerschaum.utils.debug import dprint

    _parameters = self.parameters
    if 'fetch' not in _parameters:
        _parameters['fetch'] = {}

    def _edit_api():
        from meerschaum.utils.prompt import prompt
        info(
            f"Please enter the keys of the source pipe from '{self.connector}'.\n" +
            "Type 'None' for None, or empty when there is no default. Press [CTRL+C] to skip."
        )

        _keys = { 'connector_keys' : None, 'metric_key' : None, 'location_key' : None }
        for k in _keys:
            _keys[k] = _parameters['fetch'].get(k, None)

        for k, v in _keys.items():
            try:
                _keys[k] = prompt(k.capitalize().replace('_', ' ') + ':', icon=True, default=v)
            except KeyboardInterrupt:
                continue
            if _keys[k] in ('', 'None', '\'None\'', '[None]'):
                _keys[k] = None

        _parameters['fetch'].update(_keys)
        self.parameters = _parameters

    def _edit_sql():
        import pathlib, os, textwrap
        from meerschaum.config._paths import PIPES_CACHE_RESOURCES_PATH
        from meerschaum.utils.misc import edit_file
        definition_filename = str(self) + '.sql'
        definition_path = PIPES_CACHE_RESOURCES_PATH / definition_filename

        sql_definition = _parameters['fetch'].get('definition', None)
        if sql_definition is None:
            sql_definition = ''
        sql_definition = textwrap.dedent(sql_definition).lstrip()

        try:
            definition_path.touch()
            with open(definition_path, 'w+') as f:
                f.write(sql_definition)
        except Exception as e:
            return False, f"Failed writing file '{definition_path}':\n" + str(e)

        edit_file(definition_path)
        try:
            with open(definition_path, 'r') as f:
                file_definition = f.read()
        except Exception as e:
            return False, f"Failed reading file '{definition_path}':\n" + str(e)

        if sql_definition == file_definition:
            return False, f"No changes made to definition for pipe '{self}'."

        if ' ' not in file_definition:
            return False, f"Invalid SQL definition for pipe '{self}'."

        if debug:
            dprint("Read SQL definition:\n\n" + file_definition)
        _parameters['fetch']['definition'] = file_definition
        self.parameters = _parameters
        return True, "Success"

    locals()['_edit_' + str(self.connector.type)]()
    return self.edit(interactive=False, debug=debug, **kw)
