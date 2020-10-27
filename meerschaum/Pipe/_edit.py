#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Edit a Pipe's parameters here.
"""

def edit(
        self,
        patch : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Edit a Pipe's configuration.
    If `patch` is True, update parameters by cascading rather than overwriting.
    """
    from meerschaum.config._paths import PIPES_RESOURCES_PATH
    from meerschaum.utils.misc import edit_file
    import pathlib, os
    parameters_filename = str(self) + '.yaml'
    parameters_path = pathlib.Path(os.path.join(PIPES_RESOURCES_PATH, parameters_filename))
    
    import yaml

    edit_header = "#######################################"
    for i in range(len(str(self))): edit_header += "#"
    edit_header += "\n"
    edit_header += f"# Edit the parameters for the Pipe '{self}' #"
    edit_header += "\n#######################################"
    for i in range(len(str(self))): edit_header += "#"
    edit_header += "\n\n"

    from meerschaum.config import get_config
    parameters = dict(get_config('pipes', 'parameters', patch=True))
    from meerschaum.config._patch import apply_patch_to_config
    parameters = apply_patch_to_config(parameters, self.parameters)

    ### write parameters to yaml file
    with open(parameters_path, 'w') as f:
        f.write(edit_header)
        yaml.dump(parameters, f)

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
        import pprintpp
        pprintpp.pprint(self.parameters)

    return self.instance_connector.edit_pipe(self, patch=patch, debug=debug)
