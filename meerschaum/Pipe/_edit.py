#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Edit a Pipe's parameters here.
"""

def edit(
        self,
        api_connector : 'APIConnector' = None,
        patch : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Edit a Pipe's configuration.
    If `patch` is True, update parameters by cascading rather than overwriting.
    """
    if api_connector is None:
        from meerschaum import get_connector
        api_connector = get_connector(type='api')

    from meerschaum.config._paths import PIPES_RESOURCES_PATH
    from meerschaum.utils.misc import edit_file
    import pathlib, os
    parameters_filename = str(self) + '.yaml'
    parameters_path = pathlib.Path(os.path.join(PIPES_RESOURCES_PATH, parameters_filename))
    
    import yaml

    with open(parameters_path, 'w') as f:
        yaml.dump(self.parameters, f)


    editing = True
    while editing:
        edit_file(parameters_path)
        try:
            with open(parameters_path, 'r') as f:
                self.parameters = yaml.load(f.read())
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(f"Invalid format defined for '{self}':\n\n{e}")
            input(f"Press [Enter] to correct the configuration for {self}: ")
        else:
            editing = False

    if debug:
        import pprintpp
        pprintpp.pprint(self.parameters)

    return api_connector.edit_pipe(self, patch=patch, debug=debug)
