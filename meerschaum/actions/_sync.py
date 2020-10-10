#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize Pipes' data with sources

NOTE: `sync` required a SQL connection and is not intended for client use
"""

def sync(
        action : list = [''],
        **kw        
    ) -> tuple:
    """
    Sync elements
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipes'   : _sync_pipes,
    }
    return choose_subaction(action, options, **kw)

def _sync_pipes(
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    TODO implement
    """
    from meerschaum import get_pipes
    pipes = get_pipes(
        as_list=True,
        method = 'registered',
        debug=debug,
        **kw
    )
    if debug:
        from meerschaum import get_connector
        import pprintpp
        #  pprintpp.pprint(get_connector().__dict__)
    for p in pipes:
        p.sync(debug=debug)

    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
sync.__doc__ += _choices_docstring('sync')

