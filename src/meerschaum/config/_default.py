#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Write the default configuration values to config.yaml.
"""

from meerschaum.utils.misc import generate_password
import yaml, sys, shutil, os.path
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources


defaut_meerschaum_config = {
    'connectors' : {
        'sql' : {
            'main' : {
                'flavor'   : 'timescaledb',
                'host'     : 'localhost',
                'username' : 'meerschaum',
                'password' : 'meerschaum',
                'database' : 'mrsm_main',
                'port'     : 5432,
            },
            'meta'     : {
            },
            'local'        : {
                'flavor'   : 'sqlite',
                'database' : 'mrsm_local'
            },
        },
        'api' : {
            'main' : {
                'host'     : 'localhost',
                'username' : 'meerschaum',
                'password' : generate_password(),
                'protocol' : 'http',
                'port'     : 8000,
            }
        },
    },
}
default_system_config = {
    'connectors' : {
        'all' : {
            ### pandas implementation
            ### (change to modin.pandas when to_sql works)
            'pandas'       : 'pandas',
        },
        'sql' : {
            'method'       : 'multi',
            'chunksize'    : 1000,
            'pool_size'    : 5,
            'max_overflow' : 10,
            'pool_recycle' : 3600,
            'poolclass'    : 'sqlalchemy.pool.QueuePool',
            'connect_args' : {},
        },

        'api' : {
        },
    },
    'shell' : {
        'timeout'          : 15,
        'default_editor'   : 'nano',
    },
    ### not to be confused with system_config['connectors']['api']
    'api' : {
        'uvicorn'          : {
            'app'          : 'meerschaum.api:fast_api',
            'port'         : defaut_meerschaum_config['connectors']['api']['main']['port'],
            'host'         : '0.0.0.0',
            'workers'      : 4,
        },
        'username'         : defaut_meerschaum_config['connectors']['api']['main']['username'],
        'password'         : defaut_meerschaum_config['connectors']['api']['main']['password'],
        'protocol'         : defaut_meerschaum_config['connectors']['api']['main']['protocol'],
        'version'          : '0.0.2',
        'endpoints'        : {
            'mrsm'         : '/mrsm',
        }
    },
}

### file path of the resources package
resources_context_manager = pkg_resources.path('meerschaum.config', 'resources')
with resources_context_manager as file_path:
    resources_path = file_path

default_config = dict()
default_config['meerschaum'] = defaut_meerschaum_config
default_config['system'] = default_system_config
default_filename = 'default_config.yaml'
default_path = os.path.join(resources_path, default_filename)
default_header_comment = """
##################################
# Edit the credentials below     #
# for the `main` SQL connection. #
##################################

"""

def copy_default_to_config(config_filename):
    try:
        src_file = default_path
        dest_file = os.path.join(resources_path, config_filename)
        shutil.copyfile(src_file, dest_file)
    except FileNotFoundError:
        write_default_config()
        return copy_default_to_config(config_filename)
    return True


def write_default_config(
        debug : bool = False,
        **kw
    ):
    """
    Overwrite the existing default_config.yaml.
    NOTE: regenerates passwords
    """
    import yaml, os
    if os.path.isfile(default_path): os.remove(default_path)
    if debug: print(f"Writing default configuration to {default_path}...")
    with open(default_path, 'w') as f:
        f.write(default_header_comment)
        yaml.dump(default_config, f)

    return True
