#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

def read_config():
    import sys, shutil, os
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.yaml import yaml
#  yaml = attempt_import('yaml')
#  try:
        #  import yaml
#  except ImportError:
        #  print("Failed to import PyYAML. Assuming we are installing in a fresh environment...", file=sys.stderr)
        #  yaml = None

    from meerschaum.config._edit import copy_default_to_config
    from meerschaum.config._paths import CONFIG_PATH, CONFIG_FILENAME

    if yaml is not None:
        try:
            with open(CONFIG_PATH, 'r') as f:
                config_text = f.read()
        except FileNotFoundError:
            print(f"NOTE: Configuration file is missing. Falling back to default configuration.")
            print(f"You can edit the configuration with `edit config` or replace the file {CONFIG_PATH}")
            copy_default_to_config()
        finally:
            ### copy is finished. Read again
            with open(CONFIG_PATH, 'r') as f:
                config_text = f.read()

### parse the yaml file
    try:
        if yaml is not None:
            ### cf dictionary
            config = yaml.load(config_text)
        else:
            from meerschaum.config._default import default_config
            config = default_config
    except Exception as e:
        print(f"Unable to parse {CONFIG_FILENAME}!")
        print(e)
        input(f"Press [Enter] to open {CONFIG_FILENAME} and fix formatting errors.")
        from meerschaum.config._default import default_system_config
        from meerschaum.utils.misc import edit_file
        edit_file(CONFIG_PATH)
        sys.exit()

    return config

