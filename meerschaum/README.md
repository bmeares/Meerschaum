# Meerschaum Modules

Below is a brief description of the Meerschaum internal modules. For further information, please visit [docs.meerschaum.io](https://docs.meerschaum.io/).

- `_internal`  
  Internal modules, such as `Plugin` and `User` implementations are defined here. Modules within `_internal` are not intended for public API use.

- `actions`  
  Default actions such as `show` and `bootstrap` are implemented as sub-modules and may be accessed from the `meerschaum.actions.actions` dictionary. Plugins are loaded into `actions.plugins` on startup, and actions from plugins are added to the actions dictionary.

  Additionally, command-line arguments and the shell implementation are defined as sub-modules.

- `api`  
  The `fastapi` app and `dash` web app are defined here.

- `config`  
  Important configuration details, such as `get_config()`, file paths, and defaults reside here.

- `connectors`  
  Connector implementations are defined here:
    - `Connector`: Base class for reading configuration details.
    - `SQLConnector`: Interact with pipes via `sqlalchemy` engines.
    - `APIConnector`: Interact with pipes via web requests.
    - `PluginConnector`: Wrapper for plugins' `fetch()` or `sync()` methods.

- `Pipe`  
  The `Pipe` class methods are defined here. Most of the methods are wrapper of instance connector methods.

- `plugins`  
  Public plugins API functions such as `@make_action` and `@api_plugin` are defined here.

- `utils`  
  The `utils` module is a parent for many useful tools, such as custom implementations for daemons, threads, processes, packages, typing, prompts, warnings, and more.


### License

Copyright 2021 Bennett Meares

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
