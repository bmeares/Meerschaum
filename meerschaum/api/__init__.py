#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

__version__ = "0.0.12"
from meerschaum.config import system_config, get_config
#  from meerschaum.connectors import get_connector
from meerschaum.utils.misc import attempt_import
from meerschaum.utils._get_pipes import get_pipes as get_pipes_sql
#  fastapi, graphene, starlette_graphql = attempt_import('fastapi', 'graphene', 'starlette.graphql', lazy=True)
fastapi = attempt_import('fastapi', lazy=True)

connector = None
def get_connector(instance_keys : str = None, debug : bool = False):
    """
    Create the connector
    """
    from meerschaum.utils.debug import dprint
    global connector
    if connector is None:
        from meerschaum.config._paths import API_UVICORN_CONFIG_PATH
        yaml = attempt_import('yaml')
        if instance_keys is None:
            try:
                with open(API_UVICORN_CONFIG_PATH, 'r') as f:
                    uvicorn_config = yaml.safe_load(f)
            except:
                uvicorn_config = dict()

            if uvicorn_config is None: uvicorn_config = dict()

            ### Default: main SQL connector
            if 'mrsm_instance' not in uvicorn_config:
                uvicorn_config['mrsm_instance'] = get_config('meerschaum', 'api_instance', patch=True)

            instance_keys = uvicorn_config['mrsm_instance']

        from meerschaum.utils.misc import parse_instance_keys
        connector = parse_instance_keys(instance_keys, debug=debug)
    if debug: dprint(f"API instance connector: {connector}")
    return connector

database = None
def get_database(instance_keys : str = None):
    #  if instance_keys is None
    global database
    if database is None:
        database = get_connector(instance_keys).db
    return database

_pipes = None
def pipes(refresh=False):
    global _pipes
    if _pipes is None or refresh:
        _pipes = get_pipes_sql(mrsm_instance=get_connector())
    return _pipes

def get_pipe(connector_keys, metric_key, location_key, refresh=False):
    """
    Index the pipes dictionary or create a new Pipe object
    """
    from meerschaum.utils.misc import is_pipe_registered
    from meerschaum import Pipe
    if location_key == '[None]': location_key = None
    p = Pipe(connector_keys, metric_key, location_key, mrsm_instance=get_connector())
    if is_pipe_registered(p, pipes()):
        return pipes(refresh=refresh)[connector_keys][metric_key][location_key]
    return p

### TODO move GraphQL queries somewhere
#  class Query(graphene.ObjectType):
    #  hello = graphene.String(
        #  name = graphene.String(default_value="stranger")
    #  )
    #  def resolve_hello(self, info, name):
        #  return 'hello' + name

sys_config = system_config['api']
fast_api = fastapi.FastAPI(title='Meerschaum API')
#  fast_api.add_route("/graphql", starlette_graphql.GraphQLApp(schema=graphene.Schema(query=Query)))

fastapi_responses, fastapi_templating, fastapi_staticfiles = attempt_import(
    'fastapi.responses',
    'fastapi.templating',
    'fastapi.staticfiles'
)
jinja2 = attempt_import('jinja2')


HTMLResponse = fastapi_responses.HTMLResponse
Request = fastapi.Request

from meerschaum.config._paths import API_RESOURCES_PATH, API_STATIC_PATH, API_TEMPLATES_PATH
fast_api.mount('/static', fastapi_staticfiles.StaticFiles(directory=str(API_STATIC_PATH)), name='static')
templates = fastapi_templating.Jinja2Templates(directory=str(API_TEMPLATES_PATH))

endpoints = sys_config['endpoints']

### import WebAPI routes
import meerschaum.api.routes as routes
import meerschaum.api._events

