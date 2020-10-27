#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

__version__ = "0.0.8"
from meerschaum.config import system_config
from meerschaum.connectors import get_connector
from meerschaum.utils.misc import attempt_import
from meerschaum.utils._get_pipes import get_pipes as get_pipes_sql
fastapi, graphene, starlette_graphql = attempt_import('fastapi', 'graphene', 'starlette.graphql', lazy=True)

connector = get_connector(type="sql")
database = connector.db
_pipes = None
def pipes(refresh=False):
    global _pipes
    if _pipes is None or refresh:
        _pipes = get_pipes_sql()
    return _pipes

def get_pipe(connector_keys, metric_key, location_key, refresh=False):
    """
    Index the pipes dictionary or create a new Pipe object
    """
    from meerschaum.utils.misc import is_pipe_registered
    from meerschaum import Pipe
    if location_key == '[None]': location_key = None
    p = Pipe(connector_keys, metric_key, location_key)
    if is_pipe_registered(p, pipes()):
        return pipes(refresh=refresh)[connector_keys][metric_key][location_key]
    return p

### TODO move GraphQL queries somewhere
class Query(graphene.ObjectType):
    hello = graphene.String(
        name = graphene.String(default_value="stranger")
    )
    def resolve_hello(self, info, name):
        return 'hello' + name

sys_config = system_config['api']
fast_api = fastapi.FastAPI(title='Meerschaum API')
fast_api.add_route("/graphql", starlette_graphql.GraphQLApp(schema=graphene.Schema(query=Query)))

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

