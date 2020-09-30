#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

__version__ = "0.0.7"
from meerschaum.config import system_config
from meerschaum.connectors import get_connector
from meerschaum.utils._get_pipes import get_pipes as get_pipes_sql
from meerschaum.utils.misc import attempt_import
fastapi, graphene, starlette_graphql = attempt_import('fastapi', 'graphene', 'starlette.graphql', lazy=True)

connector = get_connector(type="sql", label="meta")
database = connector.db
pipes = get_pipes_sql()

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

