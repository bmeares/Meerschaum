#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from meerschaum.config import system_config
from meerschaum.connectors import get_connector
from meerschaum.utils.misc import attempt_import
fastapi, graphene, starlette_graphql = attempt_import('fastapi', 'graphene', 'starlette.graphql', lazy=True)

connector = get_connector(type="sql", label="meta")
database = connector.db

### TODO move GraphQL queries somewhere
class Query(graphene.ObjectType):
    hello = graphene.String(
        name = graphene.String(default_value="stranger")
    )
    def resolve_hello(self, info, name):
        return 'hello' + name

sys_config = system_config['api']
fast_api = fastapi.FastAPI()
fast_api.add_route("/graphql", starlette_graphql.GraphQLApp(schema=graphene.Schema(query=Query)))

__version__ = sys_config['version']
endpoints = sys_config['endpoints']

### import WebAPI routes
import meerschaum.api.routes as routes
import meerschaum.api._events

#  @fast_api.on_event("startup")
#  async def startup():
    #  async def connect(
        #  max_retries : int = 40,
        #  retry_wait : int = 3,
        #  debug : bool = False
    #  ):
        #  import time
        #  retries = 0
        #  while retries < max_retries:
            #  if debug:
                #  print(f"Trying to connect to the database")
                #  print(f"Attempt ({retries + 1} / {max_retries})")
            #  try:
                #  await database.connect()
            #  except Exception as e:
                #  print(e)
                #  print(f"Connection failed. Retrying in {retry_wait} seconds...")
                #  time.sleep(retry_wait)
                #  retries += 1
            #  else:
                #  if debug: print("Connection established!")
                #  break
    #  await connect(debug=True)

#  @fast_api.on_event("shutdown")
#  async def startup():
    #  print("Closing database connection...")
    #  await database.disconnect()


