#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from meerschaum.config import system_config
from meerschaum.connectors import get_connector
from fastapi import FastAPI
import graphene
from starlette.graphql import GraphQLApp

connector = get_connector(label="meta", debug=True)
database = connector.db

### TODO move GraphQL queries somewhere
class Query(graphene.ObjectType):
    hello = graphene.String(
        name = graphene.String(default_value="stranger")
    )
    def resolve_hello(self, info, name):
        return 'hello' + name

sys_config = system_config['api']
fast_api = FastAPI()
fast_api.add_route("/graphql", GraphQLApp(schema=graphene.Schema(query=Query)))

__version__ = sys_config['version']
endpoints = sys_config['endpoints']

### import WebAPI routes
import meerschaum.api.routes as routes
import meerschaum.api._events

