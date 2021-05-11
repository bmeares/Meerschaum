#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

# saved as greeting-client.py
import Pyro4
Pyro4.config.SERIALIZER = 'pickle'

#  greeting_maker = Pyro4.Proxy("PYRONAME:example.greeting")    # use name server object lookup uri shortcut

conn = Pyro4.Proxy('PYRONAME:test.sql')
#  print(conn)
#  help(conn)
df = conn.read('SELECT * FROM \"plugin_stress_TEST\"')
print(df)
print(type(df))
#  print(greeting_maker.get_fortune(name))
