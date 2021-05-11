#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from meerschaum.connectors.sql import SQLConnector
import Pyro4
Pyro4.config.SERIALIZER = 'pickle'

ExposedSQLConnector = Pyro4.expose(SQLConnector)
print(ExposedSQLConnector)
daemon = Pyro4.Daemon()
ns = Pyro4.locateNS()
uri = daemon.register(ExposedSQLConnector, 'test.sql')
print(uri)
ns.register('test.sql', uri)
daemon.requestLoop()

#  @Pyro4.expose
#  class GreetingMaker(object):
    #  def get_fortune(self, name):
        #  return "Hello, {0}. Here is your fortune message:\n" \
               #  "Tomorrow's lucky number is 12345678.".format(name)

#  daemon = Pyro4.Daemon()                # make a Pyro daemon
#  ns = Pyro4.locateNS()                  # find the name server
#  uri = daemon.register(GreetingMaker)   # register the greeting maker as a Pyro object
#  ns.register("example.greeting", uri)   # register the object with a name in the name server

#  print("Ready.")
#  daemon.requestLoop()                   # start the event loop of the server to wait for calls
