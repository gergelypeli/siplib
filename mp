#! /usr/bin/python

import async
import mgw

controller_addr = ("localhost", 10000)
metapoll = async.Metapoll()
cm = mgw.ContextManager(metapoll, controller_addr)

while True:
    metapoll.do_poll()
