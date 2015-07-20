#! /usr/bin/python

import mp

controller_addr = ("localhost", 10000)
metapoll = Metapoll()
cm = ContextManager(metapoll, controller_addr)

while True:
    metapoll.do_poll()
