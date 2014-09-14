#!/usr/bin/python

import sys, os

if len(sys.argv) != 2:
	print 'USAGE: ' + sys.argv[0] + ' <graph name>'
	sys.exit()

graph = sys.argv[1]

gGph = graph + '.gph'
gNlist = graph + '.nlist'

if not os.path.exists(gNlist):
	print('./gph2nlist.py ' + gGph + ' > ' + gNlist)
	os.system('./gph2nlist.py ' + gGph + ' none > ' + gNlist)

