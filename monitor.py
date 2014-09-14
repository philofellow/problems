#! /usr/bin/python

import sys, os, time, logging
from helper import *

ConfigNodeLog()

if sys.argv[1] == 'start':
	walltime = int(sys.argv[2])*3600
	Start()
	time.sleep(walltime - 300)
	Stop()
	os.system('killall -u $USER')
if sys.argv[1] == 'stop':
	Stop()
	os.system('killall -u $USER')
if sys.argv[1] == 'clean':
	Stop()

