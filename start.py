#! /usr/bin/python

import sys, os, logging
from helper import *

if(len(sys.argv) != 2):
	print 'USAGE: ' + sys.argv[0] + ' walltime(h)'
	sys.exit()

ConfigCommonLog()

master = GetMasterIp()
slave = GetSlaveIps()
sahad_home = GetSahadHome() 

logging.info('will start monitor on ' + master + ' (namenode)')
os.system('ssh ' + master + ' \'' + sahad_home + '/monitor.py start ' + sys.argv[1] + '\' &')
for n in slave:
	logging.info('will start monitor on ' + n)
	os.system('ssh ' + n + ' \'' + sahad_home + '/monitor.py start ' + sys.argv[1] + '\' &')
