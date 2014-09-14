#! /usr/bin/python

import sys, os, logging
from helper import *

if(len(sys.argv) != 1):
	print 'USAGE: ' + sys.argv[0]
	sys.exit()

ConfigCommonLog()

master = GetMasterIp()
slave = GetSlaveIps()
sahad_home = GetSahadHome() 

logging.info('will stop monitor on ' + master + ' (namenode)')
DoCommand('ssh ' + master + ' \'' + sahad_home + '/monitor.py stop\' &')
for n in slave:
	logging.info('will stop monitor on ' + n)
	DoCommand('ssh ' + n + ' \'' + sahad_home + '/monitor.py stop\' &')

DoCommand('ssh ' + master + ' \'' + sahad_home + '/monitor.py clean\' &')
for n in slave:
	DoCommand('ssh ' + n + ' \'' + sahad_home + '/monitor.py clean\' &')
