#! /usr/bin/python
from utils import *
import sys, os, time, logging

ConfigNodeLog()

graphs = ['test_graph']
path_len = '5'

logging.info('computing on graphs: ' + str(graphs))
sahad_home = GetSahadHome() + '/experiments'
for g in graphs:
	logging.info('begin iteration for graph ' + g + ' with path len ' + path_len)
	logging.info(sahad_home + '/run.py ' + g + ' ' + path_len + ' &')
	os.system(sahad_home + '/run.py ' + g + ' ' + path_len + ' &')
	time.sleep(1200)
	logging.info(sahad_home + '/stat.py ' + g + ' ' + path_len + ' &')
	DoCommand(sahad_home + '/stat.py ' + g + ' ' + path_len + ' &')
	time.sleep(60)
	logging.info('computation for graph %s done!' % g)
	
logging.info('computing on graphs: ' + str(graphs) + ' completed')
