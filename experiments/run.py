#! /usr/bin/python
from utils import *
import sys, os, time, logging

ConfigNodeLog()
if(len(sys.argv) != 3):
	logging.info('USAGE: ' + sys.argv[0] + ' graph path_len')
	sys.exit()

run_dir = GetSahadHome() + '/experiments/'
logging.info('loading params...')
graph = run_dir + 'graph/' + sys.argv[1] + '.wnlist'
path_len = sys.argv[2]
logging.info('params are graph: ' + graph + ', path length: ' + path_len)

logging.info('stopping hadoop...')
DoCommand('stop-all.sh')
time.sleep(20) # wait hadoop to be ready
logging.info('remove any sahad specific library from $HADOOP_HOME/lib')
DoCommand('rm -f $HADOOP_HOME/lib/sahad-lib.jar')
logging.info('remove logs')
DoCommand('rm -rf $HADOOP_HOME/logs/*')
logging.info('starting hadoop...')
DoCommand('start-all.sh')
time.sleep(60) # wait hadoop to be ready

#os.system('./make.sh') # make java binary file
ls = DoCommand('hadoop fs -ls').split('\n')
for l in ls[1:]:
	dir = l.split(' ')[-1]
	os.system('hadoop fs -rmr ' + dir)

logging.info('copying graph files to hdfs...')
os.system('hadoop fs -mkdir graph')
start = time.time()
os.system('hadoop fs -put ' + graph + ' graph')
logging.info('time to copy graph to HDFS: ' + str(time.time() - start) + ' seconds.')

time.sleep(60) # wait hdfs file copy to be ready

logging.info('begin computation')

os.system('hadoop jar ' + run_dir + 'bin/mwp.jar SubgraphCounting ' 
		+ path_len + ' >> ' + GetMasterNodeLogName())
