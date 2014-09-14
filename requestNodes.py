#! /usr/bin/python
import os, sys, time, logging
from helper import *

if len(sys.argv) != 3:
	print "USAGE: " + sys.argv[0] + " walltime(h) nodeNum"
	print 'Be sure that $JAVA_HOME, $HADOOP_HOME and $HADOOP_CONF_DIR are properly set for all nodes. This can be done by setting them in your .bashrc'
	sys.exit()

print 'Be sure that $JAVA_HOME, $HADOOP_HOME and $HADOOP_CONF_DIR are properly set for all nodes. This can be done by setting them in your .bashrc'

os.system('rm -f *.log')

hadConfFolder = GetHadoopConfDir() 
walltime = str(int(sys.argv[1])*3600)
nodeNum = sys.argv[2]
DoCommand('rm ' + hadConfFolder + '/*')
DoCommand('cp conf/* ' + hadConfFolder)

qsubFile = hadConfFolder + '/torqueHadoop.qsub' 
WriteQsubFile(qsubFile, walltime, nodeNum)
jobID = DoCommand('qsub ' + qsubFile) 

WriteSahadEnv(hadConfFolder)
ConfigCommonLog()
logging.info('====== STARTING A NEW RUN ========')
logging.info('begin requesting resource, walltime = ' + sys.argv[1] + 'h, nodeNum = ' + nodeNum)

while(1):
	jobInfo = DoCommand('qstat -n ' + jobID)
	logging.info(jobInfo)
	jobStat = GetJobStat(jobInfo) 
	logging.info('job stat: ' + jobStat)
	time.sleep(20) # check whether job state becomes 'R' for every 20 seconds
	if jobStat == 'R': # nodes reserved
		nodeList = GetNodeList(jobInfo)
		logging.info('nodes reserved: ' + str(nodeList))
		logging.info('hadoop conf files is stored at ' + hadConfFolder)
		WriteHadoopConfig(nodeList, hadConfFolder)
		logging.info('you have been successfully assigned ' + str(nodeList) + ' for your hadoop job')
		logging.info(nodeList[0] + ' is the master and rest are the slaves')
		local_folder = GetHdfsDir()
		logging.info('hdfs is inited at ' + local_folder + '/hadoop-$USER')
		logging.info('job will be killed and ' + local_folder + '/hadoop-$USER will be emptied after ' + walltime + ' seconds')
		logging.info('it is RECOMMENDED you run stop.py after finish the job to fully release the resources')
		logging.info('starting monitor')
		os.system('./start.py ' + str(int(walltime)/3600) + ' &')
		sys.exit()
	elif jobStat == 'C':
		logging.warning('qsub job is abruptly terminated, no resources are requested')
		sys.exit()
