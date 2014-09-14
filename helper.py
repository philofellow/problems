#! /usr/bin/python
import os, sys, time, logging
PATH = os.path.realpath(__file__)
sys.path.append(PATH[:PATH.rfind('/')] + '/experiments/')
from utils import *

def WriteSahadEnv(folder):
	fname = '/sahad.env'
	f = open(folder + fname, 'w')
	path = os.path.realpath(__file__)
	f.write(path[:path.rfind('/')])
	f.close()

def WriteQsubFile(qsubFile, walltime, nodeNum):
	ppn = GetLocalPPN()
	host = DoCommand('hostname')[:-1]
	if host == 'athena1':
		host = 'athena'
	fqsub = open(qsubFile, 'w')
	fqsub.write('#!/bin/bash\n')
	fqsub.write('#PBS -l walltime=' + walltime + '\n')
	fqsub.write('#PBS -l nodes=' + nodeNum + ':ppn=' + ppn + '\n')
	fqsub.write('#PBS -W group_list=' + host + '\n')
	fqsub.write('#PBS -q ' + host + '_q\n')
	fqsub.write('#PBS -A ' + host +'\n')
	fqsub.write('cd $PBS_O_WORKDIR\n')
	fqsub.write('sleep ' + walltime + ';\n')
	fqsub.write('exit;\n') 
	fqsub.close()


def WriteXMLProperty(f, name, value, description):
    f.write('<property>\n')
    f.write('<name>' + name + '</name>\n')
    f.write('<value>' + value + '</value>\n')
    f.write('<description>' + description + '</description>\n')
    f.write('</property>\n\n')

def WriteHead(f):
    f.write('<?xml version="1.0"?>\n')
    f.write('<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n')
    f.write('<!-- Put site-specific property overrides in this file. -->\n\n')

def WriteHadoopConfig(nodeList, folder):
    # write master and slave files
    master = open(folder + '/masters', 'w')
    slave = open(folder + '/slaves', 'w')
    master.write(nodeList[0] + '\n')
    for node in nodeList[1:]:
        slave.write(node + '\n')
    master.close()
    slave.close()

    numSimMapper = 1
    numSimReducer = 4 
    numMapper = len(nodeList[1:])
    numReducer = len(nodeList[1:])*numSimReducer
    block_size = str(100*1024*1024)

    # core-site.xml
    coreSiteXml = open(folder + '/core-site.xml', 'w')
    WriteHead(coreSiteXml)
    coreSiteXml.write('<configuration>\n\n')
    WriteXMLProperty(coreSiteXml, 'hadoop.tmp.dir', GetHdfsDir() + '/hadoop-${user.name}',
            'a base for other temporary directories')
    WriteXMLProperty(coreSiteXml, 'fs.default.name', 'hdfs://' + nodeList[0] + ':54310',
            'name of the default file system')
    WriteXMLProperty(coreSiteXml, 'pds.host.file', folder + '/slaves', '')
    coreSiteXml.write('</configuration>\n')
    coreSiteXml.close()
	# hdfs-site.xml
    hdfsSiteXml = open(folder + '/hdfs-site.xml', 'w')
    WriteHead(hdfsSiteXml)
    hdfsSiteXml.write('<configuration>\n\n')
    WriteXMLProperty(hdfsSiteXml, 'dfs.replication', '1', '')
    WriteXMLProperty(hdfsSiteXml, 'dfs.block.size', block_size, '')
    hdfsSiteXml.write('</configuration>\n')
    hdfsSiteXml.close()

    # mapred-site.xml
    mapredSiteXml = open(folder + '/mapred-site.xml', 'w')
    WriteHead(mapredSiteXml)
    mapredSiteXml.write('<configuration>\n\n')
    WriteXMLProperty(mapredSiteXml, 'mapred.job.tracker', 'hdfs://' + nodeList[0] + ':54311',
            'host and port that the mapreduce job tracker runs at')
    WriteXMLProperty(mapredSiteXml, 'mapreduce.jobtracker.address', nodeList[0] + ':54311', '')
    WriteXMLProperty(mapredSiteXml, 'mapred.tasktracker.map.tasks.maximum', str(numSimMapper),
            'max number of map tasks run simultaneously by a task tracker')
    WriteXMLProperty(mapredSiteXml, 'mapred.map.tasks', str(numMapper),
            'number of map tasks run by a task tracker')
    WriteXMLProperty(mapredSiteXml, 'mapred.tasktracker.reduce.tasks.maximum', str(numSimReducer),
            'max number of reduce tasks run simultaneously by a task tracker')
    WriteXMLProperty(mapredSiteXml, 'mapred.reduce.tasks', str(numReducer),
            'number of reduce tasks run by a task tracker')
    #WriteXMLProperty(mapredSiteXml, 'mapred.reduce.tasks', str((maxTaskPerNode)*len(nodeList[1:])),
    #WriteXMLProperty(mapredSiteXml, 'mapred.reduce.tasks', str(len(nodeList[1:]) * len(nodeList[1:])),
    #       'reduce tasks per job')
    # WriteXMLProperty(mapredSiteXml, 'mapred.map.tasks.speculative.execution', 'false', '')
    #WriteXMLProperty(mapredSiteXml, 'mapred.jobtracker.taskScheduler',
    #       'org.apache.hadoop.mapred.PreDefinedScheduler', '')
    WriteXMLProperty(mapredSiteXml, 'mapred.min.split.size', block_size, '')
    mapredSiteXml.write('</configuration>\n')
    mapredSiteXml.close()

def Stop():
    logging.info('stop monitor on ' + GetMyIp())
    if GetMyIp() == GetMasterIp():
        DoCommand('stop-all.sh')

    DoCommand('rm -rf ' + GetHdfsDir() + '/hadoop-$USER')

def Start():
    logging.info('monitor is started on ' + GetMyIp() + ' successfully')
    if GetMyIp() == GetMasterIp():
        DoCommand('hadoop namenode -format')
        logging.info('namenode is formatted')
        time.sleep(10)
        DoCommand('start-all.sh')
        os.system(GetSahadHome() + '/experiments/run-a-bunch.py &')

def GetNodeList(job_info):
	nodes = job_info.split('\n')[6:]
	nodeList = []
	for line in nodes:
		nodeInLine = line.split('+')
		for node in nodeInLine:
			node = node.strip()
			host = GetHostName() 
			if host == 'pecos':
				node = node[:-2]
			if node not in nodeList and node != '':
				nodeList.append(node)
	ipList = []
	for n in nodeList:
		ip = DoCommand('host ' + n).split()[-1]
		ipList.append(ip)
	return ipList

def GetJobStat(job_info):
	return job_info.split('\n')[5].split()[-2]

def GetLocalPPN():
	host = GetHostName() 
	ppn = '8'
	if host == 'pecos':
		ppn = '8'
	elif host == 'athena1':
		ppn = '32'
	return ppn
