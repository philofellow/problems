#! /usr/bin/python
import os, sys, time, logging
from subprocess import Popen, PIPE

def GetHostName():
	return DoCommand('hostname')[:-1]

def GetHadoopConfDir():
	return DoCommand('echo $HADOOP_CONF_DIR')[:-1]

def GetSlaveNum():
	return str(len(GetSlaveIps()))

def GetSahadHome():
	sahad_env = GetHadoopConfDir() + '/sahad.env'
	f = open(sahad_env, 'r')
	return f.readline()

def GetHostType():
	f = open('/etc/hosts', 'r')
	hosts = f.read()
	if 'cc.vt.edu' in hosts:
		return 'athena'
	if 'ice.vbi.vt.edu' in hosts:
		return 'pecos'

def ConfigCommonLog():
	logging.basicConfig(format = '%(asctime)s, %(levelname)s, %(message)s', filename = GetSahadHome() + '/sahad.log', level = logging.INFO)

def ConfigNodeLog():
	my_ip = GetMyIp()
	name = GetSahadHome() + '/sahad.' + my_ip 
	if my_ip == GetMasterIp():
		name += 'm.log'
	else:
		name += '.log'
	logging.basicConfig(format = '%(asctime)s, %(levelname)s, %(message)s', filename = name, level = logging.INFO)

def GetMyIp():
	if GetHostType() == 'pecos':
		line = DoCommand('ip -f inet addr show ib0 | grep inet')
	elif GetHostType() == 'athena':
		line = DoCommand('/sbin/ip -f inet addr show eth0 | grep inet')
	ip = line.split()[1].split('/')[0]
	return ip

def GetMyHostName():
	return DoCommand('hostname')[:-1]

def GetMasterIp():
	masterFile = GetHadoopConfDir() + '/masters'
	return GetNodes(masterFile)[0]

def GetSlaveIps():
	slaveFile = GetHadoopConfDir() + '/slaves'
	return GetNodes(slaveFile)

def GetMasterNodeLogName():
	return GetSahadHome() + '/sahad.' + GetMasterIp() + 'm.log' 

def DoCommand(com):
	p = Popen(com, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = p.communicate()
	return stdout

def GetNodes(file):
	f = open(file, 'r')
	nodes = []
	for line in f:
		node = line.split()
		for n in node:
			n = n.strip()
			if n not in nodes and n != '\n' and n != '':
				nodes.append(n)
	return nodes

def GetHdfsDir():
	host = DoCommand('hostname')[:-1]
	local_folder = ''
	if host == 'pecos' or host[0:2] == 'r1' or host[0:2] == 'r2': # pecos or its nodes
		local_folder = '/localscratch'
	elif 'athena' in host:
		local_folder = '/local'
	return local_folder

