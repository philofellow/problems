#! /usr/bin/python

from utils import *
import sys, os, sys, logging

def Round(size): # return size in K, M, G, or T
	label = ['B','K', 'M', 'G', 'T']
	size = float(size)
	pos = 0
	while size > 1024:
		size = size/1024
		pos += 1
	return str(int(size)) + label[pos]

def GetSecondOfFile(path):
	com = 'hadoop fs -stat ' + path
	data = DoCommand(com).split('\n')[-2].split()
	date = int(data[0].split('-')[2])
	ptime = data[1].split(':')
	hour = int(ptime[0]) + date*24
	minute = int(ptime[1]) + hour*60
	second = int(ptime[2]) + minute*60
	return second

ConfigNodeLog()
maxPerNode = '4'
hadConfFolder = DoCommand('echo $HADOOP_CONF_DIR')[:-1]
hadHomeFolder = DoCommand('echo $HADOOP_HOME')[:-1]
run_dir = GetSahadHome() + '/experiments/'
nodes = str(len(GetNodes(hadConfFolder + '/slaves')))

if(len(sys.argv) != 3):
	logging.info('USAGE: ' + sys.argv[0] + ' graph path_len')
	sys.exit()

logging.info('begin obtaining stats')

graph = sys.argv[1]
path_len = sys.argv[2]
output = graph + '.' + path_len + '.data'
colorNum = templateName.split('-')[0]
historyFolder = hadHomeFolder + '/logs/history'
plotFolder = run_dir + 'result/performance/' + graph + '.' + path_len + '.' \
		+ str(time.time())
if not os.path.exists(plotFolder):
	DoCommand('mkdir -p ' + plotFolder)
logging.info('svg figures will be stored in ' + plotFolder)
res_file = run_dir + 'result/' + output
logging.info('data will be appended to ' + res_file)

# get size and count results
data = DoCommand('hadoop fs -lsr').split('\n')[:-1]
fileSize = dict()
count = 'n/a'
complete = False

for l in data:
	list = l.split()
	size = int(list[4])
	fileName = list[-1]
	if 'part' in fileName:
		sub_path = fileName.split('/')[3]
		if sub_path not in fileSize:
			fileSize[sub_path] = size
		else:
			fileSize[sub_path] += size
		if 'final' in template and size > 0:
			complete = True
			mwp_len = DoCommand('hadoop fs -cat ' + fileName).split()[1]
			#	count = DoCommand('./factor.py ' + count + ' ' + isom + ' ' + colorNum)[:-1]
			mwp_len = str('%.2e' % float(mwp_len))

for key in fileSize:
	fileSize[key] = Round(fileSize[key])
gFileSize = Round(int(DoCommand('ls -l ' + run_dir + 'graph/' + graph + '.wnlist')[:-1].split()[4]))
fileSize['graph'] = gFileSize

# get running time of the run
rTime = '0'
bTime = GetSecondOfFile('/user/zhaozhao/graph')
if complete is True:
	eTime = GetSecondOfFile('/user/zhaozhao/final')
	rTime = str(eTime - bTime)

# writing running stats
if not os.path.exists(res_file):
	f = open(res_file, 'w')
	f.write('#nodes\tpath_len\ttime(sec)\tmaxPerNode\t{subPathSize}\n')
	f.close()

f = open(res_file, 'a')
f.write(nodes + '\t' + count + '\t' + rTime + '\t' + maxPerNode + '\t')
for key in fileSize:
	f.write(key + ':' + fileSize[key] + ' ')
f.write('\n')
f.close()

# generating svg files
logging.info('generating svg files from folder ' + historyFolder)

for root, subFolders, files in os.walk(historyFolder):
	for filename in files:
		hFile = os.path.join(root, filename)
		if hFile[-3:] == 'xml' or hFile[-3:] == 'crc': continue
		hSplit = hFile.split('_')
		subT = hSplit[-1]
		timeS = hSplit[-4]
		svgFile = plotFolder + '/' + graph + '.' + path_len + '.' + timeS + '.' + subT + '.svg'
		logging.info('./statstoxml.py < ' + hFile + ' | ./xml2svg.py > ' + svgFile)
		DoCommand(run_dir + 'statstoxml.py < ' + hFile + ' | ' + run_dir + 'xml2svg.py > ' + svgFile)
		#DoCommand('./statstoxml.py < ' + hFile + ' > ' + svgFile + '.xml')
