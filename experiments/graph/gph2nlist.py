#! /usr/bin/python

# This program will transform a galib format file to neighbor list as following:
# v1	u1,u2,...,uk, label1
# v2	q1,q2,...,qm, label2
# .....
# 
# The label is either uniformly randomly assigned, or the age 
# from the demographic file, as user specified
#############################################################

import os
import sys
import random

def LoadVertexDegree(line):
	data = line.split()
	return data[0], int(data[1])

def LoadDem(demFile):
	demMap = dict()
	col = 3 # the column of the data that we are interested, i.e. age
	f = open(demFile, 'r')
	f.readline() # skip first line
	for line in f:
		dem = line.split()
		v = dem[0]
		age = int(dem[col-1])
		if age <= 6: demMap[v] = 0 # kid
		elif age <= 18 and age > 6: demMap[v] = 1 # youth 
		elif age <= 55 and age > 18: demMap[v] = 2 # adult 
		elif age > 55: demMap[v] = 3 # old
	f.close()
	return demMap

def OutputNeighbor(f, deg, label, vmap):
	for i in range(d):
		u = f.readline().split()[0]
		sys.stdout.write(vMap[u] + ',')
	sys.stdout.write(' ' + label +'\n')

# generate new id of the node starting from 0
def BuildNodeMap(gFile):
	vmap = dict()
	vid = 0
	f = open(gFile, 'r')
	n = int(f.readline()[:-1])
	for i in range(n):
		v, d = LoadVertexDegree(f.readline())
		vmap[v] = str(vid)
		vid += 1
		for i in range(d):
			f.readline()
	f.close()
	return vmap

#################################################################3

if len(sys.argv) != 4 and len(sys.argv) != 3:
	print "USAGE: " + sys.argv[0] + ' graph.gph none/rand/dem [rand_range/demofile]\n';
	sys.exit()

vMap = BuildNodeMap(sys.argv[1]);

f = open(sys.argv[1], 'r')
n = int(f.readline()[:-1])

if sys.argv[2] == 'none': # non-labeled graph
	for i in range(n):
		v, d = LoadVertexDegree(f.readline())
		sys.stdout.write(vMap[v] + '\t')
		OutputNeighbor(f, d, '', vMap)


if sys.argv[2] == 'rand': #randomly generate label between 1 to sys.argv[3]
	for i in range(n):
		v, d = LoadVertexDegree(f.readline())
		rand = random.randrange(int(sys.argv[3]))
		sys.stdout.write(vMap[v] + '\t')
		OutputNeighbor(f, d, str(rand), vMap)

if sys.argv[2] == 'dem': #generate label from the age of demographic file
	demMap = LoadDem(sys.argv[3])
	for i in range(n):
		v, d = LoadVertexDegree(f.readline())
		sys.stdout.write(vMap[v] + '\t')
		OutputNeighbor(f, d, str(emMap[v]), vMap)

f.close()
