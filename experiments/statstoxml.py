#! /usr/bin/python

from xml.dom.minidom import getDOMImplementation
import sys
import re

newdoc = getDOMImplementation().createDocument(None, "root", None);
#hadoop 1.0.4 use TRACKER_NAME for unfinished MapAttempt/ReduceAttempt, and HOSTNAME in finished tasks.
#e.g.: 
#TRACKER_NAME="tracker_r1i2n6\.ice\.vbi\.vt\.edu:localhost/127\.0\.0\.1:37404
#HOSTNAME="/default-rack/r1i2n6\.ice\.vbi\.vt\.edu"

trackermatcher = re.compile("_.*?:");
hostmatcher = re.compile(r"/[^/]*?$");
spawnmatcher = re.compile(r"^\d+_");

def testXml():
	q = {"SPILL_START":123, "SPILL_END":525}
	InsertSingleMetric("localhost", "map", "tid001", "START", "123");
	InsertSingleMetric("localhost", "map", "tid001", "STOP", "123");
	InsertSingleMetric("localhost", "reduce", "tid001", "START", "123");
	InsertSingleMetric("otherhost", "map", "tid001", "START", "123");
	InsertSingleMetric("otherhost", "map", "tid001", "STOP", "123");
	InsertMultiMetrics("otherhost", "map", "tid001", "spill", q);
	InsertSpawnedMetric("localhost","map", "tid001", "spill-123", "START", 123);
	print newdoc.toprettyxml()
	quit()

def testRe():
	print getTrackerFromTrackerName("tracker_impl02.almaden.ibm.com:localhost/127.0.0.1:54125")
	quit()

def testRe2():
	print getTrackerFromHostName("/default-rack/r1i2n3\.ice\.vbi\.vt\.edu")
	quit()

def testRe3():
	print getTrackerFromHostName("r1i2n5\.ice\.vbi\.vt\.edu")
	quit()

# gets tracker from hostname log entry
def getTrackerFromTrackerName(hostname):
	return trackermatcher.search(hostname).group()[1:-1]

def getTrackerFromHostName(hostname):
	#return hostmatcher.search(hostname).group()[1:]
	return hostname.split('/')[-1]

# Get value for a given key in a given line from the log, or None if property is not there
def getval(line, key):
	for token in line.split(' '):
		if token.startswith(key+'="'):
			return token[len(key)+2:-1]

# getval hacked to work only with COUNTERS
def getcounters(line):
	key="COUNTERS"
	pos = line.find(key)
	if pos < 0:
		return None
	return line[pos+len(key)+2:-2]

def get(root, frname, tag):
	for nd in root.getElementsByTagName(frname):
		if (nd.parentNode == root and nd.getAttribute("name") == tag):
			return nd

	tmp = newdoc.createElement(frname)
	tmp.setAttribute("name", tag);
	return root.appendChild(tmp)

def InsertSingleMetric(node, state, task, event, timestamp):
	global newdoc
	hostnode = get(newdoc.documentElement, "host", node);
	statenode = get(hostnode, "state", state);
	tasknode = get(statenode, "task", task);
	tmp = get(tasknode, "event", event);
	tmp.appendChild(newdoc.createTextNode(str(timestamp)));
	# print "Inserted child with timestamp = " + timestamp;

def InsertSpawnedMetric(node, state, task, spawn, event, timestamp):
	global newdoc
	hostnode = get(newdoc.documentElement, "host", node);
	statenode = get(hostnode, "state", state);
	tasknode = get(statenode, "task", task);
	spawnnode = get(tasknode, "spawn", spawn);
	tmp = get(spawnnode, "event", event);
	tmp.appendChild(newdoc.createTextNode(str(timestamp)));

def InsertMultiMetrics(node, state, task, fname, eventdir):
	global newdoc
	hostnode = get(newdoc.documentElement, "host", node);
	statenode = get(hostnode, "state", state);
	tasknode = get(statenode, "task", task);
	target = get(tasknode, "spawn", fname);
	for entry in eventdir:
		tmp = newdoc.createElement("event");
		tmp.setAttribute("name", entry);
		tmp.appendChild(newdoc.createTextNode(str(eventdir[entry])));
		target.appendChild(tmp);


#testRe()
#testRe2()
#testRe3()
#testXml()

locate = {}

'''
for line in sys.stdin:
	line = line[:-1];
	if line.startswith("MapAttempt"):
		# either START_TIME or FINISH_TIME
		if (line.find("FINISH_TIME") == -1):
			print getTrackerFromTrackerName(getval(line, "TRACKER_NAME"))
		else:
			print getTrackerFromHostName(getval(line, "HOSTNAME"))
'''

for line in sys.stdin:
	line = line[:-1];
	if line.startswith("MapAttempt"):
		# either START_TIME or FINISH_TIME
		if (line.find("FINISH_TIME") == -1):
			# start event
			InsertSingleMetric(getTrackerFromTrackerName(getval(line, "TRACKER_NAME")), "map", getval(line, "TASKID"), "START", getval(line, "START_TIME"));
		else:
			tid = getval(line, "TASKID")
			#print '\n', line, '\n'
			host = getTrackerFromHostName(getval(line, "HOSTNAME"))
			# finish or kill event
			if getval(line, "TASK_STATUS") == "SUCCESS":
				locate[tid] = host
				InsertSingleMetric(host, "map", tid, "FINISH", getval(line, "FINISH_TIME"))
			else:
				InsertSingleMetric(host, "map", tid, "KILL", getval(line, "FINISH_TIME"))


	elif line.startswith("Task") and line.find("TASK_STATUS") != -1:
		tid = getval(line, "TASKID")
		host = locate[tid]
		state = getval(line, "TASK_TYPE").lower()
		match = "Performance Counters."
		perfcounters = [counter[len(match):].split(":") for counter in getcounters(line).split(',') if counter.startswith(match)]
		for counter in perfcounters:
			if spawnmatcher.match(counter[0]):
				# create a new spawn
				res = spawnmatcher.search(counter[0])
				InsertSpawnedMetric(host, state, tid, "spill-" + res.group()[:-1], counter[0][res.end():], counter[1])
			else:
				InsertSingleMetric(host, state, tid, counter[0], counter[1]);


	elif line.startswith("ReduceAttempt"):
		tid = getval(line, "TASKID")
		# either START_TIME or FINISH_TIME
		if (line.find("FINISH_TIME") == -1):
			host = getTrackerFromTrackerName(getval(line, "TRACKER_NAME"))
			# start event
			InsertSingleMetric(host, "reduce", tid, "START", getval(line, "START_TIME"));
		else:
			host = getTrackerFromHostName(getval(line, "HOSTNAME"))
			# finish or kill event
			if getval(line, "TASK_STATUS") == "SUCCESS":
				locate[tid] = host
				InsertSingleMetric(host, "reduce", tid, "FINISH", getval(line, "FINISH_TIME"))
				InsertSingleMetric(host, "reduce", tid, "SORT", getval(line, "SHUFFLE_FINISHED"))
				InsertSingleMetric(host, "reduce", tid, "REDUCE", getval(line, "SORT_FINISHED"))
			else:
				InsertSingleMetric(host, "reduce", tid, "KILL", getval(line, "FINISH_TIME"))

print newdoc.toprettyxml()
