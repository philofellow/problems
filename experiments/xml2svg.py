#!/usr/bin/python

"""
Code processes an xml file (from stdin) and outputs an svg plot of the events vs. time.
The xml tree must have some <task> element.
Each task consists of <event> and <spawn> elements. <spawn> consists of <events> only.
<event> elements are a text field: <event name="FINISH">12331124</event>, signaling that FINISH event happened at 12331124.

Programmed by Spyros Blanas, 2008.
"""
# Plotting options
HEIGHT = 3		# bar height
STRIDE = HEIGHT		# space between different tasks
FREESPACE = 0	# min space between tasks in same line
XSCALE = 1000.	# scale down x axis by that many times

import xml.dom.minidom
import sys

FREESPACE *= XSCALE
newdoc = xml.dom.minidom.getDOMImplementation().createDocument(None, "svg", None);

inf = 1e40000
borderx = [inf, -inf]
bordery = [inf, -inf]
xoffset = 0
colorguide={}

def genColorList():
	steps = ["00", "80", "C0", "FF"]
	ret = []
	for i in xrange(0,4):
		for j in xrange(0,4):
			for k in xrange(0,4):
				ret.append("#" + steps[i] + steps[j] + steps[k])
	return ret

#colorlist = genColorList();
colorlist = ["aqua", "black", "blue", "fuchsia", "gray", "green", "lime", "maroon", "navy", "olive", "purple", "red", "silver", "teal", '''"white",''' "yellow"] # w3c colors
#colorlist = ["#1", "maroon", "#3", "blue", "#CCCCCC", "#FFC000", "#7", "#8", "green", "red", "red", "fuchsia"] # better looking colors

def testPlotting():
	global newdoc
	addRawBox(100,100,200,200, "red")
	addRawBox(100,200,200,300, "green")
	addRawBox(1000,2000,2000,3000, "blue")
	addRawLine(200,200, 1000,2000)
	postproc()
	print newdoc.toprettyxml()

def postproc():
#	per SVG spec x, y are ignored for the outermost tag
#	newdoc.documentElement.setAttribute("x", str(borderx[0]))
#	newdoc.documentElement.setAttribute("y", str(bordery[0]))
#	newdoc.documentElement.setAttribute("width", str(borderx[1]-borderx[0]))
#	newdoc.documentElement.setAttribute("height", str(bordery[1]-bordery[0]))
	newdoc.documentElement.setAttribute("xmlns", "http://www.w3.org/2000/svg")
	newdoc.documentElement.setAttribute("width", str(borderx[1]))
	newdoc.documentElement.setAttribute("height", str(bordery[1]))

	addRawText(borderx[1]/2, -10, "time (sec)")

	# adding vertical lines per 10sec
	for t in xrange(0, long(borderx[1]), 10):
		addRawLine(t, 0, t, bordery[1]);
		addRawText(t-2, -3, t)

	i=0

	# adding legend
	xmax = borderx[1] 
	for transition in colorguide:
		addRawBox(xmax, i, xmax+10, i+HEIGHT, colorguide[transition])
		addRawText(xmax + 11, i, transition[0] + " > " + transition[1]);
		i+=HEIGHT+HEIGHT;

def getMinimum(nd):
	global XSCALE
	ret = inf
	for node in nd.getElementsByTagName("event"):
		assert node.firstChild.nodeType == node.TEXT_NODE
		ret = min(ret, long(float(node.firstChild.data)))
	return ret/XSCALE

def addRawBox(x1, y1, x2, y2, color):
	xmin = min(x1,x2)
	width = abs(x2-x1)
	ymin = min(y1,y2)
	height = abs(y2-y1)

	borderx[0] = min(borderx[0], xmin)
	borderx[1] = max(borderx[1], xmin+width)
	bordery[0] = min(bordery[0], ymin)
	bordery[1] = max(bordery[1], ymin+height)

	tmp = newdoc.createElement("rect")
	tmp.setAttribute("x", str(xmin))
	tmp.setAttribute("y", str(ymin))
	tmp.setAttribute("width", str(width))
	tmp.setAttribute("height", str(height))
	tmp.setAttribute("style", "fill:" + color) # + ";stroke:black;stroke-width:1")
	newdoc.documentElement.appendChild(tmp);

def addBox(x1, y1, x2, y2, color):
	global xoffset
	addRawBox(x1 - xoffset, y1, x2 - xoffset, y2, color)

def addRawLine(x1, y1, x2, y2):
	borderx[0] = min(borderx[0], x1, x2)
	borderx[1] = max(borderx[1], x1, x2)
	bordery[0] = min(bordery[0], y1, y2)
	bordery[1] = max(bordery[1], y1, y2)

	tmp = newdoc.createElement("line")
	tmp.setAttribute("x1", str(x1))
	tmp.setAttribute("x2", str(x2))
	tmp.setAttribute("y1", str(y1))
	tmp.setAttribute("y2", str(y2))
	tmp.setAttribute("style", "stroke:grey;stroke-width:0.1")
	newdoc.documentElement.appendChild(tmp);

def addLine(x1, y1, x2, y2):
	global xoffset
	addRawLine(x1 - xoffset, y1, x2 - xoffset, y2)

def addRawText(x, y, text):
	tmp = newdoc.createElement("text")
	tmp.setAttribute("x", str(x))
	tmp.setAttribute("y", str(y+3))
	tmp.setAttribute("style", "font-size:4px")
	tmp.appendChild(newdoc.createTextNode(str(text)))
	newdoc.documentElement.appendChild(tmp);

def getAllTaskNodes(node):
	"""Returns a list of all the task nodes in the tree"""
	return node.getElementsByTagName("task")

def getRuns(node):
	"""
	Returns a list of runs for that node.
	A run is a sorted list of events happening in the same thread.
	"""
	ret = []
	spawnnodes = node.getElementsByTagName("spawn")
	mainrun = [(long(float(eventnode.firstChild.data)), eventnode.getAttribute("name")) 
				for eventnode in node.getElementsByTagName("event") 
				if eventnode.parentNode == node]
	mainrun.sort()
	ret.append(mainrun)
	for child in spawnnodes:
		ret.append(getRuns(child)[0]); # assuming no nested spawns here
	return ret

def getTransitions(allruns):
	"""Returns a list of (event1, event2), which are all the transitions in the document"""
	transitions = []
	for task in allruns:
		for run in task:
			for i in xrange(0, len(run)-1):
				tpl = (run[i][1], run[i+1][1])
				if tpl not in transitions:
					transitions.append(tpl)
	return transitions

def plotRun(run, y):
	"""Plots one run in the current y coordinate.""" 
	global HEIGHT, XSCALE, colorguide
	for i in xrange(0, len(run) - 1):
		# plot from event run[i][1] to run[i+1][1]
		addBox(run[i][0]/XSCALE, y, run[i+1][0]/XSCALE, y+HEIGHT, colorguide[(run[i][1],run[i+1][1])])

def plotRunList(runlist, y):
	"""Plots one runlist in the current y coordinate, returning the height of the thing.""" 
	firsttime = True	# hack++ to print non-overlapping spawns (the common case) correctly
	for run in runlist:
		plotRun(run, y)
		if firsttime:
			y += HEIGHT
			firsttime = False;
	return len(runlist)*HEIGHT

def getRunMinMax(run):
	"""Returns a tuple with (xmin,xmax) for this run"""
	return (run[0][0], run[-1][0])

def getRunListMinMax(runlist):
	"""Returns a tuple with (xmin,xmax) for this list of runs"""
	global inf
	mi = +inf
	ma = -inf
	for run in runlist:
		answ = getRunMinMax(run);
		mi = min(answ[0], mi)
		ma = max(answ[1], ma)
	return (mi,ma)

def plotList(someruns, cury, label):
	"""Plots those runs in a compact way, returning the total height of the plot."""
	global HEIGHT, STRIDE, FREESPACE
	slotrunlist = [ [] ]
	slotmax = [ 0 ]

	# pack
	for runlist in sorted(someruns):
		# does this runlist fit in any slot?
		xbounds = getRunListMinMax(runlist);
		slotthatfits = -1
		for i in xrange(0, len(slotmax)):
			if slotmax[i] + FREESPACE < xbounds[0]:
				slotthatfits = i
				break

		# if no, create a new slot
		if slotthatfits == -1:
			slotthatfits = len(slotmax)
			slotmax.append(0);
			slotrunlist.append([]);

		# use the slot
		slotrunlist[slotthatfits].append(runlist);
		slotmax[slotthatfits] = xbounds[1]

	#	print slotthatfits, "<-", getRunListMinMax(runlist), runlist

#	for i in xrange(0, len(slotmax)):
#		print "---------------------"
#		print "SLOT MAX TIME:", slotmax[i]
#		for runlist in slotrunlist:
#			print "RUN:", runlist

	# plot
	heightused = 0
	totalheight = 0
	for slot in slotrunlist:
		# print every listrun in this slot
		cury += STRIDE + heightused
		totalheight += STRIDE + heightused
		heightused = 0
		for listrun in slot:
			uh = plotRunList(listrun, cury)
		#	print "PLOTTING HEIGHT: ", uh, "OF LIST:", listrun
			heightused = max(heightused, uh)
	#quit()
	addRawText(-50, cury-totalheight/2, label)
	return totalheight

#testPlotting()

# parse
dom = xml.dom.minidom.parse(sys.stdin);
xoffset = getMinimum(dom)
allruns = []
for node in getAllTaskNodes(dom.documentElement):
	tmp = getRuns(node)
	allruns.append(tmp);

# create color table, assigning each event transition to a new color
i = 0
for transition in getTransitions(allruns):
	colorguide[transition]=colorlist[i]
	i = (i + 1) % len(colorlist)
del i

# plot
y=0
for host in dom.documentElement.getElementsByTagName("host"):
	for phase in host.getElementsByTagName("state"):
		allruns = []
		for node in getAllTaskNodes(phase):
			tmp = getRuns(node)
			allruns.append(tmp);
		heightused = plotList(allruns, y, host.getAttribute("name"))
		y += heightused + 3*STRIDE	# something fancier here, like a line, someday...
		

postproc()
print newdoc.toprettyxml()
