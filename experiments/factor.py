#! /usr/bin/python
import os
import sys

def pow(a, b):
	x = a
	for i in range(1, b):
		x = x*a
	return x

def factorial(a):
	for i in range(1, a):
		a = a*i
	return a

if len(sys.argv) != 4:
	print 'USAGE: ' + sys.argv[0] + ' value self-isom colorNum'
	sys.exit()

value = float(sys.argv[1])
isom = int(sys.argv[2])
colorNum = int(sys.argv[3])

result = value*pow(colorNum, colorNum)/factorial(colorNum)/isom
print '%e' % result
