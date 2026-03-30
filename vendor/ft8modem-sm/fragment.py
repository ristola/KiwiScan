#!/usr/bin/env -S python3 -B
#
#   fragment.py - packet splitting math
#
#   This module is a single method, but it is placed in its
#   own module to allow easier unit testing.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#


# for trunc(...)
import math


#
#  fragment(s, m)
#
#  fragment a list 's' into a list of pieces of maximum size 'm'
#     with the actual size of the pieces as close to each other
#     as possible, given the initial size of 's'
#
def fragment(s, m):
	if not s:
		return [ ]
	n = len(s)
	f = n / m
	count = int(math.trunc(f))
	if f > count:
		count += 1
	size = n / count
	accum = size

	# now fragment the set
	result = [ ]
	while s:
		ts = int(round(accum))
		accum = accum + size - ts
		
		if len(s) < ts:
			result.append(s)
			return result
		result.append(s[0:ts])
		s = s[ts:]
	return result


#
#  entry point (unit tests)
#
if __name__ == '__main__':
	import sys
	import random

	n = 0 # total records
	m = 5 # max records

	def flatten(xss):
		return [x for xs in xss for x in xs]

	errors = 0
	while n <= 26:
		items = [ random.randint(0, 9) for _ in range(n) ]
		frags = fragment(items, m)
		flatt = flatten(frags)
		sys.stdout.write("fragment(..., %d) ->\n" % m)
		sys.stdout.write("\tin: %s\n" % items)
		sys.stdout.write("\tre: %s" % flatt)
		if flatt == items:
			sys.stdout.write(" (OK)\n")
		else:
			sys.stdout.write(" (FAIL)\n")
			errors += 1
		sys.stdout.write("\tout: %s\n" % frags)
		sys.stdout.write("\tsizes: ")
		for f in frags:
			sys.stdout.write("%d, " % len(f))
		sys.stdout.write("\n")
		n += 1
	
	sys.stdout.write("\nThere were %d errors.\n\n" % errors)

# EOF
