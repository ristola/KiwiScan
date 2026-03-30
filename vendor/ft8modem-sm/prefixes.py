#!/usr/bin/env -S python3 -B
#
#   prefixes.py - call prefixes
#
#   Copyright (C) 2025 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

import io
import os
import os.path


# global table
prefix_table = { }


#
#  init() - load 'prefix_table'
#
def init():
	global prefix_table

	me = os.path.abspath(__file__)
	me_dir = os.path.dirname(me)
	me_file = os.path.join(me_dir, 'prefixes.txt')

	for fn in [ me_file, './prefixes.txt' ]:
		if not os.path.isfile(fn):
			continue
		lines = None
		with io.open(fn, 'r') as prefix_file:
			lines = [ line.rstrip() for line in prefix_file ]
		for line in lines:
			ps, country = line.split('\t', 1)
			ps = ps.strip()
			country = country.strip()
			if '-' in ps:
				start, stop = ps.split('-')
				start = start.strip()
				stop = stop.strip()
				#print(start, stop) # DEBUG:
				p = start
				k1 = p[0]
				if k1 not in prefix_table.keys():
					prefix_table[k1] = [ ]
				prefix_table[k1].append((p, country))
				while p != stop:
					#print("'%s' -> '%s'\n" % (p, stop)) # DEBUG:
					p = p[:-1] + chr(1 + ord(p[-1]))
					k1 = p[0]
					if k1 not in prefix_table.keys():
						prefix_table[k1] = [ ]
					prefix_table[k1].append((p, country))
			else:
				k1 = ps[0]
				if k1 not in prefix_table.keys():
					prefix_table[k1] = [ ]
				prefix_table[k1].append((ps, country))
		return True
	return False


#
#  get_prefix(call) -> (prefix, country)
#
def get_prefix(call):
	global prefix_table

	if not prefix_table:
		init()

	# TODO: this is O(n * m) - very slow; find a way to speed it up
	cleancall = call.upper()
	k1 = cleancall[0]
	if k1 not in prefix_table.keys():
		return None
	for i in prefix_table[k1]:
		if cleancall.startswith(i[0]):
			return i
	return None


#
#  entry point - unit tests
#
if __name__ == '__main__':
	import sys
	init()
	#for i in prefix_table:
	#	sys.stdout.write("%s\n" % str(i))
	for i in [ 'bw3fff', 'by3fff', 'bz3fff' ]:
		sys.stdout.write("%s -> %s\n" % (i, get_prefix(i)))

# EOF: prefixes.py
