#!/usr/bin/env -S python3 -B
#
#    tailall.py
#
#    Python utility for following and parsing the ALL.LOG file.
#
#    Copyright (C) 2019,2024 by Matt Roberts.
#    License: GNU GPL3 (www.gnu.org)
#
#


# system modules
import time
import sys

# local modules
import callsigns
import tailfile
from spots import *


#
#  split_tail(s) - for compound messages, split into two
#
def split_tail(s):
	if not s:
		return []
	msgs = s.split(';')
	result = [ ]
	if len(msgs) == 2:
		parts0 = msgs[0].strip().split(' ', 2)
		parts1 = msgs[1].strip().split(' ', 2)
		if len(parts1) == 3 and len(parts0) == 2:
			result.append("%s %s %s" % (parts0[0], parts1[1], parts0[1]))
			result.append("%s %s %s" % (parts1[0], parts1[1], parts1[2]))
		else:
			result = msgs
	else:
		result = msgs
	return result


#
#  error_callback(s) - default callback for error conditions
#
def error_callback(s):
	sys.stderr.write("Error: %s\n" % s)
	sys.stderr.flush()


#
#  tailall(...) - wrapper around taillog, that breaks out the various
#                 data items into a dictionary
#
def tailall(path, callback, ecallback=error_callback, cb_timeout=60):
	def inner_callback(line):
		if not line:
			callback(None)
			return

		lines = None
		try:
			lines = split_tail(line)
		except Exception as ex:
			if ecallback:
				ecallback("split_tail(...) raised %s" % str(ex))
			return # bail

		# if no data, stop
		if not lines:
			return

		# parse the line into a dictionary
		for l2 in lines:
			result = None
			try:
				result = parse_spot(l2)
			except Exception as ex:
				if ecallback:
					ecallback("parse_spot(...) raised %s\n" % str(ex))
				continue # skip it

			# if no spot was produced, skip this line
			if not result:
				continue

			# callback with the filled dictionary
			callback(result)
		
	# run the tail with the callback above (blocking)
	try:
		tailfile.tailfile(path, inner_callback, ecallback, cb_timeout=cb_timeout)
	except KeyboardInterrupt as ex:
		pass


#
#  unit tests
#
if __name__ == '__main__':
	import os.path

	# test split
	compound = 'PY2EW RR73; K4RDZ <4U1UN> -08'
	msgs = split_tail(compound)
	sys.stdout.write("Test split_tail(%s) => %s\n" % (compound, msgs))

	# path to the test file
	fn = '~/.local/share/WSJT-X/ALL.TXT'
	if sys.argv[1:]:
		fn = sys.argv[1]
	fn = os.path.expanduser(fn) 

	# DEBUG:
	sys.stdout.write("Tail file is %s\n" % fn)

	# define a callback that dumps each spot dictionary
	def callback(d):
		if not d:
			sys.stdout.write('Empty data\n')
			return
		sys.stdout.write('Dict: "%s"\n' % str(d))

	# run the test
	try:
		tailall(fn, callback)
	except KeyboardInterrupt as ex:
		pass
