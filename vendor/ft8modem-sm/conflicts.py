#!/usr/bin/env -S python3 -B
#
#   conflicts.py
#
#   Locate running processes that might conflict with starting a new one.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

import psutil
import os
import os.path

#
#  find_ft8report(path) - try to find another ft8report already running
#
#     path = path passed by -a option
#
def find_ft8report(path = None):
	# list of matching process IDs
	result = [ ]

	# get my own PID to avoid false positive
	mypid = os.getpid()

	# if path given, expand it to fully-qualified string
	if path:
		path = os.path.expanduser(path)
		path = os.path.abspath(path)

	# search the processes
	for pid in psutil.pids():
		# skip my own PID
		if pid == mypid:
			continue

		found = False
		a = False
		other_path = None

		# (try to) read the process info
		cl = None
		try:
			p = psutil.Process(pid)
			cl = p.cmdline()
		except:
			cl = None
		if not cl: # if that failed, just move on
			continue

		# look at the command-line tokens
		for token in cl:
			bn = os.path.basename(token)
			if bn == 'ft8report':
				found = True
			if found:
				if token == '-a':
					a = True
				elif a:
					other_path = token
					break

		# if no path given, match just on command
		if found and not path:
			result.append(pid)
			continue

		# if path was given, also match on the -a option's path
		if found and other_path:
			other_path = os.path.expanduser(other_path)
			other_path = os.path.abspath(other_path)
			if path == other_path:
				result.append(pid)

	# return whatever was found
	return result


#
#  entry point - unit tests
#
if __name__ == '__main__':
	import sys

	# read the path if given
	path = None
	if sys.argv[1:]:
		path = sys.argv[1]
	
	# run the function, and report results
	others = find_ft8report(path)
	for pid in others:
		sys.stdout.write("PID %d matches.\n" % pid)

# EOF: conflicts.py
