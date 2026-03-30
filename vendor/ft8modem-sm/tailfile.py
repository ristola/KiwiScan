#!/usr/bin/env -S python3 -B
#
#   tailfile.py
#
#   Similar to tail(1) on a text file, run with -f option.
#   Automatically detects when file has been replaced, and
#   tails the new file.
#
#   The read operations are line-oriented, so the function
#   assumes newline-terminated text content.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#


import os
import os.path
import io
import time


#
#  CONSTANTS: these are the "tail modes"
#

# "tail first" - seek to the end of the file on first open, and tail
#    it from there forward; if the file becomes unavailable, or if
#    the file is replaced, the new/restored file is tailed from the
#    start of the file forward; this would be typical behavior for
#    e.g., a log file that is occasionally archived and restarted
TAIL_MODE_TAIL_FIRST = 0

# "tail all" - always seek to the end of the file every time it is
#    opened or reopened; this may cause a few lines to be missed
#    if the file is replaced or becomes unavailable
TAIL_MODE_TAIL_ALL = 1

# "read all" - never seek; read the file in its entirety and report
#    all of its lines to the callback; if the file is replaced,
#    subsequent reads also start at the top of the file
TAIL_MODE_READ_ALL = 2


#
#  tail_safe_error_callback(msg, ecallback) - do an error callback safely
#
def tail_safe_error_callback(msg, ecallback):
	if not ecallback:
		return
	try:
		ecallback(msg)
	except:
		pass


#
#  tail_safe_callback(buf, cb, ecb) - do the main callback safely; try to report errors
#
def tail_safe_callback(buf, callback, ecallback):
	if not callback:
		return
	try:
		return callback(buf)
	except Exception as ex:
		tail_safe_error_callback("Callback raised unexpected exception: %s" % str(ex), ecallback)


#
#  tail_stat_file(fn) - run os.stat(fn) safely
#
#     On success, returns (size, inode)
#     On failure, returns (None, None)
#
def tail_stat_file(fn):
	try:
		ss = os.stat(fn)
		return ss.st_size, ss.st_ino
	except KeyboardInterrupt as ex:
		raise # pass this one through
	except:
		return None, None


#
#  tail_keep_going(cb) - call the exit callback, to determine
#                        if the loops should keep running
#
def tail_keep_going(xcallback, ecallback):
	# if there is no exit callback, keep going
	if not xcallback:
		return True
	try:
		result = xcallback()
		return False if result else True
	except Exception as ex:
		tail_safe_error_callback("Exit callback raised unexpected exception: %s" % str(ex), ecallback)
		return False


#
#  tailfile(fn, callback, ecallback, xcallback, spin)
#
#  Follow additions to a text file, and report them via callback, line-by-line
#
#     fn         = the name/path of the file to tail
#     callback   = callback to raise on each new line in the file
#     ecallback  = callback to raise if the 'callback' raises an exception
#     xcallback  = callback to raise to ask if the loop should exit;
#                  if xcallback returns True, the tailfile(...) will exit
#     tail_mode  = if/when to do seek(...) when opening a file; see the
#                  TAIL_MODE_* constants, above; default = TAIL_MODE_TAIL_FIRST
#     spin       = how long to wait, in seconds, when polling is
#                  needed (for EOF, I/O error, etc.; default = 0.1s)
#     cb_timeout = if no new lines appear after this many seconds, call
#                  the callback with None as the argument (default = 0)
#
#     The file path and main callback are required.  Others are optional.
#
def tailfile(fn, callback, ecallback=None, xcallback=None, tail_mode=TAIL_MODE_TAIL_FIRST, spin=0.10, cb_timeout=0):
	# sanity checks
	if tail_mode > TAIL_MODE_READ_ALL or tail_mode < 0:
		raise Exception("Invalid tail mode requested")
	if spin <= 0.0:
		raise Exception("Spin delay must be positive")

	# file tracking for size and inode number
	inode = None
	fsize = None

	# expand tilde
	if fn[0] == '~':
		fn = os.path.expanduser(fn)

	#
	#  outer loop - open file, optionally seek, then tail it
	#
	count = 0
	while tail_keep_going(xcallback, ecallback):
		try:
			# until file info available, try to get it
			while tail_keep_going(xcallback, ecallback) and not inode:
				fsize, inode = tail_stat_file(fn)
				if not inode:
					time.sleep(spin)

			# open the file, and do the main I/O loop
			with io.open(fn, 'rt') as f:
				do_seek = False
				if tail_mode == TAIL_MODE_TAIL_FIRST and count == 0:
					do_seek = True
				elif tail_mode == TAIL_MODE_TAIL_ALL:
					do_seek = True

				# if this is the first open attempt, seek to the end
				if do_seek:
					f.seek(0, 2) # don't catch exception here; allow failure
				count += 1

				# start the timeout timer
				last_line = time.time()

				#
				#  the I/O loop - this is the "tail" part
				#
				while tail_keep_going(xcallback, ecallback):
					# read one line into 'buf'
					buf = None
					try:
						buf = f.readline()
					except KeyboardInterrupt as ex:
						raise # pass this one through
					except:
						buf = None

					# read the clock
					now = time.time()

					# if there was data, do the callback safely
					if buf:
						last_line = now
						tail_safe_callback(buf, callback, ecallback)
						
					else: # if the call failed due to EOF...
						# handle idle timeout
						if (cb_timeout > 0) and ((now - last_line) >= cb_timeout):
							last_line = now
							tail_safe_callback(None, callback, ecallback)

						# measure the file to make sure it didn't shrink,
						#   and that the inode didn't change
						newsize, newnode = tail_stat_file(fn)

						# if it did, restart the tail
						if inode and (newsize < fsize or newnode != inode):
							break

						# if it didn't, just store the new size
						fsize = newsize

						# slow-spin when there is no data to read
						time.sleep(spin)

		# handle exceptions
		except KeyboardInterrupt as ex:
			raise # pass this one through
		except Exception as ex:
			tail_safe_error_callback("tailfile(%s) raised unexpected exception: %s" % (fn, str(ex)), ecallback)

		# clear the state tracking
		inode = None
		fsize = None

		# slow-spin when the file is temporarily unavailable
		time.sleep(spin)


#
#  entry point - unit tests
#
if __name__ == '__main__':
	import sys

	# counter for lines received, used to test exit callback
	counter = 0
	limit = 100

	# the exit callback
	def xcb():
		global counter, limit
		return counter >= limit

	# the error callback
	def ecb(data):
		sys.stderr.write("Error: %s\n" % data.strip())

	# the data callback
	def cb(data):
		global counter
		counter += 1

		# handle idle callback
		if not data:
			sys.stdout.write("<idle callback>\n")
			sys.stdout.flush()
			return

		# write the message to the console
		sys.stdout.write("%s\n" % data.strip())
		sys.stdout.flush()
	
	# go...
	try:
		tailfile(sys.argv[1], cb, ecb, xcb)
	except KeyboardInterrupt as ex:
		sys.stderr.write("KeyboardInterrupt... exiting.\n")

# EOF: tailfile.py
