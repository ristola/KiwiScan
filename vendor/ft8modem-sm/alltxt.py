#!/usr/bin/env python3
#
#   alltxt - utilities for emitting log data in ALL.TXT format.
#
#   make_text(...) builds a log line as a string in ALL.TXT format
#   append_text(...) appends a log line to a text file in filesystem
#   socket_text(...) sends a log line string to a remote UDP listener
#
#   Copyright (C) 2023-2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

# system modules
import socket
import time
import io

# local modules
from messages import *
from parsing import *

# local socket cache
__cached_socket = None


#
#  make_text(...) - build line appropriate for append_text
#
#     fa   - the dial frequency of the receiver
#     line - the raw decode line from 'ft8modem', without the 'D:' prefix
#     tx   - True iff 'line' is a transmit spot
#     uts  - True to write date/time as Unix timestamp
#
#     The result does not have any line termination; this should be
#     added by a caller if the use of the result string reuqires it.
#
def make_text(fa, line, tx, uts):
	if not line:
		return None
	if not fa:
		fa = 0
	af = '0'
	snr = '0'
	df = '0.0'
	what = None
	mode = None
	tr = "Tx" if tx else "Rx"
	f = "%9.3f" % (fa / 1000000)
	parts = None
	try:
		parts = line.split(maxsplit=5)
		mode = code2mode(parts[4])
		af = parts[3]
		df = parts[2]
		snr = parts[1]
		what = parts[5]

		# format the new line for the log
		ts = None
		if uts:
			# unix timestamp (way easier to parse/compare)
			ts = "%-13s" % str(int(parts[0]))
		else:
			# YYMMDD_HHMMSS in GMT (WSJT-X style)
			tm = time.gmtime(int(parts[0]))
			ts = "%02d%02d%02d_%02d%02d%02d" % (tm[0] % 100, tm[1], tm[2], tm[3], tm[4], tm[5])

		# put it all gogether in a single line
		return "%s %s %s %s %6s %4s %4s %s" % (ts, f, tr, mode, snr, df, af, what)

	# if anything failed, complain
	except Exception as ex:
		send_warning("make_text failed: %s" % str(ex))
		return None


#
#  append_text(...) - append log line to ALL.TXT text file in filesystem.
#
#     fn   - the path to ALL.TXT
#     fa   - the dial frequency of the receiver
#     line - the raw decode line from 'ft8modem', without the 'D:' prefix
#     tx   - True iff 'line' is a transmit spot
#     uts  - True to write date/time as Unix timestamp
#
#     The function does not hold the log file open; it opens the file
#     temporarily to append the line, then flushes the data and closes
#     the file handle.
#
#     Returns the number of bytes appended to 'fn'.
#
def append_text(fn, fa, line, tx = False, uts = False):
	try:
		if not fn:
			return None

		# convert to a single ALL.TXT line
		line = make_text(fa, line, tx, uts)
		if not line:
			return None

		# append the line to the file and flush it,
		#   then close the file handle
		with io.open(fn, 'a') as f:
			result = f.write(line + '\n')
			f.flush()
			return result
	except Exception as ex:
		send_warning("append_text failed: %s" % str(ex))
		return None


#
#  socket_text(...) - send ALL.TXT line to UDP socket
#
#     ip   - the target host
#     port - the target port
#     fa   - the dial frequency of the receiver
#     line - the raw decode line from 'ft8modem', without the 'D:' prefix
#     tx   - True iff 'line' is a transmit spot
#     uts  - True to write date/time as Unix timestamp
#
#     The function uses a locally cached socket to send its UDP frames.
#     If the socket becomes unusable for some reason, it is recreated
#     automatically upon the next call.
#
#     Returns the number of bytes sent.
#
def socket_text(ip, port, fa, line, tx = False, uts = False):
	global __cached_socket

	try:
		if not port:
			return None

		# allocate the socket if needed
		if not __cached_socket:
			__cached_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

		# convert to a single ALL.TXT line
		line = make_text(fa, line, tx, uts)
		if not line:
			return None

		# convert the string to bytes
		data = line.encode('ascii')

		# send to the socket
		cleanup = False
		result = None
		try:
			# UDP: send the frame
			result = __cached_socket.sendto(data, (ip, port))

			# if the frame was truncated, reinitialize the socket
			if result != len(data):
				cleanup = True
		except Exception as ex:
			cleanup = True

		# if there was a problem, reinitialize the socket on next call
		if cleanup:
			__cached_socket.close()
			__cached_socket = None

		# return the number of bytes sent, or None
		return result
	except Exception as ex:
		send_warning("append_text failed: %s" % str(ex))

# EOF
