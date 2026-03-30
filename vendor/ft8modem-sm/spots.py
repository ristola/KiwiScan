#!/usr/bin/env -S python3 -B
#
#    spots.py
#
#    Parser for spot log lines, e.g., from ALL.TXT.
#
#    Copyright (C) 2024 by Matt Roberts.
#    License: GNU GPL3 (www.gnu.org)
#
#


# system modules
import time
import gzip
import lzma
import bz2
import io

# local modules
import callsigns
import parsing


#
#  parse_spot_core(line) - process a spot line
#  parse_spot(line) - safety wrapper around parse_spot(...)
#
#  Returns a dictionary or None
#
def parse_spot_core(line):
	# if no data, just give up
	if not line:
		return None

	# break up the line, make sure there is enough to parse
	parts = line.split()
	if len(parts) < 8:
		return None

	# start extracting
	result = { }
	rx = (parts[2] == 'Rx')
	tx = (parts[2] == 'Tx')
	if not rx and not tx:
		return None

	# parse the time/date
	when = parts[0]
	if '_' in when:
		# parse out the date and time substrings
		result['date'], result['time'] = when.split('_')

		# now convert to Unix time
		s = result['time'][4:6].strip()
		if not s:
			s = '00'
		t = (
			2000 + int(result['date'][0:2]), # Y
			int(result['date'][2:4]),        # M
			int(result['date'][4:6]),        # D
			int(result['time'][0:2]),        # h
			int(result['time'][2:4]),        # m
			int(s),                          # s
			-1, -1, 0)
		result['when'] = int(time.mktime(t)) - time.timezone
	else:
		# parse out the Unix time
		result['when'] = int(when)

		# now convert to date and time strings
		t = time.gmtime(result['when'])
		result['date'] = "%02d%02d%02d" % (t.tm_year % 100, t.tm_mon, t.tm_mday)
		result['time'] = "%02d%02d%02d" % (t.tm_hour, t.tm_min, t.tm_sec)

	# extract basic data
	result['freq'] = parts[1]
	result['tx'] = tx
	result['rx'] = rx
	result['mode'] = parts[3]
	result['snr'] = parts[4]
	result['dt'] = parts[5]
	result['audio'] = parts[6]
	result['what'] = [ ]
	result['ts'] = time.time()

	# parse the user message
	rawwhat = parts[7:]
	for i in rawwhat:
		if i[0] == '<' and i[-1] == '>':
			i = i[1:-1] # trim brackets from hashed calls
		result['what'].append(i)

	# try to parse out calls
	calls = [ ]
	result['to'] = None
	result['from'] = None
	result['cq'] = False
	result['grid'] = None
	is_cq = False
	if len(result['what']) > 0:
		is_cq = result['what'][0].startswith('CQ')
		for i in result['what']:
			if callsigns.iscall(i):
				calls.append(i)
			elif parsing.isgrid(i):
				result['grid'] = i
		if len(calls) == 2:
			result['to'] = calls[0]
			result['from'] = calls[1]
		elif len(calls) == 1 and is_cq:
			result['from'] = calls[0]
			result['cq'] = True

	# return whatever was read
	return result


#
#  safety wrapper
#
def parse_spot(line, safe=True):
	try:
		return parse_spot_core(line)
	except KeyboardInterrupt as ex:
		raise ex
	except Exception as ex:
		if safe:
			return None
		raise ex


#
#  readall_open(path) - helper for readall(pathy, cb)
#
#  This function opens 'path' with a text reader that is appropriate
#  for the file type, including 'xz', 'gz', or normal text readers.
#
def readall_open(path):
	if not path:
		return None
	if path.endswith('.gz'):
		return gzip.open(path, 'rt')
	if path.endswith('.bz2'):
		return bz2.open(path, 'rt')
	if path.endswith('.xz'):
		return lzma.open(path, 'rt')
	return io.open(path, 'r')


#
#  readall(path, callback) - read the ALL.TXT file and pass each decode
#                            to the provided callback; if callback is
#                            None, return the decodes as an array.
#
def readall(path, callback=None, ecallback=None, skip=0):
	lines, consumed = 0, 0
	with readall_open(path) as f:
		for line in f:
			# total counter
			lines += 1

			# skip support
			if skip and lines <= skip:
				continue

			# consumed counter
			consumed += 1

			# parse the spot
			d = parse_spot(line)
			if not d:
				continue

			# handle the spot
			if callback:
				try:
					callback(d)
				except Exception as ex:
					if ecallback:
						try:
							ecallback("readall(%s) received %s from callback" % (path, str(ex)))
						except:
							pass

	# return number of lines read, consumed
	return lines, consumed

# EOF: spots.py
