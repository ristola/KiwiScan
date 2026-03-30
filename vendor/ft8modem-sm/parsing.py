#!/usr/bin/env python3
#
#   parsing - Parsing extensions
#
#   Copyright (C) 2023,2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

# system modules
import time
import sys
import os

# local modules
import callsigns


#
#  mode2code(m)
#
def mode2code(mode):
	mode = mode.upper()
	if mode == 'FT8': return '~'
	if mode == 'FT4': return '+'
	if mode == 'JT9': return '@'
	if mode == 'JT4': return '$'
	if mode == 'JT65': return '#'
	if mode == 'WSPR': return '0'
	return '?'


#
#  code2mode(c)
#
def code2mode(code):
	if code == '~': return 'FT8'
	if code == '+': return 'FT4'
	if code == '@': return 'JT9'
	if code == '$': return 'JT4'
	if code == '#': return 'JT65'
	if code == '0': return 'WSPR'
	return '?'


#
#  isroger(s)
#
def isroger(s):
	if not s:
		return False
	s = s.upper()
	return s == 'RRR' or s == 'RR73' or (s[0] == 'R' and isreport(s))


#
#  isreport(s)
#
def isreport(s):
	if not s:
		return False
	s = s.upper()
	if len(s) == 3:
		return s[0] in '+-' and s[1:].isdigit()
	elif len(s) == 4:
		return s[0] == 'R' and s[1] in '+-' and s[2:].isdigit()
	return False


#
#  is73(s)
#
def is73(s):
	if not s:
		return False
	s = s.upper()
	return s == 'RR73' or s == '73' or s == 'TU73'


# lookup cache for grid squares
gridcache = { }

# non-grids that look like grids
non_grids = [ 'RR73', 'rr73', 'TU73', 'tu73' ]


#
#  isgrid(s)
#
def isgrid(s):
	if not s:
		return False
	result = gridcache.get(s)
	if result is not None:
		return result
	if s in non_grids:
		gridcache[s] = False
		return False
	result = \
		(len(s) == 4) and \
		(ord('A') <= ord(s[0].upper()) <= ord('R')) and \
		(ord('A') <= ord(s[1].upper()) <= ord('R')) and \
		s[2].isdigit() and s[3].isdigit()
	gridcache[s] = result
	return result


#
#  iscall(s)
#
def iscall(s):
	if not s: # empty -> False
		return False
	if '.' in s: # '...' -> False
		return False
	if s[0] == '<' and s[-1] == '>': # unwrap hashed calls
		s = s[1:-1]
	return callsigns.iscall(s) # chain to the real call detector


#
#  basecall(s) - return call with prefixes and suffixes removed
#
def basecall(s):
	if not iscall(s):
		return None
	s = s.upper()
	if s.startswith('<') and s.endswith('>'):
		s = s[1:-1]
	parts = s.split('/')
	if len(parts) == 1:
		return s
	calls = [ ]
	for part in parts:
		if iscall(part):
			calls.append(part)
	if len(calls) == 0:
		return None
	if len(calls) == 1:
		return calls[0]
	result = ""
	for call in calls:
		if len(call) >= len(result):
			result = call
	if not result:
		return None
	return result


#
#  entry point - just unit tests
#
if __name__ == '__main__':
	for i in [ 'kk5jy', 'n5osl', 'n7ul', 'rr73', 'rr7a', 'em16', 'FO33', 'ab301', 'f301f', 'rr73', 'rr7a', 'em16', 'qr77' ]:
		print("isgrid(%s) = %s" % (i, isgrid(i)))
		print("iscall(%s) = %s" % (i, iscall(i)))
	print("---------------------")
	for i in [ '+13', '-05', 'R+03', 'r-3', 'RRR', 'RR73', 'RR72', '73' ]:
		print("isroger(%s)  = %s" % (i, isroger(i)))
		print("isreport(%s) = %s" % (i, isreport(i)))
		print("is73(%s)     = %s" % (i, is73(i)))
	print("---------------------")
	for i in [ 'kk5jy', 'n5osl', 'n7ul', 'kk5jy/r', 'n5osl/3', 'n7ul/qrp', 've3/ab0cd', '<ab0cd>', '<ab0cd/33>', "<...>" ]:
		print("basecall(%s) = %s" % (i, basecall(i)))

# EOF
