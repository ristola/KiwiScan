#!/usr/bin/env -S python3 -B
#
#    callsigns.py
#
#    Call validator using simple patterns.
#
#    Copyright (C) 2024 by Matt Roberts.
#    License: GNU GPL3 (www.gnu.org)
#
#


# these are the patterns for 'valid' calls
patterns = [
	'ana',
	'nana',
	'anna',
	'nanaa',
	'annaa',
	'nanaaa',
	'annaaa',
	'nanaaaa',
	'annaaaa',
	'aana',
	'anaa',
	'anaaa',
	'aanaa',
	'aanaaa',
	'aanaaaa'
]

# cache test results here
testcache = { }


#
#  convert call string into matchable pattern
#
def build_sample(s):
	result = 1
	for ch in s:
		result <<= 1
		if ch.isalpha():
			result |= 1
		elif not ch.isdigit():
			return 0
	return result


#
#  convert template pattern into numeric pattern
#
def build_pattern(s):
	result = 1
	for ch in s:
		result <<= 1
		if ch == 'a':
			result |= 1
		elif ch != 'n':
			return 0
	return result


#
#  iscallcore(s) - return true if string matches one of the call patterns
#
def iscallcore(s):
	global patterns

	# convert string patterns to integers (only once)
	if type(patterns[0]) is str:
		for i in range(len(patterns)):
			patterns[i] = build_pattern(patterns[i])
	
	# build the sample
	n = build_sample(s)

	# test it against the patterns
	return n in patterns


#
#  iscall(s) - split string on '/' and return True iff one of the substrings
#              is a valid call matching one of the patterns
#
def iscall(s):
	global testcache

	if not s:
		return False
	
	result = testcache.get(s)
	if result is not None:
		return result

	if s[0] == '0' or s[0] == '1': # valid calls don't start with '0' or '1'
		return False
	parts = s.split('/')  # break the call into components...
	for part in parts:    # ...and test each one
		if iscallcore(part):
			testcache[s] = True
			return True
	testcache[s] = False
	return False


#
#  entry point - unit tests
#
if __name__ == '__main__':
	teststrings = [
		"KK5JY",
		"KK5JY/M",
		"KK5JY/W6",
		"KK5JY/6",
		"RR73",
		"EM16",
		"KK5JY/EM16",
		"K3F",
		"KX3F",
		"K3FX",
		"KR3FX",
		"KR3F",
		"KR3FXX",
		"K3FXT",
		"KN30",
		"VE3/K3A",
		"VE3/K3AF",
		"VE3/K3AF/P",
		"K3AF/VK4",
		"9A3F",
		"9A3FX",
		"9A3FFX",
		"9A3FXTR",
		"9A3F3R",
		"9A3FR3",
		"9A3R3",
		"9A33R",
		"S93F",
		"S93FX",
		"S93FFX",
		"S93FXTR",
		"S93F3R",
		"S93FR3",
		"S93R3",
		"S933R",
		"KK5JY",
		"9A3F3R",
	]

	for s in teststrings:
		print("iscall(%s) => %s" % (s, iscall(s)))

# EOF
