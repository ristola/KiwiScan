#!/usr/bin/env -S python3 -B
#
#   pretty.py
#
#   Custom formatter to turn Hz into dotted dial-like display frequency.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

#
#  ff - print a frequency with dots, like a rig display
#
def ff(f):
	s = str(f)
	neg = False
	if s[0] == '-':
		s = s[1:]
		neg = True
	result = ''
	while len(s) > 3:
		result = s[-3:] + result
		result = '.' + result
		s = s[:-3]
	if s:
		result = s + result
	if neg:
		result = '-' + result
	return result


# entry point - unit tests
if __name__ == '__main__':
	values = [ 0, 1, 12, 123, 1234, 12345, 123456, 1234567, 12345678,
	123456789, 77123456789 ]
	for i in values:
		print(ff(i))
	for i in values:
		print(ff(-i))

# EOF
