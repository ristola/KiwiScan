#!/usr/bin/env python3
#
#   messages.py - message sending support
#
#   Copyright (C) 2023 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#

# system modules
import sys

# enable flags
TraceEnabled = True
DebugEnabled = True

# the target
__message_target = None


#
#  send_target(t)
#
def send_target(t):
	global __message_target

	if t in [ 'syslog' ]:
		__message_target = t
	else:
		raise Exception("Invalid message target: %s" % str(t))


#
#  send_message(s, prefix)
#
def send_message(s, prefix = ""):
	global __message_target

	if not s:
		return
	s = s.strip()
	if not s:
		return
	if prefix:
		prefix = prefix + ": "

	if __message_target == 'syslog':
		syslog.syslog("%s%s" % (prefix, s))
	else:
		sys.stdout.write("%s%s\n" % (prefix, s))
		sys.stdout.flush()


#
#  send_error(s)
#
def send_error(s):
	send_message(s, "ERR")
	
#
#  send_warning(s)
#
def send_warning(s):
	send_message(s, "WARN")
	
#
#  send_info(s)
#
def send_info(s):
	send_message(s, "INFO")
	
#
#  send_trace(s)
#
def send_trace(s):
	global TraceEnabled
	if not TraceEnabled:
		return
	send_message(s, "TRACE")
	
#
#  send_debug(s)
#
def send_debug(s):
	global DebugEnabled
	if not DebugEnabled:
		return
	send_message(s, "DEBUG")
	
# EOF
