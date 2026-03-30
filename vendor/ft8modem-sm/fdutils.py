#!/usr/bin/env -S python3 -B
#
#   fdutils.py - helper methods for dealing with file descriptors
#
#   Copyright (C) 2023-2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#


from messages import *


#
#  socket_send_all(sock, s) - send bytes until done
#
def socket_send_all(sock, _bytez):
	total = 0
	while _bytez:
		n = sock.send(_bytez)
		total += n
		_bytez = _bytez[n:]
	return total


#
#  read_all_whatever(fd, size)
#
#  Read everything from the FD in a loop; if the FD runs
#  out of data, return the accumulated buffer.
#
#     fd      - the file descriptor
#     size    - max bytes to read per read() call
#     newdata - initial buffer, either '' or b'', depending
#               on the data type expected
#
#  This works on pipes and stdio file descriptors, but
#  not on sockets.
#
def read_all_whatever(fd, newdata, size):
	buf = fd.read(size)
	#loops = 0
	while buf:
		newdata += buf
		buf = fd.read(size)
		#loops += 1
	#sys.stderr.write("Trace: read_all_whatever(...) looped %d times\n" % loops)
	return newdata


#
#  read_all_bytes(fd, size) - read all bytes from a file descriptor
#
#     fd   - the file descriptor
#     size - max bytes to read per read() call
#
def read_all_bytes(fd, size):
	return read_all_whatever(fd, b'', size)


#
#  read_all_str(fd, size) - read all characters from a file descriptor
#
#     fd   - the file descriptor
#     size - max bytes to read per read() call
#
def read_all_str(fd, size):
	return read_all_whatever(fd, '', size)


#
#  write_pipe_safe(sink, data)
#
#  Write to pipe without throwing if it blocks.
#
def write_pipe_safe(sink, data):
	# encode strings to ASCII
	if type(data) == str:
		data = data.encode('ascii')

	# try to do the write, up to five failures
	ct = 0
	while ct >= 0 and ct < 5:
		try:
			sink.write(data)
			ct = -1
		except BlockingIOError as ex:
			time.sleep(0.1)
			ct += 1
	if ct >= 5:
		send_error("Could not write data; retries exceeded")

	# try to do the flush, up to five failures
	ct = 0
	while ct >= 0 and ct < 5:
		try:
			sink.flush()
			ct = -1
		except BlockingIOError as ex:
			time.sleep(0.1)
			ct += 1
	if ct >= 5:
		send_error("Could not flush data; retries exceeded")

# EOF
