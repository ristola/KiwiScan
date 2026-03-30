#!/usr/bin/env -S python3 -B
#
#   udpaf.py
#
#   UDP sender for audio data.  This is the optional protocol
#   used between ft8sdr and ft8modem for SDR receivers.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

# system modules
import socket
import sys

#
#  class AudioUDP - a UDP sender for audio data; buffers data to
#                   ensure fixed-length frames, also generates
#                   sequence numbers to be checked by the receiver.
#
class AudioUDP:
	#
	#  ctor - port = UDP port; win = audio window in BYTES
	#
	def __init__(self, port, win=256):
		if win % 4:
			raise Exception("Audio window must be divisible by four (4)")

		# set up object state
		self.win = win
		self.buf = b''
		self.addr = ('127.0.0.1', port)
		self.seq = 0

		# bind a socket
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	

	#
	#  send(buffer) - send data towards the receiver
	#
	def send(self, buffer):
		self.buf += buffer

		while len(self.buf) >= self.win:
			# dequeue 'win' bytes of audio
			tosend = self.buf[0:self.win]
			self.buf = self.buf[self.win:]

			# append sequence number, LSB first
			tosend += bytes([ self.seq & 0xFF, (self.seq >> 8) & 0xFF ])
			self.seq += 1 # then increment it

			# send the data
			try:
				count = len(tosend)
				sent = self.sock.sendto(tosend, self.addr)
				if sent != count:
					sys.stderr.write('Warning: sendto(...) result was %d, expected %d\n' % (sent, count))
			except Exception as ex:
				sys.stderr.write('Error: sendto(...) failed: %s\n' % str(ex))


#
#  entry point - unit tests
#
if __name__ == '__main__':
	import time

	if not sys.argv[1:]:
		sys.stderr.write("Please supply UDP port number.\n")
		sys.exit(1)

	port = int(sys.argv[1])
	win = 256

	# build the sender
	udp = AudioUDP(port, win)

	# start sending
	try:
		while True:
			wave = 8 * ((b'\xFF\x00' * 8) + (b'\xFF\xFE' * 8)) # should generate ~ -42dB at receiver
			udp.send(wave)
			time.sleep(0.005) # roughly 200 frames/sec
	except KeyboardInterrupt as ex:
		sys.exit(0)

# EOF
