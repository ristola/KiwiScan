#!/usr/bin/env -S python3 -B
#
#   rigctld.py - emulate rigctld(1) interface for simple clients
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#


# system modules
import socketserver
import threading
import sys


#
#  globals - these are the "rig" state
#
#  TODO: move these into the RigControlServer class, to
#        make the module interface cleaner
#
FA = 52000000
FB = 52000000
Mode = 'USB'
RealMode = 'USB'
Split = 0
Ptt = 0


# limits / constants
MinFreq = 500000
MaxFreq = 1700000000

# these are the rigctld mode strings
ValidModes  = [ 'USB', 'LSB', 'CW',  'CWR', 'RTTY', 'RTTYR', 'PKTLSB', 'PKTUSB' ]

# these are the corresponding rtl_fm mode strings; NOTE: these MUST be in lowercase
MappedModes = [ 'usb', 'lsb', 'usb', 'lsb', 'usb',  'lsb',   'lsb',    'usb'    ]


#
#  get_real_mode(s) - take a user mode string from the 'M' command,
#                     and return the actual rtl_fm mode string that
#                     is closest to it.
#
def get_real_mode(s):
	global ValidModes, MappedModes 
	for k, v in zip(ValidModes, MappedModes):
		if s == k:
			return v
	return None


#
#  RigControlHandler - a custom handler for emulating rigctld(1) to a single client.
#
class RigControlHandler(socketserver.StreamRequestHandler):
	def handle(self):
		global FA, FB, Mode, Split, Ptt
		global MinFreq, MaxFreq, ValidModes

		while True:
			try:
				# read one line from the client
				cmd = self.rfile.readline().strip()

				# quit?
				if cmd == b'Q' or cmd == b'q':
					return

				# getters
				if cmd == b'f':
					self.wfile.write(b'%s\n' % str(FA).encode('ascii'))
				elif cmd == b'i':
					self.wfile.write(b'%s\n' % str(FB).encode('ascii'))
				elif cmd == b's':
					self.wfile.write(b'%d\n' % Split)
				elif cmd == b't':
					self.wfile.write(b'%d\n' % Ptt)
				elif cmd == b'm':
					self.wfile.write(b'%s\n' % Mode.encode('ascii'))
				elif cmd == b'v':
					self.wfile.write(b'VFOA\n')

				# setters
				elif cmd.startswith(b'F '):   # VFO-A
					fa = int(cmd[2:])
					if fa >= MinFreq and fa <= MaxFreq:
						FA = fa
						self.wfile.write(b'RPRT 0\n')
					else:
						self.wfile.write(b'RPRT -1\n')
				elif cmd.startswith(b'I '):   # VFO_B
					fb = int(cmd[2:])
					if fb >= MinFreq and fb <= MaxFreq:
						FB = fb
						self.wfile.write(b'RPRT 0\n')
					else:
						self.wfile.write(b'RPRT -1\n')
				elif cmd.startswith(b'S '):   # Split
					newval = int(cmd[2:])
					if newval in [ 0, 1 ]:
						Split = newval
						self.wfile.write(b'RPRT 0\n')
					else:
						self.wfile.write(b'RPRT -1\n')
				elif cmd.startswith(b'T '):   # PTT
					newval = int(cmd[2:])
					if newval in [ 0, 1 ]:
						Ptt = newval
						self.wfile.write(b'RPRT 0\n')
					else:
						self.wfile.write(b'RPRT -1\n')
				elif cmd.startswith(b'M '):   # Mode
					newval = cmd[2:].decode('ascii').strip().upper()
					newMode = None
					for k, v in zip(ValidModes, MappedModes):
						if k == newval:
							newMode = k, v
							break
					if newMode:
						Mode = newMode[0]
						RealMode = newMode[1]
						self.wfile.write(b'RPRT 0\n')
					else:
						self.wfile.write(b'RPRT -1\n')
				elif cmd.startswith(b'V '):   # set VFO
					newval = cmd[2:].strip().upper()
					if newval == b'VFOA': # only support VFOA
						self.wfile.write(b'RPRT 0\n')
					else:
						self.wfile.write(b'RPRT -1\n')

				# everything else
				else:
					self.wfile.write(b'RPRT -1\n')
			except Exception as ex:
				self.wfile.write(b'RPRT -1 (%s)\n' % str(ex).encode('ascii'))


#
#  customized TCP server class
#
#  mostly doesnt spew large stack traces when a client disconnects unexpectedly
#
class RigControlTCPServer(socketserver.ThreadingTCPServer):
	# the constructor chain
	def __init__(self, port):
		socketserver.ThreadingTCPServer.__init__(self, ('localhost', port), RigControlHandler)
		self.daemon_threads = True
		self.allow_reuse_address = True
	
	# override the error handler to be less chatty
	def handle_error(self, request, client_address):
		sys.stderr.write("Warning: client %s raised an unexpected error and has disconnected.\n" % str(client_address))
		sys.stderr.flush()


#
#  self-contained server class to emulate rigctld(1)
#
class RigControlServer:
	def __init__(self, port):
		# allocate the socketserver itself
		self.server = RigControlTCPServer(port)
	
	def start(self):
		# start the server running in a background thread
		self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
		self.thread.start()

	def stop(self):
		# shut down and clean up
		self.server.shutdown()
		self.server.server_close()
		self.thread.join()
	
	def set_vfo_a(self, fa):
		global FA
		FA = int(fa)
	
	def set_vfo_b(self, fb):
		global FB
		FB = int(fb)
	
	def get_vfo_a(self):
		global FA
		return FA

	def get_vfo_b(self):
		global FB
		return FB

	# set the user mode string, as seen by rigctld interface
	def set_mode(self, m):
		global Mode
		Mode = m.strip().upper()
	
	# return the user mode string, as seen by rigctld interface
	def get_mode(self):
		global Mode
		return Mode

	# return the PTT state
	def get_ptt(self):
		global Ptt
		return Ptt

	# return the mode string to use for rtl_fm
	def get_real_mode(self):
		global Mode, ValidModes, MappedModes
		newMode = None
		for k, v in zip(ValidModes, MappedModes):
			if k == Mode:
				newMode = k, v
				break
		if newMode:
			return newMode[1]
		return None


#
#  entry point (unit tests)
#
if __name__ == '__main__':
	import sys
	import time

	# make the server
	server = RigControlServer(4532)

	# start the server
	server.set_vfo_a(28074000)
	server.set_vfo_b(28075500)
	server.set_mode('USB')
	server.start()

	# loop and show changes
	try:
		last_fa = 0
		last_fb = 0
		last_mode = ''
		last_ptt = None
		while True:
			# FA changed
			fa = server.get_vfo_a()
			if fa != last_fa:
				last_fa = fa
				sys.stderr.write("VFOA is now %d\n" % fa)

			# FB changed
			fb = server.get_vfo_b()
			if fb != last_fb:
				last_fb = fb
				sys.stderr.write("VFOB is now %d\n" % fb)
				if fb == 14070000:
					break

			# Mode changed
			mode = server.get_mode()
			if mode != last_mode:
				last_mode = mode
				real_mode = server.get_real_mode()
				sys.stderr.write("Mode is now %s (%s)\n" % (mode, real_mode))

			# PTT changed
			ptt = server.get_ptt()
			if ptt != last_ptt:
				last_ptt = ptt
				sys.stderr.write("PTT is now %s\n" % ('TX' if ptt else 'RX'))

			# be nice
			time.sleep(0.1)
	except KeyboardInterrupt as ex:
		pass

	# stop the server
	server.stop()

	# clean up
	del(server)

# EOF
