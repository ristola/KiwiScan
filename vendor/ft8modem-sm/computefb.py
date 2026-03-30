#!/usr/bin/env -S python3 -B
#
#   computefb - split frequency math
#
#   This module is a single method, but it is placed in its
#   own module to allow easier unit testing.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

# for 'trunc' function
import math


#
#  compute_fb(fa, af_tx, af_user)
#
#  When in split mode, compute the VFO B frequency for transmission:
#     fa      - VFO-A frequency, in Hz
#     af_tx   - AF to modulate the transmitter
#     af_user - desired AF of carrier relative to 'fa'
#     res     - the resolution of the VFO-B, in Hz
#
#  Return (fb, af):
#     fb - the dial frequency for VFO-B
#     af - the AF to send to the transmitter (adjusted af_tx)
#
def compute_fb(fa, af_tx, af_user, res = 1):
	# compute the ideal target, assuming resolution = 1
	target = fa + af_user - af_tx  # dial target (ideal)
	af_eff = af_tx                 # modulator AF (ideal)

	# then offset 'target' as needed to correct for lack of VFO-B resolution
	if res > 1:
		# first, find the closest VFO-B step below the ideal RF target
		new_target = int(math.trunc(target / res)) * res

		# then correct the modulator AF for the difference between the two
		af_eff = (target - new_target) + af_tx
		target = new_target
	
	# return whatever we got
	return target, af_eff


#
#  entry point (unit tests)
#
if __name__ == '__main__':
	import sys

	# the test environment
	fa = 14074000 # Hz
	sf = 2000     # Hz

	# the test cases
	resz = [ 1, 10, 100, 500, 1000 ]

	# result counters
	success = failure = cases = 0

	# for each test case
	for res in resz:
		# describe the new test environment
		sys.stdout.write("FA: %d; Res: %d\n" % (fa, res))

		# run each test case at the new resolution
		for af in range(0, 3000, 17):
			# run the test
			fb, af_eff = compute_fb(fa, sf, af, res)

			# assess pass/fail
			inband = (af_eff >= sf) and (af_eff <= (sf + 1000))
			onfreq = (fb + af_eff) == (fa + af)
			result = inband and onfreq

			# accumulate statistics
			cases += 1
			if result:
				success += 1
			else:
				failure += 1

			# talk about each test
			sys.stdout.write("AF: %d -> FB: %d, AFeff: %d (Req: %d, Real: %d ==> %s)\n" % (
				af, fb, af_eff,
				fa + af,     # the requested carrier frequency
				fb + af_eff, # the computed carrier frequency
				"OK" if result else "FAIL"))

	# show the summary stats
	sys.stdout.write("Cases  : %d\n" % cases)
	sys.stdout.write("Success: %d\n" % success)
	sys.stdout.write("Failure: %d\n" % failure)

# EOF
