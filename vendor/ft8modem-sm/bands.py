#!/usr/bin/env -S python3 -B
#
#   bands.py - frequency and mode database
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#


#
#  frequency table: mode   -> band -> frequency (Hz)
#                   string -> int  -> int
#
freqs = {
	'ft8' : {
		160: 1840000,
		80 : 3573000,
		60 : 5357000,
		40 : 7074000,
		30 : 10136000,
		20 : 14074000,
		17 : 18100000,
		15 : 21074000,
		12 : 24915000,
		10 : 28074000,
		6  : 50313000,
		2  : 144174000,
	},
	'ft4' : {
		160: 1840000,
		80 : 3575000,
		60 : 5357000,
		40 : 7047500,
		30 : 10140000,
		20 : 14080000,
		17 : 18104000,
		15 : 21140000,
		12 : 24919000,
		10 : 28180000,
		6  : 50318000,
		2  : 144170000,
	},
	'jt9' : {
		160: 1839000,
		80 : 3572000,
		60 : 5357000,
		40 : 7078000,
		30 : 10140000,
		20 : 14078000,
		17 : 18104000,
		15 : 21078000,
		12 : 24919000,
		10 : 28078000,
		6  : 50312000,
		2  : 144120000,
	},
	'jt65' : {
		160: 1838000,
		80 : 3576000,
		60 : 5357000,
		40 : 7076000,
		30 : 10139000,
		20 : 14076000,
		17 : 18102000,
		15 : 21078000,
		12 : 24917000,
		10 : 28078000,
		6  : 50276000,
		2  : 144430000,
	},
	'wspr' : {
		160: 1836600,
		80 : 3568600,
		60 : 5357000,
		40 : 7038600,
		30 : 10138700,
		20 : 14095600,
		17 : 18104600,
		15 : 21094600,
		12 : 24924600,
		10 : 28124600,
		6  : 50293000,
		2  : 14448900,
	},
} # end freqs

# build this from the above
rfreqs = { }


#
#  bands() - return list of bands in the freqs table
#
def bands():
	global freqs
	return list(freqs['ft8'].keys()) # the MHz are all the same, so use the FT8 ones


#
#  modes() - return list of modes in the freqs table
#
def modes():
	global freqs
	return list(freqs.keys())


#
#  get_freq(mode, band) - return calling frequency in Hz
#
def get_freq(mode, band):
	global freqs
	try:
		return freqs[mode.lower()][band]
	except:
		return None


#
#  get_band(f) - convert frequency to wavelength band
#
def get_band(f):
	global freqs, rfreqs

	# build the band reverse-lookup table on the first try
	if not rfreqs:
		for mode in freqs.keys():
			rfreqs[mode] = { }
			for band in freqs[mode].keys():
				rfreqs[mode][int(freqs[mode][band] / 1000000)] = band

		# DEBUG:
		#print(rfreqs)

	# parameter checks
	if not f:
		return None
	if f <= 0:
		return None

	# convert the frequency to MHz
	mhz = int(f)
	if mhz > 100000: # if MHz was actually Hz...
		mhz = int(mhz / 1000000) # ... convert

	# collapse large bands to a single number
	if mhz in [ 144, 145, 146, 147 ]:
		mhz = 144
	if mhz in [ 50, 51, 52, 53 ]:
		mhz = 50
	if mhz in [ 28, 29 ]:
		mhz = 28

	# then search for a band matching MHz
	try:
		return rfreqs['ft8'][mhz]
	except:
		return None

	# TODO: remove this; old code
	for b in bands():
		if int(get_freq('ft8', b) / 1000000) == mhz:
			return b
	return None
		

#
#  band_above() - return the band above (in frequency) this one
#
def band_above(m):
	global freqs

	if not m:
		return None
	for b in bands():
		if b < m:
			return b
	return None


#
#  band_below() - return the band above (in frequency) this one
#
def band_below(m):
	global freqs

	if not m:
		return None
	result = None
	for b in bands():
		if b > m:
			result = b
	return result


#
#  slot_time(mode) - return complete slot time for given mode
#
def slot_time(mode):
	if not mode:
		return None
	key = mode.lower()

	# available modes and slot-times
	times = {
		'ft4' : 7.5,
		'ft8' : 15.0,
		'jt9' : 60.0,
		'jt65' : 60.0,
		'wspr' : 120.0
	}

	# find the mode and return the time
	if key not in times.keys():
		return None
	return times[key]


#
#  tx_time(mode) - return the number of seconds of a transmission in given mode
#
def tx_time(mode):
	if not mode:
		return None
	key = mode.lower()

	# available modes and slot-times
	times = {
		'ft4' : 4.48,
		'ft8' : 12.64,
		'jt9' : 49.0,
		'jt65' : 46.8,
		'wspr' : 110.6
	}

	# find the mode and return the time
	if key not in times.keys():
		return None
	return times[key]


#
#  entry point - unit tests
#
if __name__ == '__main__':
	import sys
	sys.stdout.write("bands() = %s\n" % bands())
	sys.stdout.write("modes() = %s\n" % modes())
	for i in [ 145000000, 50313000, 28180000, 24911000, 21099000, 18123000, 14071000, 10139000, 7044000, 3580000, 1844000 ]:
		sys.stdout.write('-----------------------\n')
		b = get_band(i)
		sys.stdout.write("get_band(%d) = %s\n" % (i, b))
		sys.stdout.write("band_above(%d) = %s\n" % (b, band_above(b)))
		sys.stdout.write("band_below(%d) = %s\n" % (b, band_below(b)))

# EOF
