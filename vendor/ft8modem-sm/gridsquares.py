#!/usr/bin/env -S python3 -B
#
#   gridsquares.py
#
#   Basic geodetic calculations:
#     + maidenhead grid square <-> latitude, longitude
#     + distance and bearing between coordinates
#
#   Adapted from BD_2004.pas, originally by:
#      + Michael R. Owen, W9IP,
#      + Paul Wade, N1BWT, W1GHZ
#   Original references captured inline.
#
#   C# translation and adaptation by Matt Roberts, KK5JY.
#      Updated 2023-02-20: Fix coord-to-grid conversion.
#   Python translation and adaptation by Matt Roberts, KK5JY.
#      Updated 2025-07-03: Updates to grid checking.
#
#

# for trig, trunc, fabs, etc.
import math


#
#  check_grid(grid) - validate grid square string
#
#  grid - The grid square string
#
#  returns bool
#
def check_grid(grid):
	if not grid:
		return False

	# must be even length
	if len(grid) % 2 != 0:
		return False
	
	counter = 0
	alpha = True
	for i in range(len(grid)):
		if alpha:
			ch = grid[i].upper()
			if not ch.isalpha():
				return False
			och = ord(ch)
			if och < ord('A') or och > ord('R'):
				return False
		else:
			if not grid[i].isdigit():
				return False
		counter += 1
		if counter == 2:
			alpha = not alpha
			counter = 0
	return True


#
#  center(grid) - finds the lat/lon of center of the sub-square
#
#  grid - Grid-squre string
#
#  returns ( lat, lon )
#
def center(grid):
	if not grid:
		raise Exception("Grid argument was empty")
	if len(grid) == 4:
		grid = grid + "LL"; # {choose middle if only 4-character}
	if len(grid) != 6:
		raise Exception ("Invalid grid square length")
	if not check_grid(grid):
		raise Exception("Grid argument was invalid")

	grid = grid.upper()

	# center
	lonmin = (5.0 * (ord(grid[4]) - ord('A'))) + 2.5;

	londeg = 180.0 - (20.0 * (ord(grid[0]) - ord('A'))) - (2.0 * (ord(grid[2]) - ord('0')));
	#        ^^^^^^^^^^^^^^^^^ tens of deg ^^^^^^^^^^^^    ^^^^^^^^^^^^ two deg ^^^^^^^^^

	lon = math.fabs(londeg - (lonmin / 60.0))
	if grid[0] <= 'I':
		lon = -lon

	latdeg = -90.0 + (10.0 * (ord(grid[1]) - ord('A'))) + (ord(grid[3]) - ord('0'));
	#        ^^^^^^^^^^^^ tens of degrees ^^^^^^^^^^^      ^^^^^^  degrees  ^^^^^^

	latmin = 2.5 * (ord(grid[5]) - ord('A')) + 1.25;
	#        ^^^^^^^^^^^^ minutes ^^^^^^^^     ^^^^ for center

	lat = math.fabs(latdeg + (latmin / 60.0))
	if grid[1] <= 'I':
		lat = -lat
	return (lat, lon)


#
#  coord_to_grid(lat, lon) - convert latitude and longitude to grid square.
#
#  lat - Latitude
#  lon - Longitude
#
#  returns string, six-digit grid square
#
def coord_to_grid(lat, lon):
	G4 = lon + 180
	C = math.trunc(G4 / 20)
	M1 = chr(C + 65)

	R = math.fabs(lon / 20)
	R = math.trunc((R - math.trunc(R)) * 20)
	C = math.trunc(R / 2)
	if lon < 0:
		C = math.fabs(C - 9)
	M3 = chr(int(C + 48))

	M = math.fabs(lon * 60)
	M = ((M / 120) - math.trunc(M / 120)) * 120
	M = math.trunc(M + 0.001)
	C = math.trunc(M / 5)
	if  lon < 0:
		C = math.fabs(C - 23)
	M5 = chr(int(C + 97))

	L4 = lat + 90
	C = math.trunc(L4 / 10)
	M2 = chr(int(C + 65))

	R = math.fabs(lat / 10)
	R = math.trunc(((R - math.trunc(R)) * 10))
	C = math.trunc(R)
	if lat < 0:
		C = math.fabs(C - 9)
	M4 = chr(int(C + 48))

	M = math.fabs(lat * 60)
	M = ((M / 60) - math.trunc(M / 60)) * 60
	C = math.trunc(M / 2.5)
	if lat < 0:
		C = math.fabs(C - 23)
	M6 = chr(int(C + 97))

	# put it all together
	return ''.join([ M1, M2, M3, M4, M5, M6 ])


#
#  grids_to_distance_and_bearing(grid_fr, grid_to)
#
#  Calculate distance and bearing between two grid squares on the globe.
#
#  grid_fr - Starting latitude
#  grid_to - Ending latitude
#
#  returns ( az_from, az_to, dist )
#
def grids_to_distance_and_bearing(grid_fr, grid_to):
	# validate the inputs
	if not check_grid(grid_fr):
		raise Exception("grid_fr is invalid")
	if not check_grid(grid_to):
		raise Exception("grid_to is invalid")

	# handle zero-length calculation, which the math doesn't like
	if grid_fr.strip().upper() == grid_to.strip().upper():
		return (0.0, 0.0, 0.0)

	# calculate the grid square center coordinates
	fr = center(grid_fr)
	to = center(grid_to)

	# calculate the azimuth and distance
	return coords_to_distance_and_bearing(fr, to) # -> (az_fr, az_to, dist)


#
#  coords_to_distance_and_bearing(fr, to)
#
#  Calculate distance and bearing between two points on the globe.
#
#  fr - Starting coordinate (lat, lon)
#  to - Ending coordinate (lat, lon)
#
#  returns ( AzimuthFrom, AzimuthTo, Distance )
#
def coords_to_distance_and_bearing(fr, to):
	# extract the floating values
	lat_fr, lon_fr = fr
	lat_to, lon_to = to

	# handle zero-length calculation, which the math doesn't like
	if lat_fr == lat_to and lon_fr == lon_to:
		return ( 0.0, 0.0, 0.0 )

	#
	#  [Adapted from code] Taken directly from:
	#  Thomas, P.D., 1970, Spheroidal Geodesics, reference systems,
	#      & local geometry, U.S. Naval Oceanographic Office SP-138, 165 pp.
	#  assumes North Latitude and East Longitude are positive
	#  EpLat, EpLon = MyLat, MyLon
	#  Stlat, Stlon = HisLat, HisLon
	#  AzimuthTo, AzimuthFrom = direct & reverse azimuith (degrees)
	#  Distance = distance (km)
	#

	# earth ellipsoid {Clarke, 1866 ellipsoid}
	#   ref: https://en.wikipedia.org/wiki/Earth_ellipsoid
	AL = 6378206.4 # meters
	BL = 6356583.8 # meters

	D2R = math.pi / 180.0; #  {degrees to radians conversion factor}
	Pi2 = 2.0 * math.pi

	AzimuthFrom = AzimuthTo = Distance = 0.0
	BOA = BL / AL
	F = 1.0 - BOA
	P1R = lat_fr * D2R
	P2R = lat_to * D2R
	L1R = lon_fr * D2R
	L2R = lon_to * D2R
	DLR = L2R - L1R
	T1R = math.atan(BOA * math.tan(P1R))
	T2R = math.atan(BOA * math.tan(P2R))
	TM = (T1R + T2R) / 2.0
	DTM = (T2R - T1R) / 2.0
	STM = math.sin(TM)
	CTM = math.cos(TM)
	SDTM = math.sin(DTM)
	CDTM = math.cos(DTM)
	KL = STM * CDTM
	KK = SDTM * CTM
	SDLMR = math.sin(DLR / 2.0)
	L = SDTM * SDTM + SDLMR * SDLMR * (CDTM * CDTM - STM * STM)
	CD = 1.0 - 2.0 * L
	DL = math.acos(CD); # was ArcCos(...)
	SD = math.sin(DL)
	T = DL / SD
	U = 2.0 * KL * KL / (1.0 - L)
	V = 2.0 * KK * KK / L
	D = 4.0 * T * T
	X = U + V
	E = -2.0 * CD
	Y = U - V
	A = -D * E
	FF64 = F * F / 64.0
	Distance = AL * SD * (T - (F / 4.0) * (T * X - Y) + FF64 * (X * (A + (T - (A + E) / 2.0) * X) + Y * (-2.0 * D + E * Y) + D * X * Y)) / 1000.0
	TDLPM = math.tan((DLR + (-((E * (4.0 - X) + 2.0 * Y) * ((F / 2.0) * T + FF64 * (32.0 * T + (A - 20.0 * T) * X - 2.0 * (D + 2.0) * Y)) / 4.0) * math.tan(DLR))) / 2.0)
	HAPBR = math.atan2(SDTM, (CTM * TDLPM))
	HAMBR = math.atan2(CDTM, (STM * TDLPM))
	A1M2 = Pi2 + HAMBR - HAPBR
	A2M1 = Pi2 - HAMBR - HAPBR
	AzimuthFrom = AzimuthTo = 0.0

	#
	#  The original code used this goto ladder
	#
	#b1: if ((A1M2 >= 0.0) and (A1M2 < Pi2)) goto b5
	#	else goto b2
	#b2: if (A1M2 >= Pi2) goto b3
	#	else goto b4
	#b3: A1M2 = A1M2 - Pi2
	#	goto b1
	#b4: A1M2 = A1M2 + Pi2
	#	goto b1
	#b5: if ((A2M1 >= 0.0) and (A2M1 < Pi2)) goto b9
	#	else goto b6
	#b6: if (A2M1 >= Pi2) goto b7
	#	else goto b8
	#b7: A2M1 = A2M1 - Pi2
	#	goto b5
	#b8: A2M1 = A2M1 + Pi2
	#	goto b5
	#b9: AzimuthFrom = A1M2 / D2R
	#	AzimuthTo = A2M1 / D2R

	#
	#  Python doesn't support goto, so these subfunctions emulate the ladder
	#
	def b1():
		nonlocal A1M2, Pi2
		if (A1M2 >= 0.0) and (A1M2 < Pi2): b5()
		else: b2()
	def b2():
		nonlocal A1M2, Pi2
		if A1M2 >= Pi2: b3()
		else: b4()
	def b3():
		nonlocal A1M2, Pi2
		A1M2 = A1M2 - Pi2
		b1()
	def b4():
		nonlocal A1M2, Pi2
		A1M2 = A1M2 + Pi2
		b1()
	def b5():
		nonlocal A2M1, Pi2
		if (A2M1 >= 0.0) and (A2M1 < Pi2): b9()
		else: b6()
	def b6():
		nonlocal A2M1, Pi2
		if A2M1 >= Pi2: b7()
		else: b8()
	def b7():
		nonlocal A2M1, Pi2
		A2M1 = A2M1 - Pi2
		b5()
	def b8():
		nonlocal A2M1, Pi2
		A2M1 = A2M1 + Pi2
		b5()
	def b9():
		nonlocal AzimuthFrom, AzimuthTo, A1M2, A2M1, D2R
		AzimuthFrom = A1M2 / D2R
		AzimuthTo = A2M1 / D2R

	# start walking the goto ladder
	b1()

	return ( AzimuthFrom, AzimuthTo, Distance )


#
#  entry point - unit tests
#
if __name__ == '__main__':
	import sys
	import subprocess

	grid_set_1 = [ 'em16', 'em21' ]
	grid_set_2 = [ 'ko25', 'km71', 'of87', 'pm97', 'bl10', 'fi79'  ]
	grid_set_3 = [ 'd177', 'em16l', 'e16lc', '16em', 'dz77', 'sm11' ] # these are the bad ones

	# check a few grid strings
	for i in grid_set_1 + grid_set_2 + grid_set_3:
		sys.stderr.write("check_grid(%s) -> %s\n" % (i, check_grid(i)))
	
	# convert a few grid strings
	lat = 36.1
	lon = -97.1
	sys.stderr.write("coord_to_grid(%f, %f) -> %s\n" % (lat, lon, coord_to_grid(lat, lon)))

	# do some calculations between the first grid square and the others
	auto = False
	i = grid_set_1[0]
	for j in grid_set_1 + grid_set_2:
		if i == j:
			if not auto: auto = True
			else: continue
		forward = grids_to_distance_and_bearing(i, j)
		fargs = [ i, j ] + list(map(lambda x: "%0.3f" % x, forward))
		fargs = tuple(fargs)
		sys.stderr.write("grids_to_distance_and_bearing(%s, %s) -> %s, %s, %s\n" % fargs)

		# run the calculation in reverse and check the value equality
		reverse = grids_to_distance_and_bearing(j, i)
		ok = math.isclose(reverse[0], forward[1]) and math.isclose(reverse[1], forward[0]) and math.isclose(reverse[2], forward[2])
		sys.stderr.write(" + Reverse: %s\n" % ("OK" if ok else "FAIL" + str(reverse)))

		# run the C# program GridToGrid.exe as a forward cross-check
		result = subprocess.run([ 'GridToGrid.exe', '-km', i, j ], capture_output = True)
		dist, _, _, bearing = result.stdout.decode('utf-8').split()
		bearing = float(bearing[:-1])
		dist = float(dist)
		ok = math.isclose(forward[2], dist, rel_tol=0.1, abs_tol=0.1) and math.isclose(forward[0], bearing, rel_tol=0.1, abs_tol=0.1)
		sys.stderr.write(" + Cross-check: %s\n" % ("OK" if ok else "FAIL"))

# EOF: gridsquares.py
