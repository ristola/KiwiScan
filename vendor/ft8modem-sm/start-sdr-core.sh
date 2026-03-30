#!/bin/bash
#
#   start-sdr-core.sh
#
#   Example script for starting core SDR services for
#   multi-receiver site.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

# put YOUR call here
MYCALL=N4LDR

# put YOUR grid square here (4-char, 6-char, etc.)
MYGRID=FM08

# path to the spot log file; put it where you want
TARGET=~/FT8/ft8-all.txt

# the UDP port where 'ft8cat' will send its spots
UDP_PORT=7777


# make the folder if needed
dname=`dirname $TARGET`
if [ ! -z "$dname" ] && [ ! -d "$dname" ]; then
	mkdir -v -m 0755 $dname || exit 1
fi

# start the collector, and fail if it doesn't start
ft8collect -a $TARGET -p $UDP_PORT -z || exit 1
echo "Started ft8collect"

# start the PSKreport client, pointed at the ALL.TXT from above
ft8report -a $TARGET -c $MYCALL -g $MYGRID -z
echo "Started ft8report"

# EOF: start-sdr-core.sh
