#!/bin/bash
#
#   start-sdr-node.sh
#
#   Example script for starting one complete decoder chain
#   using an RTL-SDR device.
#
#   This example assumes that ft8collect is already listening
#   for spots on UDP/7777,  and its output ALL.TXT may also be
#   feeding ft8report.
#
#   An example of running ft8collect and ft8report together, to
#   match using this script to start the receivers:
#
#      ft8collect -a ~/Documents/ft8-all.txt -p 7777 -z
#      ft8report -a ~/Documents/ft8-all.txt -c mycall -g mygrid -z
#
#   See the script start-sdr-core.sh for an example.
#
#   Only one instance of ft8collect and/or ft8report needs to
#   be run for a single PC.  This script can be run once per
#   RTL-SDR receiver device to be used to feed ft8collect.
#
#   Copyright (C) 2024 by Matt Roberts.
#   License: GNU GPL3 (www.gnu.org)
#
#

# enable SDR Q-branch direct-sampling (comment this out for an upconverter)
#Q_BRANCH=-q

# set upconverter offset (comment this out for Q-branch direct-sampling)
UP_CONVERT="-c 125M" # e.g., 125M = 125MHz upconverter

# target port for logging; this is where ft8cat will send spots to ft8collect or ft8report
UDP_SPOTS=7777

# sample rate; use 24000 for RTL-SDR unless you have a very good reason for something else
RATE=24000

# trailing window extension; make the timeslot window a bit larger
TRAIL="-w 700" # msec


# the serial number of the receiver is also the port number for TCP and UDP
PORT=$1
if [ -z "$PORT" ]; then
	echo ""
	echo "Please specify port number for this SDR instance."
	echo ""
	echo "The port number should be between 1025 and 65535.  Each SDR"
	echo "will need its own unique port number to communicate between"
	echo "the components."
	exit 1
fi

# sanity check
if [ ! -z "$UP_CONVERT" ] && [ ! -z "$Q_BRANCH" ]; then
	echo "Please don't use both UP_CONVERT and Q_BRANCH; comment out one of them."
	exit 1
fi

# first, run the SDR as a background process
ft8sdr $UP_CONVERT $Q_BRANCH -e $PORT -p $PORT -u $PORT -r $RATE -V &

# then run the modem chain, in the foreground for now, configured for UDP audio
ft8cat -A $UDP_SPOTS -Pp $PORT ft8modem -T 1 -r $RATE $TRAIL ft8 udp:$PORT

# once the modem exits, stop the SDR
kill -s SIGHUP %1

# EOF: start-sdr-node.sh
