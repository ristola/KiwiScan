======================================================
ft8modem - command-line software modem for FT8 and FT4
======================================================

Copyright (C) 2023-2024 by Matthew K. Roberts, KK5JY.
All rights reserved.

License: GPL Version 3.0


=============
PREREQUISITES
=============

This project depends on the 'rtaudio' package, available here:

https://www.music.mcgill.ca/~gary/rtaudio/

The minimum RtAudio version required is now 6.0.1.  Please follow the
instructions on Gary's website for installation.

The Python utilities, including ft8cat and ft8qso, require Python 3,
which is installed on nearly all modern Linux systems.


INSTALLATION

To build the ft8modem and support applications, run at the command line:

	$ make

To install to /usr/local/bin, run:

	$ make install

To run the core modem application:

	$ ft8modem

...which will show you the command line options and available sound cards.

More detail on each application is given below.


=========================
ft8modem - the modem core
=========================

COMMANDS

Generally, commands and their arguments are not case-sensitive.  Commands
are terminated by a newline ('\n') character, which is usually ASCII linefeed.

The modem current supports the following commands.  Each command is entered
on a line by itself.

LEVEL

	This returns the output gain, in dB.  Full scale is '0'.  Half voltage
	is '-6' and so on.

LEVEL x

	This sets the output gain, where 'x' is the gain in dB.  Note that this
	is independent of your mixer settings, which may restrict the level
	further.

	E.g., the command:

	LEVEL -20

	...will set the output gain to -20dB, which is about 10% of max volume.

	If set, the new value is persisted to the .ft8modemrc file.

DEPTH

	This returns the decoding depth of the 'jt9' utility.  Valid values are
	1, 2, or 3.

DEPTH x

	This sets the decoding depth.  Valid values are 1, 2, or 3.

	If set, the new value is persisted to the .ft8modemrc file.

LEADIN

	This returns the modulator lead-in time, in milliseconds.

LEADIN x

	This sets the modulator lead-in time, in milliseconds.  The default
	value is mode-specific, and selected automatically to a reasonble
	value for the mode requested.

	Valid values are >= 0.

	If set, the new value is persisted to the .ft8modemrc file.

MODE

	This returns the current operating mode (e.g., FT8, FT4).

MODE x

	This switches the modem to a different operating mode, where 'x' is
	one of { FT4, FT8, JT9, JT65, WSPR }. The modem will restart audio
	processing with the new mode settings.

	E.g., the command:

	MODE FT4

	...will switch the modem from its current mode to FT4 mode.

VERSION

	This returns the current version of ft8modem.

QUIT

	Exits the application.

ffff msg

	This sends a message, where ffff is the frequency, in three or four
	digits, and 'msg' is the message to send.  E.g., 

	750 CQ KK5JY EM16

	This will send the CQ message at 750 Hz audio frequency.

	Valid values for ffff are generally in the range of 200 to 3000.

STOP

	This causes the transmission to immediately stop.  If no transmission
	is occurring, this is ignored.  This command is intended to allow a
	user or script to cancel a transmission before it completes.

PURGE

	When sent during an ongoing timeslot, this command tells the modem
	to discard any decodes produced.  The 'ft8cat' utility uses this
	command to prevent partial decodes from showing up on the wrong
	band when a the 'BAND' command is sent mid-cycle.  Decodes will
	resume normally for the subsequent timeslot.

The modem will send certain status and request messages, as well:

D: x

	This is a decode.  One D: line will be emitted for each frame that is
	successfully decoded by 'jt9' as it runs.  This is the receiver output
	of the modem.  The string 'x' will contain various information about
	the decode, e.g.,:

	D: 1724534955  -5 -0.0 2527 ~  WA8RC KK5JY RR73

	The large number is a system timestamp (see time(2) man page).  The
	other columns are similar to those produced by WSJT-X, including, in
	order shown:

	SNR
	DT
	AF
	Mode (~ is FT8, + is FT4)
	Message text

E: x

	This is an encode.  The format is essentially the same as for the
	decoded, but encode messages are sent when the transmission begins,
	whereas the decode messages are sent after the end of a message.

	The DT and SNR for the encode will always be zero.

INPUT: x

	This reports the input level, measured in dB relative to full volume.
	A script or user interface can use this to detect when audio is too
	loud or too soft.

TX: n

	This is a request to a higher-level script, such as ft8cat, to put
	the radio into transmit or receive.  If 'n' == 1, the modem is requesting
	the radio be set to transmit; if 'n' == 0, the modem is requesting the
	radio to be set to receive.

DEBUG: x

	This is an information message communicating information useful for
	diagnosing or debugging the application, or reporting low-priority
	information about its progress.

TRACE: x

	This is like DEBUG, but lower priority information.

More commands or status messages will be added later as needed.


=========================
ft8cat - the CAT inteface
=========================

The 'ft8cat' utility is a Python3 script that implements additional features
to the modem.  The 'ft8cat' script runs 'ft8modem' and provides the same
commands, plus additional commands that require CAT support.

The 'ft8cat' script requires that 'rigctld' from HamLib is running on the
system, and listening to the 'localhost' address.

OPTIONS

The 'ft8cat' script takes a number of command-line options.  Run the script
with no arguments to see documentation of command-line options.

Even without options, 'ft8cat' requires the command line used for 'ft8cat',
e.g.,

	./ft8cat -s ./ft8modem ft8 127

The above example command starts 'ft8cat' in split mode, which starts
'ft8modem' in FT8 mode on sound device 127.

Run 'ft8cat' with no arguments to see available options.

COMMANDS

The 'ft8cat' current supports the following commands.  Each command is
entered on a line by itself.  If 'ft8modem' commands are given, they are
passed to the subordinate 'ft8modem' instance.

BAND x

	This sets the radio band to one of the supported ham bands.  'x' can be
	a band in meters, e.g., '20', or it can be either 'UP' or 'DOWN', which
	moves to the next higher (shorter wavelength) or lower (longer wavelength)
	band in the list.

	Supported bands are: 2, 6, 10, 12, 15, 17, 20, 30, 40, 60, 80, 160

	The table translating between bands, modes, and frequencies is
	found at the top of the file bands.py.

MODE x

	This command will change the operating mode.  Supported values
	include FT8, FT4, JT9, and JT65.  If the new mode is different from
	the current mode, the FA and FB will be changed to an appropriate
	value for the new mode and current band.

	This command restarts the ft8modem child process with a modified
	command line from the one originally provided.

The 'ft8cat' will send certain status messages, as well:

FA: x
FB: y
SPLIT: z

	These provide the current dial frequencies for the A and B VFOs, as
	well as the most recent detected split status.

More commands or status messages will be added later as needed.


=========================
ft8qso - the QSO inteface
=========================

The 'ft8qso' utility is a Python3 script that implements additional features
above 'ft8cat' to implement basic automatic sequencing of messages.

This is mostly a reference implementation for script writers.  It does not
try to anticipate all use cases.

'ft8qso' does not have any command line options, but it requires the command
line used for 'ft8cat', e.g.,

	./ft8qso ./ft8cat -s ./ft8modem ft8 127

The above example command starts 'ft8qso' which starts 'ft8cat' in split
mode, which starts 'ft8modem' in FT8 mode on sound device 127.

COMMANDS

The 'ft8cat' current supports the following commands.  Each command is
entered on a line by itself.  If 'ft8modem' commands are given, they are
passed to the subordinate 'ft8modem' instance.


MYCALL call

	This sets the callsign of the local station.

MYGRID grid

	This sets the grid of the local station, as a four-letter string.

CQ freq

	This calls CQ, one time, at the specified audio frequency.

QSO call [freq]

	This starts a QSO with the call provided, and optionally at the
	specified audio frequency.  If 'freq' isn't provided, the frequency
	of the other station's last transmission will be used.

More commands or status messages will be added later as needed.


==============================
ft8collect - the log collector
==============================

The 'ft8collect' utility is optional.  To see the available command-line
options, run 'ft8collect -h'.

When running multiple instances of 'ft8cat', such as with a multi-receiver 
site feeding FT8 spots to PSKreporter.info, this utility listens on a
UDP port, and collects spot information from 'ft8cat'.  The 'ft8cat'
instances must be started with the -A option to support this.

The 'ft8collect' records each spot into a single ALL.TXT file, suitable
for use with utilities designed to parse it.  One of these is the
'ft8report' utility, described below.


============================
ft8report - the log reporter
============================

The 'ft8report' utility is optional.  To see the available command-line
options, run 'ft8report -h'.

The 'ft8report' utility follows an ALL.TXT file as it is being updated,
and periodically reports spots from that file to PSKreporter.info.  The
update interval is approximately one minute for the first cycle, and
then five minutes thereafter.

Both 'ft8cat' and 'ft8collect' can generate real-time ALL.TXT files to
be used with 'ft8report'.


=======================
ft8sdr - an SDR manager
=======================

The 'ft8sdr' utility is optional.  To see the available command-line
options, run 'ft8sdr -h'.

When running a receive-only station, the 'ft8sdr' utility will run
'rtl_fm' on a supported receiver, and provide a CAT interface
similar to rigctld, so that 'ft8cat' can manage the receiver
frequency, and the modem mode.  This allows 'ft8cat' to treat an
SDR dongle like a more full-featured receiver, changing the
frequency and mode as desired.

This allows, among other things, FT8 scanners to be built with
inexpensive dongles, and managed by 'ft8cat'.

When using this application with the 'ft8modem' there are two ways
to feed the audio from the receiver to the 'ft8modem.'  The first
is via ALSA loopback devices.  This is documented on the internet.
Loopback devices appear as normal sound cards, allowing the apps
involved to use sound card code to communicate audio data between
each other.

The second option for feeding audio is via local UDP data.  The
modem supports a special sound device argument udp:port, where
'port' is the UDP port to listen for audio.  The 'ft8sdr' can
similarly be started with the '-u' option, which feeds the
audio data from 'rtl_fm' directly to the modem via UDP on the
loopback network device.  This option is somewhat cleaner and
doesn't depend on special ALSA sound devices.

When running multiple SDR and modem instances on a single PC,
each will need its own UDP port number.  Also, the 'ft8cat'
CAT connection will require a unique port for each instance.
For simplicity, it is recommended to run 'ft8sdr' with the
-d, -e, -p, and -u options all set to the same value as the
port number, and the 'ft8cat' and 'ft8modem' UDP port values
set to this same value.  This keeps each receiver "stack"
isolated from the others.


=========================
APPLICATION NOTES
=========================

1. WSJT-X on Raspberry Pi

When running FT8 from a Rasberry Pi, I normally recommend that
people use the 2.1.2 'jt9' decoder, because it runs much faster
on the limited CPU of the RasPi.  It's probably also best to
run it with a DEPTH value of 1 (fast).

If you want to use FT4 on Raspberry Pi, see the notes below
on 'Encoders'.


2. Encoders

Source builds of WSJT-X will install utilities for encoding
messages in FT8, JT9, JT65, and WSPR.  However, the FT4 encoder,
'ft4code', is only built in the 2.5 and later releases, and not
installed by default.  To use FT4 with the modem, the 'ft4code'
utility needs to be hand-copied from the build folder into the
system path, usually /usr/local/bin.

The source for WSJT-X 2.1.2 doesn't even build the 'ft4code'
utility, and there is no build target for it.  If you want to
use FT4 from the 2.1.2, such as when running on a Raspberry Pi,
you need to get 'ft4code' from another source.  If you build
both 2.1.2 and 2.6.1 on the same machine, you can copy 'jt9'
from the former, and the 'ft4code' utility from the latter,
and they should work fine together.  The encoder utilities,
including 'ft4code' are just text conversion programs, and
the encodings should be the same from WSJT-X 2.0.0 forward.


3. Persisent Hashing

There are available projects, such as:

	https://github.com/rand-projects/jt9-persistent-hash

...for patching jt9 in a way that supports persistent hashing
between each run of that program. I don't need that support, but
if you install a patched version of jt9 on your system, it should
run seamlessly with the ft8modem, by passing the modified
command using the moem's '-j' option, e.g.,

	ft8modem -j 'jt9 -P hash.dat' ft8 131

... or similar. The argument to -j is used in place of 'jt9'
when the modem is running the decoder.  Make sure to use quotes
around the -j option if you need to include spaces, such as is
shown in the example.


4. Multiple Receiver Sites

The most common reason to run multiple modem instances is to
create a receiver site to monitor several bands at the same
time, perhaps to report to PSKreporter, or to gather data
for propagation research.

There are example scripts in the 'examples' folder, showing
how to start multiple receivers and feed a single instance of
ft8collect and ft8report.  These scripts are similar to those
I use for my own monitor site.

<EOF>
