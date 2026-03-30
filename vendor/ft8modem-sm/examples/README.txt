======================================================
Example scripts for ft8modem
======================================================

Copyright (C) 2024 by Matthew K. Roberts, KK5JY.
All rights reserved.

License: GPL Version 3.0


=======================
Multi-SDR Monitor Sites
=======================

The scripts 'start-sdr-core.sh' and 'start-sdr-node.sh'
are example start scripts for running multiple RTL-SDR
devices on a single PC to build a monitor site.

The 'start-sdr-core.sh' script runs 'ft8collect' and
'ft8report' in the background.  These will write all
spots into a single file, then report the spots
periodically to PSKreporter.info.

You will want to edit the configuration in the script
to use your callsign and grid square for the reports.

The 'start-sdr-node.sh' script will start a single
RTL-SDR device, managed by 'ft8cat', and decoded by
its own 'ft8modem' instance.  The script requires one
argument, which is the port number where to listen
for CAT commands.

Run 'start-sdr-core.sh' once for your PC.

Run 'start-sdr-node.sh' once for each RTL-SDR device
you intend to use for the monitor site.

You are responsible for setting the receiver frequency
for each device.  This can be done either via the CAT
port you provide to 'start-sdr-node.sh' or by sending
the BAND command to standard input.


<EOF>
