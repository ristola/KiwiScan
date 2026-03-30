/*
 *
 *
 *    encode.cc
 *
 *    Conversion from text to keying symbols.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

// standard includes
#include <string>
#include <deque>
#include <stdexcept>

// system includes
#include <stdio.h>
#include <ctype.h>

// local includes
#include "stype.h"
#include "jt65.h"
#include "encode.h"

using namespace std;


//
//  KK5JY_ENCODE_USES_PIPES
//
//  Define this to use the original toolchain with pipeline 'head' and 'tail';
//     if undefined, use new extract_symbols(...) code, instead.
//#define KK5JY_ENCODE_USES_PIPES

#ifndef KK5JY_ENCODE_USES_PIPES
//
//  extract_symbosl(raw_text, tail, head, include_ws)
//
//  Extract symbols from raw *code utility output.
//
//  The 'input' string is assumed to have one or more lines, separated
//  by a newline of some type; the function takes the last 'tail' lines
//  from the end first, and then returns 'head' lines from the front,
//  with all whitespace removed.
//
//  The result is placed back into 'input'.
//
static void extract_symbols(string &input, unsigned tail, unsigned head, bool include_ws = false);
#endif

//
//  encode(mode, text) - return the keying symbols for a message
//
//  See: encode.h
//
string KK5JY::FT8::encode(const string &mode, const string &txt) {
	string tool; // the symbol encoder utility name (e.g., 'ft8code')
	string rmode = my::toLower(mode);
	unsigned int tail = 1;  // tail the last line of output by default...
	unsigned int head = 0;  // ...and use all the lines of the tail string
	bool sync = false; // true iff JT65

	// decide on the tool to run, and how much of its output to use
	if (rmode == "ft8") {
		tool = "/Applications/WSJT-X.app/Contents/MacOS/ft8code";
	} else if (rmode == "ft4") {
		tool = "/Applications/WSJT-X.app/Contents/MacOS/ft8code"; // FT4 uses ft8code (no separate ft4code exists)
	} else if (rmode == "jt9") {
		tool = "/Applications/WSJT-X.app/Contents/MacOS/jt9code";
		tail = 3; // read last three lines of jt9code output
	} else if (rmode == "jt65") {
		sync = true; // special sync pattern handling
		tool = "/Applications/WSJT-X.app/Contents/MacOS/jt65code";
		tail = 3; // read last three lines of jt65code output
	} else if (rmode == "wspr") {
		tool = "wsprcode";
		tail = 8; // read last eight lines of wsprcode output
		head = 6; // ...then the first six lines of that
	} else {
		throw runtime_error("Invalid mode provided");
	}

	// build the command line
	string cmd = tool + " \"" + txt + "\"";
#ifdef KK5JY_ENCODE_USES_PIPES
	cmd += " | tail " + to_string(-static_cast<int>(tail));
	if (head > 0)
		cmd += " | head " + to_string(-static_cast<int>(head));
#endif

	// run the utility
	FILE *code = ::popen(cmd.c_str(), "r");

	#ifdef VERBOSE_DEBUG
	cerr << "DEBUG: popen(" << cmd << ")" << endl;
	#endif

	// if something went wrong, bail
	if ( ! code) {
		#ifdef VERBOSE_DEBUG
		cerr << "DEBUG: ft8code failed" << endl;
		#endif

		// return empty string on failure
		return string();
	}

	// I/O loop on 'ft8code' output
	char iobuffer[128]; // C-style read buffer
	string linebuffer;  // stores the entire output
	linebuffer.reserve(1500); // largest encoder output + some margin
	while ( ! feof(code)) {
		// read a chunk from the encoder's standard output
		size_t ct = fread(iobuffer, 1, sizeof(iobuffer), code);
		if (ct == 0) // EOF
			break;

		// and add it to the linebuffer
		linebuffer.append(iobuffer, ct);
	}

	// close the pipe to the child
	::pclose(code);

	#ifdef VERBOSE_DEBUG
	cerr << "DEBUG: " << tool << " returned " << linebuffer.size() << " bytes." << endl;
	#endif
#ifdef KK5JY_ENCODE_USES_PIPES
	// trim the string
	linebuffer = my::strip(linebuffer);

	// JT65 has special handling
	if (sync)
		return KK5JY::JT65::convert_jt65code(linebuffer);
#else
	// JT65 has special handling
	if (sync) {
		// pull out the symbols and leave the whitespace
		extract_symbols(linebuffer, tail, head, true);

		// interleave sync pattern
		return KK5JY::JT65::convert_jt65code(linebuffer);
	}

	// pull out the symbols and remove the whitespace
	extract_symbols(linebuffer, tail, head);
#endif
	#ifdef VERBOSE_DEBUG
	cerr << "DEBUG: ft8code returned: " << linebuffer << endl;
	#endif

	// everybody else
	return linebuffer;
}


#ifndef KK5JY_ENCODE_USES_PIPES
//
//  extract_symbols(...) - extract symbols from *code utility output (see above)
//
static void extract_symbols(string &input, unsigned tail, unsigned head, bool include_ws) {
	// this is the working string; reserve enough space for the longest line
	string line;
	line.reserve(128);

	// extract a deque of lines
	deque<string> lines;
	char ending = 0;
	char ch = 0;
	for (string::const_iterator i = input.begin(); i != input.end(); ++i) {
		ch = *i;
		switch (ch) {
			case 10:
			case 13: {
				if ( ! ending)
					ending = ch;
				if (ch == ending) {
					lines.push_back(line);
					line.clear();
				}
			} break;
			default: {
				bool ws = include_ws && ::isspace(static_cast<unsigned char>(ch));
				bool sm = ch >= '0';
				if (ws || sm) {
					line.push_back(ch);
				}
			} break;
		}
	}

	// consume the last line if newline was missing
	if ( ! line.empty()) {
		lines.push_back(line);
		line.clear();
	}
		
	// build the result
	if (tail > 0) {
		while (lines.size() > tail)
			lines.pop_front();
	}
	if (head > 0) {
		while (lines.size() > head)
			lines.pop_back();
	}

	// put the remaining lines together into a single whitespace-free string
	input.clear();
	for (deque<string>::const_iterator i = lines.begin(); i != lines.end(); ++i) {
		input.append(*i);
	}
}
#endif

// EOF: encode.cc