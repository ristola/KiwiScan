/*
 *
 *
 *    jt65.h
 *
 *    Special sync-pattern handling for JT65.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *    Algorithm adapted from the JT65 simulator from
 *    WSJT-X 2.6.1, jt65sim.f90, which is also GPLv3.
 *
 *
 */

#include <string>
#include <sstream>
#include <cstdlib>
#include <stdexcept>
#include <limits>

// this is the "sync pattern" itself
static unsigned char sync_pattern[126] = {
	1,0,0,1,1,0,0,0,1,1,1,1,1,1,0,1,
	0,1,0,0,0,1,0,1,1,0,0,1,0,0,0,1,
	1,1,0,0,1,1,1,1,0,1,1,0,1,1,1,1,
	0,0,0,1,1,0,1,0,1,0,1,1,0,0,1,1,
	0,1,0,1,0,1,0,0,1,0,0,0,0,0,0,1,
	1,0,0,0,0,0,0,0,1,1,0,1,0,0,1,0,
	1,1,0,1,0,1,0,1,0,0,1,1,0,0,1,0,
	0,1,0,0,0,0,1,1,1,1,1,1,1,1
};


namespace KK5JY {
	namespace JT65 {
		//
		//  convert_jt65code(s, invert)
		//
		//  This takes the symbols generaged by jt65code(1) as a single input string,
		//  applies sync pattern interleaving, then returns a new string that is
		//  suitable for use by MFSK<T>::transmit(...).
		//
		std::string convert_jt65code(const std::string &s, bool invert_sync = false) {
			const unsigned char sync_target = invert_sync ? 1 : 0;

			// read the message syms; these are tones in the range of 0 to 63
			std::string syms;
			syms.reserve(128);
			std::string token;
			token.reserve(3);
			std::istringstream input(s);
			while (input >> token) {
				unsigned long v = 0;
				try {
					size_t pos = 0;
					v = std::stoul(token, &pos, 10);
					if (pos != token.size()) continue; // skip junk
				} catch (...) {
					continue; // skip malformed tokens
				}
				if (v > 63UL) v = 63UL;
				unsigned short tone = static_cast<unsigned short>(v);
				syms += static_cast<char>('0' + tone);
			}

			// interleave with the sync pattern
			std::string result;
			result.reserve(128);
			unsigned char * cp = sync_pattern;
			const unsigned char * const ep = sync_pattern + sizeof(sync_pattern);
			std::string::const_iterator sym = syms.begin();
			while (cp != ep) {
				if (*cp == sync_target) {
					result += static_cast<char>((*(sym++)) + 2); // raise non-sync tones by 2
				} else {
					result += '0'; // 2 tones below 'zero'
				}
				++cp;
			}

			// return the result
			return result;
		}
	}
}


// EOF