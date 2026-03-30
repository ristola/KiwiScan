/*
 *
 *
 *    version.h
 *
 *    Versioning information.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <string>

namespace KK5JY {
	namespace FT8 {
		//
		//  return the current version number
		//
		//  NOTE: the patch number below is set by script; do not hand-edit.
		//
		std::string GetModemVersion() {
			return "SM-1.10, 20251229";
		}
	}
}

// EOF