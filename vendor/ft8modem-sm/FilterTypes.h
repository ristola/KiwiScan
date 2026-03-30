/*
 *
 *
 *    FilterTypes.h
 *
 *    Filter type enumeration.
 *
 *    Copyright (C) 2013-2021 by Matt Roberts, KK5JY.
 *    All rights reserved.
 *
 *    License: GNU GPL3 (www.gnu.org)
 *    
 *
 */

#ifndef __KK5JY_FILTERTYPES_H
#define __KK5JY_FILTERTYPES_H

namespace KK5JY {
	namespace DSP {
		// different types of FIR filters
		typedef enum {
			LowPass,
			HighPass,
			BandPass,
			BandStop,
			Resonant,
			TwinPeak,
		} FirFilterTypes;
	}
}

#endif // __KK5JY_FILTERTYPES_H
