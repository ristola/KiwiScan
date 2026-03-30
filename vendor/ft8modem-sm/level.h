/*
 *
 *
 *    level.h
 *
 *    Level conversions.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __KK5JY_DSP_LEVEL_H
#define __KK5JY_DSP_LEVEL_H

#include <string>
#include <sys/time.h>
#include <math.h>

namespace KK5JY {
	namespace DSP {
		//
		//  decibels(A) - convert linear to dB
		//
		inline double decibels(double A) {
			return 20.0 * log10(A);
		}

		//
		//  linear(dB) - convert dB to linear
		//
		inline double linear(double db) {
			// V2 = V1 * 10 ^ (dB / 20)
			return pow(10, db / 20.0);
		}
	}
}

#endif // __KK5JY_DSP_LEVEL_H
