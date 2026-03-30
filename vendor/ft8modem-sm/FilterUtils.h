/*
 *
 *
 *    FilterUtils.h
 *
 *    Common utilities for filters.
 *
 *    Copyright (C) 2021 by Matt Roberts, KK5JY.
 *    All rights reserved.
 *
 *    License: GNU GPL3 (www.gnu.org)
 *    
 *
 */

#ifndef __KK5JY_FILTERUTILS_H
#define __KK5JY_FILTERUTILS_H

#include "IFilter.h"
#include "nlimits.h"
#include "osc.h"

namespace KK5JY {
	namespace DSP {
		// macro to convert integer data type into right-shift value for fixed-point multiplications
		#define IIRFILTER_INTEGER_SHIFT(datatype) ((sizeof(datatype) * 8) - 2)

		//
		//  class FilterUtils - common filter code
		//
		class FilterUtils {
			public:
				//
				//  CalculateGain(filter, fs, f*, count) - calculate the peak gain at one or more frequencies.
				//
				template <typename sample_t>
				inline static double CalculateGain(IFilter<sample_t> *filter, size_t fs, double *freqs, size_t samples) {
					double peak = 0;

					for (double *f = freqs; *f > 0; ++f) {
						// build an oscillator at the test frequency
						Osc<sample_t> source(*f, fs);

						// run 2 * 'samples' through the filter, and track the peak
						for (size_t i = 0; i != 2 * samples; ++i) {
							sample_t output = std::abs(filter->run(source.read()));
							if (i >= samples && output > peak) {
								peak = output;
							}
						}

						// clear the filter state
						filter->clear();
					}

					// the overall gain is the largest peak that was found
					return peak / norm_limits<sample_t>::maximum;
				}


				//
				//  CalculateGain(filter, Omegas, count) - calculate the peak gain at one or more frequencies.
				//
				template <typename sample_t>
				inline static double CalculateGain(IFilter<sample_t> *filter, double *Omegas, size_t samples) {
					double peak = 0;

					for (double *f = Omegas; *f > 0; ++f) {
						// build an oscillator at the test frequency
						Osc<sample_t> source(*f);

						// run 2 * 'samples' through the filter, and track the peak
						for (size_t i = 0; i != 2 * samples; ++i) {
							sample_t output = std::abs(filter->run(source.read()));
							if (i >= samples && output > peak) {
								peak = output;
							}
						}

						// clear the filter state
						filter->clear();
					}

					// the overall gain is the largest peak that was found
					return peak / norm_limits<sample_t>::maximum;
				}
		};
	}
}

#endif // __KK5JY_FILTERUTILS_H
