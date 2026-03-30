/*
 *
 *
 *    decimate.h
 *
 *    Linear interpolation decimator.
 *
 *    This class allows very fast, and quite accurate, decimation of
 *    sampled data from one sampling rate to a lower sampling rate.
 *    The larger rate need not be evenly divisible by the lower rate.
 *
 *    The decimator uses linear interpolation to estimate output sample
 *    values that are taken between input samples.
 *
 *    Note that this component only provides decimation.  It does not
 *    provide any lowpass filtering ahead of the decimation.  If the
 *    input audio has any frequency components above the Nyquist limit
 *    of the output rate, these should be removed by an appropriate
 *    filter before decimating.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __KK5JY_FT8_DECIMATE_H
#define __KK5JY_FT8_DECIMATE_H

#include <exception>
#include <cstddef>

namespace KK5JY {
	namespace DSP {
		//
		//  DecimatorLI - interface for decimator algorithms
		//
        template <typename sample_t = float>
		class IDecimator {
			public:
				// the decimator function
				virtual bool run(sample_t in, sample_t &out) = 0;
				virtual ~IDecimator() { /* nop */ };
		};


		//
		//  DecimatorLI - linear-interpolating decimator
		//
		//     real_t   - the type used for math (must be a floating type)
		//     sample_t - the sample type
		//
		template <typename real_t = double, typename sample_t = float>
		class DecimatorLI : public IDecimator<sample_t> {
			public:
				// the decimation factor
				const real_t m_Factor;

				// the sample number of the next output
				real_t m_Target;

				// one history sample
				sample_t z0;

				// sample counter
				std::size_t m_Samples;

			public:
				DecimatorLI(std::size_t fs_hi, std::size_t fs_lo);

				// the decimator function
				virtual bool run(sample_t in, sample_t &out);
		};


		//
		//  constructor(rate_in, rate_out)
		//
		//  Rates are arbitrary, as long as (rate_in >= rate_out) is true
		//
		template <typename real_t, typename sample_t>
		inline DecimatorLI<real_t, sample_t>::DecimatorLI(std::size_t fs_hi, std::size_t fs_lo)
			: m_Factor(static_cast<real_t>(fs_hi) / static_cast<real_t>(fs_lo)),
			  m_Target(static_cast<real_t>(0)), z0(static_cast<sample_t>(0)), m_Samples(0) {
			if (fs_lo > fs_hi)
				throw new std::runtime_error("Output rate must be <= input rate");
			m_Target = m_Factor;
		}


		//
		//  run(sample_in) -> sample_out
		//
		template <typename real_t, typename sample_t>
		inline bool DecimatorLI<real_t, sample_t>::run(sample_t in, sample_t &out) {
			bool result = false;

			// if it is time to emit an output sample...
			if (++m_Samples >= static_cast<std::size_t>(m_Target)) {
				// update state
				result = true;
				m_Samples = 0;

				// calculate weights (keep constants in real_t to avoid double->float warnings)
				const real_t frac = m_Target - static_cast<std::size_t>(m_Target);
				const real_t w1 = frac;
				const real_t w0 = static_cast<real_t>(1.0) - w1;

				// calculate mean in real_t, then cast to sample_t exactly once
				const real_t sum = static_cast<real_t>(0.5) *
				                   ((w1 * static_cast<real_t>(in)) +
				                    (w0 * static_cast<real_t>(z0)));
				out = static_cast<sample_t>(sum);

				// update target
				m_Target = m_Factor;
				if (w0 < static_cast<real_t>(1.0))
					m_Target -= w0;
			}

			// update history
			z0 = in;

			// return true iff 'out' was set
			return result;
		}


		//
		//  DecimatorInt - integral interval decimator
		//
		//     sample_t - the sample type
		//
		template <typename sample_t = float>
		class DecimatorInt : public IDecimator<sample_t> {
			public:
				// the decimation factor
				const std::size_t m_Factor;

				// sample counter
				std::size_t m_Samples;

			public:
				DecimatorInt(std::size_t fs_hi, std::size_t fs_lo);

				// the decimator function
				virtual bool run(sample_t in, sample_t &out);
		};


		//
		//  constructor(rate_in, rate_out)
		//
		//  rate_in must be >= rate_out; also, rate_in % rate_out must be zero
		//
		template <typename sample_t>
		inline DecimatorInt<sample_t>::DecimatorInt(std::size_t fs_hi, std::size_t fs_lo)
			: m_Factor(fs_hi / fs_lo), m_Samples(0) {
			if (fs_lo > fs_hi)
				throw new std::runtime_error("Output rate must be <= input rate");
			if (fs_hi % fs_lo)
				throw new std::runtime_error("Input rate must be divisible by output rate");
		}


		//
		//  run(sample_in) -> sample_out
		//
		template <typename sample_t>
		inline bool DecimatorInt<sample_t>::run(sample_t in, sample_t &out) {
			if (++m_Samples >= m_Factor) {
				m_Samples = 0;
				out = in;
				return true;
			}
			return false;
		}
	}
}

#endif // __KK5JY_FT8_DECIMATE_H