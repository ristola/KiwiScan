/*
 *
 *
 *    shape.h
 *
 *    Raised cosine envelope and key shaping.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __KK5JY_SHAPE_H
#define __KK5JY_SHAPE_H

#include <math.h>
#include <stdint.h>
#include "nlimits.h"
#include "IFilter.h"

namespace KK5JY {
	namespace DSP {
		// ---------------------------------------------------------------------------
		//
		//   class Shaper<T>
		//
		//   Raised-cosine shaping for envelope shaping.
		//
		// ---------------------------------------------------------------------------
		template <typename T, typename ctr_t = uint16_t>
		class Shaper {
			private:
				const ctr_t samples;
				const T phi;
				ctr_t ctr;
			public:
				Shaper(ctr_t _samp);
				T run(bool key);
				ctr_t size() const { return samples; }
		};

		//
		//  Shaper::ctor
		//
		template <typename T, typename ctr_t>
		Shaper<T, ctr_t>::Shaper(ctr_t _samp)
			: samples(_samp),
			  phi(static_cast<T>(M_PI) / static_cast<T>(samples)),
			  ctr(0) {
			// nop
		}

		//
		//  Shaper::run
		//
		template <typename T, typename ctr_t>
		T Shaper<T, ctr_t>::run(bool key) {
			if (key) {
				if (ctr < samples) {
					++ctr;
				} else {
					return norm_limits<T>::maximum;
				}
			} else {
				if (ctr > 0) {
					--ctr;
				} else {
					return static_cast<T>(0);
				}
			}

			const T half = static_cast<T>(0.5);
			const double arg = static_cast<double>(ctr) * static_cast<double>(phi);
			const T c = static_cast<T>(::cos(arg));
			return static_cast<T>(norm_limits<T>::maximum * (half + (-half * c)));
		}


		// ---------------------------------------------------------------------------
		//
		//  KeyShaper<T> - a more generic shaper that uses a prototype raised cosine
		//                 to shape FSK data sample-by-sample; this class should be
		//                 faster in the general case during encoding, since it
		//                 precomputes the prototype waveform.
		//
		// ---------------------------------------------------------------------------
		template <typename sample_t>
		class KeyShaper : public IFilter<sample_t> {
			private:
				sample_t * const shaper; // the prototype waveform
				sample_t * const ep;     // the 'end' pointer for 'shaper'
				sample_t *sp;            // working pointer into the waveform
				sample_t state;          // filter state
				sample_t scale;          // current waveform scale
				sample_t offset;         // current waveform offset
				sample_t last;           // last input sample

			public:
				KeyShaper(size_t N);

				// interface from IFilter<T>
				sample_t run(sample_t sample);
				sample_t value(void) const { return state; }
				void add(const sample_t sample) { run(sample); }
				void clear() { state = static_cast<sample_t>(0); }
		};


		//
		//  KeyShaper<T> ctor; N = shaping waveform size, in samples
		//
		template <typename sample_t>
		inline KeyShaper<sample_t>::KeyShaper(size_t _n)
			: shaper(new sample_t[_n]),
			  ep(shaper + _n), sp(shaper),
			  state(0), scale(1), offset(0), last(0) {
			// generate the prototype waveform, range [0, 1) over '_n' samples
			const double dphi = M_PI / static_cast<double>(_n);
			for (size_t ctr = 0; ctr != _n; ++ctr) {
				const double arg = static_cast<double>(ctr) * dphi;
				const sample_t half = static_cast<sample_t>(0.5);
				const sample_t c = static_cast<sample_t>(::cos(arg));
				*sp++ = static_cast<sample_t>(
					norm_limits<sample_t>::maximum * (half + (-half * c))
				);
			}

			// set 'sp' == NULL to prime algo with first sample
			sp = 0;

			#ifdef CHATTY_DEBUG
			// DEBUG:
			for (size_t ctr = 0; ctr != _n; ++ctr) {
				std::cerr << "DEBUG: s[" << ctr << "]: " << shaper[ctr] << std::endl;
			}
			#endif
		}


		//
		//  KeyShaper<T>::run(...)
		//
		template <typename sample_t>
		inline sample_t KeyShaper<sample_t>::run(sample_t sample) {
			#ifdef CHATTY_DEBUG
			// DEBUG:
			std::cerr << "DEBUG: in = " << sample << std::endl;
			#endif

			// prime the algo with the first sample; don't shape until first transition
			if ( ! sp) {
				state = last = sample;
				sp = ep;
				return sample;
			}

			// if the key input has changed...
			if (sample != last) {
				// calculate the scale and offset
				scale = static_cast<sample_t>(sample - last);
				offset = last;

				#ifdef CHATTY_DEBUG
				// DEBUG:
				std::cerr
					<< "DEBUG: last = " << last << "; in = " << sample
					<< "; scale = " << scale << "; offset = " << offset
					<< std::endl;
				#endif

				// initialize the waveform pointer
				sp = shaper;

				// update the state/history
				last = sample;
			}

			// for the duration of the prototype waveform...
			if (sp != ep) {
				// scale and offset the waveform
				state = static_cast<sample_t>((*sp++ * scale) + offset);
				return state;
			}

			// just return the input
			state = sample;
			return state;
		}
	}
}

#endif // __KK5JY_SMOOTH_H