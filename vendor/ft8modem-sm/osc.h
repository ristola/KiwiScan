/*
 *
 *   osc.h
 *
 *   Oscillator class.
 *
 *   Copyright (C) 2010-2023 by Matt Roberts.
 *   License: GNU GPL3 (www.gnu.org)
 *
 */

#ifndef __OSC_H
#define __OSC_H

#include <cmath>
#include <iostream>
#include <complex>
#include "nlimits.h"

#ifndef M_2PI
#define M_2PI (2.0 * M_PI)
#endif

namespace KK5JY {
	namespace DSP {
		//
		//  oscillator class
		//
		template <typename sample_t = double>
		class Osc {
			private:
				double m_Omega, m_P;
				double theta;

			public:
				// param ctor
				Osc(double f0, // Hz
					double fs, // Hz
					double P)  // radians
						: m_Omega(2.0 * M_PI * f0 / fs), m_P(P), theta(0.0) {
					// nop
				}

				// param ctor
				Osc(double f0, // Hz
					double fs) // Hz
						: m_Omega(2.0 * M_PI * f0 / fs), m_P(0.0), theta(0.0) {
					// nop
				}

				// param ctor
				Osc(double Omega)
						: m_Omega(Omega), m_P(0.0), theta(0.0) {
					// nop
				}

				// copy ctor
				Osc(const Osc &o)
					: m_Omega(o.m_Omega), m_P(o.m_P) {
					// nop
				}

			private: // disallowed ctors
				Osc();

			private: // utilities
				double read_core(double error);

			public:
				// read one sample
				sample_t read(double error = 0);

				// set the frequency to a new value without adjusting phase
				void setFreq(double f0, double fs);
		};


		template <typename sample_t>
		void Osc<sample_t>::setFreq(double f0, double fs) {
			m_Omega = (2.0 * M_PI * f0 / fs);
		}


		template <typename sample_t>
		inline double Osc<sample_t>::read_core(double error) {
			// compute this sample
			double result = sin(theta + m_P);

			// clamp the amount of angular precession to +/- 50%
			if (error > 0.5) {
				error = 0.5;
			} else if (error < -0.5) {
				error = -0.5;
			}

			// update the angle
			theta += (m_Omega * (1.0 + error));

			// wrap the angle
			if (theta > M_2PI)
				theta -= M_2PI;

			// return the sample
			return result;
		}


		template <typename sample_t>
		inline sample_t Osc<sample_t>::read(double error) {
			return static_cast<sample_t>(read_core(error));
		}


		template <>
		inline int8_t Osc<int8_t>::read(double error) {
			return static_cast<int8_t>(read_core(error) * norm_limits<int8_t>::maximum);
		}


		template <>
		inline int16_t Osc<int16_t>::read(double error) {
			return static_cast<int16_t>(read_core(error) * norm_limits<int16_t>::maximum);
		}


		template <>
		inline int32_t Osc<int32_t>::read(double error) {
			return static_cast<int32_t>(read_core(error) * norm_limits<int32_t>::maximum);
		}



		/*
		 *
		 *   class ComplexOsc
		 *
		 */
		template <class sample_t>
		class ComplexOsc {
			private:
				Osc<sample_t> osc_i;
				Osc<sample_t> osc_q;

			public:
				ComplexOsc(
						double f0,
						double fs
						) : osc_i(f0, fs, 0), osc_q(f0, fs, M_PI / 2) {
					// nop
				};

				std::complex<sample_t> read(double error = 0) {
					sample_t real = osc_i.read(error);
					sample_t imag = osc_q.read(error);

					return std::complex<sample_t>(real, imag);
				}
		};
	}
}

#endif