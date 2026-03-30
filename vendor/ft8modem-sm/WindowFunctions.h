/*
 *
 *
 *    WindowFunctions.h
 *
 *    Window functions.
 *
 *    Copyright (C) 2013-2021 by Matt Roberts, KK5JY.
 *    All rights reserved.
 *
 *    License: GNU GPL3 (www.gnu.org)
 *    
 *
 */

#ifndef KK5JY_WINDOWFUNCTIONS_H
#define KK5JY_WINDOWFUNCTIONS_H

#include <cmath>

namespace KK5JY {
	namespace DSP {
		/// <summary>
		/// Generate Hamming coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double HammingWindow(int n, int N) {
			return 0.54 - (0.46 * std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate Hann coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double HannWindow(int n, int N) {
			return 0.50 * (1.0 - std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate "exact" Blackman coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double BlackmanExactWindow(int n, int N) {
			return (7938.0 / 18608.0)
				- ((9240.0 / 18608.0) * std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)))
				+ ((1430.0 / 18608.0) * std::cos((4.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate Blackman coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double BlackmanWindow(int n, int N) {
			return 0.426590
				- (0.496560 * std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)))
				+ (0.076849 * std::cos((4.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate Nuttall coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double NuttallWindow(int n, int N) {
			return 0.355768
				- (0.487396 * std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)))
				+ (0.144232 * std::cos((4.0 * M_PI * (n + (N / 2))) / (N - 1)))
				- (0.012604 * std::cos((6.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate Blackman-Nuttall coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double BlackmanNuttallWindow(int n, int N) {
			return 0.3635819
				- (0.4891775 * std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)))
				+ (0.1365995 * std::cos((4.0 * M_PI * (n + (N / 2))) / (N - 1)))
				- (0.0106511 * std::cos((6.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate Blackman-Harris coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double BlackmanHarrisWindow(int n, int N) {
			return 0.35875
				- (0.48829 * std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)))
				+ (0.14128 * std::cos((4.0 * M_PI * (n + (N / 2))) / (N - 1)))
				- (0.01168 * std::cos((6.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate flat-top coefficients for an odd-length window centered at zero, of length N.
		/// </summary>
		inline double FlatTopWindow(int n, int N) {
			return 1.0
				- (1.93 * std::cos((2.0 * M_PI * (n + (N / 2))) / (N - 1)))
				+ (1.29 * std::cos((4.0 * M_PI * (n + (N / 2))) / (N - 1)))
				- (0.388 * std::cos((6.0 * M_PI * (n + (N / 2))) / (N - 1)))
				+ (0.028 * std::cos((6.0 * M_PI * (n + (N / 2))) / (N - 1)));
		}

		/// <summary>
		/// Generate rectangular window coefficients for an odd-length window centered at zero, of length N.
		/// Parameters are intentionally unnamed to avoid unused-parameter warnings.
		/// </summary>
		inline double RectangleWindow(int, int) {
			return 1.0;
		}
	}
}

#endif // KK5JY_WINDOWFUNCTIONS_H