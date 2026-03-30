/*
 *
 *
 *    cores.h
 *
 *    Read the number of CPU cores.
 *
 *    Copyright (C) 2024
 *    License: GNU GPL3
 *
 *
 */

#ifndef __KK5JY_FT8_CORES_H
#define __KK5JY_FT8_CORES_H

#include <fstream>
#include <thread>
#include <string>
#include <cctype>
#include <limits>
#include "stype.h"

namespace KK5JY {
namespace DSP {

	//
	//  cpu_cores() - return the number of physical CPU cores if available,
	//                otherwise the number of logical cores.
	//
	inline size_t cpu_cores() {
		// Try Linux /proc/cpuinfo first
		{
			const std::string pattern1("cpu cores");
			std::ifstream cpuinfo("/proc/cpuinfo");
			if (cpuinfo) {
				// First pass: look for a "cpu cores" entry (physical cores per socket)
				std::string line;
				while (std::getline(cpuinfo, line)) {
					if (line.rfind(pattern1, 0) == 0) {
						const auto idx = line.find(':');
						if (idx != std::string::npos) {
							std::string token = my::strip(line.substr(idx + 1));
							try {
								size_t val = std::stoul(token);
								if (val > 0) return val;
							} catch (...) {
								// ignore parse errors, fall through
							}
						}
					}
				}
			}
		}

		// Second pass: count "processor" lines (logical CPUs) on Linux
		{
			const std::string pattern2("processor");
			std::ifstream cpuinfo("/proc/cpuinfo");
			if (cpuinfo) {
				size_t count = 0;
				std::string line;
				while (std::getline(cpuinfo, line)) {
					if (line.size() >= pattern2.size() + 1 &&
					    line.rfind(pattern2, 0) == 0 &&
					    std::isspace(static_cast<unsigned char>(line[pattern2.size()]))) {
						++count;
					}
				}
				if (count > 0) return count;
			}
		}

		// Portable fallback: logical cores
		const unsigned hc = std::thread::hardware_concurrency();
		if (hc > 0) return static_cast<size_t>(hc);

		// Last resort
		return 0;
	}

} // namespace DSP
} // namespace KK5JY

#endif // __KK5JY_FT8_CORES_H