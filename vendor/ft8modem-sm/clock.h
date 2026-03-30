/*
 *
 *
 *    clock.h
 *
 *    Wall clock helper methods.
 *
 *    Copyright (C) 2023-2024
 *    License: GNU GPL3
 *
 *
 */

#ifndef __KK5JY_FT8_CLOCK_H
#define __KK5JY_FT8_CLOCK_H

#include <string>
#include <sys/time.h>
#include <math.h>
#include <cstdio>    // snprintf
#include <unistd.h>  // usleep

namespace KK5JY {
namespace FT8 {

	//
	//  abstime() - return absolute clock in seconds
	//
	inline double abstime() {
		// read the clock
		struct timeval tv;
		struct timezone tz;
		size_t tries = 0;
		const size_t limit = 20;
		while (tries++ < limit && ::gettimeofday(&tv, &tz) != 0) {
			::usleep(200); // usec
		}
		if (tries >= limit)
			return 0.0;

		double result = static_cast<double>(tv.tv_sec);
		result += static_cast<double>(tv.tv_usec) / 1'000'000.0;
		return result;
	}

	//
	//  timestring() - return current clock time as a string (Z)
	//
	inline std::string timestring(time_t t = 0) {
		int msec = -1;  // use int to avoid short precision warnings
		if (t == 0) {
			// read the clock
			struct timeval tv;
			struct timezone tz;
			::gettimeofday(&tv, &tz);
			t = tv.tv_sec;
			msec = static_cast<int>(tv.tv_usec / 1000);
		}

		tm now;
		gmtime_r(&t, &now);
		char buffer[24];
		if (msec >= 0)
			std::snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d.%03d",
			              now.tm_hour, now.tm_min, now.tm_sec, msec);
		else
			std::snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d",
			              now.tm_hour, now.tm_min, now.tm_sec);
		return std::string(buffer);
	}

	//
	//  class FrameClock
	//
	class FrameClock {
	private:
		// a known 'zero' seconds on the clock
		time_t m_epoch;

	public:
		// ctor
		FrameClock();

		// return the number of seconds in the current minute
		//    with microsecond precision
		double seconds(double mod, double fudge = 0.0) const volatile;

		// return the correction (explicit cast to silence -Wshorten-64-to-32 where used)
		int offset() const { return static_cast<int>(m_epoch); }
	};

	//
	//  FrameClock() ctor
	//
	inline FrameClock::FrameClock() {
		// read the clock
		struct timeval tv;
		struct timezone tz;
		::gettimeofday(&tv, &tz);

		// now convert that into HMS
		time_t tsec = tv.tv_sec;
		tm *hms = gmtime(&tsec);

		// figure out where 'zero' is relative to 'tv.tv_sec'
		tv.tv_sec -= hms->tm_sec;
		if (hms->tm_min % 2)
			tv.tv_sec -= 60;
		m_epoch = tv.tv_sec;
	}

	//
	//  FrameClock::seconds()
	//
	inline double FrameClock::seconds(double mod, double fudge) const volatile {
		// read the clock
		double now_sec = abstime() + fudge;
		double result = fmod(now_sec - m_epoch, mod);
		return result;
	}

} // namespace FT8
} // namespace KK5JY

#endif // __KK5JY_FT8_CLOCK_H