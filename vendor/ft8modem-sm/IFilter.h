/*
 *
 *
 *    IFilter.h
 *
 *    Filter interface.
 *
 *    Copyright (C) 2016-2021 by Matt Roberts,
 *    All rights reserved.
 *
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __KK5JY_IFILTER_H
#define __KK5JY_IFILTER_H

//
//  filter interface
//
template <typename sample_t>
class IFilter {
	public:
		// run the filter and return the updated value of the filter
		virtual sample_t run(const sample_t sample) = 0;

		// update the the filter, but don't return a new output value;
		//    this is helpful if calculating the return value is costly
		virtual void add(const sample_t sample) = 0;

		// return the current value of the filter without changing it
		virtual sample_t value() const = 0;

		// erase the history in the filter
		virtual void clear() = 0;

		// virtual dtor
		virtual ~IFilter() { /* nop */ };
};


//
//  an empty filter
//
template <typename sample_t>
class EmptyFilter : public IFilter<sample_t> {
	private:
		sample_t m_value;

	public:
		// run the filter and return the updated value of the filter
		sample_t run(const sample_t sample) { return (m_value = sample); }

		// update the the filter, but don't return a new output value;
		//    this is helpful if calculating the return value is costly
		void add(const sample_t sample) { m_value = sample; }

		// return the current value of the filter without changing it
		sample_t value() const { return m_value; }

		// erase the history in the filter
		void clear() { m_value = 0; }
};

#endif // __KK5JY_IFILTER_H
