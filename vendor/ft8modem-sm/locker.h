/*
 *
 *
 *    locker.h
 *
 *    Mutex and locker classes.
 *
 *    Copyright (C) 2023
 *    License: GNU GPL3
 *
 *    This is a subset of the my++ library.
 *    Copyright (C) 2000-2013,2017
 *
 *
 */

#ifndef __KK5JY_LOCKER_H
#define __KK5JY_LOCKER_H

#include <pthread.h>
#include <errno.h>   // for EBUSY

namespace my {
	//
	//   class mutex - Encapsulates a mutex for thread synchronization.
	//
	class mutex {
	private:
		pthread_mutex_t m_mutex;
		pthread_mutexattr_t m_attr;
		friend class condition;

	public:
		enum types {
		#ifdef PTHREAD_MUTEX_NORMAL
			normal         = PTHREAD_MUTEX_NORMAL,
			error_checking = PTHREAD_MUTEX_ERRORCHECK,
			recursive      = PTHREAD_MUTEX_RECURSIVE,
			default_type   = PTHREAD_MUTEX_DEFAULT
		#else
			fast           = PTHREAD_MUTEX_FAST_NP,
			recursive      = PTHREAD_MUTEX_RECURSIVE_NP,
			error_checking = PTHREAD_MUTEX_ERRORCHECK_NP,
			default_type   = fast
		#endif
		};

	public:
		explicit mutex(types type = default_type);
		~mutex(void);

		bool lock(void);
		bool tryLock(void);
		bool unlock(void);
	};

	// ctor
	inline my::mutex::mutex(mutex::types kind) {
		pthread_mutexattr_init(&m_attr);
		// Use the type argument to silence unused-parameter warning
		(void)kind;
		// If you want type support, uncomment:
		// pthread_mutexattr_settype(&m_attr, kind);
		pthread_mutex_init(&m_mutex, &m_attr);
	}

	// dtor
	inline my::mutex::~mutex(void) {
		pthread_mutexattr_destroy(&m_attr);
		pthread_mutex_destroy(&m_mutex);
	}

	inline bool my::mutex::lock(void) {
		return (pthread_mutex_lock(&m_mutex) == 0);
	}

	inline bool my::mutex::tryLock(void) {
		int rtn = pthread_mutex_trylock(&m_mutex);
		if (rtn == EBUSY) return false;
		return (rtn == 0);
	}

	inline bool my::mutex::unlock(void) {
		return (pthread_mutex_unlock(&m_mutex) == 0);
	}

	//
	//  class locker - scope-based locking class
	//
	class locker {
	private:
		my::mutex &m;
		bool locked;
	public:
		locker(my::mutex &_m, bool just_try = false) : m(_m) {
			locked = just_try ? m.tryLock() : m.lock();
		}
		~locker(void) { if (locked) m.unlock(); }

		bool isLocked(void) const { return locked; }
	};
}

#endif // __KK5JY_LOCKER_H