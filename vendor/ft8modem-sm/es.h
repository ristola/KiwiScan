/*
 *
 *
 *    es.h
 *
 *    Exponential smoothers.
 *
 *    Copyright (C) 2017-2021 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __KK5JY_ES_H
#define __KK5JY_ES_H

#include <cmath>
#include <cstdint>
#include <type_traits>
#include "nlimits.h"
#include "IFilter.h"

namespace KK5JY {
namespace DSP {

// ---- Alpha<...> helpers ----------------------------------------------------
// Scale floating alpha [0,1] to fixed-point domain of sample type.
template<typename T> struct AlphaType { using type = double; };     // float/double path
template<> struct AlphaType<int16_t> { using type = int32_t; };     // Q15 domain
template<> struct AlphaType<int32_t> { using type = int64_t; };     // Q31 domain

template<typename T>
inline typename AlphaType<T>::type to_fixed_alpha(double a) {
    // clamp a to [0,1] then scale to T's max and round to nearest.
    if (a < 0.0) a = 0.0;
    if (a > 1.0) a = 1.0;
    using A = typename AlphaType<T>::type;
    const double k = static_cast<double>(norm_limits<T>::maximum);
    if constexpr (std::is_same_v<A,double>) {
        return a; // floating path (no scaling)
    } else if constexpr (std::is_same_v<A,int32_t>) {
        return static_cast<int32_t>(std::lround(a * k));
    } else { // int64_t
        return static_cast<int64_t>(std::llround(a * k));
    }
}

// ---- RC conversion helpers --------------------------------------------------

inline double HighpassToAlpha(size_t fs, double fc) {
    const double rc = 1.0 / (2.0 * M_PI * fc);
    const double dt = 1.0 / static_cast<double>(fs);
    return rc / (rc + dt);
}

inline double AlphaToHighpass(double alpha, size_t fs) {
    const double dt = 1.0 / static_cast<double>(fs);
    const double rc = (dt * (1.0 - alpha)) / alpha;
    return 1.0 / (2.0 * M_PI * rc);
}

inline double LowpassToAlpha(size_t fs, double fc) {
    const double rc = 1.0 / (2.0 * M_PI * fc);
    const double dt = 1.0 / static_cast<double>(fs);
    return dt / (rc + dt);
}

inline double AlphaToLowpass(double alpha, size_t fs) {
    const double dt = 1.0 / static_cast<double>(fs);
    const double rc = (dt * (1.0 - alpha)) / alpha;
    return 1.0 / (2.0 * M_PI * rc);
}

// ---------------------------------------------------------------------------
//
//   class Smoother<T>  (LPF)
//
// ---------------------------------------------------------------------------
template <typename T>
class Smoother : public IFilter<T> {
public:
    const typename AlphaType<T>::type Alpha;  // float: [0..1], int: fixed-point
private:
    T state;
public:
    Smoother(double alpha, T seed = 0);
    T run(T in);
    void clear(void) { state = static_cast<T>(0); }
    T value() const { return state; }
    void add(T s) { run(s); }
};

// generic (floating) ctor
template <typename T>
inline Smoother<T>::Smoother(double alpha, T seed)
    : Alpha(to_fixed_alpha<T>(alpha)), state(seed) {}

// run() for floating types
template <typename T>
inline T Smoother<T>::run(T in) {
    if constexpr (std::is_floating_point_v<T>) {
        const double a = static_cast<double>(Alpha);
        return state = static_cast<T>( (static_cast<double>(in) * a) +
                                       (static_cast<double>(state) * (1.0 - a)) );
    } else {
        // int16_t / int32_t specializations are below
        return state;
    }
}

// int16 specialization
template <>
inline int16_t Smoother<int16_t>::run(int16_t in) {
    // Q15 math
    const int32_t a  = Alpha;                            // 0..32767
    const int32_t na = norm_limits<int16_t>::maximum - a;
    const int32_t acc =
        ((static_cast<int32_t>(in)    * a)  >> 15) +
        ((static_cast<int32_t>(state) * na) >> 15);
    return state = static_cast<int16_t>(acc);
}

// int32 specialization
template <>
inline int32_t Smoother<int32_t>::run(int32_t in) {
    // Q31 math
    const int64_t a  = Alpha;                            // 0..2^31-1
    const int64_t na = static_cast<int64_t>(norm_limits<int32_t>::maximum) - a;
    const int64_t acc =
        ((static_cast<int64_t>(in)    * a)  >> 31) +
        ((static_cast<int64_t>(state) * na) >> 31);
    return state = static_cast<int32_t>(acc);
}

// ---------------------------------------------------------------------------
//
//   class Desmoother<T>  (HPF first-order)
//
// ---------------------------------------------------------------------------
template <typename T>
class Desmoother : public IFilter<T> {
public:
    const typename AlphaType<T>::type Alpha;
private:
    T x1, y1;
public:
    Desmoother(double alpha, T seed = 0);
    T run(T in);
    void clear() { x1 = y1 = static_cast<T>(0); }
    void add(T s) { run(s); }
    T value() const { return y1; }
};

// generic (floating) ctor
template <typename T>
inline Desmoother<T>::Desmoother(double alpha, T seed)
    : Alpha(to_fixed_alpha<T>(alpha)), x1(seed), y1(seed) {}

// run() for floating types
template <typename T>
inline T Desmoother<T>::run(T x0) {
    if constexpr (std::is_floating_point_v<T>) {
        const double a = static_cast<double>(Alpha);
        y1 = static_cast<T>( (a * static_cast<double>(y1)) +
                             (a * (static_cast<double>(x0) - static_cast<double>(x1))) );
        x1 = x0;
        return y1;
    } else {
        return y1; // specializations below
    }
}

// int16 specialization
template <>
inline int16_t Desmoother<int16_t>::run(int16_t x0) {
    const int32_t a = Alpha; // Q15
    y1 = static_cast<int16_t>( ((static_cast<int32_t>(a) * static_cast<int32_t>(y1)) >> 15) +
                               ((static_cast<int32_t>(a) * static_cast<int32_t>(x0 - x1)) >> 15) );
    x1 = x0;
    return y1;
}

// int32 specialization
template <>
inline int32_t Desmoother<int32_t>::run(int32_t x0) {
    const int64_t a = Alpha; // Q31
    const int64_t acc =
        ((a * static_cast<int64_t>(y1)) >> 31) +
        ((a * static_cast<int64_t>(x0 - x1)) >> 31);
    y1 = static_cast<int32_t>(acc);
    x1 = x0;
    return y1;
}

// ---------------------------------------------------------------------------
//
//   class Decay<T>  (fast-rise, smoothed decay)
//
// ---------------------------------------------------------------------------
template <typename T>
class Decay : public IFilter<T> {
public:
    const typename AlphaType<T>::type Alpha;
private:
    T state;
public:
    Decay(double alpha, T seed = 0);
    T run(T in);
    T value() const { return state; }
    void add(T in) { run(in); }
    void clear() { state = static_cast<T>(0); }
};

// generic (floating) ctor
template <typename T>
inline Decay<T>::Decay(double alpha, T seed)
    : Alpha(to_fixed_alpha<T>(alpha)), state(seed) {}

// run() for floating types
template <typename T>
inline T Decay<T>::run(T in) {
    if (in >= state) return state = in;
    if constexpr (std::is_floating_point_v<T>) {
        const double a = static_cast<double>(Alpha);
        return state = static_cast<T>( (static_cast<double>(in) * a) +
                                       (static_cast<double>(state) * (1.0 - a)) );
    } else {
        return state; // specializations below
    }
}

// int16 specialization
template <>
inline int16_t Decay<int16_t>::run(int16_t in) {
    if (in >= state) return state = in;
    const int32_t a  = Alpha; // Q15
    const int32_t na = norm_limits<int16_t>::maximum - a;
    const int32_t acc =
        ((static_cast<int32_t>(in)    * a)  >> 15) +
        ((static_cast<int32_t>(state) * na) >> 15);
    return state = static_cast<int16_t>(acc);
}

// int32 specialization
template <>
inline int32_t Decay<int32_t>::run(int32_t in) {
    if (in >= state) return state = in;
    const int64_t a  = Alpha; // Q31
    const int64_t na = static_cast<int64_t>(norm_limits<int32_t>::maximum) - a;
    const int64_t acc =
        ((static_cast<int64_t>(in)    * a)  >> 31) +
        ((static_cast<int64_t>(state) * na) >> 31);
    return state = static_cast<int32_t>(acc);
}

} // namespace DSP
} // namespace KK5JY

#endif // __KK5JY_ES_H