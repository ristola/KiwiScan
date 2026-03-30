/*
 *
 *
 *    presets.h
 *
 *    MFSK presets for different modes and keying filter types.
 *
 *    These only affect transmitted audio (the modulator).
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __KK5JY_FT8_PRESETS
#define __KK5JY_FT8_PRESETS

#include <stdexcept>   // for std::runtime_error

#include "stype.h"
#include "shape.h"
#include "es.h"

namespace KK5JY {
namespace FT8 {

// bit rates for the different modes
static constexpr double bps_ft8  = 6.25;                 // bps
static constexpr double bps_ft4  = 12000.0 / 576.0;      // ~20.833 bps (original working value)
static constexpr double bps_jt65 = 11025.0 / 4096.0;     // ~2.692 bps
static constexpr double bps_jt9  = 12000.0 / 6912.0;     // ~1.736 bps
static constexpr double bps_wspr = 12000.0 / 8192.0;     // ~1.4648 bps

// enumeration of modulator filters
typedef enum {
    DefaultShaper       = 0,
    ExponentialSmoother = 1,
    RaisedCosine        = 2
} ShaperTypes;

//
//  GetShaperRC(...) - get bit-shaper using RC filter
//
template <typename sample_t>
inline IFilter<sample_t>* GetShaperRC(const std::string& mode, size_t fs, double k = 1.0) {
    // preferred values (can be modified by 'k')
    const double k_ft8  = 1.5;   // * bps = cutoff freq
    const double k_ft4  = 0.825; // * bps = cutoff freq
    const double k_jt65 = 1.0;   // * bps = cutoff freq (TODO: tune this)
    const double k_jt9  = 1.0;   // * bps = cutoff freq (TODO: tune this)
    const double k_wspr = 1.0;   // * bps = cutoff freq (TODO: tune this)

    const std::string umode = my::toUpper(mode);

    // return the RC-simulated filter based on mode
    double alpha = 0.0;
    if (umode == "FT8") {
        alpha = KK5JY::DSP::LowpassToAlpha(fs, k_ft8 * bps_ft8 * k);
    } else if (umode == "FT4") {
        alpha = KK5JY::DSP::LowpassToAlpha(fs, k_ft4 * bps_ft4 * k);
    } else if (umode == "JT65") {
        alpha = KK5JY::DSP::LowpassToAlpha(fs, k_jt65 * bps_jt65 * k);
    } else if (umode == "JT9") {
        alpha = KK5JY::DSP::LowpassToAlpha(fs, k_jt9 * bps_jt9 * k);
    } else if (umode == "WSPR") {
        alpha = KK5JY::DSP::LowpassToAlpha(fs, k_wspr * bps_wspr * k);
    } else {
        throw std::runtime_error("Invalid mode: " + mode);
    }

    // return new filter
    return new KK5JY::DSP::Smoother<sample_t>(alpha);
}

//
//  GetShaperCosine(...) - get bit-shaper using raised-cosine
//
template <typename sample_t>
inline IFilter<sample_t>* GetShaperCosine(const std::string& mode, size_t fs, double k = 1.0) {
    // preferred values (can be modified by 'k')
    const double k_ft8  = 2.75; // msec
    const double k_ft4  = 4.0;  // msec
    const double k_jt65 = 8.0;  // msec
    const double k_jt9  = 22.5; // msec
    const double k_wspr = 5.0;  // msec (TODO: tune this)

    const std::string umode = my::toUpper(mode);

    // calculate N (and max_N, as a check value)
    size_t N = 0;
    size_t max_N = 0;
    if (umode == "FT8") {
        N     = static_cast<size_t>((k * k_ft8  * static_cast<double>(fs)) / 1000.0);
        max_N = static_cast<size_t>(static_cast<double>(fs) / bps_ft8 / 2.0);
    } else if (umode == "FT4") {
        N     = static_cast<size_t>((k * k_ft4  * static_cast<double>(fs)) / 1000.0);
        max_N = static_cast<size_t>(static_cast<double>(fs) / bps_ft4 / 2.0);
    } else if (umode == "JT65") {
        N     = static_cast<size_t>((k * k_jt65 * static_cast<double>(fs)) / 1000.0);
        max_N = static_cast<size_t>(static_cast<double>(fs) / bps_jt65 / 2.0);
    } else if (umode == "JT9") {
        N     = static_cast<size_t>((k * k_jt9  * static_cast<double>(fs)) / 1000.0);
        max_N = static_cast<size_t>(static_cast<double>(fs) / bps_jt9 / 2.0);
    } else if (umode == "WSPR") {
        N     = static_cast<size_t>((k * k_wspr * static_cast<double>(fs)) / 1000.0);
        max_N = static_cast<size_t>(static_cast<double>(fs) / bps_wspr / 2.0);
    } else {
        throw std::runtime_error("Invalid mode: " + mode);
    }

    // sanity checks
    if (N == 0)
        throw std::runtime_error("N must be > 0");
    if (N >= max_N)
        throw std::runtime_error("N must be < (bps / 2)");

    // return a raised-cosine keying filter
    return new KK5JY::DSP::KeyShaper<sample_t>(N);
}

//
//  GetPreferredShaper(...) - get best bit-shaper
//
template <typename sample_t>
inline IFilter<sample_t>* GetPreferredShaper(const std::string& mode, size_t fs, double k = 1.0) {
    return GetShaperCosine<sample_t>(mode, fs, k);
}

} // namespace FT8
} // namespace KK5JY

#endif // __KK5JY_FT8_PRESETS