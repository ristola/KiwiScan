/*
 *
 *
 *    mfsk.h
 *
 *    General-purpose MFSK modulator.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __KK5JY_DSP_MFSK_H
#define __KK5JY_DSP_MFSK_H

#include <cstddef>   // size_t
#include <cstdint>
#include <cmath>     // std::llround
#include <string>
#include <limits>    // clamp cast

#include "IFilter.h"
#include "shape.h"
#include "osc.h"
#include "es.h"

// number of cycles to shape on each end
#ifndef KK5JY_MFSK_SHAPER_CYCLES
#  define KK5JY_MFSK_SHAPER_CYCLES (10)
#endif

namespace KK5JY {
namespace DSP {
namespace MFSK {

//
//  class Modulator<T>
//
template <typename T>
class Modulator {
private:
    // samples-per-bit ratio
    const size_t bit_ratio;

    const double m_fs;   // the sampling rate
    double m_f0;         // the lowest tone in the group
    double m_bps;        // transmitted symbol rate (per second)
    double m_shift;      // frequency shift between adjacent tones
    std::string m_msg;   // the message symbols
    size_t m_idx;        // current symbol index
    size_t m_ctr;        // number of samples emitted for current symbol
    size_t m_lead;       // how much silence to emit up front (samples)
    size_t m_leadctr;    // lead-in counter
    T m_Volume;          // output volume (normalized)

    // the oscillator
    Osc<T> m_osc;

    // the shift-shaper; smooths transition between symbols, which
    // reduces keying artifacts, improving spectral purity
    IFilter<T>* m_lpf;

    // the envelope shaper; used to generate ramp-in and -out; this
    // eliminates key-clicks at the start and stop of envelope
    Shaper<T> m_A;

    // text enqueued so far
    std::string m_Text;

public:
    Modulator(
        double fs,      // sampling frequency
        double f0,      // lowest tone frequency
        double bps,     // symbols per second
        double shift,   // shift between adjacent tones
        IFilter<T>* lpf = nullptr); // the keying filter/shaper

public:
    // send a message; optionally change the lowest tone frequency
    void transmit(const std::string& message, double f0 = 0);

    // reset the modulator state
    void clear();

    // reads out the encode wave data
    size_t read(T* buffer, size_t count);

    // set the lead-in silence (samples)
    size_t setLead(size_t newVal) { return (m_lead = newVal); }

    // get the lead-in silence (samples)
    size_t getLead(void) const { return m_lead; }

    // set the volume (normalized)
    T setVolume(T newVal) { return (m_Volume = newVal); }

    // get the volume (normalized)
    T getVolume(void) const { return m_Volume; }

    // get the center frequency
    double getFreq(void) const { return m_f0; }

    // get the text sent so far
    std::string getText(void) const { return m_Text; }
};

//
//  Modulator<T>::Modulator
//
template <typename T>
inline Modulator<T>::Modulator(double fs, double f0, double bps, double shift, IFilter<T>* lpf)
    : bit_ratio([&]{
          const double denom = (bps != 0.0) ? bps : 1.0;
          long long br = std::llround(fs / denom);
          if (br < 1) br = 1;
          return static_cast<size_t>(br);
      }()),
      m_fs(fs),
      m_f0(f0),
      m_bps(bps),
      m_shift(shift),
      m_osc(f0, fs),
      m_lpf(nullptr),
      m_A(static_cast<uint16_t>(std::min<size_t>(
              static_cast<size_t>(KK5JY_MFSK_SHAPER_CYCLES * (fs / (f0 != 0.0 ? f0 : 1.0)) ),
              static_cast<size_t>(std::numeric_limits<uint16_t>::max())
          )))
{
    if (lpf) {
        m_lpf = lpf;
    } else {
        m_lpf = new EmptyFilter<T>;
    }

    // set lead time and volume
    m_lead = static_cast<size_t>(m_fs / 8.0);         // 0.125 s
    m_Volume = static_cast<T>(0.9);                   // 90%

    // set other stuff to known-good state
    clear();
}

//
//  Modulator<T>::transmit(...)
//
template <typename T>
inline void Modulator<T>::transmit(const std::string& message, double f0) {
    // if base frequency provided, use it; otherwise keep previous frequency
    if (f0 > 0) {
        m_f0 = f0;
    }

    // remove out-of-range chars from input (but still allow digits & up to 67 symbols)
    std::string clean;
    clean.reserve(message.size());
    for (char ch : message) {
        if (ch >= '0' && ch <= ('0' + 66)) // TODO: make this range an option
            clean += ch;
    }
    m_msg = clean;
    if (m_ctr == 0)
        m_leadctr = 0;
    m_Text += message;
}

//
//  Modulator<T>::clear()
//
template <typename T>
inline void Modulator<T>::clear() {
    m_Text.clear();
    m_msg.clear();
    m_idx = 0;
    m_ctr = 0;
    m_leadctr = 0;
}

//
//  Modulator<T>::read(...)
//
template <typename T>
inline size_t Modulator<T>::read(T* buffer, size_t count) {
    if (m_msg.empty())
        return 0;

    size_t samples = 0;

    // lead-in silence generation
    while (m_leadctr < m_lead && samples < count) {
        *buffer++ = static_cast<T>(0);
        ++m_leadctr;
        ++samples;
    }

    // if the lead-in silence consumed the whole buffer, quit
    if (samples == count)
        return count;

    // fetch the current symbol
    char ch = m_msg[m_idx];

    // calculate current output frequency
    double f = m_f0 + (m_shift * (static_cast<int>(ch) - '0'));

    // main modulator loop
    do {
        // compute the unfiltered envelope value
        bool lead_out =
            (m_idx == (m_msg.size() - 1)) &&
            (m_ctr >= static_cast<size_t>(bit_ratio - m_A.size()));

        // Gate on except during lead-out
        const bool gate = !lead_out;

        // generate the waveform; add lead-in/out where needed
        m_osc.setFreq(m_lpf->run(static_cast<T>(f)), m_fs);
        *buffer++ = static_cast<T>(m_osc.read() * m_Volume * m_A.run(gate));
        //                     ^ LO ^         *  ^vol^      *   ^env^

        // if time for the next symbol...
        if (++m_ctr == bit_ratio) {
            m_ctr = 0;

            // if everything sent, stop now
            if (++m_idx == m_msg.size()) {
                clear();
                return samples + 1;
            }

            // fetch next character and compute the new frequency
            ch = m_msg[m_idx];
            f = m_f0 + (m_shift * (static_cast<int>(ch) - '0'));
        }
    } while (++samples != count);

    // return the number of samples written to 'buffer'
    return samples;
}

} // namespace MFSK
} // namespace DSP
} // namespace KK5JY

#endif // __KK5JY_DSP_MFSK_H