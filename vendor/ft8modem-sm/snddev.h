/*
 *    snddev.h
 *    Sound interface and decoding framework.
 *    Core of 'ft8modem'.
 *    Copyright (C) 2023-2024
 *    License: GNU GPL3
 */

#ifndef __KK5JY_FT8_SNDDEV_H
#define __KK5JY_FT8_SNDDEV_H

#include <ctype.h>
#include <atomic>
#include <string>

#include "sc.h"
#include "mfsk.h"
#include "decode.h"
#include "encode.h"
#include "clock.h"
#include "FirFilter.h"
#include "locker.h"
#include "decimate.h"
#include "presets.h"

enum TimeSlots {
     NextSlot = 0,
     OddSlot  = 1,
     EvenSlot = 2
};

class ModemSoundDevice : public SoundCard {
private:
     const int minimum_db = -144;
     const int dec_cutoff = 4500;
     const int dec_taps   = 25;

private:
     KK5JY::FT8::FrameClock m_Clock;
     KK5JY::DSP::FirFilter<float> *m_Filter;
     KK5JY::DSP::IDecimator<float> *m_Decimator;

     KK5JY::FT8::Decode<float> *m_Current;
     KK5JY::FT8::Decode<float> *m_Decoding;

     KK5JY::DSP::MFSK::Modulator<float> *m_MFSK;

     std::string m_TempDir, m_Mode, m_LastSent, m_Decoder, m_Extension;
     KK5JY::FT8::ShaperTypes m_Shaper;
     size_t m_Rate, m_Lead;
     double m_FrameStart, m_FrameEnd, m_FrameSize, m_TxWinStart, m_TxWinEnd;
     double m_Trail, m_Fudge, m_bps, m_shift;
     short m_Depth, m_Threads, m_TrimIdx;
     float m_Volume;
     char m_ModeCode;
     volatile char m_NewMode;
     volatile float m_MaxInput;
     volatile uint64_t m_Intervals;
     volatile bool m_Keep;
     volatile double m_Start;
     volatile bool m_Sending, m_Active, m_Abort, m_Purge, m_Loop;
     float *m_LoopBuffer;
     float *m_NetBuffer;
     size_t m_NetSize;
     int m_warndb;

     // NEW: track samples written in the current capture slot and the last WAV path
     size_t      m_SamplesThisSlot;
     std::string m_LastWav;

     enum TimeSlots m_Slot;

     my::mutex m_Mutex;

     std::atomic<double> _lastRmsDb{-200.0};

private:
     size_t decimate_buffer(float *input, size_t count, float *output);
     void update_volume(float *input, size_t count);

public:
     ModemSoundDevice(const std::string &mode, size_t id, size_t rate, size_t win, const std::string &ext = "", const std::string &tempRoot = "");
     ~ModemSoundDevice();

     void run();
     bool transmit(const std::string &message, double f0, TimeSlots slot = NextSlot);
     bool cancelTransmit(void);

     using SoundCard::start;
     bool start() override;       // start capture+playback plumbing

     void stop() override;
     bool isActive(void) const volatile { return m_Active; }

     void udp_audio(uint8_t *raw_data, size_t bytes);

     // ----- settings & getters -----
     std::string setDecoder(std::string d) { return (m_Decoder = d); }
     std::string getDecoder(void) const { return m_Decoder; }
     std::string getExtension(void) const { return m_Extension; }
     KK5JY::FT8::ShaperTypes setShaper(KK5JY::FT8::ShaperTypes m) { return (m_Shaper = m); }
     KK5JY::FT8::ShaperTypes getShaper(void) const { return m_Shaper; }
     short setThreads(short t) { return (m_Threads = t); }
     short getThreads(void) const { return m_Threads; }
     bool setMonitor(bool m) { return (m_Loop = m); }
     bool getMonitor(void) const { return m_Loop; }
     short setDepth(short depth);
     short getDepth(void) const { return m_Depth; }
     size_t setLead(size_t newVal) { return (m_Lead = newVal); }
     size_t getLead(void) const { return m_Lead; }
     double setTrail(double newVal);
     double getTrail(void) const { return m_Trail; }
     double setFudge(double newVal) { return (m_Fudge = newVal); }
     double getFudge(void) const { return m_Fudge; }
     float setVolume(float newVal) { return (m_Volume = newVal); }
     float getVolume(void) const { return m_Volume; }
     std::string setTemp(const std::string &s);
     std::string getTemp() const { return m_TempDir; }
     bool setKeep(bool keep) { return (m_Keep = keep); }
     bool getKeep() const { return m_Keep; }
     int setWarndB(int db) { return (m_warndb = db); }
     int getWarndB() { return m_warndb; }
     void setFilter(int taps);

     // last input RMS in dBFS
     double lastRmsDb() const;

     // used by ft8modem.cc
     void purge(void) { m_Purge = true; }

protected:
     void event(float *in, float *out, size_t count) override;
};

inline double ModemSoundDevice::setTrail(double newVal) {
     const double maxVal = m_FrameStart - (m_FrameEnd + 0.25);
     if (newVal > maxVal) newVal = maxVal;
     return (m_Trail = newVal);
}

inline short ModemSoundDevice::setDepth(short depth) {
     if (depth >= 1 && depth <= 3) { m_Depth = depth; }
     return m_Depth;
}

#endif // __KK5JY_FT8_SNDDEV_H