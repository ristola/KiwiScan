#ifndef KK5JY_SC_H
#define KK5JY_SC_H

#include <atomic>
#include <vector>
#include <cstring>
#include <cmath>
#include <chrono>
#include <thread>
#include <algorithm>
#include <memory>

#if __has_include(<rtaudio/RtAudio.h>)
    #include <rtaudio/RtAudio.h>
#elif __has_include("rtaudio/RtAudio.h")
    #include "rtaudio/RtAudio.h"
#elif __has_include(<RtAudio.h>)
  #include <RtAudio.h>
#else
  #error "RtAudio.h not found"
#endif

#include "pipes.h"

// Thin wrapper around RtAudio that provides:
// - Safe shutdown (avoid use-after-free in callback)
// - Mono in/out scratch buffers, auto (de)interleave
// - Robust CoreAudio open with duplex -> input-only -> output-only fallback
class SoundCard {
public:
    // Back-compat ctor: same channels for input & output
    inline SoundCard(unsigned id, unsigned rate, unsigned short channels, unsigned int win)
    : m_Id(id), m_Rate(rate), m_InChReq(channels), m_OutChReq(channels), m_BufferFrames(win),
      m_InputOpen(false), m_OutputOpen(false), m_Active(false), m_ShuttingDown(false) {}

    // Separate input/output channel counts
    inline SoundCard(unsigned id, unsigned rate,
                     unsigned short inChannels,
                     unsigned short outChannels,
                     unsigned int win)
    : m_Id(id), m_Rate(rate), m_InChReq(inChannels), m_OutChReq(outChannels), m_BufferFrames(win),
      m_InputOpen(false), m_OutputOpen(false), m_Active(false), m_ShuttingDown(false) {}

    virtual ~SoundCard() { stop(); }

    inline unsigned card() const { return m_Id; }

    // Lifecycle expected by snddev.{h,cc}
    virtual bool start() { return startRecording(); }

    // Prefer full-duplex; gracefully degrade if device can't
    virtual bool startRecording() {
        safeClose();

        if (!openBestStream()) return false;

        try {
            if (m_Audio && !m_Audio->isStreamRunning()) m_Audio->startStream();
            return true;
        } catch (...) {
            KK5JY::FT8::send_error("RtAudio startStream failed");
            return false;
        }
    }

    virtual bool startPlayback() { return startRecording(); }

    // Safe stop/close that coordinates with the realtime callback
    virtual void stop() {
        // Tell callback to exit ASAP
        m_ShuttingDown.store(true, std::memory_order_release);

        // Ask RtAudio to stop
        try {
            if (m_Audio && m_Audio->isStreamRunning()) m_Audio->stopStream();
        } catch (...) {}

        // Give CoreAudio thread a moment to unwind (max ~500ms)
        for (int i = 0; i < 50; ++i) {
            if (!m_Audio || !m_Audio->isStreamRunning()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        // Close the stream
        try {
            if (m_Audio && m_Audio->isStreamOpen()) m_Audio->closeStream();
        } catch (...) {}

        // Clear state/buffers
        m_InputOpen.store(false,  std::memory_order_relaxed);
        m_OutputOpen.store(false, std::memory_order_relaxed);
        m_Active.store(false,     std::memory_order_relaxed);
        m_InScratch.clear();
        m_OutScratch.clear();

        // Allow future start()
        m_ShuttingDown.store(false, std::memory_order_release);
    }

    // App must implement this (mono in/out)
    virtual void event(float *inBuffer, float *outBuffer, size_t samples) = 0;

protected:
    // RtAudio callback trampoline
    static int rtCallback(void *outputBuffer, void *inputBuffer,
                          unsigned int nFrames, double /*streamTime*/,
                          RtAudioStreamStatus /*status*/, void *userData)
    {
        SoundCard *self = static_cast<SoundCard*>(userData);

        // During shutdown, output silence and request stop
        if (self->m_ShuttingDown.load(std::memory_order_acquire)) {
            if (outputBuffer && self->m_OutChAct >= 1)
                std::memset(outputBuffer, 0, sizeof(float) * nFrames * self->m_OutChAct);
            return 1; // non-zero → RtAudio stops the stream
        }

        float *inF  = nullptr;
        float *outF = nullptr;

        // Deinterleave first input channel to mono scratch
        if (inputBuffer && self->m_InChAct >= 1) {
            if (self->m_InScratch.size() < nFrames) self->m_InScratch.resize(nFrames);
            const float *inInter = static_cast<const float*>(inputBuffer);
            float *dst = self->m_InScratch.data();
            for (unsigned i = 0; i < nFrames; ++i)
                dst[i] = inInter[i * self->m_InChAct + 0];
            inF = self->m_InScratch.data();
        }

        // Prepare mono output scratch for app to fill
        if (outputBuffer && self->m_OutChAct >= 1) {
            if (self->m_OutScratch.size() < nFrames) self->m_OutScratch.resize(nFrames);
            std::memset(outputBuffer, 0, sizeof(float) * nFrames * self->m_OutChAct);
            outF = self->m_OutScratch.data();
        }

        // Application callback (mono)
        self->event(inF, outF, nFrames);

        // Copy mono scratch to device channels
        if (outputBuffer && self->m_OutChAct >= 1 && outF) {
            float *outInter = static_cast<float*>(outputBuffer);
            if (self->m_OutChAct == 1) {
                std::memcpy(outInter, outF, sizeof(float) * nFrames);
            } else {
                for (unsigned i = 0; i < nFrames; ++i) {
                    float s = outF[i];
                    for (unsigned ch = 0; ch < self->m_OutChAct; ++ch)
                        outInter[i * self->m_OutChAct + ch] = s;
                }
            }
        }

        if (nFrames) self->m_Active.store(true, std::memory_order_relaxed);
        return 0;
    }

    // Try duplex; on failure, try input-only; then output-only
    inline bool openBestStream() {
        if (!m_Audio) m_Audio = std::make_unique<RtAudio>();

        // Probe device capabilities
        RtAudio::DeviceInfo info;
        try {
            info = m_Audio->getDeviceInfo(m_Id);
        } catch (...) {
            KK5JY::FT8::send_error("RtAudio getDeviceInfo failed");
            return false;
        }

        // Desired (mono if available), clamped to device capability
        unsigned inWant  = std::min<unsigned>(m_InChReq  ? m_InChReq  : 1, (unsigned)std::max(0u, (unsigned)info.inputChannels));
        unsigned outWant = std::min<unsigned>(m_OutChReq ? m_OutChReq : 1, (unsigned)std::max(0u, (unsigned)info.outputChannels));

        // Helper to attempt an open with provided in/out counts
        auto tryOpen = [&](unsigned inCh, unsigned outCh) -> bool {
            RtAudio::StreamParameters inParams{}, outParams{};
            RtAudio::StreamParameters *inPtr  = nullptr;
            RtAudio::StreamParameters *outPtr = nullptr;

            if (inCh > 0)  { inParams.deviceId = m_Id;  inParams.nChannels = inCh;  inParams.firstChannel = 0; inPtr  = &inParams; }
            if (outCh > 0) { outParams.deviceId = m_Id; outParams.nChannels = outCh; outParams.firstChannel = 0; outPtr = &outParams; }

            if (!inPtr && !outPtr) return false;

            RtAudio::StreamOptions opts;
#ifdef __APPLE__
            opts.flags |= RTAUDIO_MINIMIZE_LATENCY;
#endif
            try {
                m_Audio->openStream(outPtr, inPtr, RTAUDIO_FLOAT32, m_Rate, &m_BufferFrames, &SoundCard::rtCallback, this, &opts);
                m_InChAct  = inCh;
                m_OutChAct = outCh;
                m_InputOpen.store(inCh  > 0, std::memory_order_relaxed);
                m_OutputOpen.store(outCh > 0, std::memory_order_relaxed);
                return true;
            } catch (...) {
                return false;
            }
        };

        // 1) Duplex if both sides exist
        if (inWant > 0 && outWant > 0) {
            if (tryOpen(1, 1)) return true;
            if (inWant != 1 || outWant != 1) {
                if (tryOpen(inWant, outWant)) return true;
            }
        }

        // 2) Input-only
        if (inWant > 0) {
            if (tryOpen(1, 0)) return true;
            if (inWant != 1 && tryOpen(inWant, 0)) return true;
        }

        // 3) Output-only
        if (outWant > 0) {
            if (tryOpen(0, 1)) return true;
            if (outWant != 1 && tryOpen(0, outWant)) return true;
        }

        KK5JY::FT8::send_error("Audio device cannot be opened with any supported channel layout (duplex/input/output).");
        return false;
    }

    // Close any existing stream without assumptions
    inline void safeClose() {
        m_ShuttingDown.store(true, std::memory_order_release);
        try { if (m_Audio && m_Audio->isStreamRunning()) m_Audio->stopStream(); } catch (...) {}
        try { if (m_Audio && m_Audio->isStreamOpen())    m_Audio->closeStream(); } catch (...) {}
        m_InputOpen.store(false,  std::memory_order_relaxed);
        m_OutputOpen.store(false, std::memory_order_relaxed);
        m_Active.store(false,     std::memory_order_relaxed);
        m_InChAct  = 0;
        m_OutChAct = 0;
        m_ShuttingDown.store(false, std::memory_order_release);
    }

    // Members
    unsigned m_Id;
    unsigned m_Rate;
    unsigned short m_InChReq;   // requested channels
    unsigned short m_OutChReq;  // requested channels
    unsigned int   m_BufferFrames;

    // Effective channels actually opened (0,1,2,...)
    unsigned m_InChAct  {0};
    unsigned m_OutChAct {0};

    std::atomic<bool> m_InputOpen;
    std::atomic<bool> m_OutputOpen;
    std::atomic<bool> m_Active;
    std::atomic<bool> m_ShuttingDown;

    std::unique_ptr<RtAudio> m_Audio;
    std::vector<float> m_InScratch;
    std::vector<float> m_OutScratch;
};

#endif // KK5JY_SC_H