/*
 *
 *
 *    snddev.cc
 *
 *    Sound interface and decoding framework.
 *
 *    This is the core of the 'ft8modem' application.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

// C / C++ std
#include <cmath>
#include <cstdlib>
#include <deque>
#include <atomic>
#include <sys/stat.h>
#include <cstdint>   // uint8_t, int16_t
#include <cstdio>    // snprintf
#include <ctime>     // time_t, gmtime_r
#include <unistd.h>  // getuid, unlink

// include the class header
#include "snddev.h"

// db-linear conversions
#include "level.h"

// for path queries
#include "pathtools.h"

// locked standard output methods
#include "pipes.h"

// ============================
// Local RMS helper
// ============================
namespace {
inline double compute_rms_db(const float* mono, size_t count) {
    if (!mono || count == 0) return -200.0;
    double sumsq = 0.0;
    for (size_t i = 0; i < count; ++i) {
        const double s = mono[i];
        sumsq += s * s;
    }
    const double rms = (count > 0) ? std::sqrt(sumsq / static_cast<double>(count)) : 0.0;
    if (rms <= 1e-12) return -200.0;
    return 20.0 * std::log10(rms); // dBFS
}
} // namespace


//
//  ModemSoundDevice::ctor
//
//  NOTE: With the updated SoundCard that accepts separate input/output
//        channel counts, open 1ch IN (IC-7300 RX) and 2ch OUT (CoreAudio TX).
//
ModemSoundDevice::ModemSoundDevice(const std::string &mode, size_t id, size_t rate, size_t win, const std::string &ext, const std::string &tempRoot) :
#if defined(__APPLE__)
        // macOS/CoreAudio: 1 in / 2 out avoids mono-output failures
        SoundCard(static_cast<unsigned int>(id),
                  static_cast<unsigned int>(rate),
                  1,
                  2,
                  static_cast<unsigned int>(win)),
#else
        // Other platforms: mono in / mono out
        SoundCard(static_cast<unsigned int>(id),
                  static_cast<unsigned int>(rate),
                  1,
                  1,
                  static_cast<unsigned int>(win)),
#endif
        m_Filter(nullptr), m_Current(nullptr), m_Decoding(nullptr), m_MFSK(nullptr),
        m_Keep(false), m_Start(0)
{
    m_Mode = my::strip(my::toUpper(mode));

    // track capture-slot accounting
    m_SamplesThisSlot = 0;
    m_LastWav.clear();

    // pick a temp folder (user can override this after construction, if desired)
    std::deque<std::string> tempDirs;

    if (!tempRoot.empty()) {
        tempDirs.push_back(tempRoot);
    }

#ifdef __APPLE__
    // macOS: prefer per-user tmp inside $HOME, then TMPDIR, then /tmp
    if (const char* home = std::getenv("HOME")) {
        tempDirs.push_back(std::string(home) + "/tmp/");
    }
    if (const char* tmp = std::getenv("TMPDIR")) {
        // ensure it ends with '/'
        std::string td = tmp;
        if (!td.empty() && td.back() != '/') td.push_back('/');
        tempDirs.push_back(td);
    }
    tempDirs.push_back("/tmp/");
#else
    // Linux/Unix defaults
    std::string uid_spec = "/run/user/" + std::to_string(::getuid()) + "/"; // uid-specific ramdisk
    tempDirs.push_back(uid_spec);    // this should be on a ramdisk
    tempDirs.push_back("/var/run/"); // this is on the same ramdisk
    tempDirs.push_back("/tmp/");     // likely on disk
    tempDirs.push_back("/var/tmp/"); // likely on disk
#endif

    // choose the first usable base directory; create it if needed
    for (const auto& base : tempDirs) {
        if (KK5JY::Path::isDir(base) || KK5JY::Path::isLink(base)) {
            m_TempDir = base;
            break;
        }
        // Try to create the base if parent exists
        size_t slash = base.rfind('/', base.size() > 1 ? base.size() - 2 : 0);
        std::string parent = (slash != std::string::npos) ? base.substr(0, slash + 1) : "/";
        if (KK5JY::Path::isDir(parent)) {
            if (::mkdir(base.c_str(), 0700) == 0 || KK5JY::Path::isDir(base)) {
                m_TempDir = base;
                break;
            }
        }
    }

    // last resort
    if (m_TempDir.empty()) {
        m_TempDir = "/tmp/";
    }

    // make sure something sane was found (and try to create it if missing)
    if ( ! KK5JY::Path::isDir(m_TempDir)) {
        (void)::mkdir(m_TempDir.c_str(), 0700);
    }
    if (m_TempDir.empty() || ! KK5JY::Path::isDir(m_TempDir)) {
        throw std::runtime_error("Could not automatically find an appropriate temporary directory.");
    }

    // now add some structure; folder should be {m_TempDir}/ft8modem/
    if ( m_TempDir[m_TempDir.size() -1] != '/')
        m_TempDir += '/';
    m_TempDir += "ft8modem/";
    if ( ! KK5JY::Path::isDir(m_TempDir)) {
        if (::mkdir(m_TempDir.c_str(), 0700) != 0) {
            throw std::runtime_error("Could not create temporary folder: " + m_TempDir);
        }
    }

    m_Extension = ext;
    if ( ! m_Extension.empty()) {
        m_TempDir += m_Extension;
        m_TempDir += '/';
        if ( ! KK5JY::Path::isDir(m_TempDir)) {
            if (::mkdir(m_TempDir.c_str(), 0700) != 0) {
                throw std::runtime_error("Could not create temporary folder: " + m_TempDir);
            }
        }
    }

    m_Depth = 1;
    m_Rate = rate;
    if (m_Rate < 12000)
        throw std::runtime_error("Sound card rate must be at least 12kHz");
    m_Sending = false;
    m_Lead = static_cast<size_t>(0.300 * m_Rate); // 300ms
    m_Volume = 0.5f; // 50%
    m_Abort = false;
    m_NewMode = 0;
    m_MaxInput = 0;
    m_Intervals = 0;
    m_Threads = 0; // auto-detect
    m_Loop = false; // don't decode our own transmissions by default
    m_LoopBuffer = new float[win];
    m_NetBuffer = nullptr;
    m_NetSize = 0;
    m_Trail = 0.0;
    m_Fudge = 0.0;
    m_warndb = 0;

    // allocate the decimation filter
    setFilter(dec_taps);

    // allocate the decimator
    if (m_Rate == 12000) {
        // no decimator needed
        m_Decimator = nullptr;
    } else if (m_Rate % 12000) {
        // decimator for arbitrary rate
        m_Decimator = new KK5JY::DSP::DecimatorLI<float, float>(
            static_cast<size_t>(rate), // sound card rate
            static_cast<size_t>(12000) // 'jt9' rate
        );
    } else {
        // decimator for evenly divisible rate
        m_Decimator = new KK5JY::DSP::DecimatorInt<float>(
            static_cast<size_t>(rate), // sound card rate
            static_cast<size_t>(12000) // 'jt9' rate
        );
    }

    // configure mode-specific timings
    if (m_Mode == "FT8") {
        m_TrimIdx = 7;       // starting position of spot line
        m_ModeCode = '~';
        m_TxWinStart = 0.0;  // soonest window position to start
        m_TxWinEnd = 2.0;    // latest window position to start in current slot
        m_FrameSize = 15.0;  // the slot length
        m_FrameStart = 14.9; // start recording
        m_FrameEnd = 13.4;   // stop recording
        m_bps = KK5JY::FT8::bps_ft8;
        m_shift = m_bps;
        m_Trail = 0.3;       // default trailing capture for FT8
        // m_Trail = setTrail(m_Trail); // (FT8 default already safe)
    } else if (m_Mode == "FT4") {
        m_TrimIdx = 7;
        m_ModeCode = '+';
        m_TxWinStart = 0.0;
        m_TxWinEnd = 1.0;
        m_FrameSize = 15.0 / 2;
        m_FrameStart = 14.9 / 2; // 7.45s
        m_FrameEnd = 13.4 / 2;   // 6.70s
        m_bps = KK5JY::FT8::bps_ft4;
        m_shift = m_bps;

        // Assign then clamp so (FrameEnd + Trail) < FrameStart.
        m_Trail = 0.9;
        m_Trail = setTrail(m_Trail);
    } else if (m_Mode == "JT65") {
        m_TrimIdx = 5;
        m_ModeCode = '#';
        m_TxWinStart = 0.0;
        m_TxWinEnd = 5.0;
        m_FrameSize = 60.0;
        m_FrameStart = 59.8;
        m_FrameEnd = 52.0;
        m_Lead = static_cast<size_t>(1.0 * m_Rate); // 1s lead-in for JT65
        m_bps = KK5JY::FT8::bps_jt65;
        m_shift = m_bps;
    } else if (m_Mode == "JT9") {
        m_TrimIdx = 5;
        m_ModeCode = '@';
        m_TxWinStart = 0.0;
        m_TxWinEnd = 2.0;
        m_FrameSize = 60.0;
        m_FrameStart = 59.8;
        m_FrameEnd = 52.0;
        m_Lead = static_cast<size_t>(1.0 * m_Rate); // 1s lead-in for JT9
        m_bps = KK5JY::FT8::bps_jt9;
        m_shift = m_bps;
    } else if (m_Mode == "WSPR") {
        m_TrimIdx = 5;
        m_ModeCode = '0';
        m_TxWinStart = 0.0;
        m_TxWinEnd = 5.0;
        m_FrameSize = 120.0;
        m_FrameStart = 119.9;
        m_FrameEnd = 112.0;
        m_Lead = static_cast<size_t>(1.0 * m_Rate); // 1s lead-in for WSPR
        m_bps = KK5JY::FT8::bps_wspr;
        m_shift = m_bps;
    } else {
        throw std::runtime_error("Unsupported mode provided");
    }
}


//
//  ModemSoundDevice::dtor
//
ModemSoundDevice::~ModemSoundDevice(void) {
    setFilter(0); // delete the filter
    if (m_LoopBuffer) {
        delete [] m_LoopBuffer;
        m_LoopBuffer = nullptr;
        delete [] m_NetBuffer;
        m_NetBuffer = nullptr;
        delete m_Decimator;
        m_Decimator = nullptr;
    }
}


//
//  ModemSoundDevice::setTemp(...) - set the temp folder
//
std::string ModemSoundDevice::setTemp(const std::string &s) {
    // empty values not allowed
    if (s.empty())
        throw std::runtime_error("Must specify valid directory name");

    // assign member, and possibly make the directory
    m_TempDir = s;
    mode_t newMode = 0700; // only owner can read/write to the directory
    if ( ! KK5JY::Path::isDir(m_TempDir)) {
        // make the directory if it doesn't exist
        if (::mkdir(m_TempDir.c_str(), newMode)) {
            throw std::runtime_error("Could not make temp directory: " + m_TempDir);
        }
    } else {
        // change the directory mode if it already exists
        if (::chmod(m_TempDir.c_str(), newMode)) {
            throw std::runtime_error("Could not change directory mode: " + m_TempDir);
        }
    }

    // make sure to terminate the path with '/'
    if (m_TempDir[m_TempDir.size() - 1] != '/')
        m_TempDir += '/';
    return m_TempDir;
}


//
//  ModemSoundDevice::setFilter(taps)
//
void ModemSoundDevice::setFilter(int taps) {
    // remove the old filter
    if (m_Filter) {
        delete m_Filter;
        m_Filter = nullptr;
    }

    // if no filter specified, leave it NULL
    if (taps <= 0) {
        return;
    }

    // allocate new decimation filter
    m_Filter = new KK5JY::DSP::FirFilter<float>(
        KK5JY::DSP::FirFilterTypes::LowPass, // type
        taps,        // taps
        dec_cutoff,  // cutoff
        m_Rate,      // rate
        KK5JY::DSP::BlackmanNuttallWindow);
}


//
//  ModemSoundDevice::start() - start running the receiver
//
bool ModemSoundDevice::start() {
    // DEBUG:
    KK5JY::FT8::send_debug("Using " + m_TempDir + " for temp files.");

    // do nothing in UDP mode
    if (card() == 0)
        return true;

    // go
    return startRecording();
}


//
//  ModemSoundDevice::stop() - stop running the sound card
//
void ModemSoundDevice::stop() {
    // only do sound card ops when not in UDP mode
    if (card() > 0) {
        // stop the sound device
        SoundCard::stop();
    }

    // stop pending decoders
    KK5JY::FT8::Decode<float> *toDelete = m_Decoding;
    if (toDelete) {
        toDelete->clear();
    }
    if ( ! m_Loop) {
        toDelete = m_Current;
        if (toDelete) {
            toDelete->clear();
        }
    }

    // remove 'jt9' temp files
    std::string toUnlink = m_TempDir + "decoded.txt";
    if (KK5JY::Path::isFile(toUnlink)) {
        ::unlink(toUnlink.c_str());
    }
    toUnlink = m_TempDir + "timer.out";
    if (KK5JY::Path::isFile(toUnlink)) {
        ::unlink(toUnlink.c_str());
    }
}


//
//  ModemSoundDevice::run()
//
void ModemSoundDevice::run() {
    // service decoder output
    KK5JY::FT8::Decode<float> *decoding = m_Decoding;
    if (decoding) {
        // capture 'done' flag
        bool done = decoding->isDone();

        // fetch the decodes so far...
        std::deque<std::string> buffer;
        if ((decoding->getDecodes(buffer) > 0) && ! m_Purge) {
            // ...and print them
            double when = ::round(decoding->GetCaptureStart());
            for (auto i = buffer.begin(); i != buffer.end(); ++i) {
                // generate a decode spot
                KK5JY::FT8::send_message(
                    "D",
                    m_Mode + " " + std::to_string(static_cast<time_t>(when)) + " " +
                    (i->substr(static_cast<std::string::size_type>(m_TrimIdx)))
                );
            }
        }

        // if the decoder is done
        if (done) {
            m_Decoding = nullptr;

            // dispose of the decoder instance
            double consumed = decoding->GetRuntime();
            size_t total = m_Purge ? 0 : decoding->count();
            delete decoding;

            // talk about the results
            size_t msec = static_cast<size_t>(consumed * 1000.0);
            KK5JY::FT8::send_debug("Decoder consumed " + std::to_string(msec) + " msec; " +
                std::to_string(total) + " decodes; completed @ " + KK5JY::FT8::timestring());
            // clear the purge flag, since no more decodes will happen
            //   until the end of the next slot
            m_Purge = false;
        }
    }

    // read any new-mode request in a thread-safe way
    char newMode = 0;
    {
        // lock critical section while checking/changing mode flag
        my::locker lock(m_Mutex);
        newMode = m_NewMode;
        m_NewMode = 0;
    }

    // don't do the change if in UDP mode
    if (card() == 0)
        return;

    // change mode, if needed
    //   NOTE: a stop() call is implied by each start*() call, and thus unnecessary here
    switch (newMode) {
        case 'P':
            if ( ! startPlayback())
                KK5JY::FT8::send_error("Could not start playback on sound device.");
            break;
        case 'R':
            if ( ! startRecording())
                KK5JY::FT8::send_error("Could not start recording on sound device.");
            break;
        default: break;
    }
}


//
//  ModemSoundDevice::update_volume(...) - update the peak volume from a buffer
//
void ModemSoundDevice::update_volume(float *input, size_t count) {
    const float *       vp = input;
    const float * const ep = input + count;
    float s = 0;
    while (vp != ep) {
        s = ::fabs(*vp++);
        if (s > m_MaxInput)
            m_MaxInput = s;
    }
}


//
//  ModemSoundDevice::decimate_buffer(...) - helper for decimator
//
//  output must be at least as large as input
//
size_t ModemSoundDevice::decimate_buffer(float *input, size_t count, float *output) {
    // run decimation LPF across the input
    float *ip = input;
    float *op = output;
    const float * ep = input + count;
    if (m_Filter) {
        while (ip != ep) {
            *op++ = m_Filter->run(*ip++);
        }
    }

    // decimate the output buffer in-place
    ip = output;
    op = output;
    ep = output + count;
    float sample = 0.0f;
    size_t ct = 0;
    while (ip != ep) {
        if (m_Decimator->run(*ip++, sample)) {
            *op++ = sample;
            ++ct;
        }
    }

    // return the number of samples placed into 'output'
    return ct;
}


//
//  ModemSoundDevice::transmit(...)
//
bool ModemSoundDevice::transmit(const std::string &message, double f0, TimeSlots slot) {
    // store this as the most recently send message
    m_LastSent = message;

    // encode to keying symbols
    std::string linebuffer = KK5JY::FT8::encode(m_Mode, message);
    
    // DEBUG: Show symbol count and timing parameters
    KK5JY::FT8::send_debug("TX " + m_Mode + ": symbols=" + std::to_string(linebuffer.size()) +
                          " bps=" + std::to_string(m_bps) + 
                          " shift=" + std::to_string(m_shift) +
                          " rate=" + std::to_string(m_Rate));
    
    // Check if encoding failed
    if (linebuffer.empty()) {
        KK5JY::FT8::send_error("Encoding failed for message: " + message);
        return false;
    }

    // store the TX slot
    m_Slot = slot;

    // lock critical section from here to end of function
    my::locker lock(m_Mutex);

    // either update or start a new modulator for this message
    if ( ! m_MFSK) {
        // queue a new modulator to handle the message
        IFilter<float> *kf = nullptr;
        switch (m_Shaper) {
            case KK5JY::FT8::ExponentialSmoother:
                kf = KK5JY::FT8::GetShaperRC<float>(m_Mode, m_Rate);
                break;
            case KK5JY::FT8::RaisedCosine:
                kf = KK5JY::FT8::GetShaperCosine<float>(m_Mode, m_Rate);
                break;
            default:
                kf = KK5JY::FT8::GetPreferredShaper<float>(m_Mode, m_Rate);
                break;
        }
        m_MFSK = new KK5JY::DSP::MFSK::Modulator<float>(
            m_Rate, f0, m_bps, m_shift, kf);
        m_MFSK->setLead(m_Lead);
        m_MFSK->setVolume(m_Volume);
    }

    // update the message in the modulator
    m_MFSK->transmit(linebuffer, f0);

    // success
    return true;
}


//
//  ModemSoundDevice::cancelTransmit
//
bool ModemSoundDevice::cancelTransmit() {
    // lock critical section from here to end of function
    my::locker lock(m_Mutex);

    m_Abort = true; // tell the event handler to stop and clean up
    return m_MFSK != nullptr; // return true if there was anything to cancel
}


//
//  ModemSoundDevice::event - sound card event handler
//
void ModemSoundDevice::event(float *in, float *out, size_t count) {
    // read the frame clock
    double sec = m_Clock.seconds(m_FrameSize, m_Fudge);

    // set active flag
    if (count) m_Active = true;

    //
    //  RECEIVER: feed the current decoder
    //
    KK5JY::FT8::Decode<float> *decoder = m_Current;
    if (decoder) {
        // if there is input data, and not transmitting, feed the decoder
        if (in && ! m_Sending) {
            // measure the peak input volume
            update_volume(in, count);

            // compute and store last input RMS (dBFS) for AUTO detector
            _lastRmsDb.store(compute_rms_db(in, count), std::memory_order_relaxed);

            if (m_Rate == 12000) {
                // copy data into decode module directly
                size_t wrote = decoder->write(in, count);
                (void)wrote;
                m_SamplesThisSlot += wrote;
            } else {
                // decimate the input in-place
                size_t ct = decimate_buffer(in, count, in);

                // copy data into decode module
                size_t wrote = decoder->write(in, ct);
                (void)wrote;
                m_SamplesThisSlot += wrote;
            }
        } else if (!in) {
            // No input this window; mark very low
            _lastRmsDb.store(-200.0, std::memory_order_relaxed);
        }

        // if frame ended, move current decoder to 'decoding' state
        if (sec > (m_FrameEnd + m_Trail) && sec < m_FrameStart) {
            //
            //  end current WAV recording
            //

            // Only hand over to decode if we actually captured samples
            bool haveSamples = (m_SamplesThisSlot > 0);
            bool haveFile    = (!m_LastWav.empty() && KK5JY::Path::isFile(m_LastWav));

            if (m_Decoding) {
                // dispose of the most recent timeslot
                m_Current->clear();
                delete m_Current;
                decoder = m_Current = nullptr;

                // and issue a warning
                KK5JY::FT8::send_warning("Prior timeslot has not finished decoding, discarding data.");
            } else if (haveSamples && haveFile) {
                // move the most recent capture to "decoding"
                m_Decoding = m_Current;
                decoder = m_Current = nullptr;

                // record the stop time of the most recent timeslot
                double stop = KK5JY::FT8::abstime();

                // start the decoding process in the background
                m_Decoding->startDecode();

                // talk about the decode window
                size_t window = static_cast<size_t>(1000.0 * (stop - m_Start));
                KK5JY::FT8::send_trace("End capture of " + std::to_string(window) + " msec; " +
                    KK5JY::FT8::timestring(static_cast<time_t>(m_Start)) + " to " +
                    KK5JY::FT8::timestring(static_cast<time_t>(stop)));
            } else {
                // No samples or no file → skip decode to avoid Fortran open error
                KK5JY::FT8::send_trace(
                    haveFile
                    ? "Skipping decode: no samples captured this slot."
                    : "Skipping decode: WAV not found (" + m_LastWav + ").");
                if (m_Current) {
                    m_Current->clear();
                    delete m_Current;
                    m_Current = nullptr;
                }
            }

            // report the highest input level
            if (m_Intervals++) { // this skips the first timeslot
                int db = (m_MaxInput == 0)
                           ? minimum_db
                           : static_cast<int>(KK5JY::DSP::decibels(m_MaxInput));
                if (db > 0) db = 0;
                if (db < minimum_db) db = minimum_db;

                KK5JY::FT8::send_message("INPUT", std::to_string(db));

                if (m_warndb < 0 && db < m_warndb)
                    KK5JY::FT8::send_warning("Input level is low.");
            }
            m_MaxInput = 0;
            m_SamplesThisSlot = 0;
            m_LastWav.clear();
        }
    } else { // no decoder
        if (sec >= m_FrameStart || sec < m_FrameEnd) {
            //
            //  start a new WAV recording
            //

            // capture the start time
            m_Start = KK5JY::FT8::abstime();

            // read the clock
            time_t t0 = time(0);
            tm now;
            gmtime_r(&t0, &now);

            // generate a timestamp for the WAV file
            char pbuf[20];
            now.tm_sec = static_cast<int>(m_FrameSize * ::round(now.tm_sec / m_FrameSize)) % 60;
            if (now.tm_sec == 0)
                now.tm_min = (now.tm_min + 1) % 60;
            if (now.tm_min == 0)
                now.tm_hour = (now.tm_hour + 1) % 24;
            if (m_FrameSize >= 60.0)
                snprintf(pbuf, sizeof(pbuf), "%02d%02d.wav", now.tm_hour, now.tm_min);
            else
                snprintf(pbuf, sizeof(pbuf), "%02d%02d%02d.wav", now.tm_hour, now.tm_min, now.tm_sec);

            // generate the full path and remember it (used for sanity checks later)
            m_LastWav = m_TempDir + std::string("000101_") + pbuf;

            // create a new decoder and configure it
            decoder = m_Current = new KK5JY::FT8::Decode<float>(m_Mode, m_LastWav, m_TempDir, KK5JY::FT8::abstime(), m_Depth);
            m_Current->setKeep(m_Keep);
            if (m_Threads > 0)
                m_Current->setThreads(m_Threads);
            if ( ! m_Decoder.empty())
                m_Current->setDecoder(m_Decoder);

            m_SamplesThisSlot = 0; // reset counter at slot start
        }
    }


    //
    //  TRANSMITTER - if a message is pending, send it at the next slot
    //

    // lock critical section from here to end of function
    my::locker lock(m_Mutex);

    // capture the abort flag, but clear it regardless of whether it is used
    bool abort = false;
    if (m_Abort) {
        m_Abort = false;
        abort = true;
        if (m_MFSK && ! m_Sending) {
            delete m_MFSK;
            m_MFSK = nullptr;
        }
    }

    // if ready to transmit, but not yet sending, and at the start of
    //    the frame time, enable the transmitter
    bool inWindow = ( ! m_Sending) && m_MFSK && (sec > m_TxWinStart) && (sec < m_TxWinEnd);
    if (inWindow) {
        bool thisSlot = ! m_Slot; // if NextSlot selected, transmit now
        if ( ! thisSlot) {
            int slot_num = static_cast<int>(m_Clock.seconds(240, m_Fudge) / m_FrameSize);
            thisSlot |= ((m_Slot == OddSlot) && (slot_num % 2));     // in odd window
            thisSlot |= ((m_Slot == EvenSlot) && ! (slot_num % 2));  // in even window
        }
        if (thisSlot) {
            time_t now = static_cast<time_t>(::round(KK5JY::FT8::abstime()));
            KK5JY::FT8::send_message("TX", "1");
            KK5JY::FT8::send_trace("Enable modulator @ " + KK5JY::FT8::timestring());
            KK5JY::FT8::send_message("E",  // generate TX spot
                std::to_string(now) + "   0  0.0 " + std::to_string(static_cast<size_t>(m_MFSK->getFreq())) +
                " " + m_ModeCode + "  " + m_LastSent);

            m_LastSent.clear();
            m_Sending = true;

            // switch sound card to transmit (playback)
            m_NewMode = 'P';
        }
    }

    // if still only doing reception, stop now
    if ( ! out)
        return;

    // if already sending, keep going
    size_t ct = 0;
    if (m_Sending) {
        // generate data right into the I/O buffer
        ct = m_MFSK->read(out, count);

        // if data exhausted, shut down modulator
        if (abort || ! ct) {
            KK5JY::FT8::send_message("TX", "0");
            KK5JY::FT8::send_trace("Disable modulator @ " + KK5JY::FT8::timestring());

            m_Sending = false;
            delete m_MFSK;
            m_MFSK = nullptr;

            // switch card to receive (recording)
            m_NewMode = 'R';
        }
    }

    // zero out the rest of the transmit buffer
    const float * const ep = out + count; // end of output buffer
          float *       zp = out + ct;    // first sample to zero
    while (zp != ep) {
        *zp++ = 0.0f;
    }

    // if monitoring, also send data to the decoder
    if (m_Loop && count && decoder) {
        if (m_Rate == 12000) {
            // copy data into decode module
            size_t wrote = decoder->write(out, count);
            (void)wrote;
            m_SamplesThisSlot += wrote;
        } else {
            // update the peak volume
            update_volume(out, count);

            // decimate the output into the scratch buffer
            size_t dct = decimate_buffer(out, count, m_LoopBuffer);

            // copy data into decode module
            size_t wrote = decoder->write(m_LoopBuffer, dct);
            (void)wrote;
            m_SamplesThisSlot += wrote;
        }
    }
}


//
//  ModemSoundDevice::udp_audio(...) - UDP audio input
//
void ModemSoundDevice::udp_audio(uint8_t *raw_data, size_t bytes) {
    // calculate the number of S16_LE samples in the raw_data
    size_t samples = (bytes / 2);

    // (re)allocate the input buffer, as needed
    if (m_NetSize < samples) {
        if (m_NetBuffer)
            delete [] m_NetBuffer;
        m_NetBuffer = new float[samples];
        m_NetSize = samples;
    }

    // cast the input into the net buffer
    uint8_t *ip = raw_data;
    float *op = m_NetBuffer;
    int16_t sample;
    for (size_t i = 0; i != samples; ++i) {
        sample = static_cast<int16_t>(*ip++);                // lsb
        sample |= static_cast<int16_t>(static_cast<int16_t>(*ip++) << 8); // msb
        *op++ = static_cast<float>(sample) / 32768.0f;
    }

    // call the sound event handler
    event(m_NetBuffer, nullptr, samples);
}


// ============================
// lastRmsDb() accessor
// ============================
double ModemSoundDevice::lastRmsDb() const {
    return _lastRmsDb.load(std::memory_order_relaxed);
}

// EOF: snddev.cc