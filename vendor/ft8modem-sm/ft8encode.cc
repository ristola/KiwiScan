/*
 *
 *
 *    ft8encode.cc
 *
 *    WAV encoder for FT8.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>
#include <stdexcept>
#include <string>
#include <cstdlib>   // srandom, random, RAND_MAX
#include <ctime>     // time
#include <cmath>     // std::llround

#include "encode.h"
#include "mfsk.h"
#include "sf.h"
#include "stype.h"
#include "presets.h"

using namespace KK5JY::DSP::MFSK;
using namespace KK5JY::FT8;
using namespace std;

//
//  get_noise(level)
//
static float get_noise(float level) {
    // keep it all in float-land to avoid double->float warnings
    const float r = static_cast<float>(random()) / static_cast<float>(RAND_MAX);
    return level * (r - 0.5f);
}

//
//  main(...)
//
int main(int argc, char** argv) {
    if (argc < 6) {
        cerr << "\nUsage: " << argv[0] << " <mode> <fs> <f0> <wav> '<txt>' [<kwargs>]\n\n"
             << "       Generate an FT8 or FT4 message into a WAV file.\n\n"
             << "       <mode> is one of { FT4, FT8, JT9, JT65, WSPR }\n"
             << "       <fs> is the sampling frequency of the WAV file to create\n"
             << "       <f0> is the lowest MFSK frequency to generate\n"
             << "       <wav> is the name of the WAV file to generate\n"
             << "       <txt> is the message text, and should be quoted\n\n"
             << "       Keyword args can include (mostly for testing):\n"
             << "       'win'   is the sample window size\n"
             << "       'shape' selects the shaper in { es, cosine }\n"
             << "       'bench' selects bench-testing mode\n"
             << "       'k'     adjusts the default MFSK filter preset\n\n";
        return 1;
    }

    // extract arguments
    const std::string mode = argv[1];
    const std::string wav  = argv[4];
    const std::string txt  = argv[5];

    // use stod instead of atof to avoid implicit double conversions
    const double rate_d = std::stod(argv[2]);
    const double f0     = std::stod(argv[3]);

    // store an integer version for places that want counts/sizes
    const size_t rate_i = static_cast<size_t>(rate_d > 0.0 ? std::llround(rate_d) : 0);

    double bps   = 0.0;
    double shift = 0.0;
    double k_fc  = 1.0;

    size_t win   = 128;
    bool bench   = false; // bench-testing flag
    std::string shaper;

    // keyword arguments
    if (argc >= 7) {
        for (int i = 6; i < argc; ++i) {
            const std::string arg(argv[i]);

            if (arg.size() > 4 && arg.rfind("win=", 0) == 0) {
                // window is a size_t
                win = std::stoul(arg.substr(4));
                if (win == 0) {
                    cerr << "Window must be > 0" << endl;
                    return 1;
                }
                std::cerr << "Set win = " << win << std::endl;
            } else if (arg.size() > 2 && arg.rfind("k=", 0) == 0) {
                k_fc = std::stod(arg.substr(2));
                if (k_fc <= 0.0) {
                    cerr << "k must be > 0" << endl;
                    return 1;
                }
                std::cerr << "Set k = " << k_fc << std::endl;
            } else if (arg.size() > 6 && arg.rfind("bench=", 0) == 0) {
                bench = (arg[6] != '0');
            } else if (arg.size() > 6 && arg.rfind("shape=", 0) == 0) {
                shaper = arg.substr(6);
                std::cerr << "Set shaper to " << shaper << std::endl;
            }
        }
    }

    const std::string rmode = my::toLower(mode);

    if (rmode == "ft8") {
        bps = bps_ft8;  shift = bps;
    } else if (rmode == "ft4") {
        bps = bps_ft4;  shift = bps;
    } else if (rmode == "jt65") {
        bps = bps_jt65; shift = bps;
    } else if (rmode == "jt9") {
        bps = bps_jt9;  shift = bps;
    } else if (rmode == "wspr") {
        bps = bps_wspr; shift = bps;
    } else {
        cerr << "Invalid mode." << std::endl;
        return 1;
    }

    // open a new WAV file (constructor expects integral rate)
    SoundFile output(wav,
                     static_cast<int>(rate_i),
                     1,
                     SoundFile::major_formats::wav,
                     SoundFile::minor_formats::s16);

    // create a new modulator
    IFilter<float>* kf = nullptr;
    if (shaper == "cosine") {
        kf = GetShaperCosine<float>(rmode, rate_i, k_fc);
    } else if (shaper == "es") {
        kf = GetShaperRC<float>(rmode, rate_i, k_fc);
    } else if (!shaper.empty()) {
        throw std::runtime_error("Invalid shaper provided");
    } else {
        kf = GetPreferredShaper<float>(rmode, rate_i, k_fc);
    }

    KK5JY::DSP::MFSK::Modulator<float> mfsk(rate_d, f0, bps, shift, kf);
    mfsk.setVolume(0.5f);

    // encode
    const std::string message = KK5JY::FT8::encode(mode, txt);
    mfsk.transmit(message, f0);

    // write
    size_t    count   = 0;
    size_t    samples = 0;
    float*    buffer  = new float[win];
    size_t    target  = 0;
    float     noise   = 0.0f;

    // when bench-testing...
    if (bench) {
        // seed the PRNG with the clock
        srandom(static_cast<unsigned>(time(nullptr)));

        // for JT65, add about -14dB of noise; for some reason the decoder needs it
        if (rmode == "jt65") {
            noise = 0.2f;
        }
        // for WSPR, also add some noise
        else if (rmode == "wspr") {
            noise = 0.02f;
        }
        // for others, add about -40dB of noise
        else {
            noise = 0.01f;
        }
    }

    // pointers for walking the data
    float* const ep = buffer + win;
    float*       zp = buffer;

    // add more lead-in for certain modes
    if (rmode == "jt65" || rmode == "wspr" || rmode == "jt9") {
        target = static_cast<size_t>(0.87 * static_cast<double>(rate_i));
        for (size_t i = 0; i != win; ++i) buffer[i] = 0.0f;

        while (samples < target) {
            // noise it up if needed
            if (noise != 0.0f) {
                zp = buffer;
                while (zp != ep) *zp++ = get_noise(noise);
            }

            // write the buffer
            const sf_count_t wrote = output.write(buffer, static_cast<sf_count_t>(win));
            if (wrote > 0) samples += static_cast<size_t>(wrote);
            else break; // safety
        }
    }

    // write the tones
    do {
        count = mfsk.read(buffer, win);
        if (count != 0) {
            if (noise != 0.0f) {
                zp = buffer;
                while (zp != ep) {
                    *zp = ((1.0f - noise) * *zp) + get_noise(noise);
                    ++zp;
                }
            }
            const sf_count_t ct2 = output.write(buffer, static_cast<sf_count_t>(count));
            if (ct2 < 0 || static_cast<size_t>(ct2) != count) throw runtime_error("Output mismatch");
            samples += count;
        }
    } while (count != 0);

    // zero out the unused part
    zp = buffer;
    while (zp != ep) {
        *zp++ = (noise != 0.0f) ? get_noise(noise) : 0.0f;
    }

    // add half second of silence at the end
    target = samples + (rate_i / 2);

    // ...but when bench-testing...
    if (bench) {
        // ...use more for certain modes
        if (rmode == "jt65" || rmode == "jt9") {
            target = samples + (12 * rate_i);
        } else if (rmode == "wspr") {
            target = samples + static_cast<size_t>(8.4 * static_cast<double>(rate_i));
        }
    }

    // write out the trailing silence
    do {
        if (noise != 0.0f) {
            zp = buffer;
            while (zp != ep) *zp++ = get_noise(noise);
        }
        const sf_count_t wr = output.write(buffer, static_cast<sf_count_t>(win));
        if (wr <= 0) break;
        samples += static_cast<size_t>(wr);
    } while (samples < target);

    // DEBUG:
    cerr << "Wrote " << samples << " samples ("
         << (static_cast<double>(samples) / static_cast<double>(rate_i))
         << " sec)." << endl;

    delete[] buffer;
    return 0;
}

// EOF