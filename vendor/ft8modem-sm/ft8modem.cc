/*
 *
 *
 *    ft8modem.cc
 *
 *    Software modem for FT8, etc.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 */

#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cctype>
#include <sstream>
#include <filesystem>

#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <strings.h>
#include <pwd.h>

// for socket/UDP support
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/in.h>
#include <netinet/ip.h>

#if __has_include(<rtaudio/RtAudio.h>)
  #include <rtaudio/RtAudio.h>
#elif __has_include(<RtAudio.h>)
  #include <RtAudio.h>
#endif

#include "snddev.h"
#include "level.h"
#include "pipes.h"
#include "version.h"

using namespace std;
using namespace KK5JY::FT8;
using namespace KK5JY::DSP;
using namespace KK5JY::DSP::MFSK;

namespace fs = std::filesystem;

// size of I/O buffers to the sound device
static const unsigned WindowSize = 256;

// lowest audio frequency that the user can request for transmitting
static const unsigned af_low = 1;

// highest audio frequency that the user can request for transmitting
static const unsigned af_high = 3100;

// when using UDP, this is the maximum time to wait for new audio data (usec)
static const suseconds_t max_idle = 200000; // 200 msec

// this is the select timeout delay (usec)
static const suseconds_t select_timeout = 50000; // 50 msec


// --------------------- helpers ------------------------

static std::string default_temp_root() {
    if (const char* env_root = std::getenv("FT8MODEM_TMPDIR")) {
        std::string value(env_root);
        if (!value.empty()) {
            return value;
        }
    }
    if (const char* env_root = std::getenv("TMPDIR")) {
        std::string value(env_root);
        if (!value.empty()) {
            return value + "/ft8modem";
        }
    }
    return "/tmp/ft8modem";
}

static bool ensure_dir(const std::string& path) {
    try {
        fs::create_directories(path);
        return true;
    } catch (...) {
        return false;
    }
}

static std::string build_temp_path(const std::string& root, const std::string& ext) {
    return root + "/" + ext;
}

// Normalize device strings for robust substring matching:
// - Uppercase
// - Convert non-alnum to single spaces
// - Collapse duplicate spaces and trim trailing
static std::string normdev(const std::string& s) {
    std::string out; out.reserve(s.size());
    bool lastSpace = true;
    for (char ch : s) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (std::isalnum(c)) {
            out.push_back(static_cast<char>(std::toupper(c)));
            lastSpace = false;
        } else {
            if (!lastSpace) { out.push_back(' '); lastSpace = true; }
        }
    }
    if (!out.empty() && out.back()==' ') out.pop_back();
    return out;
}

static std::vector<unsigned int> getAudioDeviceIds(RtAudio& adc)
{
#if defined(RTAUDIO_VERSION_MAJOR) && (RTAUDIO_VERSION_MAJOR >= 6)
    return adc.getDeviceIds();
#else
    std::vector<unsigned int> ids;
    const unsigned int deviceCount = adc.getDeviceCount();

    ids.reserve(deviceCount);
    for (unsigned int deviceId = 0; deviceId < deviceCount; ++deviceId) {
        ids.push_back(deviceId);
    }

    return ids;
#endif
}

// --- device listing helper (used by -L, usage, and AUDIO) --------
static void printAudioDevices()
{
    try {
        RtAudio adc;
        typedef std::vector<unsigned int> idlist_t;
        idlist_t ids = getAudioDeviceIds(adc);
        size_t shown = 0;

        for (idlist_t::const_iterator i = ids.begin(); i != ids.end(); ++i) {
            RtAudio::DeviceInfo info = adc.getDeviceInfo(*i);
            size_t best_rate = 0;

            // skip devices with no inputs
            if (info.inputChannels == 0)
                continue;

            for (size_t r = 0; r != info.sampleRates.size(); ++r) {
                size_t rate = info.sampleRates[r];
                if (rate > best_rate && rate <= 48000)
                    best_rate = rate;
            }

            if (best_rate >= 12000) {
                std::cout
                    << "          + ID = " << *i
                    << ": \"" << info.name << "\""
                    << ", best rate = " << best_rate << "\n";
                ++shown;
            }
        }

        if (!shown) {
            std::cout << "          (no suitable input devices detected)\n";
        }
    } catch (const std::exception &e) {
        std::cerr << "          (device enumeration failed: " << e.what() << ")\n";
    }
}


//
//  usage()
//
static void usage(const string &s) {
    cout << endl;
    cout << "Usage: " << s << " [options] <mode> [<device>]" << endl;
    cout << endl;
    cout << "    Starts a software modem for FT8, FT4, and similar HF digital modes." << endl;
    cout << endl;
    cout << "       <mode> is one of { FT4, FT8, JT9, JT65, WSPR }" << endl;
    cout << "       <device> is the sound card ID, and may be one of these devices:" << endl;
    cout << endl;

    // scan the devices for promising candidates (original behavior)
    printAudioDevices();

    cout << endl;
    cout << "    If your sound device is not shown above, it is likely because it" << endl;
    cout << "    reports no inputs to the operating system, or it is already in" << endl;
    cout << "    use by another program." << endl;
    cout << endl;
    cout << "    Device ID can also be a string.  This string is compared against" << endl;
    cout << "    each device name, and if the string is found within a device name," << endl;
    cout << "    that device is used." << endl;
    cout << endl;
    cout << "    Device ID can also have the form 'udp:port', where 'port' is a UDP" << endl;
    cout << "    port on the local machine.  In this case, the modem does not open" << endl;
    cout << "    a sound device, but rather reads raw sound data from a UDP socket." << endl;
    cout << "    This is only for use with the -u option of ft8sdr, for SDR receivers," << endl;
    cout << "    as the protocol is specific to the ft8modem." << endl;
    cout << endl;
    cout << "    The device ID will be persisted to .ft8modemrc, and that value" << endl;
    cout << "    will be used next time the modem is run, if not provided.  As" << endl;
    cout << "    such, it is optional, but must be provided at least once.  The" << endl;
    cout << "    command line value, if provided, takes precedence." << endl;
    cout << endl;
    cout << "    Options:" << endl;
    cout << "         -i <msec>     set transmit lead-in time, in milliseconds" << endl;
    cout << "         -e <name>     set temp-file instance folder name" << endl;
    cout << "         -t <path>     set full path to the temp directory (default /tmp/ft8modem)" << endl;
    cout << "         -T <threads>  set maximum number of decoder threads" << endl;
    cout << "         -j <decoder>  set path to 'jt9' program to use" << endl;
    cout << "         -r <rate>     force sample rate of the sound device to <rate> Hz" << endl;
    cout << "         -f <msec>     apply a frame clock offset; mostly for SDRs" << endl;
    cout << "         -w <msec>     adjust the trailing window timing; mostly for SDRs" << endl;
    cout << "         -y            disable the decimation filter for rate > 12kHz" << endl;
    cout << "         -L            list audio devices and exit" << endl;
    cout << "         -d            enable DEBUG logging at startup" << endl;
    cout << endl;
    cout.flush();
}


//
//  get_rc_path()
//
static string get_rc_path() {
    passwd *pw = getpwuid(getuid());
    if ( ! pw) {
        return string();
    }
    string path = pw->pw_dir;
    path += "/.ft8modemrc";
    return path;
}


//
//  clean_db(f) - format dB figure to one decimal place
//
std::string clean_db(float db) {
    stringstream clean;
    clean << setprecision(1) << fixed << db;
    return clean.str();
}


//
//  isdigits(s) - returns true iff the entire string is numeric
//
static bool isdigits(const string &s) {
    if (s.empty()) return false;
    for (char c : s) if (!::isdigit((unsigned char)c)) return false;
    return true;
}


//
//  findDeviceByName(s);
//
static int findDeviceByName(const string &name) {
    const std::string key = normdev(name);
    RtAudio adc;

    typedef std::vector<unsigned int> idlist_t;
    idlist_t ids = getAudioDeviceIds(adc);
    for (idlist_t::const_iterator i = ids.begin(); i != ids.end(); ++i) {
        RtAudio::DeviceInfo info = adc.getDeviceInfo(*i);
        const std::string haystack = normdev(info.name);
        if (!key.empty() && haystack.find(key) != std::string::npos)
            return static_cast<int>(*i);
    }
    return -1;
}


//
//  parse the frequency/slot token
//
static TimeSlots getFrequencyAndSlot(const std::string &token, size_t &out_freq) {
    if (token.empty())
        throw runtime_error("Invalid frequency/slot provided.");

    // try to read the time slot
    TimeSlots eo = NextSlot;
    const char eoc = static_cast<char>(std::toupper(static_cast<unsigned char>(token.back())));
    const bool hasSlot = ! ::isdigit(eoc);
    const std::string::size_type eof = hasSlot ? token.size() - 1 : token.size();
    const std::string freq = token.substr(0, eof);

    // validate the frequency first
    if ( ! my::isDigit(freq)) {
        throw runtime_error("Invalid frequency provided: " + freq);
    }

    // parse the slot if it is given
    if (hasSlot) {
        switch(eoc) {
            case 'E':
                eo = EvenSlot;
                break;
            case 'O':
                eo = OddSlot;
                break;
            default:
                throw runtime_error("Invalid slot specified; must be 'E' or 'O'.");
        }
    }

    // read the frequency, and set the output parameter
    try {
        out_freq = static_cast<size_t>(stoi(freq));
    } catch (...) {
        throw runtime_error("Invalid frequency provided: " + freq);
    }

    // return the slot
    return eo;
}


//
//  helper to (re)create a ModemSoundDevice with current settings
//
static std::unique_ptr<ModemSoundDevice> makeAudioDevice(
    const std::string& mode,
    int devid,
    unsigned int rate,
    const std::string& ext,
    ShaperTypes shaper,
    bool loop,
    bool no_filter,
    const std::string& tempPath,
    const std::string& sthreads,
    const std::string& jt9,
    bool keep,
    float level,
    int lead_ms, int trail_ms, int fudge_ms, int warndb
) {
    std::string root = tempPath.empty() ? default_temp_root() : tempPath;
    auto p = std::make_unique<ModemSoundDevice>(mode, devid, rate, WindowSize, ext, root);
    p->setVolume(static_cast<float>(linear(level)));
    p->setDepth(2); // default; caller will override if needed
    p->setShaper(shaper);
    p->setMonitor(loop);
    if (no_filter) p->setFilter(0);
    if (!sthreads.empty()) { int threads = stoi(sthreads); if (threads > 0) p->setThreads(static_cast<short>(threads)); }
    if (!jt9.empty()) p->setDecoder(jt9);
    if (keep) p->setKeep(true);
    if (lead_ms >= 0)  p->setLead((rate * static_cast<unsigned int>(lead_ms)) / 1000U);
    if (trail_ms >  0) p->setTrail(static_cast<double>(trail_ms) / 1000.0);
    if (fudge_ms != 0) p->setFudge(static_cast<double>(fudge_ms) / 1000.0);
    if (warndb) p->setWarndB(warndb);

    // Always set explicit temp folder and ensure directories
    std::string full = build_temp_path(root, ext);
    (void)ensure_dir(root);
    (void)ensure_dir(full);
    p->setTemp(full);

    return p;
}


//
//  main()
//
int main(int argc, char**argv) {
    // read the '.ft8modemrc' file
    float level = 0;
    int depth = 2;
    string devname;

    // keep maps with timing adjustment values; the mode isn't parsed
    //   until later, so store them all until we know which to use
    typedef std::map<std::string, int> timings_t;
    timings_t leads;  // lead-in values
    timings_t trails; // window adjustments

    // read the '.ft8modemrc' file
    {
        ifstream rc(get_rc_path());

        string line;
        while (std::getline(rc, line)) {
            line = my::trim(line);
            if (line.empty() || line[0] == '#') continue;

            size_t idx = line.find(' ');
            if (idx == string::npos) continue;
            string key = my::toUpper(my::trim(line.substr(0, idx)));
            string value = my::toUpper(my::trim(line.substr(idx + 1)));

            if (key == "LEVEL") {
                float newval = stof(value);
                if (newval <= 0 && newval >= -80) level = newval;
            } else if (key == "DEPTH") {
                int newval = stoi(value);
                if (newval > 0) depth = newval;
            } else if ((key.size() > 7) && (key.substr(0, 7) == "LEADIN.")) {
                int newval = stoi(value);
                if (newval >= 0) leads[my::toUpper(key.substr(7))] = newval;
            } else if ((key.size() > 6) && (key.substr(0, 6) == "TRAIL.")) {
                int newval = stoi(value);
                if (newval >= 0) trails[my::toUpper(key.substr(6))] = newval;
            } else if (key == "DEVICE") {
                devname = value;
            }
        }
    }

    // read command-line options
    typedef std::vector<std::string> optlist_t;
    optlist_t nonopts;
    std::string tempPath;
    std::string sthreads;
    std::string jt9;
    std::string modfilter;
    std::string ext;
    unsigned int rate = 0;
    bool rate_forced = false; // track if -r was supplied
    int lead = -1;  // transmit lead-in time
    int trail = -1; // receive window trail extension
    int fudge = 0;  // frame clock offset
    int warndb = 0; // dB level at which to warn user of low-level (must be <= 0)
    bool keep = false; // flag to keep WAV after each decode
    bool loop = false; // flag to decode transmitted audio, too
    bool no_filter = false; // flag to disable decimation filter
    bool debug_on = false;

    // track current debug state so we can truly toggle it at runtime
    static bool g_debug_state = false;

    {
        int opt;
        // NOTE: includes 'd' to force DEBUG on at startup
        while ((opt = getopt(argc, argv, "de:i:j:t:T:f:hklLm:r:v::yw:W:")) != -1) {
            switch (opt) {
                case 'd': debug_on = true; break;
                case 'e': ext = optarg; break;
                case 'i': lead = atoi(optarg); break;
                case 'f': fudge = atoi(optarg); break;
                case 'l': loop = true; break;
                case 'L': printAudioDevices(); return 0; // list and exit
                case 'j': jt9 = optarg; break;
                case 'k': keep = true; break;
                case 'r': rate = static_cast<unsigned int>(atoi(optarg)); rate_forced = true; break;
                case 't': tempPath = optarg; break;
                case 'm': modfilter = my::toLower(optarg); break;
                case 'T': sthreads = optarg; break;
                case 'v': cout << "ft8modem version " << GetModemVersion() << endl; return 0;
                case 'y': no_filter = true; break;
                case 'w': trail = atoi(optarg); break;
                case 'W': warndb = atoi(optarg); break;
                case 'h': default: usage(argv[0]); return 1;
            }
        }

        // collect non-option arguments
        while (optind < argc) {
            nonopts.push_back(argv[optind++]);
        }

        // make sure non-option list is reasonable size
        if (nonopts.empty() || nonopts.size() > 2) {
            usage(argv[0]);
            return 1;
        }
    }

    // enable DEBUG if requested via -d
    if (debug_on) { set_debug(true); g_debug_state = true; }

    // sanity checks
    if (ext.size() && tempPath.size()) {
        cerr << "The -e and -t options cannot be used together." << endl;
        return 1;
    }

    // read the MODE
    string mode = my::toUpper(nonopts[0]);

    // apply lead-in if user didn't supply one
    if ((lead < 0) && (leads.find(mode) != leads.end())) {
        lead = leads[mode];
    }

    // apply trail if user didn't supply one
    if ((trail < 0) && (trails.find(mode) != trails.end())) {
        trail = trails[mode];
    }

    // read the DEVICE NAME
    if (nonopts.size() == 2)
        devname = nonopts[1];

    // use numeric if that's what was given
    int devid = -1;
    int port = 0;
    if (isdigits(devname)) {
        devid = stoi(devname);
    } else if (devname.size() > 4 && my::toLower(devname.substr(0, 4)) == "udp:") {
        port = stoi(devname.substr(4));
        if (port <= 0 || port > 65535) {
            send_error("Port must be between 1 and 65535");
            return 1;
        }
    } else if (!devname.empty()) {
        devid = findDeviceByName(devname);
    }

    // make sure there is a valid ID (or UDP)
    if (devid < 0 && port <= 0) {
        usage(argv[0]);
        return 1;
    }

    // validate mode
    if (mode != "FT8" && mode != "FT4" && mode != "JT65" && mode != "JT9" && mode != "WSPR") {
        send_error("Invalid mode requested: " + mode);
        return 1;
    }

    // pick the shaper
    ShaperTypes shaper = ShaperTypes::DefaultShaper;
    if (modfilter == "es") {
        send_debug("Modulator bit-shaper type is ExponentialSmoother");
        shaper = ShaperTypes::ExponentialSmoother;
    } else if (modfilter == "cosine") {
        send_debug("Modulator bit-shaper type is RaisedCosine");
        shaper = ShaperTypes::RaisedCosine;
    } else if ( ! modfilter.empty()) {
        send_error("Invalid shaper type '" + modfilter + "' - valid types in { es, cosine }");
        return 1;
    }

    // find the best sampling rate (only if not forced and using a device)
    if (!rate_forced && rate == 0) {
        if (devid > 0) {
            const unsigned int hi_rate = 48000; // cleanly decimates to 12kHz
            RtAudio audioInfo;
            RtAudio::DeviceInfo info = audioInfo.getDeviceInfo(static_cast<unsigned int>(devid));
            for (std::vector<unsigned int>::const_iterator i = info.sampleRates.begin(); i != info.sampleRates.end(); ++i) {
                if (*i > rate && *i <= hi_rate) {
                    rate = *i;
                }
            }
            if (rate == 0) {
                send_error("Could not find a usable sample rate from " + std::to_string(info.sampleRates.size()) + " candidates.");
                return 1;
            }
            send_debug("Sound device is '" + info.name + "'" + " - ID = " + std::to_string(devid));
        } else { // UDP mode
            if (port > 0) {
                send_error("UDP audio source requires option '-r' to specify sample rate");
                return 1;
            }
        }
    }
    send_debug("Sound sampling rate is " + std::to_string(rate) + " Hz.");
    cout.flush();

    // cap level to 100%
    if (level > 0)
        level = 0;

    // UDP setup
    int sock = 0;
    if (devid < 0 && port > 0) {
        // use zero ID to mean UDP port
        devid = 0;

        // build a UDP socket
        sock = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (sock <= 0) {
            send_error("Could not allocate UDP audio socket");
            return 1;
        }

        // bind to loopback
        struct sockaddr_in bindaddr;
        ::memset(&bindaddr, 0, sizeof(bindaddr));
        bindaddr.sin_family = AF_INET;
        bindaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        bindaddr.sin_port = htons(static_cast<uint16_t>(port));
        int ok = ::bind(sock, reinterpret_cast<const struct sockaddr*>(&bindaddr), sizeof(bindaddr));
        if (ok < 0) {
            send_error("Could not bind UDP port " + to_string(port));
            return 1;
        }

        // if no extension given, use the UDP port
        if (ext.empty()) {
            ext = "udp-" + std::to_string(port);
        }
    }

    // if no extension given, use the device ID
    if (ext.empty()) {
        ext = "audio-" + std::to_string(devid);
    }

    // initialize sound card object (unique_ptr so we can recreate on MODE/AUDIO)
    std::unique_ptr<ModemSoundDevice> audio =
        makeAudioDevice(mode, devid, rate, ext, shaper, loop, no_filter, tempPath,
                        sthreads, jt9, keep, level, lead, trail, fudge, warndb);

    audio->setDepth(static_cast<short>(depth)); // persist depth from RC

    // start the sound card recording
    if ( ! audio->start()) {
        send_error("Could not start recording on audio device.");
        return 1;
    }

    // report version as first message
    send_message("VERSION", GetModemVersion());
    
    // display blank line and available commands
    std::cout << std::endl;
    send_message("COMMANDS AVAILABLE", "LEVEL, DEPTH, LEADIN, MODE, VERSION, AUDIO, DEBUG, QUIT");

    // DEBUG (gated by pipes.h):
    send_debug("Output level is " + clean_db(level) + " dB");
    send_debug("Decoding depth is " + std::to_string(depth));
    send_debug("Lead-in is " + to_string(audio->getLead()) + " samples (" +
        to_string(static_cast<int>((1000 * audio->getLead())) / static_cast<int>(rate)) + " msec)");
    if (trail > 0)
        send_debug("Trailing window adjustment is " + std::to_string(trail) + " msec");
    if (fudge != 0)
        send_debug("Frame clock offset is " + std::to_string(fudge) + " msec");

    // report the mode to the caller (pipes will inject into E:)
    send_message("MODE", mode);

    // allocate socket address for client
    struct sockaddr_in client;
    ::memset(&client, 0, sizeof(client));
    int lastport = 0;

    // read transmit messages
    string msg = "";
    fd_set rs;
    timeval tv;
    char iobuffer[64];     // stdin ingress buffer (small chunks are fine)
    bool active = false;   // reports when the sound source goes active
    uint8_t sockbuf[1024]; // the socket ingress buffer
    size_t quiet = 0;      // watches for breaks in the audio data
    int32_t nextseq = -1;  // tracks the sequence number
    size_t lastwin = 0;
    while (true) {
        // watch for stdin
        FD_ZERO(&rs);
        FD_SET(0, &rs); // stdin
        int nfds = 1;
        if (sock > 0) { FD_SET(sock, &rs); nfds = max(nfds, sock+1); }
        tv.tv_sec = 0;
        tv.tv_usec = select_timeout;
        int ct = ::select(nfds, &rs, 0, 0, &tv);

        // DEBUG: notify when sound card goes active the first time
        if ( ! active) {
            active = audio->isActive();
            if (active)
                send_debug("Sound callback is active.");
        }

        // process decoder messages and sound card mode changes
        audio->run();

        // handle select(...) errors
        if (ct < 0) {
            send_warn("select(...) returned -1, errno = " + to_string(errno));
            continue;
        }

        // UDP: if socket data, handle it
        if (sock > 0) {
            // data waiting?
            if (FD_ISSET(sock, &rs)) {
                // set up the incoming address structure
                sockaddr * addrptr = reinterpret_cast<sockaddr*>(&client);
                socklen_t addrlen = sizeof(client);

                // do the receive call
                ct = static_cast<int>(::recvfrom(sock, sockbuf, sizeof(sockbuf), 0, addrptr, &addrlen));

                // check the port number, to see if it has changed
                if (lastport && client.sin_port != lastport) {
                    send_warn("Sender port number changed; audio may be corrupt or missing");
                }

                // save the most recent port number
                lastport = client.sin_port;

                // check the length
                if ((ct - 2) % 4) {
                    send_warn("Data payload was " + to_string(ct - 2) + "bytes, but should be divisible by four (4)");
                }

                // extract the sequence field
                uint8_t *sp = sockbuf + (ct - 2);
                int32_t seq = *sp++;
                seq |= static_cast<int32_t>(*sp << 8);

                // check the sequence number
                if ((nextseq > 0) && (nextseq != seq)) {
                    send_warn(
                        "Data sequence was " + to_string(seq) +
                        ", but expected " + to_string(nextseq) +
                        "; audio may be corrupt or missing");
                }

                // compute the next expected sequence number
                nextseq = (seq + 1) & 0xFFFF; // 16-bit unsigned value

                // feed the 'audio' object
                audio->udp_audio(sockbuf, static_cast<size_t>(ct - 2));
                lastwin = static_cast<size_t>(ct - 2);

                // reset the quiet detection
                quiet = 0;
            } else {
                // keep sound handler alive with silence if UDP stalls
                suseconds_t elapsed = select_timeout - tv.tv_usec;
                quiet += static_cast<size_t>(elapsed);
                if (quiet >= max_idle) {
                    quiet = 0;
                    size_t quiet_time = lastwin ? lastwin : 256;
                    bzero(sockbuf, sizeof(sockbuf));
                    audio->udp_audio(sockbuf, quiet_time);
                }
            }
        }

        // if no stdin data, skip the rest
        if ( ! FD_ISSET(0, &rs)) {
            continue;
        }

        // read from stdin
        ct = static_cast<int>(::read(0, iobuffer, sizeof(iobuffer)));

        // if EOF, close the program
        if (ct <= 0)
            break;

        // process data from stdin
        for (int i = 0; i != ct; ++i) {
            char ch = iobuffer[i];

            // Allow common punctuation that appears in device names (e.g., ':', '/', '_', '()' '[]' ',')
            if (isalnum((unsigned char)ch) || ch==' ' || ch=='.' || ch=='-' || ch=='+' ||
                ch==':' || ch=='/' || ch=='_' || ch=='(' || ch==')' ||
                ch=='[' || ch==']' || ch==',') {
                msg += ch;
            }

            // terminate line
            if (ch == '\n') {
                // uppercase everything
                msg = my::strip(my::toUpper(msg));

                if (msg == "STOP") {
                    if (audio->cancelTransmit())
                        send_warn("Cancel transmit");
                    msg.clear();
                    continue;
                } else if (msg == "LEVEL") {
                    send_message("LEVEL", clean_db(static_cast<float>(decibels(audio->getVolume()))));
                    msg.clear();
                    continue;
                } else if (msg == "DEPTH") {
                    send_message("DEPTH", std::to_string(audio->getDepth()));
                    msg.clear();
                    continue;
                } else if (msg == "LEADIN") {
                    send_message("LEADIN", std::to_string((1000 * audio->getLead()) / rate));
                    msg.clear();
                    continue;
                } else if (msg == "VERSION") {
                    send_message("VERSION", GetModemVersion());
                    msg.clear();
                    continue;
                } else if (msg == "PURGE") {
                    audio->purge();
                    msg.clear();
                    continue;
                } else if (msg == "Q" || msg == "QUIT") {
                    goto do_exit;
                }

                // -------- DEBUG (runtime) ----------
                if (msg == "DEBUG") {
                    // Toggle our local state, push to the logging framework,
                    // and report explicit status per your request.
                    g_debug_state = !g_debug_state;
                    set_debug(g_debug_state);
                    send_ok(g_debug_state ? "DEBUG ON" : "DEBUG OFF");
                    msg.clear();
                    continue;
                }
                // -----------------------------------

                // -------- AUDIO (list or switch) ----------
                if (msg.rfind("AUDIO", 0) == 0) {
                    std::string arg = my::strip(msg.substr(5));
                    if (arg.empty()) {
                        // List devices + show current
                        std::cout << "\nAvailable audio devices:\n";
                        printAudioDevices();
                        try {
                            if (devid > 0) {
                                RtAudio probe;
                                auto info = probe.getDeviceInfo(static_cast<unsigned int>(devid));
                                std::cout << "\nCurrent input: ID=" << devid << "  \"" << info.name << "\"\n\n";
                                send_ok("AUDIO CURRENT ID=" + std::to_string(devid) + " NAME=\"" + info.name + "\"");
                            } else if (port > 0) {
                                std::cout << "\nCurrent input: UDP port " << port << "\n\n";
                                send_ok("AUDIO CURRENT UDP:" + std::to_string(port));
                            }
                        } catch (...) { /* best-effort */ }
                        msg.clear();
                        continue;
                    }
                    // switching requested
                    int newDev = -1;
                    if (isdigits(arg)) {
                        newDev = stoi(arg);
                    } else {
                        // disallow switching to UDP via AUDIO (keep AUDIO = local devices)
                        if (arg.size() > 4 && my::toLower(arg.substr(0,4)) == "udp:") {
                            send_error("AUDIO does not switch to UDP; use device argument at startup.");
                            msg.clear();
                            continue;
                        }
                        newDev = findDeviceByName(arg);
                    }

                    if (newDev <= 0) {
                        send_error("AUDIO: device not found: " + arg);
                        msg.clear();
                        continue;
                    }

                    // Compute rate if not forced
                    unsigned int newRate = rate;
                    if (!rate_forced) {
                        newRate = 0;
                        const unsigned int hi_rate = 48000;
                        try {
                            RtAudio infoSrc;
                            auto info = infoSrc.getDeviceInfo(static_cast<unsigned int>(newDev));
                            for (auto r : info.sampleRates) if (r > newRate && r <= hi_rate) newRate = r;
                            if (newRate == 0) {
                                send_error("AUDIO: no usable sample rate for device.");
                                msg.clear();
                                continue;
                            }
                        } catch (...) {
                            send_error("AUDIO: failed to probe device rates.");
                            msg.clear();
                            continue;
                        }
                    }

                    // stop, rebuild, start
                    audio->stop();
                    devid = newDev;          // update current device id
                    if (port > 0) {          // if we were in UDP mode, clear it
                        port = 0;
                    }
                    ext = "audio-" + std::to_string(devid);
                    if (!rate_forced) rate = newRate;

                    // keep current level/depth from active device
                    float curLevelDb = static_cast<float>(decibels(audio->getVolume()));
                    short curDepth   = static_cast<short>(audio->getDepth());

                    auto next = makeAudioDevice(mode, devid, rate, ext, shaper, loop, no_filter,
                                                tempPath, sthreads, jt9, keep, curLevelDb,
                                                lead, trail, fudge, warndb);
                    next->setDepth(static_cast<short>(curDepth));

                    bool ok = next->start();
                    if (!ok) {
                        send_error("AUDIO: could not start selected device.");
                        (void)audio->start(); // try to restore previous device
                        msg.clear();
                        continue;
                    }

                    // Success
                    audio = std::move(next);
                    try {
                        RtAudio probe;
                        auto info = probe.getDeviceInfo(static_cast<unsigned int>(devid));
                        send_ok("AUDIO SWITCHED ID=" + std::to_string(devid) + " NAME=\"" + info.name + "\"");
                        std::cout << "Now decoding from device ID=" << devid << "  \"" << info.name << "\" @ " << rate << " Hz\n";
                    } catch (...) {
                        send_ok("AUDIO SWITCHED ID=" + std::to_string(devid));
                    }
                    msg.clear();
                    continue;
                }
                // -------------------------------------------

                // -------- MODE switching ----------
                if (msg.rfind("MODE", 0) == 0) {
                    std::string arg = my::strip(msg.substr(4));
                    if (arg.empty()) {
                        // Show current mode
                        send_message("MODE", mode);
                        msg.clear();
                        continue;
                    }
                    std::string want = my::toUpper(arg);

                    if (want != "FT4" && want != "FT8" && want != "JT65" && want != "JT9" && want != "WSPR") {
                        if (want == "AUTO") {
                            send_error("AUTO is not supported in this build.");
                        } else {
                            send_error("Invalid MODE '" + want + "'");
                        }
                        msg.clear();
                        continue;
                    }

                    audio->stop();

                    // per-mode overrides if present in rc
                    int lead2  = (leads.find(want)  != leads.end())  ? leads[want]  : lead;
                    int trail2 = (trails.find(want) != trails.end()) ? trails[want] : trail;

                    float curLevelDb = static_cast<float>(decibels(audio->getVolume()));
                    short curDepth   = static_cast<short>(audio->getDepth());

                    auto next = makeAudioDevice(want, devid, rate, ext, shaper, loop, no_filter,
                                                tempPath, sthreads, jt9, keep, curLevelDb,
                                                lead2, trail2, fudge, warndb);
                    next->setDepth(static_cast<short>(curDepth));

                    bool ok = next->start();
                    if (!ok) {
                        send_error("Could not start recording on sound device.");
                        (void)audio->start();
                        msg.clear();
                        continue;
                    }

                    audio = std::move(next);
                    mode = want;
                    send_message("MODE", want);
                    send_ok("MODE " + want);
                    msg.clear();
                    continue;
                }
                // -----------------------------------

                size_t idx = msg.find(' ');
                if (idx == string::npos) {
                    if (my::isDigit(msg)) {
                        send_error("No message provided.");
                    } else {
                        send_error("Invalid command: " + msg);
                    }
                    msg.clear();
                    continue;
                }

                // pick off the frequency (or command)
                string freq = my::toUpper(msg.substr(0, idx));

                // and the message (or argument)
                msg = my::toUpper(my::strip(msg.substr(idx + 1)));

                // handle set commands
                if (freq == "LEVEL") {
                    float level2 = 0.0f;
                    bool adjust = false;

                    // if the new value prefixed by 'A', this is an adjustment
                    if (::toupper(msg[0]) == 'A') {
                        msg = msg.substr(1);
                        adjust = true;
                    }

                    // read the float part of the value
                    try {
                        level2 = stof(msg);
                    } catch (...) {
                        send_error("Invalid level given: " + msg);
                        continue;
                    }

                    // if this is an adjustment, add to the current level
                    if (adjust) {
                        level2 = level2 + static_cast<float>(decibels(audio->getVolume()));
                    }

                    // nearest tenth of a dB
                    level2 = ::nearbyintf(10.0f * level2) / 10.0f;

                    // clamp and set
                    if (level2 <= 0 && level2 >= -80) {
                        audio->setVolume(static_cast<float>(linear(level2)));
                        send_ok("Level now " + clean_db(level2) + " dB");
                    } else {
                        send_error("Invalid level provided; must be between -80 and 0.");
                    }
                    msg.clear();
                    continue;
                } else if (freq == "DEPTH") {
                    int depth2 = stoi(msg);
                    if (depth2 >= 1 && depth2 <= 3) {
                        audio->setDepth(static_cast<short>(depth2));
                        send_ok("Depth now " + std::to_string(depth2));
                    } else {
                        send_error("Invalid depth provided; must be 1 to 3.");
                    }
                    msg.clear();
                    continue;
                } else if (freq == "LEADIN") {
                    int lead2 = stoi(msg);
                    if (lead2 >= 0) {
                        unsigned int samples = (static_cast<unsigned int>(lead2) * rate) / 1000U;
                        audio->setLead(samples);
                        send_ok("Lead-in now " + std::to_string(lead2));
                    } else {
                        send_error("Invalid lead-in provided; must be >= 0.");
                    }
                    msg.clear();
                    continue;
                } else if ( ! ::isdigit((unsigned char)freq[0])) {
                    send_error("Unknown command: " + freq);
                    msg.clear();
                    continue;
                }

                // handle even/odd
                size_t f = 0;
                TimeSlots eo;
                try {
                    eo = getFrequencyAndSlot(freq, f);
                } catch (const std::exception &e) {
                    send_error(e.what());
                    msg = "";
                    continue;
                }

                // if it is in range, transmit
                if (f >= af_low && f <= af_high) {
                    send_ok("Send @ " + std::to_string(static_cast<int>(f)) + "Hz: '" + msg + "'");
                    audio->transmit(msg, static_cast<float>(f), eo);
                    msg = "";
                } else {
                    // if not, complain
                    send_error("Frequency out of range: " + to_string(f));
                }

                // clear the message buffer
                msg.clear();
            }
        }
    }

do_exit: // clean up, save config, and exit

    // stop the sound card
    audio->stop();

    // save the RC file
    {
        ofstream rc(get_rc_path());
        rc << "DEVICE " << devname << endl;
        rc << "LEVEL "  << clean_db(static_cast<float>(decibels(audio->getVolume()))) << endl;
        rc << "DEPTH "  << audio->getDepth() << endl;

        // store the lead-out value for the current mode
        if (lead >= 0) {
            string key = "LEADIN." + mode + ' ';
            rc << key << ((1000 * audio->getLead()) / rate) << endl;
        }

        // store the list of lead-out values for the other modes
        for (timings_t::const_iterator i = leads.begin(); i != leads.end(); ++i) {
            if ((i->first) != mode) {
                string key = "LEADIN." + (i->first) + ' ';
                rc << key << leads[i->first] << endl;
            }
        }

        // store the lead-out value for the current mode
        if (trail >= 0) {
            string key = "TRAIL." + mode + ' ';
            rc << key << static_cast<int>(1000 * audio->getTrail()) << endl;
        }

        // store the list of lead-out values for the other modes
        for (timings_t::const_iterator i = trails.begin(); i != trails.end(); ++i) {
            if ((i->first) != mode) {
                string key = "TRAIL." + (i->first) + ' ';
                rc << key << trails[i->first] << endl;
            }
        }
    }

    // done
    return 0;
}

// EOF