/*
 *
 *
 *    af2udp.cc
 *
 *    Read pipe data from standard input, and write as sequenced UDP frames.
 *    Intended for piping rtl_fm data to ft8modem.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>
#include <string>
#include <cstring>
#include <chrono>
#include <thread>
#include <vector>

#include <errno.h>
#include <unistd.h>
#include <sys/types.h>

// for socket/UDP support
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>

#include "version.h"

using namespace std;

// default read window size
#define DEFAULT_WINDOW (256)

//
//  usage(...)
//
static void usage(const std::string &argv0) {
    cerr << endl;
    cerr << "Usage: " << argv0 << " <port> [<win>] [<rate>]" << endl;
    cerr << endl;
    cerr << "    Read S16_LE single-channel audio data from standard input," << endl;
    cerr << "    packetize it into UDP frames, and send them to ft8modem." << endl;
    cerr << endl;
    cerr << "    Options include:" << endl;
    cerr << "         <port> is the local UDP port where 'ft8modem' is listening for audio." << endl;
    cerr << "         <win>  is the read window size; default is " << DEFAULT_WINDOW << " bytes." << endl;
    cerr << "         <rate> paces outgoing frames for a given sample rate in Hz." << endl;
    cerr << endl;
    exit(1);
}

//
//  main(...)
//
int main (int argc, char ** argv) {
    if (argc == 1 || argc > 4) {
        usage(argv[0]);
    }

    if (strcmp(argv[1], "-v") == 0) {
        cerr << "af2udp version " << KK5JY::FT8::GetModemVersion() << endl;
        return 0;
    }

    // defaults
    int win = DEFAULT_WINDOW;
    int port = 0;
    int rate = 0;

    // read options
    if (argc >= 2)
        port = atoi(argv[1]);
    if (argc == 3)
        win = atoi(argv[2]);
    if (argc >= 4)
        rate = atoi(argv[3]);

    if (win <= 0) {
        cerr << "Error: Window must be greater than zero." << endl;
        return 1;
    }

    if (port <= 0 || port > 65535) {
        cerr << "Error: Port must be greater than zero, and less than 65536." << endl;
        return 1;
    }

    if (rate < 0) {
        cerr << "Error: Sample rate must not be negative." << endl;
        return 1;
    }

    // build the target address structure
    struct sockaddr_in addr;
    ::memset(&addr, 0, sizeof(addr)); // shouldn't be needed for C++, but...
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    std::vector<unsigned char> buffer(static_cast<size_t>(win) + 2U);
    uint16_t seq = 0;
    int sock = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock < 0) {
        cerr << "Error: unable to create socket: " << strerror(errno) << endl;
        return 1;
    }
    std::chrono::steady_clock::time_point next_send = std::chrono::steady_clock::now();

    while (true) {
        ssize_t ct = ::read(0, buffer.data(), static_cast<size_t>(win));
        if (ct <= 0)
            return 0;

        // append sequence
        buffer[static_cast<size_t>(ct)]     = static_cast<unsigned char>(seq & 0xFF);
        buffer[static_cast<size_t>(ct) + 1] = static_cast<unsigned char>((seq & 0xFF00) >> 8);
        ++seq;

        // send the frame
        ssize_t to_send = ct + 2;
        ssize_t result = ::sendto(sock,
                                  buffer.data(),
                                  static_cast<size_t>(to_send),
                                  0,
                                  reinterpret_cast<sockaddr*>( & addr),
                                  static_cast<socklen_t>(sizeof(sockaddr_in)));
        if (result != to_send) {
            cerr << "Warning: Tried to send " << to_send << " bytes, but only sent " << result << " bytes." << endl;
            cerr.flush();
        }

        if (rate > 0) {
            const double seconds = static_cast<double>(ct) / (2.0 * static_cast<double>(rate));
            next_send += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::duration<double>(seconds)
            );
            auto now = std::chrono::steady_clock::now();
            if (next_send > now) {
                std::this_thread::sleep_until(next_send);
            } else {
                next_send = now;
            }
        }
    }
}

// EOF: af2udp.cc