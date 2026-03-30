/*
 *
 *
 *    test_decode.cc
 *
 *    Simple test stand for the Decode module.
 *
 *    Copyright (C) 2023 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>
#include <unistd.h>
#include "decode.h"

using namespace KK5JY::FT8;
using namespace std;

int main(int argc, char**argv) {
    if (argc != 2) {
        cerr << "Please supply input WAV file name." << endl;
        return 1;
    }

    // copy input WAV data into the decode module
    SoundFile input(argv[1]);
    Decode<double> decode("ft8", "230101_050500.wav", ".", KK5JY::FT8::abstime());

    double iobuffer[128];
    sf_count_t ct = 0;
    size_t total = 0;

    while ((ct = input.read(iobuffer, static_cast<sf_count_t>(128))) != 0) {
        decode.write(iobuffer, static_cast<size_t>(ct));
        total += static_cast<size_t>(ct);
    }
    cerr << "Wrote " << total << " samples to WAV." << endl;

    // run the decode module
    bool ok = decode.startDecode();
    cerr << "Decode object started: " << ok << endl;

    // wait for it to finish
    while (!decode.isDone()) {
        sleep(1);
    }

    // read out the results
    cout << "Results:" << endl;
    std::deque<std::string> results;
    size_t ct_decodes = decode.getDecodes(results);
    (void)ct_decodes; // not used below, but kept for clarity

    for (const auto& line : results) {
        cout << " --> " << line << endl;
    }
}