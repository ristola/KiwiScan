/*
 *
 *
 *    test_decode.cc
 *
 *    Simple test stand for the clock module.
 *
 *    Copyright (C) 2023 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>
#include <unistd.h>
#include "clock.h"

using namespace KK5JY::FT8;
using namespace std;

int main(int argc, char**argv) {
    (void)argc; // silence unused warnings
    (void)argv;

    FrameClock clock;
    cout << "FrameClock::offset() = " << clock.offset() << endl;

    // run abstime() in a loop
    while (true) {
        cout
            << timestring()
            << "; abstime() => " << fixed << abstime()
            << "; seconds(7.5) = " << clock.seconds(7.5)
            << "; seconds(15) = " << clock.seconds(15)
            << "; seconds(60) = " << clock.seconds(60)
            << "; seconds(120) = " << clock.seconds(120)
            << endl;
        usleep(250000);
    }
}