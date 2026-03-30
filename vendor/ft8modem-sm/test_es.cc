/*
 *
 *
 *    test_es.cc
 *
 *    Simple test stand for the smoother.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>
#include <cmath>   // std::fabs

#include "es.h"

using namespace KK5JY::DSP;
using namespace std;

typedef float sample_t;

int main(int argc, char**argv) {
    (void)argc; (void)argv;

    float A = 0.1f;
    size_t total = 2048;
    size_t k = 128;

    sample_t v = 0, last = 0;
    size_t ct = 0;
    sample_t increment = 1;

    IFilter<sample_t> *filter = new Smoother<sample_t>(A);

    for (size_t i = 0; i != total; ++i) {
        sample_t filtered = filter->run(v);
        if (filtered != last)
            cout << v << " -> " << filtered << endl;
        last = filtered;

        if (++ct >= k) {
            ct = 0;
            if (std::fabs(v) >= 3.0f)
                increment = static_cast<sample_t>(-increment);
            v += increment;
        }
    }
}