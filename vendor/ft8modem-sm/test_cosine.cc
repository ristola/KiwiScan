/*
 *
 *
 *    test_cosine.cc
 *
 *    Simple test stand for the raised cosine shaper.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>

#include "shape.h"

using namespace KK5JY::DSP;
using namespace std;

typedef float sample_t;

int main(int /*argc*/, char** /*argv*/) {
    size_t N = 32;
    size_t total = 2048;
    size_t k = 128;

    sample_t v = 0, last = 0;
    size_t ct = 0;
    sample_t increment = 1;

    IFilter<sample_t> *filter = new KeyShaper<sample_t>(N);

    for (size_t i = 0; i != total; ++i) {
        sample_t filtered = filter->run(v);
        if (filtered != last)
            cout << filtered << endl;
        last = filtered;

        if (++ct >= k) {
            ct = 0;
            if (fabs(v) >= 3)
                increment *= -1;
            v += increment;
        }
    }

    delete filter;
    return 0;
}