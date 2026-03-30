/*
 *
 *
 *    cpucores.cc
 *
 *    Count the number of CPU cores on a PC.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>
#include "cores.h"

using namespace KK5JY::DSP;
using namespace std;

int main(int argc, char**argv) {
	if (argc > 1) {
		cerr << endl;
		cerr << "Usage: " << argv[0] << endl;
		cerr << endl;
		return 1;
	}

	cout << "Machine has " << cpu_cores() << " CPU cores." << endl;

	return 0;
}

// EOF
