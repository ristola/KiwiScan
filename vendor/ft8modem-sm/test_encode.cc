/*
 *
 *
 *    test_encode.cc
 *
 *    Simple test stand for the encode.h code.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#include <iostream>
#include "encode.h"

using namespace KK5JY::FT8;
using namespace std;

int count_syms(const std::string &s) {
	int result = 0;
	for (std::string::const_iterator i = s.begin(); i != s.end(); ++i) {
		if ((*i) >= '0')
			++result;
	}
	return result;
}

int main(int argc, char **argv) {
	if (argc != 3) {
		cerr << "Usage: " << argv[0] << " <mode> <text>" << endl;
		return 1;
	}
	
	// read the command line
	string mode = argv[1];
	string text = argv[2];

	// do it
	string result = encode(mode, text);
	cout
		<< "Encode '" << argv[2] << "' in mode '" << argv[1]
		<< "' returned " << count_syms(result) << " symbols." << endl;
	if (result.size())
		cout << result << endl;
	cout << endl;
}
