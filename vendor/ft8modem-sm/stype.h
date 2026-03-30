#ifndef __STYPE_H
#define __STYPE_H

#include <cctype>
#include <string>
#include <list>

namespace my {

	inline std::string ltrim (const std::string &s) {
		auto si = s.begin();
		while ((si != s.end()) && std::isspace(static_cast<unsigned char>(*si))) ++si;
		return std::string(si, s.end());
	}

	inline std::string rtrim (const std::string &s) {
		auto si = s.begin();
		auto ei = s.begin();
		while (si != s.end()) {
			if (!std::isspace(static_cast<unsigned char>(*si++)))
				ei = si;
		}
		return s.substr(0, static_cast<std::string::size_type>(ei - s.begin()));
	}

	inline std::string trim   (const std::string &s) { return ltrim(rtrim(s)); }
	inline std::string lstrip (const std::string &s) { return ltrim(s); }
	inline std::string rstrip (const std::string &s) { return rtrim(s); }
	inline std::string strip  (const std::string &s) { return ltrim(rtrim(s)); }

	inline std::string to_something (const std::string &s, int (*func)(int)) {
		std::string rtn(s);
		for (auto &ch : rtn) ch = static_cast<char>(func(static_cast<unsigned char>(ch)));
		return rtn;
	}

	inline bool is_something (const std::string &s, int (*func)(int)) {
		for (char ch : s) {
			if (func(static_cast<unsigned char>(ch)) == 0) return false;
		}
		return true;
	}

	inline std::string toLower (const std::string &s) { return to_something (s, std::tolower); }
	inline std::string toUpper (const std::string &s) { return to_something (s, std::toupper); }

	inline bool isAlnum  (const std::string &s) { return is_something (s, std::isalnum); }
	inline bool isAlpha  (const std::string &s) { return is_something (s, std::isalpha); }
	// Portable ASCII check
	inline bool isAscii  (const std::string &s) {
		for (char ch : s) {
			if (static_cast<unsigned char>(ch) > 0x7F) return false;
		}
		return true;
	}
	#ifdef __USE_GNU
	inline bool isBlank  (const std::string &s) { return is_something (s, std::isblank); }
	#endif
	inline bool isControl(const std::string &s) { return is_something (s, std::iscntrl); }
	inline bool isDigit  (const std::string &s) { return is_something (s, std::isdigit); }
	inline bool isGraph  (const std::string &s) { return is_something (s, std::isgraph); }
	inline bool isLower  (const std::string &s) { return is_something (s, std::islower); }
	inline bool isPrint  (const std::string &s) { return is_something (s, std::isprint); }
	inline bool isPunct  (const std::string &s) { return is_something (s, std::ispunct); }
	inline bool isSpace  (const std::string &s) { return is_something (s, std::isspace); }
	inline bool isUpper  (const std::string &s) { return is_something (s, std::isupper); }
	inline bool isXDigit (const std::string &s) { return is_something (s, std::isxdigit); }

	std::list<std::string> split (const std::string &s);
	std::list<std::string> split (const std::string &s, std::string::value_type delim, bool strip = false);
}

#endif /* __STYPE_H */