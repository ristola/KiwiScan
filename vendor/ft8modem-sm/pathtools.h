/*
 *
 *
 *    pathtools.h
 *
 *    Basic path query functions.
 *
 *    Copyright (C) 2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */


#ifndef __KK5JY_PATH_TOOLS_H
#define __KK5JY_PATH_TOOLS_H

// system includes
#include <string>
#include <unistd.h>
#include <sys/stat.h>

namespace KK5JY {
	namespace Path {
		//
		//  return true iff 'path' is a link
		//
		inline bool isLink(const std::string &path) {
			struct stat sb;
			if (::stat(path.c_str(), &sb) == 0 && (sb.st_mode & S_IFLNK))
				return true;
			return false;
		}

		//
		//  return true iff 'path' is a directory
		//
		inline bool isDir(const std::string &path) {
			struct stat sb;
			if (::stat(path.c_str(), &sb) == 0 && (sb.st_mode & S_IFDIR))
				return true;
			return false;
		}

		//
		//  return true iff 'path' is a regular file
		//
		inline bool isFile(const std::string &path) {
			struct stat sb;
			if (::stat(path.c_str(), &sb) == 0 && (sb.st_mode & S_IFREG))
				return true;
			return false;
		}
	}
}

#endif // __KK5JY_PATH_TOOLS_H
