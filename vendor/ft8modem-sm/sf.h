/*
 *
 *
 *    sf.h
 *
 *    C++ wrapper around libsndfile methods.
 *
 *    Copyright (C) 2006-2024 by Matt Roberts,
 *    All rights reserved.
 *
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */

#ifndef __SF_H
#define __SF_H

#include <string>
#include <exception>

#include <sndfile.h>
#include <stdint.h>
#include <string.h>

class SoundFile {
	public:
		class exception : public std::exception {
			private:
				std::string msg;
			public:
				exception(const std::string &s) : msg(s) { /* noop */ };
				~exception(void) throw() { /* noop */ };

				const char * what (void) const throw() { return msg.c_str(); };
		};

		enum major_formats {
			wav  = SF_FORMAT_WAV,
			aiff = SF_FORMAT_AIFF,
			au   = SF_FORMAT_AU,
			raw  = SF_FORMAT_RAW,
		};

		enum minor_formats {
			u8  = SF_FORMAT_PCM_U8,
			s8  = SF_FORMAT_PCM_S8,
			s16 = SF_FORMAT_PCM_16,
			s24 = SF_FORMAT_PCM_24,
			s32 = SF_FORMAT_PCM_32,

			flt = SF_FORMAT_FLOAT,
			dbl = SF_FORMAT_DOUBLE,

			ulaw = SF_FORMAT_ULAW,
			alaw = SF_FORMAT_ALAW,
		};

		enum byte_order {
			defl   = SF_ENDIAN_FILE,
			little = SF_ENDIAN_LITTLE,
			big    = SF_ENDIAN_BIG,
			cpu    = SF_ENDIAN_CPU,
		};

	private:
		SNDFILE *sf_ptr;
		SF_INFO  sf_info;

	protected:
		SNDFILE *sfp(void) const throw() { return sf_ptr; };

	public:
		// ctors
		SoundFile(void) throw();          // does not open any file
		SoundFile(const std::string &fn); // open for read
		SoundFile(                        // open for write
				const std::string &fn,
				int new_rate,
				int new_channels,
				major_formats new_maj_format,
				minor_formats new_min_format,
				byte_order new_order = defl
				);

		// dtor
		~SoundFile(void) throw() { close(); }

		// open/close
		void close(void) throw();
		void open(const std::string &fn); // open for read
		void openraw(                     // open for read
				const std::string &fn,
				int new_rate,
				minor_formats new_min_format,
				byte_order new_order = defl
				);
		void open(                        // open for write
				const std::string &fn,
				int new_rate,
				int new_channels,
				major_formats new_maj_format,
				minor_formats new_min_format,
				byte_order new_order = defl
				);

		// check to see if file open
		bool isopen(void) throw() { return (sf_ptr != NULL); };

	public: // accessor functions
		sf_count_t frames   (void) const throw() { return sf_info.frames;     };
		int        rate     (void) const throw() { return sf_info.samplerate; };
		int        channels (void) const throw() { return sf_info.channels;   };
		int        sections (void) const throw() { return sf_info.sections;   };
		bool       seekable (void) const throw() { return sf_info.seekable;   };

	public:

		// read/write functions (single sample)
		sf_count_t read (short  &ref) { return sf_read_short(sf_ptr, &ref, 1); };
		sf_count_t read (int    &ref) { return sf_read_int(sf_ptr, &ref, 1); };
		sf_count_t read (float  &ref) { return sf_read_float(sf_ptr, &ref, 1); };
		sf_count_t read (double &ref) { return sf_read_double(sf_ptr, &ref, 1); };

		sf_count_t write (short  val) { return sf_write_short(sf_ptr, &val, 1); };
		sf_count_t write (int    val) { return sf_write_int(sf_ptr, &val, 1); };
		sf_count_t write (float  val) { return sf_write_float(sf_ptr, &val, 1); };
		sf_count_t write (double val) { return sf_write_double(sf_ptr, &val, 1); };

		// read/write functions (by sample)
		sf_count_t read (short  *ptr, sf_count_t items) { return sf_read_short(sf_ptr, ptr, items); };
		sf_count_t read (int    *ptr, sf_count_t items) { return sf_read_int(sf_ptr, ptr, items); };
		sf_count_t read (float  *ptr, sf_count_t items) { return sf_read_float(sf_ptr, ptr, items); };
		sf_count_t read (double *ptr, sf_count_t items) { return sf_read_double(sf_ptr, ptr, items); };

		sf_count_t write (short  *ptr, sf_count_t items) { return sf_write_short(sf_ptr, ptr, items); };
		sf_count_t write (int    *ptr, sf_count_t items) { return sf_write_int(sf_ptr, ptr, items); };
		sf_count_t write (float  *ptr, sf_count_t items) { return sf_write_float(sf_ptr, ptr, items); };
		sf_count_t write (double *ptr, sf_count_t items) { return sf_write_double(sf_ptr, ptr, items); };

		// readf/writef functions (by frame)
		sf_count_t readf (short  *ptr, sf_count_t frames) { return sf_readf_short(sf_ptr, ptr, frames); };
		sf_count_t readf (int    *ptr, sf_count_t frames) { return sf_readf_int(sf_ptr, ptr, frames); };
		sf_count_t readf (float  *ptr, sf_count_t frames) { return sf_readf_float(sf_ptr, ptr, frames); };
		sf_count_t readf (double *ptr, sf_count_t frames) { return sf_readf_double(sf_ptr, ptr, frames); };

		sf_count_t writef (short  *ptr, sf_count_t frames) { return sf_writef_short(sf_ptr, ptr, frames); };
		sf_count_t writef (int    *ptr, sf_count_t frames) { return sf_writef_int(sf_ptr, ptr, frames); };
		sf_count_t writef (float  *ptr, sf_count_t frames) { return sf_writef_float(sf_ptr, ptr, frames); };
		sf_count_t writef (double *ptr, sf_count_t frames) { return sf_writef_double(sf_ptr, ptr, frames); };

		// raw I/O functions
		sf_count_t read_raw (uint8_t *ptr, sf_count_t items) { return sf_read_raw(sf_ptr, ptr, items); };
		sf_count_t write_raw (uint8_t *ptr, sf_count_t items) { return sf_write_raw(sf_ptr, ptr, items); };

		// string I/O functions
		//const char *read_string  (const char *ptr) { return sf_get_string(sf_ptr); };
		//sf_count_t  write_string (const char *ptr) { return sf_set_string(sf_ptr, ptr); };

		// seek functions
		sf_count_t seek (sf_count_t frames, int whence) { return sf_seek(sf_ptr, frames, whence); };
};


/*
 *
 *   SoundFile::SoundFile()
 *
 */

inline SoundFile::SoundFile (void) throw() : sf_ptr(NULL) {
	// zero out the sf_info structure
	memset(&sf_info, 0, sizeof(sf_info));
}


/*
 *
 *   SoundFile::SoundFile(fn, ...)
 *
 */

inline SoundFile::SoundFile(
		const std::string &fn,
		int new_rate,
		int new_channels,
		major_formats new_maj_format,
		minor_formats new_min_format,
		byte_order new_order
		) : sf_ptr(NULL) {
	// just call open
	open(fn, new_rate, new_channels, new_maj_format, new_min_format, new_order);
}



/*
 *
 *   SoundFile::open(fn, ...) - write
 *
 */

inline void SoundFile::open(
		const std::string &fn,
		int new_rate,
		int new_channels,
		major_formats new_maj_format,
		minor_formats new_min_format,
		byte_order new_order
		) {
	// call close, just in case
	close();

	// zero out the sf_info structure
	memset(&sf_info, 0, sizeof(sf_info));

	// initialize the sf_info structures
	sf_info.samplerate = new_rate;
	sf_info.channels   = new_channels;
	sf_info.format     = (new_maj_format | new_min_format | new_order);

	// open the file
	if (fn == "" || fn == "-") {
		sf_ptr = sf_open_fd(1, SFM_WRITE, &sf_info, 1);
	} else {
		sf_ptr = sf_open(fn.c_str(), SFM_WRITE, &sf_info);
	}

	// throw exception on error
	if (!sf_ptr)
		throw exception("Could not open file (WRITE): " + fn);
}


/*
 *
 *   SoundFile::SoundFile(fn)
 *
 */

inline SoundFile::SoundFile (const std::string &fn) : sf_ptr(NULL) {
	// just call 'open'
	open(fn);
}


/*
 *
 *   SoundFile::open(fn) - read
 *
 */

inline void SoundFile::open (const std::string &fn) {
	// call close, just in case
	close();

	// zero out the sf_info structure
	memset(&sf_info, 0, sizeof(sf_info));

	// open the file
	if (fn == "" || fn == "-") {
		sf_ptr = sf_open_fd(0, SFM_READ, &sf_info, 1);
	} else {
		sf_ptr = sf_open(fn.c_str(), SFM_READ, &sf_info);
	}

	// throw exception on error
	if (!sf_ptr)
		throw exception("Could not open file (READ): " + fn);
}


/*
 *
 *   SoundFile::openraw(fn, ...) - read
 *
 */

inline void SoundFile::openraw(
		const std::string &fn,
		int new_rate,
		minor_formats new_min_format,
		byte_order new_order
		) {
	// call close, just in case
	close();

	// zero out the sf_info structure
	memset(&sf_info, 0, sizeof(sf_info));

	// initialize the sf_info structures
	sf_info.samplerate = new_rate;
	sf_info.channels   = 1;
	sf_info.format     = (SF_FORMAT_RAW | new_min_format | new_order);

	// open the file
	if (fn == "" || fn == "-") {
		sf_ptr = sf_open_fd(0, SFM_READ, &sf_info, 1);
	} else {
		sf_ptr = sf_open(fn.c_str(), SFM_READ, &sf_info);
	}

	// throw exception on error
	if (!sf_ptr)
		throw exception("Could not open file (READ): " + fn);
}

/*
 *
 *   SoundFile::close()
 *
 */

inline void SoundFile::close(void) throw() {
	// if the file is open, close it
	if (sf_ptr)
		sf_close(sf_ptr);

	// zero out the sf_ptr pointer
	sf_ptr = NULL;

	// zero out the sf_info structure
	memset(&sf_info, 0, sizeof(sf_info));
}

#endif /* __SF_H */

// EOF: sf.h
