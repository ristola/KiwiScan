/*
 *
 *
 *    decode.h
 *
 *    Decode task module.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 *
 */


#ifndef __KK5JY_FT8_DECODE_H
#define __KK5JY_FT8_DECODE_H

// C++ STL types
#include <string>
#include <deque>
#include <algorithm>
#include <limits>

// for popen(...) and file I/O
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

// for chmod(...)
#include <sys/stat.h>

// for WAV file interface
#include "sf.h"

// string operations
#include "stype.h"

// clock operations
#include "clock.h"

// CPU core counter
#include "cores.h"

// mutual exclusion locks
#include "locker.h"

// for path queries
#include "pathtools.h"

// sending messages to caller
#include "pipes.h"

// for DEBUG only
#ifdef VERBOSE_DEBUG
#include <iostream>
#include <typeinfo>
#endif

// the highest frequency to decode
#define FT8_DECODE_HIGH ("3100") // Hz


namespace KK5JY {
	namespace FT8 {
		// the worker thread
		void *decoder_thread(void *parent);

		//
		//  class DecodeBase
		//
		//  NOTE: This is the base class of the template class Decode<T>.
		//        It contains enough of the data members for the background
		//        thread to interact with a Decode<T> through a non-template
		//        pointer type.
		//
		class DecodeBase {
			protected:
				// common/shared data elements
				std::string m_Mode; // mode, e.g., "ft8"
				std::string m_Path; // path to the WAV file
				std::string m_Temp; // the temp folder
				std::string m_Decoder; // the decoder, 'jt9' by default
				std::deque<std::string> m_Buffer; // the decode buffer
				volatile double m_DecodeStartTime;
				volatile double m_CaptureStartTime;
				volatile double m_Start, m_Stop;
				volatile size_t m_Samples;
				volatile size_t m_Decodes;
				volatile short m_Threads;
				volatile short m_Depth;
				volatile bool m_Done;
				volatile bool m_Keep;

				my::mutex m_Mutex;

				const size_t JT9_RATE  = 12000; // Hz
				const size_t WSPR_RATE = 12000; // Hz

				// allow thread worker to access private data
				friend void *decoder_thread(void *parent);

			public:
				DecodeBase();
				virtual ~DecodeBase();

				double GetDecodeStart() const { return m_DecodeStartTime; }
				double GetCaptureStart() const { return m_CaptureStartTime; }

				short setThreads(short t) { return (m_Threads = t); }
				short getThreads() { return m_Threads; }

				bool setKeep(bool keep) { return (m_Keep = keep); }
				bool getKeep() { return m_Keep; }

				std::string setDecoder(const std::string &d) { return (m_Decoder = d); }
				std::string getDecoder(void) { return m_Decoder; }

				double GetRuntime() const { return m_Stop - m_Start; }
		};


		//
		//  DecodeBase::ctor
		//
		inline DecodeBase::DecodeBase()
			: m_Start(0),
			  m_Stop(0),
			  m_Decodes(0),
			  m_Depth(1),
			  m_Done(false),
			  m_Keep(false) {
			// Clamp size_t -> short to avoid narrowing warnings
			const size_t cores = KK5JY::DSP::cpu_cores();
			const size_t smax  = static_cast<size_t>(std::numeric_limits<short>::max());
			m_Threads = static_cast<short>(std::min(cores, smax));
		}


		//
		//  DecodeBase::dtor
		//
		inline DecodeBase::~DecodeBase() {
			if (KK5JY::Path::isFile(m_Path) && ! m_Keep)
				::unlink(m_Path.c_str());
		}


		//
		//  class Decode<T>
		//
		template <typename T>
		class Decode : public DecodeBase {
			private:
				SoundFile *m_WAV;

			public:
				Decode(
					const std::string &mode,
					const std::string &wav_path,
					const std::string &tmp_path,
					double start,
					short depth = 2);
				virtual ~Decode();

			public:
				// add more WAV data to be decoded
				size_t write(T* buffer, size_t count);

				// close the WAV file and start the decoding process
				bool startDecode();

				// copy the decodes into the buffer provided
				size_t getDecodes(std::deque<std::string> &buffer);

				// returns true iff the decoder is finished
				bool isDone() const volatile { return m_Done; }

				// returns the number of decodes so far (or total, if isDone() == true)
				size_t count() const volatile { return m_Decodes; }

				// aborts capture operation and deletes temp file(s)
				void clear();
		};


		template <typename T>
		inline Decode<T>::Decode(
				const std::string &mode,
				const std::string &wav_path,
				const std::string &tmp_path,
				double start,
				short depth) {
			// store the file name and start time
			m_Mode = my::strip(my::toLower(mode));
			if (m_Mode == "wspr")
				m_Decoder = "wsprd";
			else
				m_Decoder = "jt9";
			m_Path = wav_path;
			m_Temp = tmp_path;
			m_CaptureStartTime = start;
			m_DecodeStartTime = 0;
			m_Depth = depth;
			m_Samples = 0;
			m_Done = false;

			// if the file already exists, delete it; this can happen
			//    when using the -k option of 'ft8modem'
			if (KK5JY::Path::isFile(wav_path)) {
				KK5JY::FT8::send_warning("Decoder file already exists: " + wav_path);
				::unlink(wav_path.c_str()); // remove the file before replacing it
			}

			// open the sound file
			m_WAV = new SoundFile(wav_path, static_cast<int>(JT9_RATE), 1,
                SoundFile::major_formats::wav,
				SoundFile::minor_formats::s16);
			::chmod(wav_path.c_str(), 0600); // only owner can r/w the WAV file
		}


		template <typename T>
		inline Decode<T>::~Decode() {
			if (m_WAV) {
				SoundFile *toDelete = m_WAV;
				toDelete->close();
				delete toDelete;
				m_WAV = 0;
			}
		}


		template <typename T>
		inline size_t Decode<T>::write(T* buffer, size_t count) {
			// sanity checks
			if (m_Done || ! m_WAV)
				return 0;

			// update the sample counter
			m_Samples += count;

			// write data to the file (convert size_t <-> sf_count_t safely)
			const sf_count_t to_write = static_cast<sf_count_t>(count);
			const sf_count_t written  = m_WAV->write(buffer, to_write);
			return static_cast<size_t>(written);
		}


		//
		//  clear()
		//
		template <typename T>
		inline void Decode<T>::clear() {
			if ( ! m_WAV) {
				return;
			}

			// close the wave file
			SoundFile *toDelete = m_WAV;
			toDelete->close();
			delete toDelete;
			m_WAV = 0;
			m_Samples = 0;
			m_Start = m_Stop = 0;

			// remove the wave file
			if ( ! m_Keep)
				::unlink(m_Path.c_str());
			m_Done = true;
		}


		//
		//  startDecode()
		//
		template <typename T>
		inline bool Decode<T>::startDecode() {
			if ( ! m_WAV) {
				return false;
			}

			// calculate the size of a full frame
			size_t full_frame = 0;
			if (m_Mode == "ft8")
				full_frame = static_cast<size_t>(14.9 * JT9_RATE);
			else if (m_Mode == "ft4")
				full_frame = static_cast<size_t>(7.4  * JT9_RATE);
			else if (m_Mode == "jt65")
				full_frame = static_cast<size_t>(60.0 * JT9_RATE);
			else if (m_Mode == "jt9")
				full_frame = static_cast<size_t>(60.0 * JT9_RATE);
			else if (m_Mode == "wspr")
				full_frame = static_cast<size_t>(120.0 * WSPR_RATE);

			// if there isn't enough data, just give up now
			if (m_Samples < (full_frame / 3)) {
				KK5JY::FT8::send_trace(
					"Skipping decode: insufficient samples " + std::to_string(m_Samples) +
					" of " + std::to_string(full_frame) + " for " + m_Path
				);
				clear();
				return true;
			}

			// pad the WAV file if needed to make them all the same length
			const size_t iosz = 120;
			T pad[iosz];
			T *pp = pad;
			T * const ep = pp + iosz;
			while (pp != ep)
				*pp++ = static_cast<T>(0.0);
			while (m_Samples < full_frame) {
				m_WAV->write(pad, static_cast<sf_count_t>(iosz));
				m_Samples += iosz;
			}

			// capture the start time
			m_DecodeStartTime = abstime();

			// close the wave file
			size_t captured_samples = m_Samples;
			SoundFile *toDelete = m_WAV;
			toDelete->close();
			delete toDelete;
			m_WAV = 0;
			m_Samples = 0;

			KK5JY::FT8::send_trace(
				"Starting decode: captured_samples=" + std::to_string(captured_samples) +
				" padded_frame=" + std::to_string(full_frame) +
				" path=" + m_Path + " temp=" + m_Temp
			);

			// build new thread attributes
			::pthread_attr_t attrs;
			pthread_attr_init(&attrs);
			// set the thread type as 'detached' so we don't have to 'join' it later
			pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

			// start the thread
			pthread_t id = 0;
			int result = pthread_create(&id, &attrs, decoder_thread, static_cast<DecodeBase*>(this));

			// return result
			return id != 0 && result == 0;
		}


		template <typename T>
		inline size_t Decode<T>::getDecodes(std::deque<std::string> &buffer) {
			// this function is a critical section
			my::locker lock(m_Mutex);

			// if there is nothing to do, exit quickly
			if (m_Buffer.empty())
				return 0;

			// copy from our buffer into the caller's
			size_t result = 0;
			std::deque<std::string>::const_iterator i;
			for (i = m_Buffer.begin(); i != m_Buffer.end(); ++i) {
				buffer.push_back(*i);
				++result; // total this call
				++m_Decodes; // total overall
			}

			// clear the local buffer so we can't fetch it again
			m_Buffer.clear();

			// return number of items copied
			return result;
		}


		//
		//  returns true iff 'line' contains a decode report
		//
		inline static bool isValidDecode(const std::string &line) {
			if (line.size() < 4)
				return false;
			// Cast to unsigned char before isdigit to avoid UB/warnings
			const unsigned char c0 = static_cast<unsigned char>(line[0]);
			const unsigned char c1 = static_cast<unsigned char>(line[1]);
			if (isdigit(c0) && isdigit(c1)) // ft8, ft4, jt65
				return true;
			if (line.substr(0, 4) == "****") // jt9
				return true;
			return false;
		}


		//
		//  the worker thread
		//
		inline void *decoder_thread(void *parent) {
			#ifdef VERBOSE_DEBUG
			std::cerr << "thread start" << std::endl;
			#endif

			DecodeBase *decode = reinterpret_cast<DecodeBase*>(parent);
			if ( ! decode)
				pthread_exit(0);

			try {
				#ifdef VERBOSE_DEBUG
				std::cerr << "try" << std::endl;
				std::cerr << "file is " << decode->m_Path << std::endl;
				#endif

				// start building the command line
				std::string cmd = decode->m_Decoder;
				if (cmd.empty()) {
					if (decode->m_Mode == "wspr")
						cmd = "wsprd";
					else
						cmd = "jt9";
				}

				// add the mode argument
				bool is_jt9 = false;
				if (decode->m_Mode == "ft8") {
					cmd += " --ft8 ";
					is_jt9 = true;
				} else if (decode->m_Mode == "ft4") {
					cmd += " --ft4 ";
					is_jt9 = true;
				} else if (decode->m_Mode == "jt65") {
					cmd += " --jt65 ";
					is_jt9 = true;
				} else if (decode->m_Mode == "jt9") {
					cmd += " --jt9 ";
					is_jt9 = true;
				}

				// add data/temp (writable) path
				cmd += " -a ";
				cmd += decode->m_Temp;

				// add jt9(1)-specific options
				if (is_jt9) {
					// add data/temp path
					cmd += " -t ";
					cmd += decode->m_Temp;

					// add decoding depth
					cmd += " -d ";
					cmd += static_cast<char>(decode->m_Depth + '0');

					// add high decode limit
					cmd += " -H ";
					cmd += FT8_DECODE_HIGH;

					// add thread count
					if (decode->m_Threads >= 2) {
						cmd += " -m ";
						cmd += std::to_string(static_cast<int>(decode->m_Threads));
					}
				}

				cmd += ' ';
				cmd += decode->m_Path;
				decode->m_Start = KK5JY::FT8::abstime(); // record start time of 'jt9'
				if (decode->m_Start <= 0) {
					std::cerr << "Bug: abstime() returned invalid value: " << decode->m_Start << std::endl;
					std::cerr.flush();
				}
				FILE *jt9 = ::popen(cmd.c_str(), "r");

				#ifdef VERBOSE_DEBUG
				std::cerr << "popen(" << cmd << ")" << std::endl;
				if (jt9)
					std::cerr << "jt9 started" << std::endl;
				#endif

				// I/O loop on 'jt9' output
				const size_t bufsz = 128;
				char iobuffer[bufsz];
				std::string linebuffer;

				while (jt9) {
					// fread returns a size_t
					size_t ct = ::fread(iobuffer, 1, bufsz, jt9);

					if (ct == 0) {
						// End or error: either way, close and break
						::pclose(jt9);
						jt9 = nullptr;
					} else {
						// append the bytes read; size types now match
						linebuffer.append(iobuffer, ct);
					}

					#ifdef VERBOSE_DEBUG
					if (ct > 0)
						std::cerr << "jt9 sent " << ct << " bytes" << std::endl;
					#endif

					// read out each of the lines from the buffer
					std::string::size_type idx = linebuffer.find('\n');
					while (idx != std::string::npos) {
						std::string line = my::strip(linebuffer.substr(0, idx));
						if (isValidDecode(line)) {
							my::locker lock(decode->m_Mutex); // lock this block
							decode->m_Buffer.push_back(line);
						}
						linebuffer = linebuffer.substr(idx + 1);
						idx = linebuffer.find('\n');
					}
				}

				// record the stop time
				decode->m_Stop = KK5JY::FT8::abstime();
			} catch (const std::exception &ex) {
				KK5JY::FT8::send_error("decoder_thread(...) caught exception " + std::string(typeid(ex).name()) + ": " + ex.what());
			} catch (...) {
				KK5JY::FT8::send_error("decoder_thread(...) caught unknown exception.");
			}

			#ifdef VERBOSE_DEBUG
			std::cerr << "thread complete" << std::endl;
			#endif

			// remove the WAV file
			if ( ! decode->m_Keep)
				::unlink(decode->m_Path.c_str());

			// set the "done" flag
			decode->m_Done = true;

			// and terminate the thread with no status
			pthread_exit(0);
		}
	}
}

#endif // __KK5JY_FT8_DECODE_H