#pragma once
/*
 *
 *
 *    encode.h
 *
 *    Conversion from text to keying symbols.
 *
 *    Copyright (C) 2023-2024 by Matt Roberts.
 *    License: GNU GPL3 (www.gnu.org)
 *
 */

#include <string>

namespace KK5JY {
namespace FT8 {

    /**
     * Encode a message into keying symbols for the given mode.
     *
     * @param mode One of {"ft8","ft4","jt9","jt65","wspr"} (case-insensitive handling should be in the caller).
     * @param txt  A mode-valid message (validation is expected to be done prior to calling).
     * @return     A string of keying symbols usable by MFSK::Modulator<T>::transmit(...).
     */
    [[nodiscard]] std::string encode(const std::string& mode, const std::string& txt);

} // namespace FT8
} // namespace KK5JY

// EOF