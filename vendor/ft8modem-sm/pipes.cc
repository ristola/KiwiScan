// pipes.cc
#include "pipes.h"

#include <atomic>
#include <cctype>
#include <iostream>
#include <mutex>
#include <string>

namespace KK5JY {
namespace FT8 {

// -------- Runtime logging mask (1=WARN, 2=DEBUG, 4=TRACE) --------
static constexpr unsigned kWarn  = 0x1u;
static constexpr unsigned kDebug = 0x2u;
static constexpr unsigned kTrace = 0x4u;

static std::atomic<unsigned> g_log_mask{0u}; // default muted

void set_log_mask(unsigned m) {
    g_log_mask.store(m, std::memory_order_relaxed);
}

// Compatibility wrapper (used by ft8modem.cc for -d)
void set_debug(bool on) {
    unsigned m = g_log_mask.load(std::memory_order_relaxed);
    if (on) {
        m |= (kWarn | kDebug);
    } else {
        m &= ~(kWarn | kDebug);
    }
    set_log_mask(m);
}

// Toggle-only behavior you wanted for the DEBUG command
void toggle_debug() {
    unsigned m = g_log_mask.load(std::memory_order_relaxed);
    if (m & kDebug) {
        m &= ~(kWarn | kDebug); // turn both off together
    } else {
        m |=  (kWarn | kDebug); // turn both on together
    }
    set_log_mask(m);
}

void set_trace(bool on) {
    unsigned m = g_log_mask.load(std::memory_order_relaxed);
    if (on) {
        m |= kTrace;
    } else {
        m &= ~kTrace;
    }
    set_log_mask(m);
}

// -------- Shared state --------
static std::mutex  g_io_mutex;
static std::string g_current_mode = "FT8"; // default if none announced

// Helpers
static inline size_t skip_ws(const std::string& s, size_t i) {
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
    return i;
}
static inline bool is_mode_char(unsigned char c) {
    return std::isalnum(c) || c=='+' || c=='-' || c=='_' || c=='/';
}
static inline size_t scan_mode_end(const std::string& s, size_t i) {
    while (i < s.size() && is_mode_char(static_cast<unsigned char>(s[i]))) ++i;
    return i;
}

// Capture mode from "MODE: <token>" (anywhere) or from a "D:" token’s first word
static void maybe_capture_mode_from_line(const std::string& line) {
    // MODE:
    {
        const std::string kMode = "MODE:";
        size_t p = line.find(kMode);
        if (p != std::string::npos) {
            p = skip_ws(line, p + kMode.size());
            size_t q = scan_mode_end(line, p);
            if (q > p) { g_current_mode = line.substr(p, q - p); return; }
        }
    }
    // D:
    {
        const std::string kD = "D:";
        size_t pos = 0;
        while (true) {
            pos = line.find(kD, pos);
            if (pos == std::string::npos) break;
            bool ok_boundary = (pos == 0) || std::isspace(static_cast<unsigned char>(line[pos - 1]));
            pos += kD.size();
            if (!ok_boundary) continue;

            size_t p = skip_ws(line, pos);
            size_t q = scan_mode_end(line, p);
            if (q > p) { g_current_mode = line.substr(p, q - p); return; }
        }
    }
}

// Inject "<MODE> " right after a token "E:" that is at start of line or preceded by whitespace.
// If an alphabetic token already follows E:, leave unchanged (assume mode already present).
static std::string inject_mode_after_E_in_line(const std::string& line) {
    const std::string kEcho = "E:";
    size_t pos = 0;
    while (true) {
        pos = line.find(kEcho, pos);
        if (pos == std::string::npos) return line;

        bool ok_boundary = (pos == 0) || std::isspace(static_cast<unsigned char>(line[pos - 1]));
        if (!ok_boundary) { pos += kEcho.size(); continue; }
        break; // candidate found
    }

    size_t insertPos = pos + kEcho.size();
    if (insertPos < line.size() && line[insertPos] == ' ') ++insertPos;

    size_t look = skip_ws(line, insertPos);
    if (look < line.size() && std::isalpha(static_cast<unsigned char>(line[look]))) {
        // Already has an alphabetic token after E: (likely mode) -> leave unchanged
        return line;
    }

    std::string out = line;
    out.insert(insertPos, g_current_mode + " ");
    return out;
}

// -------- Public API --------
void send_message(const std::string& tag, const std::string& text) {
    // Build final line exactly as printed
    std::string line = tag + ": " + text;

    // Update current mode from the final line
    maybe_capture_mode_from_line(line);

    // Inject mode after any E:
    line = inject_mode_after_E_in_line(line);

    std::lock_guard<std::mutex> lock(g_io_mutex);
    std::cout << line << std::endl;
}

void send_error(const std::string& text) {
    std::lock_guard<std::mutex> lock(g_io_mutex);
    std::cerr << "ERROR: " << text << std::endl;
}

void send_ok(const std::string& text) {
    std::lock_guard<std::mutex> lock(g_io_mutex);
    std::cout << "OK: " << text << std::endl;
}

void send_warn(const std::string& text) {
    if ((g_log_mask.load(std::memory_order_relaxed) & kWarn) == 0u) return;
    std::lock_guard<std::mutex> lock(g_io_mutex);
    std::cout << "WARN: " << text << std::endl;
}

void send_warning(const std::string& text) { send_warn(text); }

void send_debug(const std::string& text) {
    if ((g_log_mask.load(std::memory_order_relaxed) & kDebug) == 0u) return;
    std::lock_guard<std::mutex> lock(g_io_mutex);
    std::cout << "DEBUG: " << text << std::endl;
}

void send_trace(const std::string& text) {
    if ((g_log_mask.load(std::memory_order_relaxed) & kTrace) == 0u) return;
    std::lock_guard<std::mutex> lock(g_io_mutex);
    std::cout << "TRACE: " << text << std::endl;
}

} // namespace FT8
} // namespace KK5JY