// pipes.h
#pragma once
#include <string>

namespace KK5JY {
namespace FT8 {

// Logging mask control
void set_log_mask(unsigned m);

// Keep this for compatibility with existing calls (e.g., -d).
// Enables/disables WARN+DEBUG explicitly.
void set_debug(bool on);

// New: toggle DEBUG (and WARN) on/off.
void toggle_debug();

// TRACE can still be set explicitly.
void set_trace(bool on);

// Message helpers
void send_message(const std::string& tag, const std::string& text);
void send_error(const std::string& text);
void send_ok(const std::string& text);
void send_warn(const std::string& text);
void send_warning(const std::string& text); // alias
void send_debug(const std::string& text);
void send_trace(const std::string& text);

} // namespace FT8
} // namespace KK5JY