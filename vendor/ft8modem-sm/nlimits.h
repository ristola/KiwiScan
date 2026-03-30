// nlimits.h  (header-only, C++17)
#ifndef KK5JY_DSP_NLIMITS_H
#define KK5JY_DSP_NLIMITS_H

#include <cstdint>
#include <limits>
#include <type_traits>

namespace KK5JY { namespace DSP {

template<typename sample_t>
struct norm_limits {
    // Full-scale constants for different sample types (header-only).
    static inline constexpr sample_t maximum =
        std::is_same<sample_t, float>::value     ? static_cast<sample_t>(1.0f) :
        std::is_same<sample_t, double>::value    ? static_cast<sample_t>(1.0)  :
        std::is_same<sample_t, int16_t>::value   ? static_cast<sample_t>(32767):
        /* fallback */                              std::numeric_limits<sample_t>::max();

    static inline constexpr sample_t minimum =
        std::is_same<sample_t, float>::value     ? static_cast<sample_t>(-1.0f) :
        std::is_same<sample_t, double>::value    ? static_cast<sample_t>(-1.0)  :
        std::is_same<sample_t, int16_t>::value   ? static_cast<sample_t>(-32768):
        /* fallback */                              std::numeric_limits<sample_t>::lowest();
};

}} // namespace KK5JY::DSP

#endif // KK5JY_DSP_NLIMITS_H