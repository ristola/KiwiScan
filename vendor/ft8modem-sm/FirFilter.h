#ifndef __KK5JY_FIRFILTER_H
#define __KK5JY_FIRFILTER_H

#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <string>
#include <algorithm>
#include <limits>

#include "WindowFunctions.h"
#include "FilterTypes.h"
#include "FilterUtils.h"
#include "IFilter.h"

//#define VERBOSE_DEBUG
#ifdef VERBOSE_DEBUG
#include <iostream>
#endif

namespace KK5JY {
namespace DSP {

#ifndef local_min
#define local_min(a, b) std::min(a, b)
#endif

// --- internal scaling helper to avoid norm_limits<> ---
template<typename T> constexpr T scale_max();
template<> constexpr int16_t scale_max<int16_t>() { return static_cast<int16_t>(32767); }
template<> constexpr float   scale_max<float>()   { return 1.0f; }
template<> constexpr double  scale_max<double>()  { return 1.0;  }

// define specific function pointer types
using WindowFunction = double (*)(int n, int N);
using CoefFunction1  = double (*)(double d, int i, int N);
using CoefFunction2  = double (*)(double d1, double d2, int i, int N);

class FirFilterException : public std::exception {
    std::string userMsg;
public:
    explicit FirFilterException(const std::string &s) : userMsg(s) {}
    ~FirFilterException() noexcept override = default;
    const char *what() const noexcept override { return userMsg.c_str(); }
};

class FirFilterUtils {
public:
    static double IdealLowPass(double omega_c, int n, int /*N*/) {
        return (n == 0) ? (omega_c / M_PI) : (std::sin(omega_c * n) / (M_PI * n));
    }
    static double IdealHighPass(double omega_c, int n, int /*N*/) {
        return (n == 0) ? (1.0 - (omega_c / M_PI)) : (-std::sin(omega_c * n) / (M_PI * n));
    }
    static double IdealResonant(double omega_c, int n, int N) {
        return std::cos(omega_c * n) / (N / M_PI);
    }
    static double IdealBandPass(double omega_1, double omega_2, int n, int /*N*/) {
        if (n == 0) return (omega_2 - omega_1) / M_PI;
        return (std::sin(omega_2 * n) / (M_PI * n)) - (std::sin(omega_1 * n) / (M_PI * n));
    }
    static double IdealBandStop(double omega_1, double omega_2, int n, int /*N*/) {
        if (n == 0) return 1.0 - ((omega_2 - omega_1) / M_PI);
        return (std::sin(omega_1 * n) / (M_PI * n)) - (std::sin(omega_2 * n) / (M_PI * n));
    }
    static double IdealTwinPeak(double omega_1, double omega_2, int n, int N) {
        return IdealResonant(omega_1, n, N) + IdealResonant(omega_2, n, N);
    }

private:
    static double *GenerateCoefficients(WindowFunction f, CoefFunction1 g, int length, double omega_c) {
        const int limit = length / 2;
        double* result = new double[static_cast<std::size_t>(length)];
        for (int i = -limit; i <= limit; ++i) {
            const double wn = f(i, length);
            const double hn = g(omega_c, i, length);
            result[i + limit] = (wn * hn);
#ifdef VERBOSE_DEBUG
            std::cerr << "h[" << i << "]=" << hn << " w[" << i << "]=" << wn
                      << " c[" << (i+limit) << "]=" << result[i+limit] << "\n";
#endif
        }
        return result;
    }
    static double *GenerateCoefficients(WindowFunction f, CoefFunction2 g, int length, double omega_c1, double omega_c2) {
        const int limit = length / 2;
        double* result = new double[static_cast<std::size_t>(length)];
        for (int i = -limit; i <= limit; ++i) {
            const double wn = f(i, length);
            const double hn = g(omega_c1, omega_c2, i, length);
            result[i + limit] = (wn * hn);
#ifdef VERBOSE_DEBUG
            std::cerr << "h[" << i << "]=" << hn << " w[" << i << "]=" << wn
                      << " c[" << (i+limit) << "]=" << result[i+limit] << "\n";
#endif
        }
        return result;
    }

    template <typename sample_t>
    static sample_t *fromDouble(double *d, std::size_t len) {
        sample_t *result = new sample_t[len];
        const auto k = scale_max<sample_t>();
        for (std::size_t i = 0; i != len; ++i) result[i] = static_cast<sample_t>(d[i] * k);
        delete[] d;
        return result;
    }

public:
    template <typename T>
    static T* GenerateLowPassCoefficients(WindowFunction wf, int length, double omega_c);
    template <typename T>
    static T* GenerateHighPassCoefficients(WindowFunction wf, int length, double omega_c);
    template <typename T>
    static T* GenerateResonantCoefficients(WindowFunction wf, int length, double omega_c);
    template <typename T>
    static T* GenerateBandPassCoefficients(WindowFunction wf, int length, double omega_c1, double omega_c2);
    template <typename T>
    static T* GenerateTwinPeakCoefficients(WindowFunction wf, int length, double omega_c1, double omega_c2);
    template <typename T>
    static T* GenerateBandStopCoefficients(WindowFunction wf, int length, double omega_c1, double omega_c2);

public:
    // FIR cores
    static int16_t Filter(int16_t input, int16_t *&hp, int16_t * const history, int16_t * const hist_end,
                          int16_t * const coefs, int16_t * const coef_end) {
        int16_t* cp = coefs;
        *hp++ = input;
        if (hp == hist_end) hp = history;

        int32_t acc = 0;
        while (cp != coef_end) {
            acc += (static_cast<int32_t>(*hp++) * static_cast<int32_t>(*cp++)) >> 15;
            if (hp == hist_end) hp = history;
        }
        if (acc > std::numeric_limits<int16_t>::max()) acc = std::numeric_limits<int16_t>::max();
        if (acc < std::numeric_limits<int16_t>::min()) acc = std::numeric_limits<int16_t>::min();
        return static_cast<int16_t>(acc);
    }

    static float Filter(float input, float *&hp, float * const history, float * const hist_end,
                        float * const coefs, float * const coef_end) {
        float* cp = coefs;
        *hp++ = input;
        if (hp == hist_end) hp = history;

        float output = 0.0f;
        while (cp != coef_end) {
            output += *hp++ * *cp++;
            if (hp == hist_end) hp = history;
        }
        return output;
    }

    static double Filter(double input, double *&hp, double * const history, double * const hist_end,
                         double * const coefs, double * const coef_end) {
        double* cp = coefs;
        *hp++ = input;
        if (hp == hist_end) hp = history;

        double output = 0.0;
        while (cp != coef_end) {
            output += *hp++ * *cp++;
            if (hp == hist_end) hp = history;
        }
        return output;
    }
};

// ---- Coefficient generators (int16) ----
template <>
inline int16_t* FirFilterUtils::GenerateLowPassCoefficients<int16_t>(WindowFunction f, int length, double omega_c) {
    return fromDouble<int16_t>(GenerateCoefficients(f, IdealLowPass, length, omega_c), static_cast<std::size_t>(length));
}
template <>
inline int16_t* FirFilterUtils::GenerateHighPassCoefficients<int16_t>(WindowFunction f, int length, double omega_c) {
    return fromDouble<int16_t>(GenerateCoefficients(f, IdealHighPass, length, omega_c), static_cast<std::size_t>(length));
}
template<>
inline int16_t* FirFilterUtils::GenerateResonantCoefficients<int16_t>(WindowFunction f, int length, double omega_c) {
    return fromDouble<int16_t>(GenerateCoefficients(f, IdealResonant, length, omega_c), static_cast<std::size_t>(length));
}
template<>
inline int16_t* FirFilterUtils::GenerateTwinPeakCoefficients<int16_t>(WindowFunction f, int length, double oc1, double oc2) {
    return fromDouble<int16_t>(GenerateCoefficients(f, IdealTwinPeak, length, oc1, oc2), static_cast<std::size_t>(length));
}
template <>
inline int16_t* FirFilterUtils::GenerateBandPassCoefficients<int16_t>(WindowFunction f, int length, double oc1, double oc2) {
    return fromDouble<int16_t>(GenerateCoefficients(f, IdealBandPass, length, oc1, oc2), static_cast<std::size_t>(length));
}
template <>
inline int16_t* FirFilterUtils::GenerateBandStopCoefficients<int16_t>(WindowFunction f, int length, double oc1, double oc2) {
    return fromDouble<int16_t>(GenerateCoefficients(f, IdealBandStop, length, oc1, oc2), static_cast<std::size_t>(length));
}

// ---- Coefficient generators (double) ----
template <>
inline double* FirFilterUtils::GenerateLowPassCoefficients<double>(WindowFunction f, int length, double omega_c) {
    return GenerateCoefficients(f, IdealLowPass, length, omega_c);
}
template <>
inline double* FirFilterUtils::GenerateHighPassCoefficients<double>(WindowFunction f, int length, double omega_c) {
    return GenerateCoefficients(f, IdealHighPass, length, omega_c);
}
template<>
inline double* FirFilterUtils::GenerateResonantCoefficients<double>(WindowFunction f, int length, double omega_c) {
    return GenerateCoefficients(f, IdealResonant, length, omega_c);
}
template<>
inline double* FirFilterUtils::GenerateTwinPeakCoefficients<double>(WindowFunction f, int length, double oc1, double oc2) {
    return GenerateCoefficients(f, IdealTwinPeak, length, oc1, oc2);
}
template <>
inline double* FirFilterUtils::GenerateBandPassCoefficients<double>(WindowFunction f, int length, double oc1, double oc2) {
    return GenerateCoefficients(f, IdealBandPass, length, oc1, oc2);
}
template <>
inline double* FirFilterUtils::GenerateBandStopCoefficients<double>(WindowFunction f, int length, double oc1, double oc2) {
    return GenerateCoefficients(f, IdealBandStop, length, oc1, oc2);
}

// ---- Coefficient generators (float) ----
template <>
inline float* FirFilterUtils::GenerateLowPassCoefficients<float>(WindowFunction f, int length, double omega_c) {
    return fromDouble<float>(GenerateCoefficients(f, IdealLowPass, length, omega_c), static_cast<std::size_t>(length));
}
template <>
inline float* FirFilterUtils::GenerateHighPassCoefficients<float>(WindowFunction f, int length, double omega_c) {
    return fromDouble<float>(GenerateCoefficients(f, IdealHighPass, length, omega_c), static_cast<std::size_t>(length));
}
template<>
inline float* FirFilterUtils::GenerateResonantCoefficients<float>(WindowFunction f, int length, double omega_c) {
    return fromDouble<float>(GenerateCoefficients(f, IdealResonant, length, omega_c), static_cast<std::size_t>(length));
}
template<>
inline float* FirFilterUtils::GenerateTwinPeakCoefficients<float>(WindowFunction f, int length, double oc1, double oc2) {
    return fromDouble<float>(GenerateCoefficients(f, IdealTwinPeak, length, oc1, oc2), static_cast<std::size_t>(length));
}
template <>
inline float* FirFilterUtils::GenerateBandPassCoefficients<float>(WindowFunction f, int length, double oc1, double oc2) {
    return fromDouble<float>(GenerateCoefficients(f, IdealBandPass, length, oc1, oc2), static_cast<std::size_t>(length));
}
template <>
inline float* FirFilterUtils::GenerateBandStopCoefficients<float>(WindowFunction f, int length, double oc1, double oc2) {
    return fromDouble<float>(GenerateCoefficients(f, IdealBandStop, length, oc1, oc2), static_cast<std::size_t>(length));
}

// ======================= Generic FIR filter =======================
template <typename sample_t, typename coef_t = sample_t>
class FirFilter : public IFilter<sample_t> {
private:
    int m_Length{};
    coef_t *m_InputPos{};
    coef_t *m_History{};
    coef_t *m_Coefs{};
    coef_t *m_HistEnd{};
    coef_t *m_CoefEnd{};
    sample_t m_Value{};

    double corr_f1{}, corr_f2{};

    void Clear() {
        if (m_History) {
            for (int i = 0; i != m_Length; ++i) m_History[i] = static_cast<coef_t>(0);
        }
        m_InputPos = m_History;
        m_Value = static_cast<sample_t>(0);
    }

public:
    int GetLength() const { return m_Length; }
    double GetOverallGain();

    void GetCoefficients(double *target) const {
        const coef_t *cp = m_Coefs;
        for (int i = 0; i != m_Length; ++i) target[i] = static_cast<double>(cp[i]);
    }

    FirFilter(FirFilterTypes type, int length, double f1, double f2, size_t fs, WindowFunction wf = HammingWindow);
    FirFilter(FirFilterTypes type, int length, double fc, size_t fs, WindowFunction wf = HammingWindow);
    FirFilter(int length, const coef_t * coefs);

    sample_t run(sample_t sample) {
        return (m_Value = FirFilterUtils::Filter(sample, m_InputPos, m_History, m_HistEnd, m_Coefs, m_CoefEnd));
    }
    sample_t value() const { return m_Value; }
    void add(sample_t sample) { run(sample); }
    void clear() { Clear(); }
};

template <typename sample_t, typename coef_t>
inline double FirFilter<sample_t, coef_t>::GetOverallGain() {
    double freqs[3] = { corr_f1, corr_f2, -1 };
    Clear();
    double gain = FilterUtils::CalculateGain(this, freqs, m_Length);
    Clear();
    return gain;
}

template <typename sample_t, typename coef_t>
inline FirFilter<sample_t, coef_t>::FirFilter(FirFilterTypes type, int length, double f1, double f2, size_t fs, WindowFunction wf) {
    if ((length % 2) == 0) ++length;
    if (!wf) wf = RectangleWindow;

    m_Length = length;
    const double oc1 = fs ? (2 * M_PI * f1 / fs) : f1;
    const double oc2 = fs ? (2 * M_PI * f2 / fs) : f2;

    switch (type) {
        case BandPass:
            m_Coefs = FirFilterUtils::GenerateBandPassCoefficients<coef_t>(wf, m_Length, oc1, oc2);
            corr_f1 = 2.0 * M_PI * (f2 + f1) / (2.0 * fs);
            break;
        case BandStop:
            m_Coefs = FirFilterUtils::GenerateBandStopCoefficients<coef_t>(wf, m_Length, oc1, oc2);
            corr_f1 = 2.0 * M_PI * local_min(f2, f1) / (2.0 * fs);
            break;
        case TwinPeak:
            m_Coefs = FirFilterUtils::GenerateTwinPeakCoefficients<coef_t>(wf, m_Length, oc1, oc2);
            corr_f1 = 2.0 * M_PI * f1 / fs;
            corr_f2 = 2.0 * M_PI * f2 / fs;
            break;
        default:
            throw FirFilterException("This constructor is only for bandpass/bandstop/twin-peak filters");
    }
    m_History = m_InputPos = new sample_t[static_cast<size_t>(m_Length)];
    m_HistEnd = m_History + m_Length;
    m_CoefEnd = m_Coefs + m_Length;
    Clear();
}

template <typename sample_t, typename coef_t>
inline FirFilter<sample_t, coef_t>::FirFilter(FirFilterTypes type, int length, double fc, size_t fs, WindowFunction wf) {
    if ((length % 2) == 0) ++length;
    if (!wf) wf = RectangleWindow;

    m_Length = length;
    const double oc = fs ? (2 * M_PI * fc / fs) : fc;

    switch (type) {
        case LowPass:
            m_Coefs = FirFilterUtils::GenerateLowPassCoefficients<coef_t>(wf, m_Length, oc);
            corr_f1 = 2.0 * M_PI * fc / (2.0 * fs);
            break;
        case HighPass:
            m_Coefs = FirFilterUtils::GenerateHighPassCoefficients<coef_t>(wf, m_Length, oc);
            corr_f1 = 2.0 * M_PI * (fc + (fs / 2)) / (2.0 * fs);
            break;
        case Resonant:
            m_Coefs = FirFilterUtils::GenerateResonantCoefficients<coef_t>(wf, m_Length, oc);
            corr_f1 = 2.0 * M_PI * fc / fs;
            break;
        default:
            throw FirFilterException("This constructor cannot be used for bandpass/bandstop/twin-peak");
    }
    m_History = m_InputPos = new sample_t[static_cast<size_t>(m_Length)];
    m_HistEnd = m_History + m_Length;
    m_CoefEnd = m_Coefs + m_Length;
    Clear();
}

template <typename sample_t, typename coef_t>
inline FirFilter<sample_t, coef_t>::FirFilter(int length, const coef_t * coefs) {
    m_Length = length;
    if ((m_Length % 2) == 0) ++m_Length;

    m_Coefs = new coef_t[static_cast<size_t>(m_Length)];
    for (int i = 0; i < length && i < m_Length; ++i) m_Coefs[i] = coefs[i];
    for (int i = length; i < m_Length; ++i) m_Coefs[i] = static_cast<coef_t>(0);

    m_History = m_InputPos = new sample_t[static_cast<size_t>(m_Length)];
    m_HistEnd = m_History + m_Length;
    m_CoefEnd = m_Coefs + m_Length;
    Clear();
}

} // namespace DSP
} // namespace KK5JY

#endif // __KK5JY_FIRFILTER_H