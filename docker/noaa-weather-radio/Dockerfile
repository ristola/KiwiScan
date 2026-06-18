FROM python:3.11-slim

# Disable numpy/scipy/OpenBLAS threading — armv7l hits OS thread limits otherwise
ENV OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    OPENBLAS_CORETYPE=ARMV7

# System deps: rtl-sdr (for pyrtlsdr), ffmpeg (audio encoding), multimon-ng (SAME/EAS decode)
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        librtlsdr-dev \
        rtl-sdr \
        ffmpeg \
        multimon-ng \
        git \
        libopenblas0 \
    && rm -rf /var/lib/apt/lists/*

# Clone noaa_weather_radio
RUN git clone --depth=1 https://github.com/TheGreatCodeholio/noaa_weather_radio.git /opt/noaa_wr

WORKDIR /opt/noaa_wr

# piwheels.org provides pre-compiled armhf binary wheels for numpy and scipy,
# avoiding the need for gcc/gfortran and hours of QEMU-emulated compilation.
# scipy>=1.15.2 may not be on piwheels yet; any >=1.10 is API-compatible for
# scipy.signal usage so we let pip pick whatever satisfies the index.
RUN pip install --no-cache-dir \
    --extra-index-url https://www.piwheels.org/simple/ \
    "pyrtlsdr==0.3.0" \
    "numpy>=1.26.4" \
    "scipy" \
    "requests>=2.32.3" \
    "python-dotenv>=1.1.0"

# EAS alert output directory (mapped to host via volume)
RUN mkdir -p /opt/noaa_wr/eas_messages

# Entrypoint patches rtl_tcp mode then runs the app
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

VOLUME ["/opt/noaa_wr/eas_messages"]

ENTRYPOINT ["/entrypoint.sh"]
