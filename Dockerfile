FROM python:3.13-slim

ARG GIT_COMMIT=unknown

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
  TZ=America/New_York \
    VIRTUAL_ENV=/opt/kiwiscan/.venv-py3 \
    PATH=/opt/kiwiscan/.venv-py3/bin:/opt/kiwiscan/vendor/kiwiclient-jks:$PATH \
    AUTO_SETUP=0 \
    AUTO_SYSTEM_DEPS=0 \
    AUTO_RELOAD=0 \
    NO_RESTART=1 \
    KIWI_SCAN_WS4010=1 \
    KIWI_SCAN_UDP4010=1 \
    PORT=4020 \
    KIWISCAN_BUILD_COMMIT=$GIT_COMMIT

LABEL org.opencontainers.image.revision=$GIT_COMMIT

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  pkg-config \
    sox \
    tzdata \
  libasound2-dev \
    libsndfile1-dev \
    libfftw3-dev \
  librtaudio-dev \
    wsjtx \
    curl \
    ca-certificates \
    procps \
    file \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/kiwiscan

COPY requirements.txt pyproject.toml README.md ./

RUN python -m venv "$VIRTUAL_ENV" \
 && "$VIRTUAL_ENV/bin/pip" install --no-cache-dir --upgrade pip \
 && "$VIRTUAL_ENV/bin/pip" install --no-cache-dir -r requirements.txt

COPY src ./src
COPY vendor ./vendor
COPY config ./config
COPY tools ./tools
COPY run_server.sh ./run_server.sh

RUN mkdir -p logs outputs \
 && chmod +x run_server.sh vendor/kiwiclient-jks/kiwirecorder.py tools/kiwi_admin_kick.py \
 && make -C /opt/kiwiscan/vendor/ft8modem-sm clean \
 && make -C /opt/kiwiscan/vendor/ft8modem-sm ft8modem af2udp \
 && install -m 0755 /opt/kiwiscan/vendor/ft8modem-sm/ft8modem /usr/local/bin/ft8modem \
 && install -m 0755 /opt/kiwiscan/vendor/ft8modem-sm/af2udp /usr/local/bin/af2udp \
 && ln -sf /opt/kiwiscan/vendor/kiwiclient-jks/kiwirecorder.py /usr/local/bin/kiwirecorder.py

EXPOSE 4010/tcp
EXPOSE 4010/udp
EXPOSE 4020

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD curl -fsS http://127.0.0.1:4020/version >/dev/null || exit 1

CMD ["bash", "run_server.sh"]
