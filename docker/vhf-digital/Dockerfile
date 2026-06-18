FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# Remove the APT cleanup hook that fails on Docker 19.x + armhf/trixie.
RUN rm -f /etc/apt/apt.conf.d/docker-clean

# libopenblas0   — required by piwheels numpy (links against system openblas)
# libatomic1     — required by piwheels scipy HiGHS extension on armhf
# wsjtx          — provides jt9 (FT8/FT4 decoder) and wsprd (WSPR decoder)
# direwolf / sox — APRS/packet audio decode (also bind-mounted from host as fallback)
# OPENBLAS_NUM_THREADS=1 avoids pthread_create failures in Docker 19.x seccomp
RUN apt-get update -o Acquire::AllowInsecureRepositories=true && \
    apt-get install -y --no-install-recommends --allow-unauthenticated \
        libopenblas0 \
        libatomic1 \
        wsjtx \
        direwolf \
        sox \
    && rm -rf /var/lib/apt/lists/*

ENV OPENBLAS_NUM_THREADS=1

WORKDIR /opt/vhf-digital

COPY requirements.txt .
RUN pip install --no-cache-dir \
    --extra-index-url https://www.piwheels.org/simple/ \
    -r requirements.txt

COPY service.py .
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN mkdir -p /opt/vhf-digital/decodes

VOLUME ["/opt/vhf-digital/decodes"]
EXPOSE 4025

ENTRYPOINT ["/entrypoint.sh"]
