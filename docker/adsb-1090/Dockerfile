FROM debian:bookworm-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update -o Acquire::AllowInsecureRepositories=true \
    && apt-get install -y --no-install-recommends --allow-unauthenticated \
        ca-certificates git build-essential pkg-config \
        librtlsdr-dev libusb-1.0-0-dev libncurses-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN git clone --depth 1 https://github.com/flightaware/dump1090.git .

# HTMLPATH tells dump1090 where the web UI files will live in the final image.
RUN make HTMLPATH=/opt/adsb/html

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update -o Acquire::AllowInsecureRepositories=true \
    && apt-get install -y --no-install-recommends --allow-unauthenticated \
        librtlsdr0 libusb-1.0-0 libncurses6 lighttpd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/adsb
COPY --from=builder /build/dump1090  ./dump1090
COPY --from=builder /build/public_html ./html

RUN mkdir -p /run/dump1090

COPY lighttpd.conf /etc/lighttpd/adsb.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 8080  — SkyAware HTTP (UI + aircraft.json)
# 30002 — Beast raw output
# 30003 — SBS/BaseStation output
# 30004 — Beast input
# 30005 — Beast binary output (feed FlightAware / FR24 / ADSB Exchange)
EXPOSE 8080 30002 30003 30004 30005

ENTRYPOINT ["/entrypoint.sh"]
