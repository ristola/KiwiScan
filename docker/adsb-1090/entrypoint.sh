#!/bin/sh
set -e

DEVICE_SERIAL="${DEVICE_SERIAL:-00001090}"
GAIN="${GAIN:-60}"
LAT="${LAT:-}"
LON="${LON:-}"
MAX_RANGE="${MAX_RANGE:-360}"

echo "ADS-B 1090 — starting"
echo "  device serial : ${DEVICE_SERIAL}"
echo "  gain          : ${GAIN} dB"
echo "  max range     : ${MAX_RANGE} NM"
[ -n "$LAT" ] && echo "  lat/lon       : ${LAT}, ${LON}"

# JSON output dir (lighttpd aliases /data/ and /skyaware/data/ to this)
mkdir -p /run/dump1090-fa

# Start lighttpd in the background
lighttpd -f /etc/lighttpd/adsb.conf -D &

# Build dump1090-fa argument list
set -- \
    --quiet \
    --device-type rtlsdr \
    --device "$DEVICE_SERIAL" \
    --gain "$GAIN" \
    --fix \
    --max-range "$MAX_RANGE" \
    --net \
    --net-ro-port 30002 \
    --net-sbs-port 30003 \
    --net-bi-port 30004 \
    --net-bo-port 30005 \
    --write-json /run/dump1090 \
    --json-location-accuracy 1

if [ -n "$LAT" ] && [ -n "$LON" ]; then
    set -- "$@" --lat "$LAT" --lon "$LON"
fi

exec /opt/adsb/dump1090 "$@"
