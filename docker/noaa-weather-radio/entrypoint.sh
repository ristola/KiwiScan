#!/bin/bash
set -e

# Write .env from environment variables so noaa_weather_radio picks them up
ENV_FILE="/opt/noaa_wr/.env"

cat > "$ENV_FILE" <<EOF
RTL_DEVICE_INDEX=0
RTL_TCP_HOST=${RTL_TCP_HOST:-}
RTL_TCP_PORT=${RTL_TCP_PORT:-7373}
RTL_GAIN=${RTL_GAIN:-38}
RTL_PPM=${RTL_PPM:-0}
CENTER_FREQ=${CENTER_FREQ:-162475000}
AUDIO_RATE=${AUDIO_RATE:-22050}
EAS_OUTPUT_DIR=${EAS_OUTPUT_DIR:-/opt/noaa_wr/eas_messages}
EAS_ENABLED=${EAS_ENABLED:-1}
RECORD_ALERTS=${RECORD_ALERTS:-1}

CH_1_ENABLED=${CH_1_ENABLED:-1}
CH_1_NAME=${CH_1_NAME:-WX2}
CH_1_ICECAST_ENABLED=${CH_1_ICECAST_ENABLED:-0}
CH_1_ICECAST=${CH_1_ICECAST:-}

CH_2_ENABLED=${CH_2_ENABLED:-1}
CH_2_NAME=${CH_2_NAME:-WX4}
CH_2_ICECAST_ENABLED=${CH_2_ICECAST_ENABLED:-0}
CH_2_ICECAST=${CH_2_ICECAST:-}

CH_3_ENABLED=${CH_3_ENABLED:-1}
CH_3_NAME=${CH_3_NAME:-WX5}
CH_3_ICECAST_ENABLED=${CH_3_ICECAST_ENABLED:-0}
CH_3_ICECAST=${CH_3_ICECAST:-}

CH_4_ENABLED=${CH_4_ENABLED:-1}
CH_4_NAME=${CH_4_NAME:-WX3}
CH_4_ICECAST_ENABLED=${CH_4_ICECAST_ENABLED:-0}
CH_4_ICECAST=${CH_4_ICECAST:-}

CH_5_ENABLED=${CH_5_ENABLED:-1}
CH_5_NAME=${CH_5_NAME:-WX6}
CH_5_ICECAST_ENABLED=${CH_5_ICECAST_ENABLED:-0}
CH_5_ICECAST=${CH_5_ICECAST:-}

CH_6_ENABLED=${CH_6_ENABLED:-1}
CH_6_NAME=${CH_6_NAME:-WX7}
CH_6_ICECAST_ENABLED=${CH_6_ICECAST_ENABLED:-0}
CH_6_ICECAST=${CH_6_ICECAST:-}

CH_7_ENABLED=${CH_7_ENABLED:-1}
CH_7_NAME=${CH_7_NAME:-WX1}
CH_7_ICECAST_ENABLED=${CH_7_ICECAST_ENABLED:-0}
CH_7_ICECAST=${CH_7_ICECAST:-}
EOF

# Patch pyrtlsdr TCP mode if RTL_TCP_HOST is set
if [ -n "$RTL_TCP_HOST" ]; then
    python3 - <<'PYEOF'
import os, re, glob

host = os.environ.get("RTL_TCP_HOST", "")
port = int(os.environ.get("RTL_TCP_PORT", "7373"))

# Find the main script and patch RtlSdr() → RtlSdr.from_params(driver='tcp', ...)
for path in glob.glob("/opt/noaa_wr/**/*.py", recursive=True):
    with open(path) as f:
        src = f.read()
    if "RtlSdr()" in src:
        patched = src.replace(
            "RtlSdr()",
            f"RtlSdr.from_params(driver_name='tcp', hostname='{host}', port={port})"
        )
        with open(path, "w") as f:
            f.write(patched)
        print(f"Patched rtl_tcp in {path}")
PYEOF
fi

exec python3 /opt/noaa_wr/nws_radio.py
