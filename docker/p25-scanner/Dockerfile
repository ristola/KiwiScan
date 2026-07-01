FROM python:3.11-slim

# numpy 1.24.x is the last series with a pre-built armv7 wheel on PyPI.
# --progress-bar=off avoids pip's rich thread that fails on resource-limited ARMv7.
RUN pip install --no-cache-dir --progress-bar=off "numpy>=1.24,<1.25"

WORKDIR /app
COPY scanner.py .

EXPOSE 8099

ENV RTL_HOST=10.13.73.201 \
    RTL_PORT=17373 \
    SERVE_PORT=8099 \
    GAIN_TENTH=350

CMD ["python", "-u", "scanner.py"]
