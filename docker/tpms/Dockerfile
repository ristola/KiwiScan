FROM python:3.11-slim

RUN apt-get update -o APT::Update::Post-Invoke="" \
    && apt-get install -y --no-install-recommends rtl-433 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/tpms

COPY requirements.txt .
RUN pip install --no-cache-dir --progress-bar off -r requirements.txt

COPY service.py .

RUN mkdir -p /opt/tpms/data

EXPOSE 4026

CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "4026", "--log-level", "info"]
