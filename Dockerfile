FROM python:3.11-slim

WORKDIR /app

# System deps sometimes required by SDR/audio libs
RUN apt-get update && apt-get install -y \
    git \
    ffmpeg \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install project via pyproject.toml
RUN pip install --no-cache-dir .

CMD ["python", "-m", "kiwi_scan"]
