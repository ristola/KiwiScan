FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --progress-bar off -r requirements.txt
COPY service.py .
EXPOSE 4027
CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "4027", "--log-level", "info"]
