# ============================================================
# Factory Model - Camera Intrusion Recording Service (Port 8081)
# ============================================================
FROM python:3.12-slim

# Install ffmpeg for capturing RTSP streams to MP4
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/factory-camera

RUN pip install --no-cache-dir flask

ENV PYTHONUNBUFFERED=1 \
    PORT=8081 \
    RECORDINGS_DIR=/opt/factory-camera/recordings

RUN mkdir -p /opt/factory-camera/recordings

COPY camera_http_service.py /opt/factory-camera/camera_http_service.py

EXPOSE 8081

CMD ["python3", "camera_http_service.py"]
