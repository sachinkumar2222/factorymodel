# ============================================================
# Factory Model - Camera System Frontend & Web Service
# ============================================================
FROM python:3.12-slim

# Install ffmpeg for MP4 video thumbnail extraction
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/factory-camera

ENV PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8082 \
    WEB_ROOT=/opt/factory-camera/web \
    RECORDINGS_DIR=/opt/factory-camera/recordings \
    THUMBNAILS_DIR=/opt/factory-camera/web/thumbnails

# Create directory structure for recordings and web assets
RUN mkdir -p /opt/factory-camera/web/thumbnails /opt/factory-camera/recordings

# Copy frontend index.html and web server
COPY index.html /opt/factory-camera/web/index.html
COPY server.py /opt/factory-camera/server.py

EXPOSE 8082

CMD ["python3", "server.py"]
