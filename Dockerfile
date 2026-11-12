FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    SAMPLES_DIR=/app/samples \
    LIMS_CHUNK_SIZE=500 \
    LIMS_CHUNK_DELAY_MS=150

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY samples ./samples
COPY wsgi.py ./

RUN useradd -r -u 10001 lims && mkdir -p /data && chown -R lims:lims /data
USER lims
VOLUME ["/data"]

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/api/health',timeout=4)" || exit 1

# 单 worker 多线程:后台装载线程与请求线程共存,SQLite 写事务串行化
CMD ["gunicorn", "--workers", "1", "--threads", "8", "--bind", "0.0.0.0:8000", \
     "--access-logfile", "-", "--error-logfile", "-", "wsgi:app"]
