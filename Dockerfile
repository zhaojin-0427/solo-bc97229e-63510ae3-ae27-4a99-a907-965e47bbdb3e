FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    SAMPLES_DIR=/app/samples \
    LIMS_CHUNK_SIZE=500 \
    LIMS_CHUNK_DELAY_MS=150 \
    LIMS_WORKERS=2 \
    LIMS_THREADS=4

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

# 两个 worker 进程共用同一 /data 目录:批次级 flock 互斥 + 每进程常驻
# 监控线程,保证同一批次不被重复推进;任一 worker 被终止后存活 worker 自动接续。
# 不用 --preload:每个 worker 独立执行应用工厂并启动自己的恢复监控线程。
CMD ["sh", "-c", "exec gunicorn --workers ${LIMS_WORKERS:-2} --threads ${LIMS_THREADS:-4} --bind 0.0.0.0:8000 --timeout 120 --access-logfile - --error-logfile - wsgi:app"]
