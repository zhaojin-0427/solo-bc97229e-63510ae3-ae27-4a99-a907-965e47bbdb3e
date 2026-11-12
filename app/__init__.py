"""Flask 应用工厂。"""
import os

from flask import Flask

from . import db, loader, routes


def create_app(config=None):
    app = Flask(__name__)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    app.config.from_mapping(
        DATA_DIR=os.environ.get("DATA_DIR", os.path.join(base_dir, "data")),
        SAMPLES_DIR=os.environ.get("SAMPLES_DIR", os.path.join(base_dir, "samples")),
        CHUNK_SIZE=int(os.environ.get("LIMS_CHUNK_SIZE", "500")),
        # 演示用:每块之间的处理间隔(秒),便于观察“装载中”的增量行为
        CHUNK_DELAY=float(os.environ.get("LIMS_CHUNK_DELAY_MS", "150")) / 1000.0,
        # 请求线程等待跨进程批次锁的最长时间(秒):增量须在成功响应前确认
        LOCK_TIMEOUT=float(os.environ.get("LIMS_LOCK_TIMEOUT_S", "60")),
        # 每个 worker 常驻监控线程的轮询间隔
        MONITOR_IDLE_SECONDS=float(os.environ.get("LIMS_MONITOR_IDLE_S", "1.0")),
        MONITOR_BUSY_SECONDS=float(os.environ.get("LIMS_MONITOR_BUSY_S", "0.2")),
    )
    if config:
        app.config.update(config)
    app.config["MAX_CONTENT_LENGTH"] = int(
        os.environ.get("LIMS_MAX_UPLOAD_MB", "50")) * 1024 * 1024

    os.makedirs(app.config["DATA_DIR"], exist_ok=True)
    db.init_schema(app.config["DATA_DIR"])

    app.register_blueprint(routes.bp)
    app.teardown_appcontext(db.close_db)

    # 进程(重)启动:恢复未完成的装载与增量确认
    loader.start_recovery_thread(app)
    return app
