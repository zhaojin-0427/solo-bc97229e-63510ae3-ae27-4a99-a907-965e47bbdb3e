"""WSGI 入口:gunicorn wsgi:app 或 python wsgi.py"""
from app import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
