import os

bind             = f"0.0.0.0:{os.environ.get('PORT', '8080')}"
workers          = 2
threads          = 4
worker_class     = "gthread"
timeout          = 120
keepalive        = 5
max_requests     = 1000
max_requests_jitter = 100
accesslog        = "-"
errorlog         = "-"
loglevel         = os.environ.get("LOG_LEVEL", "info").lower()
