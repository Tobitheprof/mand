import logging
import sys
import json
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from mand.config.settings import settings

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        }
        # Attach extra dict if present (from logger.info(..., extra={...}))
        # logging puts `extra` into record.__dict__, keep only simple types
        for k, v in record.__dict__.items():
            if k in ("args", "msg", "levelname", "levelno", "pathname", "filename", "module",
                     "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created",
                     "msecs", "relativeCreated", "thread", "threadName", "processName", "process",
                     "name"): 
                continue
            try:
                json.dumps({k: v})
                payload[k] = v
            except Exception:
                payload[k] = str(v)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

def _make_console_handler(json_logs: bool) -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if json_logs else logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"))
    return handler

def _ensure_dir(path: str):
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

def _make_file_handler(json_logs: bool) -> logging.Handler:
    _ensure_dir(settings.LOG_FILE)
    if settings.LOG_ROTATE == "time":
        handler = TimedRotatingFileHandler(
            settings.LOG_FILE,
            when=settings.LOG_WHEN,
            interval=settings.LOG_INTERVAL,
            backupCount=settings.LOG_BACKUP_COUNT,
            encoding="utf-8",
            utc=True,
        )
    else:
        handler = RotatingFileHandler(
            settings.LOG_FILE,
            maxBytes=settings.LOG_MAX_BYTES,
            backupCount=settings.LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
    handler.setFormatter(JsonFormatter() if json_logs else logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"))
    return handler

def configure_logging(
    json_logs: bool | None = None,
    level: str | None = None,
    add_console: bool = True,
    add_file: bool | None = None,
):
    """
    Central logging setup. Reads defaults from settings.* but can be overridden.
    """
    json_logs = settings.LOG_JSON if json_logs is None else json_logs
    level = settings.LOG_LEVEL if level is None else level
    add_file = settings.LOG_TO_FILE if add_file is None else add_file

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level.upper())

    if add_console:
        root.addHandler(_make_console_handler(json_logs))
    if add_file:
        root.addHandler(_make_file_handler(json_logs))
