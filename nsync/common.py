from __future__ import annotations

import json
import logging
import socket
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from .constants import DEFAULT_TIMEZONE


@dataclass(frozen=True)
class Batch:
    task_id: int
    paths: List[str]
    file_count: int
    directory_count: int
    estimated_bytes: int
    src_base: str
    dst_base: str
    created_ts: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "paths": self.paths,
            "file_count": self.file_count,
            "directory_count": self.directory_count,
            "estimated_bytes": self.estimated_bytes,
            "src_base": self.src_base,
            "dst_base": self.dst_base,
            "created_ts": self.created_ts,
        }


@dataclass
class BatchResult:
    worker_id: str
    task_id: int
    status: str
    retry_count: int
    rsync_exit_code: int
    stats: Dict[str, Any]
    errors: List[str]
    received_ts: float = field(default_factory=lambda: time.time())

    def to_dict(self) -> Dict[str, Any]:
        stats = self.stats or {}
        start_ts = stats.get("start_ts")
        end_ts = stats.get("end_ts")
        timezone = ZoneInfo(DEFAULT_TIMEZONE)
        start_ts_iso = (
            datetime.fromtimestamp(start_ts, tz=timezone).strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(start_ts, (int, float))
            else None
        )
        end_ts_iso = (
            datetime.fromtimestamp(end_ts, tz=timezone).strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(end_ts, (int, float))
            else None
        )
        return {
            "worker_id": self.worker_id,
            "task_id": self.task_id,
            "status": self.status,
            "retry_count": self.retry_count,
            "rsync_exit_code": self.rsync_exit_code,
            "stats": {
                **stats,
                "start_ts_iso": start_ts_iso,
                "end_ts_iso": end_ts_iso,
            },
            "errors": self.errors,
            "received_ts": self.received_ts,
            "received_ts_iso": datetime.fromtimestamp(
                self.received_ts, tz=timezone
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }


def utc_timestamp() -> float:
    return time.time()


def new_task_id() -> int:
    return uuid.uuid4().int


def new_worker_id() -> str:
    hostname = socket.gethostname()
    return f"{hostname}-{uuid.uuid4().hex}"


def json_dumps(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


def json_loads(payload: bytes) -> Dict[str, Any]:
    return json.loads(payload.decode("utf-8"))


def configure_logger(
    name: str,
    level: int = logging.INFO,
    pretty: bool = True,
    log_file: Optional[str] = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            data = {
                "ts": time.time(),
                "level": record.levelname,
                "name": record.name,
                "message": record.getMessage(),
            }
            if record.args and isinstance(record.args, dict):
                data.update(record.args)
            return json.dumps(data)

    class PrettyFormatter(logging.Formatter):
        @staticmethod
        def _format_value(value: Any) -> str:
            if value is None:
                return "null"
            if isinstance(value, (int, float, bool)):
                return str(value)
            if isinstance(value, str):
                if value and all(ch.isalnum() or ch in {"-", "_", ".", ":"} for ch in value):
                    return value
                return json.dumps(value, ensure_ascii=True)
            return json.dumps(value, ensure_ascii=True)

        def format(self, record: logging.LogRecord) -> str:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
            millis = int(record.msecs)
            base = f"{timestamp}.{millis:03d} {record.levelname:<5} {record.name} {record.getMessage()}"
            if record.args and isinstance(record.args, dict):
                extras = " ".join(
                    f"{key}={self._format_value(value)}" for key, value in record.args.items()
                )
                if extras:
                    return f"{base} {extras}"
            return base

    formatter: logging.Formatter = PrettyFormatter() if pretty else JsonFormatter()
    if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    if log_file and not any(
        isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", None) == log_file
        for handler in logger.handlers
    ):
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    for handler in logger.handlers:
        handler.setLevel(level)
        handler.setFormatter(formatter)
    logger.propagate = False
    return logger


def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    batch: List[Any] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def resolve_rsync_exit_code(code: int) -> str:
    if code == 0:
        return "success"
    return "failed"
