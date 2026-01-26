from __future__ import annotations

import json
import logging
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class Batch:
    task_id: int
    paths: List[str]
    file_count: int
    estimated_bytes: int
    src_base: str
    dst_base: str
    created_ts: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "paths": self.paths,
            "file_count": self.file_count,
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
    stats: Dict[str, int]
    errors: List[str]
    received_ts: float = field(default_factory=lambda: time.time())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "task_id": self.task_id,
            "status": self.status,
            "retry_count": self.retry_count,
            "rsync_exit_code": self.rsync_exit_code,
            "stats": self.stats,
            "errors": self.errors,
            "received_ts": self.received_ts,
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


def configure_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()

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

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
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
