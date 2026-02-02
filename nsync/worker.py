from __future__ import annotations

import argparse
import logging
import os
import shlex
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import zmq

from .common import (
    configure_logger,
    json_dumps,
    json_loads,
    new_worker_id,
    resolve_rsync_exit_code,
    utc_timestamp,
)


@dataclass
class WorkerConfig:
    num_worker_processes: int
    dst_host: str
    master_host: str
    claim_port: int
    result_port: int
    heartbeat_port: int
    rsync_bin: str
    rsync_args: List[str]
    retry_limit: int
    heartbeat_interval: float
    debug: bool
    log_file: Optional[str]


@dataclass
class WorkerStats:
    start_ts: float = field(default_factory=time.time)
    batches_success: int = 0
    batches_failed: int = 0
    files_processed: int = 0
    directories_processed: int = 0
    bytes_processed: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(
        self, status: str, file_count: int, directory_count: int, estimated_bytes: int
    ) -> None:
        with self.lock:
            if status == "success":
                self.batches_success += 1
                self.files_processed += file_count
                self.directories_processed += directory_count
                self.bytes_processed += estimated_bytes
            else:
                self.batches_failed += 1

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "start_ts": self.start_ts,
                "batches_success": self.batches_success,
                "batches_failed": self.batches_failed,
                "files_processed": self.files_processed,
                "directories_processed": self.directories_processed,
                "bytes_processed": self.bytes_processed,
            }


class WorkerService:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.worker_id = new_worker_id()
        log_level = logging.DEBUG if config.debug else logging.INFO
        self.logger = configure_logger(
            "nsync.worker", log_level, pretty=True, log_file=config.log_file
        )
        self.context = zmq.Context.instance()
        self.stop_event = threading.Event()
        self.stats = WorkerStats()
        self.last_progress_log = 0.0
        self.progress_interval = 5.0

    def start(self) -> None:
        self.logger.info(
            "worker_started",
            {
                "worker_id": self.worker_id,
                "master_host": self.config.master_host,
                "num_worker_processes": self.config.num_worker_processes,
            },
        )
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        processes: List[threading.Thread] = []
        for index in range(self.config.num_worker_processes):
            thread = threading.Thread(
                target=self._worker_loop, args=(index,), daemon=True
            )
            thread.start()
            processes.append(thread)

        def handle_signal(signum: int, frame: Optional[Any]) -> None:
            self.logger.info("signal", {"signum": signum})
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        try:
            while not self.stop_event.is_set():
                alive = any(thread.is_alive() for thread in processes)
                if not alive:
                    break
                time.sleep(0.2)
        finally:
            self.stop_event.set()
            self._log_summary()

    def _heartbeat_loop(self) -> None:
        socket = self.context.socket(zmq.PUSH)
        socket.connect(f"tcp://{self.config.master_host}:{self.config.heartbeat_port}")
        while not self.stop_event.is_set():
            payload = {
                "type": "heartbeat",
                "worker_id": self.worker_id,
                "timestamp": utc_timestamp(),
            }
            socket.send(json_dumps(payload))
            time.sleep(self.config.heartbeat_interval)

    def _worker_loop(self, index: int) -> None:
        claim_socket = self.context.socket(zmq.REQ)
        claim_socket.connect(
            f"tcp://{self.config.master_host}:{self.config.claim_port}"
        )
        result_socket = self.context.socket(zmq.PUSH)
        result_socket.connect(
            f"tcp://{self.config.master_host}:{self.config.result_port}"
        )
        while not self.stop_event.is_set():
            claim_payload = {
                "type": "claim",
                "worker_id": self.worker_id,
                "pid": os.getpid(),
                "timestamp": utc_timestamp(),
                "session_id": self.worker_id,
            }
            claim_socket.send(json_dumps(claim_payload))
            response = json_loads(claim_socket.recv())
            status = response.get("status")
            self.logger.debug(
                "claim_response",
                {"status": status, "worker_id": self.worker_id, "thread": index},
            )
            if status == "empty":
                time.sleep(0.005)
                continue
            if status == "done":
                self.logger.debug(
                    "claim_done", {"worker_id": self.worker_id, "thread": index}
                )
                break
            if status != "ok":
                time.sleep(0.01)
                continue
            batch = response["batch"]
            self.logger.debug(
                "batch_start",
                {
                    "worker_id": self.worker_id,
                    "thread": index,
                    "task_id": batch.get("task_id"),
                    "file_count": batch.get("file_count"),
                    "directory_count": batch.get("directory_count"),
                    "estimated_bytes": batch.get("estimated_bytes"),
                },
            )
            result = self._process_batch(batch)
            result_socket.send(json_dumps(result))

    def _process_batch(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        task_id = batch["task_id"]
        src_base = batch["src_base"]
        dst_base = batch["dst_base"]
        paths = batch["paths"]
        file_count = int(batch.get("file_count", 0) or 0)
        directory_count = int(batch.get("directory_count", 0) or 0)
        estimated_bytes = int(batch.get("estimated_bytes", 0) or 0)
        errors: List[str] = []
        status = "failed"
        exit_code = 1
        retries = 0
        for attempt in range(self.config.retry_limit + 1):
            start_ts = utc_timestamp()
            exit_code = self._run_rsync(src_base, dst_base, paths)
            status = resolve_rsync_exit_code(exit_code)
            if status == "success":
                break
            retries = attempt + 1
            warning = f"task {task_id} retry {retries} exit={exit_code}"
            self.logger.warning(warning)
            errors.append(warning)
            time.sleep(2**attempt)
        end_ts = utc_timestamp()
        self.stats.record(status, file_count, directory_count, estimated_bytes)
        self._maybe_log_progress()
        result = {
            "type": "result",
            "worker_id": self.worker_id,
            "task_id": task_id,
            "status": status,
            "retry_count": retries,
            "rsync_exit_code": exit_code,
            "stats": {
                "bytes_sent": 0,
                "bytes_received": 0,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "file_count": file_count,
                "directory_count": directory_count,
                "estimated_bytes": estimated_bytes,
            },
            "errors": errors,
        }
        self.logger.debug(
            "batch_end",
            {
                "worker_id": self.worker_id,
                "task_id": task_id,
                "status": status,
                "retry_count": retries,
                "rsync_exit_code": exit_code,
                "file_count": file_count,
                "directory_count": directory_count,
                "estimated_bytes": estimated_bytes,
            },
        )
        return result

    def _run_rsync(self, src_base: str, dst_base: str, paths: List[str]) -> int:
        dst_host = self.config.dst_host
        local_host = dst_host in {"", "localhost", "127.0.0.1"}
        self._ensure_destinations(dst_host, dst_base, paths)
        cmd = [
            self.config.rsync_bin,
            "-a",
            "--xattrs",
            "--checksum",
        ] + self.config.rsync_args
        if len(paths) == 1:
            src_path = os.path.join(src_base, paths[0])
            dst_path = (
                os.path.join(dst_base, paths[0])
                if local_host
                else f"{dst_host}:{os.path.join(dst_base, paths[0])}"
            )
            cmd.extend([src_path, dst_path])
            return subprocess.call(cmd)
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as handle:
            for path in paths:
                handle.write(path + "\n")
            list_path = handle.name
        cmd.extend(["--files-from", list_path, os.path.join(src_base, "")])
        dst_path = (
            os.path.join(dst_base, "")
            if local_host
            else f"{dst_host}:{os.path.join(dst_base, "")}".rstrip(":")
        )
        cmd.append(dst_path)
        try:
            return subprocess.call(cmd)
        finally:
            os.unlink(list_path)

    def _ensure_destinations(
        self, dst_host: str, dst_base: str, paths: List[str]
    ) -> None:
        parents = {os.path.dirname(os.path.join(dst_base, path)) for path in paths}
        parents.discard("")
        if dst_host in {"", "localhost", "127.0.0.1"}:
            for parent in parents:
                os.makedirs(parent, exist_ok=True)
            return
        for parent in parents:
            subprocess.call(["ssh", dst_host, "mkdir", "-p", parent])

    def _log_summary(self) -> None:
        snapshot = self.stats.snapshot()
        elapsed = max(time.time() - snapshot["start_ts"], 0.001)
        batches_total = snapshot["batches_success"] + snapshot["batches_failed"]
        gb_processed = snapshot["bytes_processed"] / (1024**3)
        rows = [
            ("worker_id", self.worker_id),
            ("elapsed_sec", f"{elapsed:.3f}"),
            ("batches_total", str(batches_total)),
            ("batches_success", str(snapshot["batches_success"])),
            ("batches_failed", str(snapshot["batches_failed"])),
            ("files_processed", str(snapshot["files_processed"])),
            ("directories_processed", str(snapshot["directories_processed"])),
            ("bytes_processed", str(snapshot["bytes_processed"])),
            ("gb_processed", f"{gb_processed:.6f}"),
            ("batches_per_sec", f"{snapshot['batches_success'] / elapsed:.3f}"),
            ("bytes_per_sec", f"{snapshot['bytes_processed'] / elapsed:.3f}"),
            ("gb_per_sec", f"{gb_processed / elapsed:.6f}"),
            ("files_per_sec", f"{snapshot['files_processed'] / elapsed:.3f}"),
            ("directories_per_sec", f"{snapshot['directories_processed'] / elapsed:.3f}"),
        ]
        table = _format_kv_table(rows)
        self.logger.info("worker_summary\n%s", table)

    def _maybe_log_progress(self) -> None:
        now = time.time()
        if now - self.last_progress_log < self.progress_interval:
            return
        self.last_progress_log = now
        snapshot = self.stats.snapshot()
        elapsed = max(time.time() - snapshot["start_ts"], 0.001)
        payload = {
            "worker_id": self.worker_id,
            "elapsed_sec": elapsed,
            "batches_success": snapshot["batches_success"],
            "batches_failed": snapshot["batches_failed"],
            "files_processed": snapshot["files_processed"],
            "bytes_processed": snapshot["bytes_processed"],
        }
        self.logger.info("progress", payload)


def _resolve_log_file(log_dir: str, log_prefix: str, name: str) -> Optional[str]:
    if not log_dir:
        return None
    os.makedirs(log_dir, exist_ok=True)
    prefix = log_prefix or ""
    if prefix and not prefix.endswith(("-", "_")):
        prefix = f"{prefix}-"
    filename = f"{prefix}{name}.log"
    return os.path.join(log_dir, filename)


def _format_kv_table(rows: List[tuple[str, str]]) -> str:
    key_width = max(len("metric"), max((len(key) for key, _ in rows), default=0))
    value_width = max(
        len("value"), max((len(value) for _, value in rows), default=0)
    )
    sep = f"+-{'-' * key_width}-+-{'-' * value_width}-+"
    lines = [
        sep,
        f"| {'metric':<{key_width}} | {'value':<{value_width}} |",
        sep,
    ]
    for key, value in rows:
        lines.append(f"| {key:<{key_width}} | {value:<{value_width}} |")
    lines.append(sep)
    return "\n".join(lines)


def parse_args() -> WorkerConfig:
    parser = argparse.ArgumentParser(description="nsync worker")
    parser.add_argument("--num-worker-processes", type=int, required=True)
    parser.add_argument("--dst-host", required=True)
    parser.add_argument("--master-host", default="127.0.0.1")
    parser.add_argument("--claim-port", type=int, default=5555)
    parser.add_argument("--result-port", type=int, default=5557)
    parser.add_argument("--heartbeat-port", type=int, default=5558)
    parser.add_argument("--rsync-bin", default="rsync")
    parser.add_argument("--rsync-args", default="")
    parser.add_argument("--retry-limit", type=int, default=3)
    parser.add_argument("--heartbeat-interval", type=float, default=5.0)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--log-dir", default="")
    parser.add_argument("--log-prefix", default="")
    args = parser.parse_args()
    log_file = _resolve_log_file(args.log_dir, args.log_prefix, "worker")
    return WorkerConfig(
        num_worker_processes=args.num_worker_processes,
        dst_host=args.dst_host,
        master_host=args.master_host,
        claim_port=args.claim_port,
        result_port=args.result_port,
        heartbeat_port=args.heartbeat_port,
        rsync_bin=args.rsync_bin,
        rsync_args=shlex.split(args.rsync_args),
        retry_limit=args.retry_limit,
        heartbeat_interval=args.heartbeat_interval,
        debug=args.debug,
        log_file=log_file,
    )


def main() -> None:
    config = parse_args()
    service = WorkerService(config)
    service.start()


if __name__ == "__main__":
    main()
