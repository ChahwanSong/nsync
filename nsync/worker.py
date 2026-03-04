from __future__ import annotations

import argparse
import logging
import multiprocessing
import os
import shlex
import signal
import subprocess
import tempfile
import threading
import time
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import zmq

from .common import (
    coerce_options_argv,
    configure_logger,
    json_dumps,
    json_loads,
    new_worker_id,
    resolve_rsync_exit_code,
    resolve_output_file,
    strip_rsync_delete_args,
    utc_timestamp,
)
from .constants import (
    DEFAULT_DST_HOST,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_MASTER_HOST,
    DEFAULT_NUM_WORKER_PROCESSES,
    DEFAULT_RETRY_LIMIT,
    DEFAULT_RSYNC_ARGS,
    DEFAULT_RSYNC_BIN,
    DEFAULT_WORKER_CLAIM_PORT,
    DEFAULT_WORKER_HEARTBEAT_PORT,
    DEFAULT_WORKER_RESULT_PORT,
)


ZMQ_REQUEST_TIMEOUT_MS = 3000
MAX_PROCESS_RESTARTS = 5


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


class WorkerStats:
    def __init__(self) -> None:
        self.start_ts = multiprocessing.Value("d", time.time())
        self.batches_success = multiprocessing.Value("i", 0)
        self.batches_failed = multiprocessing.Value("i", 0)
        self.files_processed = multiprocessing.Value("q", 0)
        self.directories_processed = multiprocessing.Value("q", 0)
        self.bytes_processed = multiprocessing.Value("q", 0)
        self.lock = multiprocessing.Lock()

    def record(
        self, status: str, file_count: int, directory_count: int, estimated_bytes: int
    ) -> None:
        with self.lock:
            if status == "success":
                self.batches_success.value += 1
                self.files_processed.value += file_count
                self.directories_processed.value += directory_count
                self.bytes_processed.value += estimated_bytes
            else:
                self.batches_failed.value += 1

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "start_ts": float(self.start_ts.value),
                "batches_success": int(self.batches_success.value),
                "batches_failed": int(self.batches_failed.value),
                "files_processed": int(self.files_processed.value),
                "directories_processed": int(self.directories_processed.value),
                "bytes_processed": int(self.bytes_processed.value),
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
        self.stop_event = multiprocessing.Event()
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
        processes: Dict[int, multiprocessing.Process] = {}
        restart_counts: Dict[int, int] = {}
        for index in range(self.config.num_worker_processes):
            processes[index] = self._spawn_worker_process(index)

        def handle_signal(signum: int, frame: Optional[Any]) -> None:
            self.logger.info("signal", {"signum": signum})
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        try:
            while not self.stop_event.is_set():
                alive = False
                for index, process in list(processes.items()):
                    if process.is_alive():
                        alive = True
                        continue
                    exit_code = process.exitcode
                    if self.stop_event.is_set():
                        continue
                    if exit_code == 0:
                        continue
                    restart_count = restart_counts.get(index, 0) + 1
                    restart_counts[index] = restart_count
                    self.logger.warning(
                        "worker_process_exited",
                        {
                            "worker_id": self.worker_id,
                            "process_index": index,
                            "exit_code": exit_code,
                            "restart_count": restart_count,
                        },
                    )
                    if restart_count > MAX_PROCESS_RESTARTS:
                        self.logger.error(
                            "worker_process_restart_limit_exceeded",
                            {
                                "worker_id": self.worker_id,
                                "process_index": index,
                                "max_restarts": MAX_PROCESS_RESTARTS,
                            },
                        )
                        self.stop_event.set()
                        break
                    processes[index] = self._spawn_worker_process(index)
                    alive = True
                if not alive:
                    break
                time.sleep(0.2)
        finally:
            self.stop_event.set()
            for process in processes.values():
                process.join(timeout=2)
                if process.is_alive():
                    process.terminate()
            self._log_summary()

    def _spawn_worker_process(self, index: int) -> multiprocessing.Process:
        process = multiprocessing.Process(
            target=self._worker_loop,
            args=(index,),
            daemon=True,
            name=f"nsync-worker-{index}",
        )
        process.start()
        return process

    def _heartbeat_loop(self) -> None:
        socket = self.context.socket(zmq.PUSH)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, ZMQ_REQUEST_TIMEOUT_MS)
        socket.connect(f"tcp://{self.config.master_host}:{self.config.heartbeat_port}")
        try:
            while not self.stop_event.is_set():
                payload = {
                    "type": "heartbeat",
                    "worker_id": self.worker_id,
                    "timestamp": utc_timestamp(),
                }
                try:
                    socket.send(json_dumps(payload))
                except zmq.ZMQError as exc:
                    self.logger.warning(
                        "heartbeat_send_failed",
                        {"worker_id": self.worker_id, "error": str(exc)},
                    )
                self.stop_event.wait(self.config.heartbeat_interval)
        finally:
            socket.close(0)

    def _worker_loop(self, index: int) -> None:
        context = zmq.Context()
        claim_socket = context.socket(zmq.REQ)
        result_socket = context.socket(zmq.PUSH)
        claim_socket.setsockopt(zmq.LINGER, 0)
        claim_socket.setsockopt(zmq.SNDTIMEO, ZMQ_REQUEST_TIMEOUT_MS)
        claim_socket.setsockopt(zmq.RCVTIMEO, ZMQ_REQUEST_TIMEOUT_MS)
        result_socket.setsockopt(zmq.LINGER, 0)
        result_socket.setsockopt(zmq.SNDTIMEO, ZMQ_REQUEST_TIMEOUT_MS)
        claim_socket.connect(
            f"tcp://{self.config.master_host}:{self.config.claim_port}"
        )
        result_socket.connect(
            f"tcp://{self.config.master_host}:{self.config.result_port}"
        )
        try:
            while not self.stop_event.is_set():
                claim_payload = {
                    "type": "claim",
                    "worker_id": self.worker_id,
                    "pid": os.getpid(),
                    "timestamp": utc_timestamp(),
                    "session_id": self.worker_id,
                }
                try:
                    claim_socket.send(json_dumps(claim_payload))
                    response = json_loads(claim_socket.recv())
                except zmq.Again:
                    continue
                except zmq.ZMQError as exc:
                    self.logger.warning(
                        "claim_io_failed",
                        {
                            "worker_id": self.worker_id,
                            "process_index": index,
                            "pid": os.getpid(),
                            "error": str(exc),
                        },
                    )
                    time.sleep(0.1)
                    continue
                status = response.get("status")
                self.logger.debug(
                    "claim_response",
                    {
                        "status": status,
                        "worker_id": self.worker_id,
                        "process_index": index,
                        "pid": os.getpid(),
                    },
                )
                if status == "empty":
                    time.sleep(0.005)
                    continue
                if status == "done":
                    self.logger.debug(
                        "claim_done",
                        {
                            "worker_id": self.worker_id,
                            "process_index": index,
                            "pid": os.getpid(),
                        },
                    )
                    break
                if status != "ok":
                    time.sleep(0.01)
                    continue
                batch = response["batch"]
                if "rsync_args" in response:
                    batch["rsync_args"] = response.get("rsync_args")
                self.logger.debug(
                    "batch_start",
                    {
                        "worker_id": self.worker_id,
                        "process_index": index,
                        "pid": os.getpid(),
                        "task_id": batch.get("task_id"),
                        "file_count": batch.get("file_count"),
                        "directory_count": batch.get("directory_count"),
                        "estimated_bytes": batch.get("estimated_bytes"),
                    },
                )
                try:
                    result = self._process_batch(batch)
                except Exception as exc:
                    self.logger.exception(
                        "batch_process_failed",
                        {
                            "worker_id": self.worker_id,
                            "process_index": index,
                            "pid": os.getpid(),
                            "task_id": batch.get("task_id"),
                        },
                    )
                    result = {
                        "type": "result",
                        "worker_id": self.worker_id,
                        "task_id": batch.get("task_id"),
                        "status": "failed",
                        "retry_count": self.config.retry_limit,
                        "rsync_exit_code": 1,
                        "stats": {
                            "start_ts": utc_timestamp(),
                            "end_ts": utc_timestamp(),
                            "file_count": int(batch.get("file_count", 0) or 0),
                            "directory_count": int(batch.get("directory_count", 0) or 0),
                            "estimated_bytes": int(batch.get("estimated_bytes", 0) or 0),
                        },
                        "errors": [f"worker process error: {exc}"],
                    }
                try:
                    result_socket.send(json_dumps(result))
                except zmq.Again:
                    continue
                except zmq.ZMQError as exc:
                    self.logger.warning(
                        "result_send_failed",
                        {
                            "worker_id": self.worker_id,
                            "process_index": index,
                            "pid": os.getpid(),
                            "task_id": result.get("task_id"),
                            "error": str(exc),
                        },
                    )
        finally:
            claim_socket.close(0)
            result_socket.close(0)
            context.term()

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
            rsync_args = batch.get("rsync_args")
            if isinstance(rsync_args, str):
                rsync_args = shlex.split(rsync_args)
            if rsync_args is None:
                rsync_args = self.config.rsync_args
            exit_code = self._run_rsync(src_base, dst_base, paths, rsync_args)
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

    def _run_rsync(
        self, src_base: str, dst_base: str, paths: List[str], rsync_args: List[str]
    ) -> int:
        dst_host = self.config.dst_host
        local_host = dst_host in {"", "localhost", "127.0.0.1"}
        self._ensure_destinations(dst_host, dst_base, paths)
        cmd = [
            self.config.rsync_bin,
            "--xattrs",
            "--checksum",
        ] + rsync_args
        cmd = self._ensure_rsync_archive_arg(cmd)
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

    def _ensure_rsync_archive_arg(self, cmd: List[str]) -> List[str]:
        if "-a" in cmd or "--archive" in cmd:
            return cmd
        return [cmd[0], "-a"] + cmd[1:]

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
        gbytes_processed = snapshot["bytes_processed"] / (1024**3)
        rows = [
            ("worker_id", self.worker_id),
            ("elapsed_sec", f"{elapsed:.3f}"),
            ("batches_total", str(batches_total)),
            ("batches_success", str(snapshot["batches_success"])),
            ("batches_failed", str(snapshot["batches_failed"])),
            ("files_processed", str(snapshot["files_processed"])),
            ("directories_processed", str(snapshot["directories_processed"])),
            ("bytes_processed", str(snapshot["bytes_processed"])),
            ("gbytes_processed", f"{gbytes_processed:.6f}"),
            ("batches_per_sec", f"{snapshot['batches_success'] / elapsed:.3f}"),
            ("bytes_per_sec", f"{snapshot['bytes_processed'] / elapsed:.3f}"),
            ("gbytes_per_sec", f"{gbytes_processed / elapsed:.6f}"),
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
    parser = argparse.ArgumentParser(
        description="nsync worker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--num-worker-processes",
        type=int,
        default=DEFAULT_NUM_WORKER_PROCESSES,
        help="number of worker processes",
    )
    parser.add_argument("--dst-host", default=DEFAULT_DST_HOST, help="destination host")
    parser.add_argument("--master-host", default=DEFAULT_MASTER_HOST, help="master host")
    parser.add_argument(
        "--claim-port",
        type=int,
        default=DEFAULT_WORKER_CLAIM_PORT,
        help="port for claim channel",
    )
    parser.add_argument(
        "--result-port",
        type=int,
        default=DEFAULT_WORKER_RESULT_PORT,
        help="port for result channel",
    )
    parser.add_argument(
        "--heartbeat-port",
        type=int,
        default=DEFAULT_WORKER_HEARTBEAT_PORT,
        help="port for heartbeat channel",
    )
    parser.add_argument("--rsync-bin", default=DEFAULT_RSYNC_BIN, help="rsync binary")
    parser.add_argument(
        "--options",
        dest="rsync_args",
        default=DEFAULT_RSYNC_ARGS,
        help="extra rsync options (overridden by master --options)",
    )
    parser.add_argument(
        "--retry-limit",
        type=int,
        default=DEFAULT_RETRY_LIMIT,
        help="retry count on rsync failure",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=DEFAULT_HEARTBEAT_INTERVAL,
        help="heartbeat interval in seconds",
    )
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    parser.add_argument(
        "--output",
        default="",
        help="output path prefix for log files",
    )
    args = parser.parse_args(
        coerce_options_argv(sys.argv[1:], parser._option_string_actions.keys())
    )
    log_file = resolve_output_file(args.output, "worker", ".log")
    return WorkerConfig(
        num_worker_processes=args.num_worker_processes,
        dst_host=args.dst_host,
        master_host=args.master_host,
        claim_port=args.claim_port,
        result_port=args.result_port,
        heartbeat_port=args.heartbeat_port,
        rsync_bin=args.rsync_bin,
        rsync_args=strip_rsync_delete_args(shlex.split(args.rsync_args)),
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
