from __future__ import annotations

import argparse
import errno
import logging
import multiprocessing
import os
import queue
import signal
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import uvicorn
import zmq
from fastapi import FastAPI

from .batcher import FileInfo, iter_batches, bucketize, scan_paths, scan_subtree
from .common import Batch, BatchResult, configure_logger, json_loads, json_dumps
from .constants import MAX_RESULT_HISTORY


@dataclass
class MasterState:
    total_batches: int = 0
    completed_batches: int = 0
    failed_batches: int = 0
    completed_files: int = 0
    completed_directories: int = 0
    completed_bytes: int = 0
    task_id_seq: int = 0
    start_ts: float = field(default_factory=time.time)
    results: Dict[int, BatchResult] = field(default_factory=dict)
    result_order: deque[int] = field(default_factory=deque)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    heartbeats: Dict[str, float] = field(default_factory=dict)
    in_flight: Dict[int, "InFlightBatch"] = field(default_factory=dict)
    worker_tasks: Dict[str, Set[int]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    max_results: int = MAX_RESULT_HISTORY

    def record_result(self, result: BatchResult) -> None:
        with self.lock:
            if result.task_id in self.results:
                return
            self.results[result.task_id] = result
            self.result_order.append(result.task_id)
            while len(self.result_order) > self.max_results:
                expired = self.result_order.popleft()
                self.results.pop(expired, None)
            if result.status == "success":
                self.completed_batches += 1
                stats = result.stats or {}
                self.completed_files += int(stats.get("file_count", 0) or 0)
                self.completed_directories += int(stats.get("directory_count", 0) or 0)
                self.completed_bytes += int(stats.get("estimated_bytes", 0) or 0)
            else:
                self.failed_batches += 1
            if result.errors:
                self.errors.extend(result.errors)

    def record_warning(self, message: str) -> None:
        with self.lock:
            self.warnings.append(message)

    def record_error(self, message: str) -> None:
        with self.lock:
            self.errors.append(message)

    def update_heartbeat(self, worker_id: str) -> None:
        with self.lock:
            self.heartbeats[worker_id] = time.time()

    def next_task_id(self) -> int:
        with self.lock:
            self.task_id_seq += 1
            return self.task_id_seq

    def register_in_flight(self, batch: Batch, worker_id: str) -> None:
        with self.lock:
            self.in_flight[batch.task_id] = InFlightBatch(
                batch=batch, worker_id=worker_id, claimed_ts=time.time()
            )
            self.worker_tasks.setdefault(worker_id, set()).add(batch.task_id)

    def complete_task(self, task_id: int, worker_id: Optional[str] = None) -> None:
        with self.lock:
            inflight = self.in_flight.pop(task_id, None)
            if inflight is not None:
                tasks = self.worker_tasks.get(inflight.worker_id)
                if tasks is not None:
                    tasks.discard(task_id)
                    if not tasks:
                        self.worker_tasks.pop(inflight.worker_id, None)
            elif worker_id:
                tasks = self.worker_tasks.get(worker_id)
                if tasks is not None:
                    tasks.discard(task_id)
                    if not tasks:
                        self.worker_tasks.pop(worker_id, None)

    def take_timed_out_tasks(self, timeout_sec: float) -> Tuple[List[str], List[Batch]]:
        now = time.time()
        expired_workers: List[str] = []
        batches: List[Batch] = []
        with self.lock:
            for worker_id, last_ts in list(self.heartbeats.items()):
                if now - last_ts > timeout_sec:
                    expired_workers.append(worker_id)
            for worker_id in expired_workers:
                task_ids = self.worker_tasks.pop(worker_id, set())
                for task_id in task_ids:
                    inflight = self.in_flight.pop(task_id, None)
                    if inflight is not None:
                        batches.append(inflight.batch)
                self.heartbeats.pop(worker_id, None)
        return expired_workers, batches

    def is_completed(self, task_id: int) -> bool:
        with self.lock:
            return task_id in self.results


@dataclass
class InFlightBatch:
    batch: Batch
    worker_id: str
    claimed_ts: float


@dataclass
class MasterConfig:
    src: str
    dst: str
    batch_num_files: int
    batch_size: int
    num_master_processes: int
    master_scan_depth: int
    bind_host: str
    claim_port: int
    batch_port: int
    result_port: int
    heartbeat_port: int
    api_port: int
    exit_when_done: bool
    debug: bool
    queue_threshold: int
    log_file: Optional[str]
    compress_paths: bool
    compress_max_depth: int
    heartbeat_timeout: float


class MasterService:
    def __init__(self, config: MasterConfig) -> None:
        self.config = config
        log_level = logging.DEBUG if config.debug else logging.INFO
        self.logger = configure_logger(
            "nsync.master", log_level, pretty=True, log_file=config.log_file
        )
        self.context = zmq.Context.instance()
        self.state = MasterState()
        self.queue: queue.Queue[Batch] = queue.Queue()
        self.stop_event = threading.Event()
        self.producers_done = 0
        self.producers_total = config.num_master_processes
        self.done_flag = threading.Event()
        self.last_progress_log = 0.0
        self.progress_interval = 5.0
        self.last_backpressure_log = 0.0
        self.backpressure_interval = 5.0
        self.last_requeue_log = 0.0
        self.requeue_interval = 5.0
        self.server: Optional[uvicorn.Server] = None
        self.api_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._start_batch_receiver()
        self._start_result_receiver()
        self._start_heartbeat_receiver()
        self._start_claim_server()
        self._start_requeue_monitor()
        self._start_api()
        self.logger.info(
            "master_started",
            {
                "src": self.config.src,
                "dst": self.config.dst,
                "num_master_processes": self.config.num_master_processes,
                "queue_threshold": self.config.queue_threshold,
            },
        )

    def stop(self) -> None:
        self.stop_event.set()
        self.done_flag.set()
        if self.server is not None:
            self.server.should_exit = True
        if self.api_thread is not None and self.api_thread.is_alive():
            self.api_thread.join(timeout=2)
        try:
            self.context.term()
        except zmq.ZMQError:
            pass

    def _start_batch_receiver(self) -> None:
        thread = threading.Thread(target=self._batch_receiver_loop, daemon=True)
        thread.start()

    def _start_result_receiver(self) -> None:
        thread = threading.Thread(target=self._result_receiver_loop, daemon=True)
        thread.start()

    def _start_heartbeat_receiver(self) -> None:
        thread = threading.Thread(target=self._heartbeat_receiver_loop, daemon=True)
        thread.start()

    def _start_claim_server(self) -> None:
        thread = threading.Thread(target=self._claim_server_loop, daemon=True)
        thread.start()

    def _start_requeue_monitor(self) -> None:
        thread = threading.Thread(target=self._requeue_monitor_loop, daemon=True)
        thread.start()

    def _start_api(self) -> None:
        _ensure_port_available(self.config.bind_host, self.config.api_port)
        app = create_app(self.state, self.queue, self)
        config = uvicorn.Config(
            app, host=self.config.bind_host, port=self.config.api_port, log_level="info"
        )
        self.server = uvicorn.Server(config)
        self.api_thread = threading.Thread(target=self.server.run, daemon=True)
        self.api_thread.start()

    def _batch_receiver_loop(self) -> None:
        socket = self.context.socket(zmq.PULL)
        _bind_socket_with_retry(
            socket, f"tcp://{self.config.bind_host}:{self.config.batch_port}"
        )
        while not self.stop_event.is_set():
            try:
                payload = socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.05)
                continue
            message = json_loads(payload)
            if message.get("type") == "producer_done":
                self.producers_done += 1
                self.logger.info("producer_done", {"count": self.producers_done})
                continue
            if message.get("type") == "batch":
                batch = Batch(**message["batch"])
                assigned_id = self.state.next_task_id()
                if batch.task_id != assigned_id:
                    batch = replace(batch, task_id=assigned_id)
                if self.config.queue_threshold > 0:
                    while (
                        not self.stop_event.is_set()
                        and self.queue.qsize() >= self.config.queue_threshold
                    ):
                        now = time.time()
                        if (
                            now - self.last_backpressure_log
                            >= self.backpressure_interval
                        ):
                            self.last_backpressure_log = now
                            self.logger.info(
                                "queue_backpressure depth=%6d threshold=%6d"
                                % (self.queue.qsize(), self.config.queue_threshold)
                            )
                        time.sleep(0.05)
                self.queue.put(batch)
                with self.state.lock:
                    self.state.total_batches += 1
                self.logger.debug(
                    "batch_received",
                    {
                        "task_id": batch.task_id,
                        "file_count": batch.file_count,
                        "directory_count": batch.directory_count,
                        "estimated_bytes": batch.estimated_bytes,
                        "queue_depth": self.queue.qsize(),
                    },
                )

    def _result_receiver_loop(self) -> None:
        socket = self.context.socket(zmq.PULL)
        _bind_socket_with_retry(
            socket, f"tcp://{self.config.bind_host}:{self.config.result_port}"
        )
        while not self.stop_event.is_set():
            try:
                payload = socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.05)
                continue
            message = json_loads(payload)
            if message.get("type") != "result":
                continue
            result = BatchResult(
                worker_id=message["worker_id"],
                task_id=message["task_id"],
                status=message["status"],
                retry_count=message["retry_count"],
                rsync_exit_code=message["rsync_exit_code"],
                stats=message.get("stats", {}),
                errors=message.get("errors", []),
            )
            self.state.complete_task(result.task_id, result.worker_id)
            self.state.record_result(result)
            self._maybe_log_progress()
            self.logger.debug(
                "result_received",
                {
                    "worker_id": result.worker_id,
                    "task_id": result.task_id,
                    "status": result.status,
                    "retry_count": result.retry_count,
                    "rsync_exit_code": result.rsync_exit_code,
                    "file_count": result.stats.get("file_count"),
                    "directory_count": result.stats.get("directory_count"),
                    "estimated_bytes": result.stats.get("estimated_bytes"),
                },
            )

    def _heartbeat_receiver_loop(self) -> None:
        socket = self.context.socket(zmq.PULL)
        _bind_socket_with_retry(
            socket, f"tcp://{self.config.bind_host}:{self.config.heartbeat_port}"
        )
        while not self.stop_event.is_set():
            try:
                payload = socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.05)
                continue
            message = json_loads(payload)
            worker_id = message.get("worker_id")
            if worker_id:
                self.state.update_heartbeat(worker_id)
                self.logger.debug("heartbeat", {"worker_id": worker_id})

    def _claim_server_loop(self) -> None:
        socket = self.context.socket(zmq.REP)
        _bind_socket_with_retry(
            socket, f"tcp://{self.config.bind_host}:{self.config.claim_port}"
        )
        while not self.stop_event.is_set():
            try:
                payload = socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.02)
                continue
            request = json_loads(payload)
            response: Dict[str, Any]
            try:
                response = {"status": "empty"}
                while True:
                    batch = self.queue.get_nowait()
                    if self.state.is_completed(batch.task_id):
                        continue
                    response = {"status": "ok", "batch": batch.to_dict()}
                    worker_id = request.get("worker_id")
                    if worker_id:
                        self.state.register_in_flight(batch, worker_id)
                    break
            except queue.Empty:
                if self.producers_done >= self.producers_total and self.queue.empty():
                    self.done_flag.set()
                    response = {"status": "done"}
            socket.send(json_dumps(response))
            self.logger.debug(
                "claim",
                {
                    "worker_id": request.get("worker_id"),
                    "status": response.get("status"),
                    "queue_depth": self.queue.qsize(),
                    "producers_done": self.producers_done,
                },
            )

    def _requeue_monitor_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.config.heartbeat_timeout <= 0:
                time.sleep(1.0)
                continue
            expired_workers, batches = self.state.take_timed_out_tasks(
                self.config.heartbeat_timeout
            )
            if expired_workers:
                for batch in batches:
                    self.queue.put(batch)
                if batches:
                    now = time.time()
                    if now - self.last_requeue_log >= self.requeue_interval:
                        self.last_requeue_log = now
                        self.logger.warning(
                            "requeue",
                            {
                                "expired_workers": len(expired_workers),
                                "requeued_batches": len(batches),
                            },
                        )
            time.sleep(0.5)

    def wait_until_done(self) -> None:
        while not self.stop_event.is_set():
            if self.done_flag.is_set():
                with self.state.lock:
                    if (
                        self.state.completed_batches + self.state.failed_batches
                        >= self.state.total_batches
                    ):
                        return
            time.sleep(0.1)

    def log_summary(self) -> None:
        with self.state.lock:
            total = self.state.total_batches
            completed = self.state.completed_batches
            failed = self.state.failed_batches
            files_processed = self.state.completed_files
            bytes_processed = self.state.completed_bytes
            retained_count = len(self.state.results)
            retained_limit = self.state.max_results
            queue_depth = self.queue.qsize()
            producers_done = self.producers_done
            start_ts = self.state.start_ts
        elapsed = max(time.time() - start_ts, 0.001)
        gb_processed = bytes_processed / (1024**3)
        rows = [
            ("elapsed_sec", f"{elapsed:.3f}"),
            ("batches_total", str(total)),
            ("batches_completed", str(completed)),
            ("batches_failed", str(failed)),
            ("files_processed", str(files_processed)),
            ("bytes_processed", str(bytes_processed)),
            ("gb_processed", f"{gb_processed:.6f}"),
            ("batches_per_sec", f"{completed / elapsed:.3f}"),
            ("bytes_per_sec", f"{bytes_processed / elapsed:.3f}"),
            ("gb_per_sec", f"{gb_processed / elapsed:.6f}"),
            ("files_per_sec", f"{files_processed / elapsed:.3f}"),
            ("queue_depth", str(queue_depth)),
            ("producers_done", str(producers_done)),
            ("results_retained", f"{retained_count}/{retained_limit}"),
        ]
        table = _format_kv_table(rows)
        self.logger.info("master_summary\n%s", table)

    def _maybe_log_progress(self) -> None:
        now = time.time()
        if now - self.last_progress_log < self.progress_interval:
            return
        self.last_progress_log = now
        with self.state.lock:
            pending = (
                self.state.total_batches
                - self.state.completed_batches
                - self.state.failed_batches
            )
            total = self.state.total_batches
            completed = self.state.completed_batches
            failed = self.state.failed_batches
            queue_depth = self.queue.qsize()
            producers_done = self.producers_done
            files_processed = self.state.completed_files
            bytes_processed = self.state.completed_bytes
        self.logger.info(
            "progress total=%6d completed=%6d failed=%6d pending=%6d queue=%6d producers=%3d files=%8d bytes=%12d"
            % (
                total,
                completed,
                failed,
                max(pending, 0),
                queue_depth,
                producers_done,
                files_processed,
                bytes_processed,
            )
        )


def create_app(
    state: MasterState, queue_ref: queue.Queue[Batch], service: MasterService
) -> FastAPI:
    app = FastAPI()

    @app.get("/status")
    def status() -> Dict[str, Any]:
        with state.lock:
            return {
                "total_batches": state.total_batches,
                "completed_batches": state.completed_batches,
                "failed_batches": state.failed_batches,
                "queue_depth": queue_ref.qsize(),
                "producers_done": service.producers_done,
            }

    @app.get("/progress")
    def progress() -> Dict[str, Any]:
        with state.lock:
            pending = (
                state.total_batches - state.completed_batches - state.failed_batches
            )
            return {
                "total": state.total_batches,
                "completed": state.completed_batches,
                "failed": state.failed_batches,
                "pending": max(pending, 0),
            }

    @app.get("/throughput")
    def throughput() -> Dict[str, Any]:
        with state.lock:
            elapsed = max(time.time() - state.start_ts, 0.001)
            gb_processed = state.completed_bytes / (1024**3)
            return {
                "batches_per_sec": state.completed_batches / elapsed,
                "elapsed_sec": elapsed,
                "batches_completed": state.completed_batches,
                "batches_failed": state.failed_batches,
                "files_processed": state.completed_files,
                "bytes_processed": state.completed_bytes,
                "gb_processed": gb_processed,
                "bytes_per_sec": state.completed_bytes / elapsed,
                "gb_per_sec": gb_processed / elapsed,
                "files_per_sec": state.completed_files / elapsed,
            }

    @app.get("/workers")
    def workers() -> Dict[str, Any]:
        with state.lock:
            return {
                "heartbeats": {
                    worker_id: time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(timestamp)
                    )
                    for worker_id, timestamp in state.heartbeats.items()
                }
            }

    @app.get("/logs")
    def logs() -> Dict[str, Any]:
        with state.lock:
            return {"warnings": state.warnings, "errors": state.errors}

    @app.get("/results")
    def results() -> Dict[str, Any]:
        with state.lock:
            return {
                "results": [result.to_dict() for result in state.results.values()],
                "retained_limit": state.max_results,
                "retained_count": len(state.results),
            }

    return app


def _bind_socket_with_retry(
    socket_obj: zmq.Socket, endpoint: str, retries: int = 3, delay: float = 0.2
) -> None:
    for attempt in range(retries):
        try:
            socket_obj.bind(endpoint)
            return
        except zmq.ZMQError as exc:
            if exc.errno != errno.EADDRINUSE or attempt >= retries - 1:
                raise
            time.sleep(delay)


def _ensure_port_available(
    host: str, port: int, retries: int = 3, delay: float = 0.2
) -> None:
    last_error: Optional[OSError] = None
    for attempt in range(retries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
                return
            except OSError as exc:
                last_error = exc
                if exc.errno != errno.EADDRINUSE or attempt >= retries - 1:
                    raise
        time.sleep(delay)
    if last_error:
        raise last_error


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


def _producer_main(
    config: MasterConfig, bucket_index: int, files: List[Dict[str, Any]]
) -> None:
    context = zmq.Context.instance()
    socket = context.socket(zmq.PUSH)
    socket.connect(f"tcp://{config.bind_host}:{config.batch_port}")

    def file_iter() -> Iterator[FileInfo]:
        for item in files:
            info = FileInfo(
                path=item["path"],
                size=item.get("size", 0),
                is_dir=item.get("is_dir", False),
            )
            if info.is_dir:
                yield from scan_subtree(config.src, info.path)
            else:
                yield info

    for batch in iter_batches(
        config.src,
        config.dst,
        files=file_iter(),
        max_files=config.batch_num_files,
        max_bytes=config.batch_size,
        compress_paths_enabled=config.compress_paths,
        compress_max_depth=config.compress_max_depth,
    ):
        socket.send(json_dumps({"type": "batch", "batch": batch.to_dict()}))
    socket.send(json_dumps({"type": "producer_done", "bucket": bucket_index}))


def parse_args() -> MasterConfig:
    parser = argparse.ArgumentParser(description="nsync master")
    parser.add_argument("--src", required=True)
    parser.add_argument("--dst", required=True)
    parser.add_argument("--batch-num-files", type=int, required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--num-master-processes", type=int, required=True)
    parser.add_argument("--master-scan-depth", type=int, required=True)
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--claim-port", type=int, default=5555)
    parser.add_argument("--batch-port", type=int, default=5556)
    parser.add_argument("--result-port", type=int, default=5557)
    parser.add_argument("--heartbeat-port", type=int, default=5558)
    parser.add_argument("--api-port", type=int, default=8000)
    parser.add_argument("--exit-when-done", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--queue-threshold", type=int, default=1000)
    parser.add_argument("--log-dir", default="")
    parser.add_argument("--log-prefix", default="")
    parser.add_argument("--compress-paths", action="store_true")
    parser.add_argument("--compress-max-depth", type=int, default=2)
    parser.add_argument("--heartbeat-timeout", type=float, default=15.0)
    args = parser.parse_args()
    log_file = _resolve_log_file(args.log_dir, args.log_prefix, "master")
    return MasterConfig(
        src=args.src,
        dst=args.dst,
        batch_num_files=args.batch_num_files,
        batch_size=args.batch_size,
        num_master_processes=args.num_master_processes,
        master_scan_depth=args.master_scan_depth,
        bind_host=args.bind_host,
        claim_port=args.claim_port,
        batch_port=args.batch_port,
        result_port=args.result_port,
        heartbeat_port=args.heartbeat_port,
        api_port=args.api_port,
        exit_when_done=args.exit_when_done,
        debug=args.debug,
        queue_threshold=args.queue_threshold,
        log_file=log_file,
        compress_paths=args.compress_paths,
        compress_max_depth=args.compress_max_depth,
        heartbeat_timeout=args.heartbeat_timeout,
    )


def main() -> None:
    config = parse_args()
    log_level = logging.DEBUG if config.debug else logging.INFO
    logger = configure_logger(
        "nsync.master", log_level, pretty=True, log_file=config.log_file
    )
    if not os.path.isdir(config.src):
        raise SystemExit(f"source path not found: {config.src}")

    logger.info(
        "scan_start", {"src": config.src, "scan_depth": config.master_scan_depth}
    )
    files = scan_paths(config.src, config.master_scan_depth)
    logger.info("scan_complete", {"files": len(files)})
    logger.debug("scan_complete", {"files": len(files)})
    buckets = bucketize(files, config.num_master_processes)
    service = MasterService(config)
    service.start()

    processes: List[multiprocessing.Process] = []
    for index, bucket in enumerate(buckets):
        payload = [
            {"path": item.path, "size": item.size, "is_dir": item.is_dir}
            for item in bucket
        ]
        process = multiprocessing.Process(
            target=_producer_main, args=(config, index, payload), daemon=True
        )
        process.start()
        processes.append(process)
    logger.debug("producers_started", {"count": len(processes)})
    logger.info("producers_started", {"count": len(processes)})

    stop_event = threading.Event()
    signal_info: Dict[str, Optional[int]] = {"signum": None}

    def handle_signal(signum: int, frame: Optional[Any]) -> None:
        signal_info["signum"] = signum
        stop_event.set()
        service.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while not stop_event.is_set():
        if service.stop_event.is_set():
            break
        alive = any(process.is_alive() for process in processes)
        if not alive:
            if config.exit_when_done:
                service.wait_until_done()
                service.log_summary()
                break
        time.sleep(0.2)

    for process in processes:
        process.join(timeout=2)
    if signal_info["signum"] is not None:
        logger.info("signal", {"signum": signal_info["signum"]})
    service.stop()


if __name__ == "__main__":
    main()
