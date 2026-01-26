from __future__ import annotations

import argparse
import multiprocessing
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import uvicorn
import zmq
from fastapi import FastAPI

from .batcher import FileInfo, build_batches, bucketize, scan_paths
from .common import Batch, BatchResult, configure_logger, json_loads, json_dumps


@dataclass
class MasterState:
    total_batches: int = 0
    completed_batches: int = 0
    failed_batches: int = 0
    start_ts: float = field(default_factory=time.time)
    results: Dict[str, BatchResult] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    heartbeats: Dict[str, float] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record_result(self, result: BatchResult) -> None:
        with self.lock:
            self.results[result.batch_id] = result
            if result.status == "success":
                self.completed_batches += 1
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


class MasterService:
    def __init__(self, config: MasterConfig) -> None:
        self.config = config
        self.logger = configure_logger("nsync.master")
        self.context = zmq.Context.instance()
        self.state = MasterState()
        self.queue: queue.Queue[Batch] = queue.Queue()
        self.stop_event = threading.Event()
        self.producers_done = 0
        self.producers_total = config.num_master_processes
        self.done_flag = threading.Event()

    def start(self) -> None:
        self._start_batch_receiver()
        self._start_result_receiver()
        self._start_heartbeat_receiver()
        self._start_claim_server()
        self._start_api()

    def stop(self) -> None:
        self.stop_event.set()
        self.done_flag.set()
        self.context.term()

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

    def _start_api(self) -> None:
        app = create_app(self.state, self.queue, self)
        config = uvicorn.Config(app, host=self.config.bind_host, port=self.config.api_port, log_level="info")
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()

    def _batch_receiver_loop(self) -> None:
        socket = self.context.socket(zmq.PULL)
        socket.bind(f"tcp://{self.config.bind_host}:{self.config.batch_port}")
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
                self.queue.put(batch)
                with self.state.lock:
                    self.state.total_batches += 1

    def _result_receiver_loop(self) -> None:
        socket = self.context.socket(zmq.PULL)
        socket.bind(f"tcp://{self.config.bind_host}:{self.config.result_port}")
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
                batch_id=message["batch_id"],
                status=message["status"],
                retry_count=message["retry_count"],
                rsync_exit_code=message["rsync_exit_code"],
                stats=message.get("stats", {}),
                errors=message.get("errors", []),
            )
            self.state.record_result(result)

    def _heartbeat_receiver_loop(self) -> None:
        socket = self.context.socket(zmq.PULL)
        socket.bind(f"tcp://{self.config.bind_host}:{self.config.heartbeat_port}")
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

    def _claim_server_loop(self) -> None:
        socket = self.context.socket(zmq.REP)
        socket.bind(f"tcp://{self.config.bind_host}:{self.config.claim_port}")
        while not self.stop_event.is_set():
            try:
                payload = socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.02)
                continue
            request = json_loads(payload)
            response: Dict[str, Any]
            try:
                batch = self.queue.get_nowait()
                response = {"status": "ok", "batch": batch.to_dict()}
            except queue.Empty:
                if self.producers_done >= self.producers_total and self.queue.empty():
                    self.done_flag.set()
                    response = {"status": "done"}
                else:
                    response = {"status": "empty"}
            socket.send(json_dumps(response))

    def wait_until_done(self) -> None:
        while not self.stop_event.is_set():
            if self.done_flag.is_set():
                with self.state.lock:
                    if self.state.completed_batches + self.state.failed_batches >= self.state.total_batches:
                        return
            time.sleep(0.1)


def create_app(state: MasterState, queue_ref: queue.Queue[Batch], service: MasterService) -> FastAPI:
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
            pending = state.total_batches - state.completed_batches - state.failed_batches
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
            return {
                "batches_per_sec": state.completed_batches / elapsed,
                "elapsed_sec": elapsed,
            }

    @app.get("/workers")
    def workers() -> Dict[str, Any]:
        with state.lock:
            return {"heartbeats": state.heartbeats}

    @app.get("/logs")
    def logs() -> Dict[str, Any]:
        with state.lock:
            return {"warnings": state.warnings, "errors": state.errors}

    @app.get("/results")
    def results() -> Dict[str, Any]:
        with state.lock:
            return {"results": [result.to_dict() for result in state.results.values()]}

    return app


def _producer_main(config: MasterConfig, bucket_index: int, files: List[Dict[str, Any]]) -> None:
    context = zmq.Context.instance()
    socket = context.socket(zmq.PUSH)
    socket.connect(f"tcp://{config.bind_host}:{config.batch_port}")
    batch_list = build_batches(
        config.src,
        config.dst,
        files=[FileInfo(path=item["path"], size=item["size"]) for item in files],
        max_files=config.batch_num_files,
        max_bytes=config.batch_size,
    )
    for batch in batch_list:
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
    args = parser.parse_args()
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
    )


def main() -> None:
    config = parse_args()
    logger = configure_logger("nsync.master")
    if not os.path.isdir(config.src):
        raise SystemExit(f"source path not found: {config.src}")

    files = scan_paths(config.src, config.master_scan_depth)
    buckets = bucketize(files, config.num_master_processes)
    service = MasterService(config)
    service.start()

    processes: List[multiprocessing.Process] = []
    for index, bucket in enumerate(buckets):
        payload = [{"path": item.path, "size": item.size} for item in bucket]
        process = multiprocessing.Process(target=_producer_main, args=(config, index, payload), daemon=True)
        process.start()
        processes.append(process)

    stop_event = threading.Event()

    def handle_signal(signum: int, frame: Optional[Any]) -> None:
        logger.info("signal", {"signum": signum})
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while not stop_event.is_set():
        alive = any(process.is_alive() for process in processes)
        if not alive:
            if config.exit_when_done:
                service.wait_until_done()
                break
        time.sleep(0.2)

    for process in processes:
        process.join(timeout=2)
    service.stop()


if __name__ == "__main__":
    main()
