from __future__ import annotations

import argparse
import os
import queue
import shlex
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import zmq

from .common import configure_logger, json_dumps, json_loads, new_worker_id, resolve_rsync_exit_code, utc_timestamp


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


class WorkerService:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.worker_id = new_worker_id()
        self.logger = configure_logger("nsync.worker")
        self.context = zmq.Context.instance()
        self.stop_event = threading.Event()

    def start(self) -> None:
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        processes: List[threading.Thread] = []
        for index in range(self.config.num_worker_processes):
            thread = threading.Thread(target=self._worker_loop, args=(index,), daemon=True)
            thread.start()
            processes.append(thread)

        def handle_signal(signum: int, frame: Optional[Any]) -> None:
            self.logger.info("signal", {"signum": signum})
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        while not self.stop_event.is_set():
            alive = any(thread.is_alive() for thread in processes)
            if not alive:
                break
            time.sleep(0.2)

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
        claim_socket.connect(f"tcp://{self.config.master_host}:{self.config.claim_port}")
        result_socket = self.context.socket(zmq.PUSH)
        result_socket.connect(f"tcp://{self.config.master_host}:{self.config.result_port}")
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
            if status == "empty":
                time.sleep(0.2)
                continue
            if status == "done":
                break
            if status != "ok":
                time.sleep(0.2)
                continue
            batch = response["batch"]
            result = self._process_batch(batch)
            result_socket.send(json_dumps(result))

    def _process_batch(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        batch_id = batch["batch_id"]
        src_base = batch["src_base"]
        dst_base = batch["dst_base"]
        paths = batch["paths"]
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
            warning = f"batch {batch_id} retry {retries} exit={exit_code}"
            self.logger.warning(warning)
            errors.append(warning)
            time.sleep(2**attempt)
        end_ts = utc_timestamp()
        result = {
            "type": "result",
            "worker_id": self.worker_id,
            "batch_id": batch_id,
            "status": status,
            "retry_count": retries,
            "rsync_exit_code": exit_code,
            "stats": {
                "bytes_sent": 0,
                "bytes_received": 0,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
            "errors": errors,
        }
        return result

    def _run_rsync(self, src_base: str, dst_base: str, paths: List[str]) -> int:
        dst_host = self.config.dst_host
        local_host = dst_host in {"", "localhost", "127.0.0.1"}
        self._ensure_destinations(dst_host, dst_base, paths)
        cmd = [self.config.rsync_bin, "-a", "--xattrs", "--checksum"] + self.config.rsync_args
        if len(paths) == 1:
            src_path = os.path.join(src_base, paths[0])
            dst_path = os.path.join(dst_base, paths[0]) if local_host else f"{dst_host}:{os.path.join(dst_base, paths[0])}"
            cmd.extend([src_path, dst_path])
            return subprocess.call(cmd)
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as handle:
            for path in paths:
                handle.write(path + "\n")
            list_path = handle.name
        cmd.extend(["--files-from", list_path, os.path.join(src_base, "")])
        dst_path = os.path.join(dst_base, "") if local_host else f"{dst_host}:{os.path.join(dst_base, "")}".rstrip(":")
        cmd.append(dst_path)
        try:
            return subprocess.call(cmd)
        finally:
            os.unlink(list_path)

    def _ensure_destinations(self, dst_host: str, dst_base: str, paths: List[str]) -> None:
        parents = {os.path.dirname(os.path.join(dst_base, path)) for path in paths}
        parents.discard("")
        if dst_host in {"", "localhost", "127.0.0.1"}:
            for parent in parents:
                os.makedirs(parent, exist_ok=True)
            return
        for parent in parents:
            subprocess.call(["ssh", dst_host, "mkdir", "-p", parent])


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
    args = parser.parse_args()
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
    )


def main() -> None:
    config = parse_args()
    service = WorkerService(config)
    service.start()


if __name__ == "__main__":
    main()
