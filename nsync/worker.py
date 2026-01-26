from __future__ import annotations

import argparse
import socket
from dataclasses import dataclass
from typing import Dict, Iterable

from nsync.net import BASE_PORTS, compute_port_set


@dataclass
class WorkerConfig:
    task_id: int
    base_ports: Dict[str, int]
    range_size: int
    port_retry: int
    connect_timeout: float
    initial_ports: Dict[str, int]


@dataclass
class Worker:
    config: WorkerConfig
    retry_index: int = 0
    ports: Dict[str, int] | None = None

    def _can_connect(self, ports: Dict[str, int]) -> bool:
        for port in ports.values():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(self.config.connect_timeout)
                try:
                    sock.connect(("127.0.0.1", port))
                except OSError:
                    return False
        return True

    def connect(self) -> Dict[str, int]:
        for retry_index in range(self.config.port_retry + 1):
            if retry_index == 0:
                ports = dict(self.config.initial_ports)
            else:
                ports = compute_port_set(
                    task_id=self.config.task_id,
                    base_ports=self.config.base_ports,
                    range_size=self.config.range_size,
                    retry_index=retry_index,
                )
            if self._can_connect(ports):
                self.retry_index = retry_index
                self.ports = ports
                return ports
        raise ConnectionError("Unable to connect to master ports")


def _add_base_port_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--claim-port", type=int, default=BASE_PORTS["claim"])
    parser.add_argument("--result-port", type=int, default=BASE_PORTS["result"])
    parser.add_argument("--heartbeat-port", type=int, default=BASE_PORTS["heartbeat"])
    parser.add_argument("--termination-port", type=int, default=BASE_PORTS["termination"])
    parser.add_argument("--api-port", type=int, default=BASE_PORTS["api"])


def _collect_base_ports(args: argparse.Namespace) -> Dict[str, int]:
    return {
        "claim": args.claim_port,
        "result": args.result_port,
        "heartbeat": args.heartbeat_port,
        "termination": args.termination_port,
        "api": args.api_port,
    }


def parse_args(argv: Iterable[str] | None = None) -> WorkerConfig:
    parser = argparse.ArgumentParser(description="nsync worker")
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--range-size", type=int, default=64)
    parser.add_argument("--port-retry", type=int, default=3)
    parser.add_argument("--connect-timeout", type=float, default=0.25)
    _add_base_port_args(parser)
    args = parser.parse_args(argv)
    base_ports = _collect_base_ports(args)
    initial_ports = compute_port_set(
        task_id=args.task_id,
        base_ports=base_ports,
        range_size=args.range_size,
        retry_index=0,
    )
    return WorkerConfig(
        task_id=args.task_id,
        base_ports=base_ports,
        range_size=args.range_size,
        port_retry=args.port_retry,
        connect_timeout=args.connect_timeout,
        initial_ports=initial_ports,
    )


def main(argv: Iterable[str] | None = None) -> int:
    config = parse_args(argv)
    worker = Worker(config)
    worker.connect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
