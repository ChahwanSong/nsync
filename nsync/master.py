from __future__ import annotations

import argparse
import errno
import socket
from dataclasses import dataclass, field
from typing import Dict, Iterable, List

from nsync.net import BASE_PORTS, compute_port_set


@dataclass
class MasterConfig:
    task_id: int
    base_ports: Dict[str, int]
    range_size: int
    port_retry: int
    initial_ports: Dict[str, int]


@dataclass
class Master:
    config: MasterConfig
    retry_index: int = 0
    ports: Dict[str, int] = field(default_factory=dict)
    sockets: List[socket.socket] = field(default_factory=list)

    def _bind_ports(self, ports: Dict[str, int]) -> None:
        bound: List[socket.socket] = []
        try:
            for name, port in ports.items():
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("0.0.0.0", port))
                if name == "api":
                    sock.listen(128)
                bound.append(sock)
        except OSError:
            for sock in bound:
                sock.close()
            raise
        self.sockets = bound

    def start(self) -> Dict[str, int]:
        last_error: OSError | None = None
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
            try:
                self._bind_ports(ports)
            except OSError as exc:
                last_error = exc
                if exc.errno != errno.EADDRINUSE or retry_index >= self.config.port_retry:
                    raise
                continue
            self.retry_index = retry_index
            self.ports = ports
            return ports
        if last_error is not None:
            raise last_error
        raise RuntimeError("Failed to bind ports")

    def close(self) -> None:
        for sock in self.sockets:
            sock.close()
        self.sockets.clear()


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


def parse_args(argv: Iterable[str] | None = None) -> MasterConfig:
    parser = argparse.ArgumentParser(description="nsync master")
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--range-size", type=int, default=64)
    parser.add_argument("--port-retry", type=int, default=3)
    _add_base_port_args(parser)
    args = parser.parse_args(argv)
    base_ports = _collect_base_ports(args)
    initial_ports = compute_port_set(
        task_id=args.task_id,
        base_ports=base_ports,
        range_size=args.range_size,
        retry_index=0,
    )
    return MasterConfig(
        task_id=args.task_id,
        base_ports=base_ports,
        range_size=args.range_size,
        port_retry=args.port_retry,
        initial_ports=initial_ports,
    )


def main(argv: Iterable[str] | None = None) -> int:
    config = parse_args(argv)
    master = Master(config)
    master.start()
    master.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
