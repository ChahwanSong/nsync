import socket
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from nsync.master import Master, MasterConfig
from nsync.net import compute_port_set
from nsync.worker import Worker, WorkerConfig


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _is_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _find_base_port(range_size: int, retry_count: int) -> int:
    span = range_size * (retry_count + 1) + 1
    for _ in range(200):
        base = _get_free_port()
        ports = [base + offset for offset in range(span)]
        if all(_is_port_free(port) for port in ports):
            return base
    raise RuntimeError("Unable to find free port range")


def _reserve_listener(port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
    sock.listen(4)
    return sock


def test_master_retries_on_conflict() -> None:
    task_id = 101
    range_size = 1
    base_ports = {
        "claim": _find_base_port(range_size, 1),
        "result": _find_base_port(range_size, 1),
        "heartbeat": _find_base_port(range_size, 1),
        "termination": _find_base_port(range_size, 1),
        "api": _find_base_port(range_size, 1),
    }
    initial_ports = compute_port_set(task_id, base_ports, range_size, retry_index=0)
    conflict_socket = _reserve_listener(initial_ports["claim"])
    master = Master(
        MasterConfig(
            task_id=task_id,
            base_ports=base_ports,
            range_size=range_size,
            port_retry=3,
            initial_ports=initial_ports,
        )
    )
    try:
        ports = master.start()
        assert ports == compute_port_set(task_id, base_ports, range_size, retry_index=1)
        assert master.retry_index == 1
    finally:
        conflict_socket.close()
        master.close()


def test_worker_retries_to_connect() -> None:
    task_id = 202
    range_size = 1
    base_ports = {
        "claim": _find_base_port(range_size, 1),
        "result": _find_base_port(range_size, 1),
        "heartbeat": _find_base_port(range_size, 1),
        "termination": _find_base_port(range_size, 1),
        "api": _find_base_port(range_size, 1),
    }
    retry_ports = compute_port_set(task_id, base_ports, range_size, retry_index=1)
    listeners = [_reserve_listener(port) for port in retry_ports.values()]
    worker = Worker(
        WorkerConfig(
            task_id=task_id,
            base_ports=base_ports,
            range_size=range_size,
            port_retry=3,
            connect_timeout=0.1,
            initial_ports=compute_port_set(task_id, base_ports, range_size, retry_index=0),
        )
    )
    try:
        ports = worker.connect()
        assert ports == retry_ports
        assert worker.retry_index == 1
    finally:
        for listener in listeners:
            listener.close()
