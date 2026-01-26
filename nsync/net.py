from __future__ import annotations

import hashlib
from typing import Dict


BASE_PORTS: Dict[str, int] = {
    "claim": 5555,
    "result": 5556,
    "heartbeat": 5557,
    "termination": 5558,
    "api": 8000,
}


def compute_port_set(
    task_id: int,
    base_ports: Dict[str, int],
    range_size: int,
    retry_index: int,
) -> Dict[str, int]:
    if range_size <= 0:
        raise ValueError("range_size must be positive")
    if retry_index < 0:
        raise ValueError("retry_index must be non-negative")
    if task_id < 0:
        raise ValueError("task_id must be non-negative")

    digest = hashlib.sha256(str(task_id).encode("utf-8")).hexdigest()
    offset = int(digest, 16) % range_size
    retry_offset = retry_index * range_size
    return {
        name: base + offset + retry_offset for name, base in base_ports.items()
    }
