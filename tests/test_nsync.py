import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest
sys.path.append(str(Path(__file__).resolve().parents[1]))

from nsync.batcher import compress_paths


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_compress_paths(tmp_path: Path) -> None:
    root = tmp_path / "src"
    root.mkdir()
    (root / "dir").mkdir()
    (root / "dir" / "a.txt").write_text("a")
    (root / "dir" / "b.txt").write_text("b")
    (root / "other.txt").write_text("c")

    compressed = compress_paths(str(root), ["dir/a.txt", "dir/b.txt"])
    assert compressed == ["dir"]


def test_master_worker_roundtrip(tmp_path: Path) -> None:
    pytest.importorskip("zmq")
    pytest.importorskip("fastapi")
    pytest.importorskip("uvicorn")
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    for index in range(3):
        (src / f"file{index}.txt").write_text(f"value-{index}")

    claim_port = _free_port()
    batch_port = _free_port()
    result_port = _free_port()
    heartbeat_port = _free_port()
    api_port = _free_port()

    master_cmd = [
        sys.executable,
        "-m",
        "nsync.master",
        "--src",
        str(src),
        "--dst",
        str(dst),
        "--batch-num-files",
        "2",
        "--batch-size",
        "1024",
        "--num-master-processes",
        "1",
        "--master-scan-depth",
        "2",
        "--bind-host",
        "127.0.0.1",
        "--claim-port",
        str(claim_port),
        "--batch-port",
        str(batch_port),
        "--result-port",
        str(result_port),
        "--heartbeat-port",
        str(heartbeat_port),
        "--api-port",
        str(api_port),
        "--exit-when-done",
    ]

    worker_cmd = [
        sys.executable,
        "-m",
        "nsync.worker",
        "--num-worker-processes",
        "1",
        "--dst-host",
        "localhost",
        "--master-host",
        "127.0.0.1",
        "--claim-port",
        str(claim_port),
        "--result-port",
        str(result_port),
        "--heartbeat-port",
        str(heartbeat_port),
        "--rsync-bin",
        "/bin/true",
    ]

    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    master = subprocess.Popen(master_cmd, cwd=str(tmp_path), env=env)
    try:
        deadline = time.time() + 10
        status_payload = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{api_port}/status") as response:
                    if response.status == 200:
                        status_payload = json.loads(response.read().decode("utf-8"))
                        break
            except Exception:
                time.sleep(0.1)
        worker = subprocess.Popen(worker_cmd, cwd=str(tmp_path), env=env)
        worker.wait(timeout=30)
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{api_port}/status") as response:
                status_payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            pass
        assert status_payload is not None
        assert status_payload["total_batches"] >= 1
    finally:
        master.wait(timeout=30)
