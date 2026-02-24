import queue
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("zmq")
pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")

import nsync.master as master_module
from nsync.constants import DEFAULT_TIMEZONE
from nsync.master import MasterConfig, MasterService, MasterState, create_app, parse_args


class _DummyService:
    def __init__(self) -> None:
        self.done_flag = threading.Event()
        self.producers_done = 0
        self.producers_total = 1


def _build_config(
    tmp_path: Path,
    *,
    heartbeat_timeout: float = 15.0,
    progress_log_enabled: bool = True,
    fastapi_output_enabled: bool = True,
) -> MasterConfig:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir(exist_ok=True)
    dst.mkdir(exist_ok=True)
    return MasterConfig(
        src=str(src),
        dst=str(dst),
        batch_num_files=1000,
        batch_size=1024 * 1024,
        num_master_processes=1,
        master_scan_depth=3,
        bind_host="127.0.0.1",
        claim_port=15555,
        batch_port=15556,
        result_port=15557,
        heartbeat_port=15558,
        api_port=18000,
        exit_when_done=False,
        debug=False,
        progress_log_enabled=progress_log_enabled,
        fastapi_output_enabled=fastapi_output_enabled,
        queue_threshold=10000,
        log_file=None,
        heartbeat_timeout=heartbeat_timeout,
        requeue_limit=3,
        rsync_args=[],
    )


def _get_endpoint(app, path: str):
    for route in app.routes:
        if getattr(route, "path", None) == path:
            return route.endpoint
    raise AssertionError(f"endpoint not found: {path}")


def test_parse_args_no_progress(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    monkeypatch.setattr(
        sys,
        "argv",
        ["nsync.master", "--src", str(src), "--dst", str(dst), "--no-progress"],
    )
    config = parse_args()
    assert config.progress_log_enabled is False


def test_parse_args_quiet_fastapi(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "nsync.master",
            "--src",
            str(src),
            "--dst",
            str(dst),
            "--quiet-fastapi",
        ],
    )
    config = parse_args()
    assert config.fastapi_output_enabled is False


def test_start_api_disables_uvicorn_access_log_when_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = {}

    class DummyConfig:
        def __init__(
            self,
            app,
            *,
            host: str,
            port: int,
            log_level: str,
            access_log: bool,
        ) -> None:
            captured["host"] = host
            captured["port"] = port
            captured["log_level"] = log_level
            captured["access_log"] = access_log

    class DummyServer:
        def __init__(self, config) -> None:
            self.config = config

        def run(self) -> None:
            return

    monkeypatch.setattr(master_module, "_ensure_port_available", lambda *_args: None)
    monkeypatch.setattr(master_module.uvicorn, "Config", DummyConfig)
    monkeypatch.setattr(master_module.uvicorn, "Server", DummyServer)

    service = MasterService(_build_config(tmp_path, fastapi_output_enabled=False))
    service._start_api()
    assert captured["access_log"] is False
    assert captured["log_level"] == "warning"


def test_mark_producer_done_deduplicates_sources(tmp_path: Path) -> None:
    service = MasterService(_build_config(tmp_path))
    service.set_producers_total(1)
    service.logger.info = lambda *_args, **_kwargs: None
    service.logger.warning = lambda *_args, **_kwargs: None
    assert service.mark_producer_done(0, source="process_exit", exit_code=1) is True
    assert service.mark_producer_done(0, source="batch_message") is False
    assert service.producer_counts() == (1, 1)


def test_status_done_true_when_producer_message_missing(tmp_path: Path) -> None:
    service = MasterService(_build_config(tmp_path))
    service.set_producers_total(1)
    with service.state.lock:
        service.state.total_batches = 3
        service.state.completed_batches = 3
    service.mark_producer_done(0, source="process_exit", exit_code=1)
    app = create_app(service.state, queue.Queue(), service)
    status_endpoint = _get_endpoint(app, "/status")
    payload = status_endpoint()
    assert payload["producers_done"] == 1
    assert payload["producers_total"] == 1
    assert payload["done"] is True


def test_master_progress_logging_can_be_disabled(tmp_path: Path) -> None:
    service = MasterService(_build_config(tmp_path, progress_log_enabled=False))
    with service.state.lock:
        service.state.total_batches = 1
    info_calls = []
    service.logger.info = lambda message, *args: info_calls.append((message, args))
    service._maybe_log_progress()
    assert info_calls == []


def test_heartbeat_timeout_logs_warning(tmp_path: Path) -> None:
    service = MasterService(_build_config(tmp_path, heartbeat_timeout=1.0))
    worker_id = "worker-1"
    stale_ts = time.time() - 10.0
    with service.state.lock:
        service.state.heartbeats[worker_id] = stale_ts
        service.state.last_heartbeats[worker_id] = stale_ts
    warning_calls = []
    service.logger.warning = lambda message, *args: warning_calls.append((message, args))
    service.logger.info = lambda *_args: None
    service._process_timed_out_workers()
    timeout_logs = [entry for entry in warning_calls if entry[0] == "worker_heartbeat_timeout"]
    assert timeout_logs
    payload = timeout_logs[0][1][0]
    assert payload["worker_id"] == worker_id
    assert worker_id not in service.state.heartbeats
    assert worker_id in service.state.last_heartbeats


def test_workers_endpoint_includes_last_heartbeat_per_worker() -> None:
    state = MasterState()
    worker_id = "worker-1"
    state.update_heartbeat(worker_id)
    with state.lock:
        last_ts = state.heartbeats[worker_id]
    state.take_timed_out_tasks(timeout_sec=0.0)
    app = create_app(state, queue.Queue(), _DummyService())
    workers_endpoint = _get_endpoint(app, "/workers")
    payload = workers_endpoint()
    expected = datetime.fromtimestamp(last_ts, tz=ZoneInfo(DEFAULT_TIMEZONE)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    assert worker_id not in payload["heartbeats"]
    assert payload["workers"][worker_id]["last_heartbeat"] == expected
