import sys
from pathlib import Path

import pytest

pytest.importorskip("zmq")

from nsync.worker import parse_args


def test_parse_args_output_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "logs" / "node-a"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "nsync.worker",
            "--output",
            str(output),
        ],
    )
    config = parse_args()
    assert config.log_file == f"{output}-worker.log"
