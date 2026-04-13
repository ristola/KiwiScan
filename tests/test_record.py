from __future__ import annotations

import sys
from pathlib import Path

import kiwi_scan.record as record_mod


def test_find_kiwirecorder_falls_back_to_vendor(monkeypatch) -> None:
    monkeypatch.setattr(record_mod.shutil, "which", lambda _name: None)

    path = record_mod.find_kiwirecorder()

    expected = Path(record_mod.__file__).resolve().parents[2] / "vendor" / "kiwiclient-jks" / "kiwirecorder.py"
    assert path == str(expected)


def test_run_record_uses_python_for_vendor_script_and_passes_rx_chan(monkeypatch, tmp_path: Path) -> None:
    recorder_path = tmp_path / "kiwirecorder.py"
    recorder_path.write_text("# stub\n", encoding="utf-8")
    captured: dict[str, object] = {}

    monkeypatch.setattr(record_mod, "find_kiwirecorder", lambda: str(recorder_path))

    def _fake_run(cmd, check):
        captured["cmd"] = list(cmd)
        captured["check"] = check
        return None

    monkeypatch.setattr(record_mod.subprocess, "run", _fake_run)

    out_dir = tmp_path / "recordings"
    result = record_mod.run_record(
        record_mod.RecordRequest(
            host="kiwi.local",
            port=8073,
            password="secret",
            user="receiver-scan",
            freq_hz=14.035e6,
            rx_chan=1,
            duration_s=60,
            mode="cw",
            out_dir=out_dir,
        )
    )

    assert result == out_dir
    assert out_dir.exists()
    assert captured["check"] is True
    assert captured["cmd"] == [
        sys.executable,
        str(recorder_path),
        "--server-host",
        "kiwi.local",
        "--server-port",
        "8073",
        "--user",
        "receiver-scan",
        "--freq",
        "14035.0",
        "--modulation",
        "cw",
        "--tlimit",
        "60",
        "--dir",
        str(out_dir),
        "--rx-chan",
        "1",
        "--password",
        "secret",
    ]