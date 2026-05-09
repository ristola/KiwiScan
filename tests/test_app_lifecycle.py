from __future__ import annotations

import threading

import kiwi_scan.app_lifecycle as app_lifecycle


class _MgrStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.host = "10.13.1.236"
        self.port = 8074
        self.rx_chan = 3
        self.save_calls = 0

    def _save_config(self) -> None:
        self.save_calls += 1


def test_sync_preferred_startup_kiwi_switches_to_port_8073() -> None:
    mgr = _MgrStub()

    result = app_lifecycle._sync_preferred_startup_kiwi(
        mgr,
        {
            "found": [
                {"host": "10.13.1.236", "port": 8074, "sdr_hw": "KiwiSDR 2"},
                {"host": "10.13.1.235", "port": 8073, "sdr_hw": "KiwiSDR 1"},
            ],
            "source": "lan_scan",
        },
    )

    assert result == {"host": "10.13.1.235", "port": 8073, "changed": True}
    assert mgr.host == "10.13.1.235"
    assert mgr.port == 8073
    assert mgr.rx_chan is None
    assert mgr.save_calls == 1