import kiwi_scan.kiwi_discovery as kiwi_discovery
from kiwi_scan.kiwi_discovery import DEFAULT_KIWI_HOST, LEGACY_DEFAULT_KIWI_HOST, is_unconfigured_kiwi_host, normalize_kiwi_host, preferred_discovered_kiwi, sort_discovered_kiwis


def test_unconfigured_kiwi_host_accepts_new_and_legacy_defaults():
    assert is_unconfigured_kiwi_host(DEFAULT_KIWI_HOST)
    assert is_unconfigured_kiwi_host("1.2.3.4")
    assert is_unconfigured_kiwi_host("")
    assert is_unconfigured_kiwi_host("localhost")


def test_unconfigured_kiwi_host_rejects_real_configured_host():
    assert not is_unconfigured_kiwi_host("192.168.1.42")
    assert not is_unconfigured_kiwi_host(LEGACY_DEFAULT_KIWI_HOST)


def test_normalize_kiwi_host_maps_placeholder_to_default():
    assert normalize_kiwi_host("1.2.3.4") == DEFAULT_KIWI_HOST
    assert normalize_kiwi_host(LEGACY_DEFAULT_KIWI_HOST) == LEGACY_DEFAULT_KIWI_HOST
    assert normalize_kiwi_host("192.168.1.42") == "192.168.1.42"


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_discover_kiwis_scans_multiple_ports_for_lan_hosts(monkeypatch):
    open_endpoints = {
        ("10.13.1.235", 8073),
        ("10.13.1.236", 8074),
    }

    def _raise_url_error(*args, **kwargs):
        raise OSError("offline")

    def _fake_create_connection(address, timeout):
        endpoint = (str(address[0]), int(address[1]))
        if endpoint in open_endpoints:
            return _FakeConnection()
        raise OSError("closed")

    monkeypatch.setattr(kiwi_discovery, "urlopen", _raise_url_error)
    monkeypatch.setattr(kiwi_discovery, "_private_prefixes_for_lan_scan", lambda client_ip: ["10.13.1"])
    monkeypatch.setattr(kiwi_discovery.socket, "create_connection", _fake_create_connection)
    monkeypatch.setattr(kiwi_discovery, "_looks_like_kiwi_http", lambda host, port, timeout_s: (host, port) in open_endpoints)
    monkeypatch.setattr(
        kiwi_discovery,
        "read_kiwi_status",
        lambda host, port, timeout_s: {"name": f"Kiwi {port}", "sdr_hw": f"KiwiSDR {port}", "loc": host},
    )

    result = kiwi_discovery.discover_kiwis(
        client_ip="10.13.1.20",
        port=8073,
        ports=[8074],
        timeout_s=0.01,
        max_hosts=8,
    )

    assert result["source"] == "lan_scan"
    assert [(item["host"], item["port"]) for item in result["found"]] == [
        ("10.13.1.235", 8073),
        ("10.13.1.236", 8074),
    ]


def test_sort_discovered_kiwis_prefers_port_8073_first():
    ordered = sort_discovered_kiwis([
        {"host": "10.13.1.236", "port": 8074, "name": "Kiwi 2"},
        {"host": "10.13.1.235", "port": 8073, "name": "Kiwi 1"},
    ])

    assert [(item["host"], item["port"]) for item in ordered] == [
        ("10.13.1.235", 8073),
        ("10.13.1.236", 8074),
    ]
    assert preferred_discovered_kiwi(ordered) == {"host": "10.13.1.235", "port": 8073, "name": "Kiwi 1"}
