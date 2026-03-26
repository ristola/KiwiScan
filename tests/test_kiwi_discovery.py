from kiwi_scan.kiwi_discovery import DEFAULT_KIWI_HOST, is_unconfigured_kiwi_host


def test_unconfigured_kiwi_host_accepts_new_and_legacy_defaults():
    assert is_unconfigured_kiwi_host(DEFAULT_KIWI_HOST)
    assert is_unconfigured_kiwi_host("192.168.1.93")
    assert is_unconfigured_kiwi_host("")
    assert is_unconfigured_kiwi_host("localhost")


def test_unconfigured_kiwi_host_rejects_real_configured_host():
    assert not is_unconfigured_kiwi_host("192.168.1.42")