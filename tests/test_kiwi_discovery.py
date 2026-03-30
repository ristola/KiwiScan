from kiwi_scan.kiwi_discovery import DEFAULT_KIWI_HOST, LEGACY_DEFAULT_KIWI_HOST, is_unconfigured_kiwi_host, normalize_kiwi_host


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