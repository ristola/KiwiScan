from __future__ import annotations

from kiwi_scan.kiwi_waterfall import _build_set_mod_cmd, _set_receiver_frequency_once


class _Opts:
    def __init__(self, *, modulation: str, lp_cut: int, hp_cut: int) -> None:
        self.modulation = modulation
        self.lp_cut = lp_cut
        self.hp_cut = hp_cut
        self.freq_pbc = False


class _FakeCmdStream:
    def __init__(self, *, modulation: str, lp_cut: int, hp_cut: int) -> None:
        self._options = _Opts(modulation=modulation, lp_cut=lp_cut, hp_cut=hp_cut)

    def _remove_freq_offset(self, freq: float) -> float:
        return float(freq)


def test_build_set_mod_cmd_normalizes_iq_passband() -> None:
    stream = _FakeCmdStream(modulation="iq", lp_cut=300, hp_cut=2700)

    cmd = _build_set_mod_cmd(s=stream, freq_khz=14025.0)

    assert cmd == "SET mod=iq low_cut=-6000 high_cut=6000 freq=14025.000"


class _FakeKiwiStream:
    last_instance = None

    def __init__(self) -> None:
        _FakeKiwiStream.last_instance = self
        self.sent: list[str] = []
        self._sample_rate = None
        self._type = None
        self._freq = 0.0
        self._camp_chan = -1
        self._stream = object()
        self._options = None

    def _send_message(self, msg: str) -> None:
        self.sent.append(str(msg))

    def connect(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def _process_msg_param(self, name: str, value: object) -> None:
        if name == "sample_rate":
            self._sample_rate = float(value)
            self._setup_rx_params()

    def run(self) -> None:
        if self._sample_rate is None:
            self._process_msg_param("sample_rate", "12000")


def test_set_receiver_frequency_succeeds_without_rx_chan_message() -> None:
    ok = _set_receiver_frequency_once(
        KiwiSDRStream=_FakeKiwiStream,
        host="kiwi.local",
        port=8073,
        rx_chan=0,
        freq_hz=14.025e6,
        password=None,
        user="DEBUG_RX0",
        timeout_s=1.0,
        hold_s=0.0,
        modulation="iq",
    )

    assert ok is True
    sent = list(_FakeKiwiStream.last_instance.sent)
    assert "SET ident_user=DEBUG_RX0" in sent
    assert "SET mod=iq low_cut=-6000 high_cut=6000 freq=14025.000" in sent