from kiwi_scan.scan import _TemporalFeatureTracker, classify_candidate_type


def test_candidate_type_classifies_keyed_cw_as_narrow_single() -> None:
    assert (
        classify_candidate_type(
            width_hz=35.0,
            type_guess="cw",
            bandplan_label="CW",
            keying_score=0.42,
            cadence_score=0.28,
            has_on_off_keying=True,
        )
        == "NARROW_SINGLE"
    )


def test_candidate_type_does_not_use_cw_bandplan_alone_for_steady_tone() -> None:
    assert (
        classify_candidate_type(
            width_hz=22.0,
            type_guess="very_narrow+cw",
            bandplan_label="CW",
            keying_score=0.02,
            cadence_score=0.01,
        )
        == "NARROW_MULTI"
    )


def test_candidate_type_classifies_ft8_like_hits_as_narrow_multi() -> None:
    assert (
        classify_candidate_type(
            width_hz=62.0,
            type_guess="digital",
            bandplan_label="Phone",
        )
        == "NARROW_MULTI"
    )


def test_candidate_type_classifies_medium_digital_bandwidth() -> None:
    assert (
        classify_candidate_type(
            width_hz=900.0,
            type_guess="digital",
            bandplan_label="RTTY",
            voice_score=0.03,
            occ_frac=0.22,
        )
        == "MEDIUM_DIGITAL"
    )


def test_candidate_type_classifies_multi_peak_midband_as_digital_cluster() -> None:
    assert (
        classify_candidate_type(
            width_hz=1607.0,
            type_guess="phone",
            bandplan_label="RTTY",
            voice_score=0.29,
            occ_frac=0.41,
            speech_score=0.18,
            narrow_peak_count=7,
            envelope_variance=0.05,
        )
        == "DIGITAL_CLUSTER"
    )


def test_candidate_type_classifies_phone_as_wideband_voice() -> None:
    assert (
        classify_candidate_type(
            width_hz=2300.0,
            type_guess="phone",
            bandplan_label="Phone",
            voice_score=0.46,
            occ_frac=0.33,
            speech_score=0.32,
            envelope_variance=0.18,
        )
        == "WIDEBAND_VOICE"
    )


def test_candidate_type_classifies_sstv_like_hits_as_wideband_image() -> None:
    assert (
        classify_candidate_type(
            width_hz=2300.0,
            type_guess="image",
            bandplan_label="Phone",
            voice_score=0.04,
            occ_frac=0.19,
        )
        == "WIDEBAND_IMAGE"
    )


def test_temporal_tracker_detects_on_off_keying_from_amplitude_series() -> None:
    tracker = _TemporalFeatureTracker()
    bin_center = 42.0
    freq_hz = 14_025_100.0

    for frame_index, rel_db in enumerate([12.0, 0.0, 11.0, 0.0, 10.0, 0.0, 11.0]):
        peak_details = []
        if rel_db > 0.0:
            peak_details = [
                {
                    "bin_center": bin_center,
                    "freq_hz": freq_hz,
                    "width_hz": 35.0,
                    "rel_db": rel_db,
                }
            ]
        tracker.update(frame_index=frame_index, peak_details=peak_details)

    summary = tracker.summarize_for_bin(bin_center=bin_center)

    assert summary["has_on_off_keying"] is True
    assert summary["keying_edge_count"] == 6
    assert float(summary["active_fraction"]) < 0.7
    assert float(summary["keying_score"]) >= 0.6