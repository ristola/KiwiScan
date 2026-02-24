from kiwi_scan.detect import PersistenceTracker, detect_peaks_with_noise_floor


def test_detect_peaks_and_persistence() -> None:
    # Build a fake spectrum with a stable peak around bin 10.
    base = [0.0] * 64

    tracker = PersistenceTracker(required_hits=3, tolerance_bins=1.5, expiry_frames=5)

    seen_any = False
    for frame in range(5):
        bins = list(base)
        # noise wiggle
        bins[3] = 0.5
        bins[20] = 0.2
        # stable peak
        bins[10] = 15.0

        noise, peaks = detect_peaks_with_noise_floor(bins, threshold_db=5.0)
        assert noise >= 0.0
        persist = tracker.update(frame, peaks)
        if frame >= 2:
            # By frame 2 (3 hits), should be persistent.
            assert any(abs(p.bin_center - 10.0) <= 1.5 for p in persist)
            seen_any = True

    assert seen_any
