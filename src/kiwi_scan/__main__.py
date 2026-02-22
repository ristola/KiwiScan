import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import time

try:
    from importlib.metadata import PackageNotFoundError, version
except Exception:  # pragma: no cover
    PackageNotFoundError = Exception  # type: ignore
    version = None  # type: ignore

from .discovery import DiscoveryWorker, FT8_WATERHOLES
from .kiwi_waterfall import KiwiClientUnavailable
from .scan import run_scan, run_sweep


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="kiwi-scan", description="KiWi SDR scanning helper")
    p.add_argument("--version", action="store_true", help="Print version and exit")

    sub = p.add_subparsers(dest="cmd")

    scan = sub.add_parser("scan", help="Subscribe to waterfall and detect peaks")
    scan.add_argument("--host", required=True, help="KiwiSDR host")
    scan.add_argument("--port", type=int, default=8073, help="KiwiSDR port")
    scan.add_argument("--password", default=None, help="KiwiSDR password (if any)")
    scan.add_argument("--user", default="kiwi-scan", help="Client user name")
    scan.add_argument(
        "--rx",
        type=int,
        default=0,
        help="Receiver index to tune (default: 0). Use --no-rx to disable explicit RX selection.",
    )
    scan.add_argument(
        "--no-rx",
        action="store_true",
        help="Do not tune/camp a specific RX channel (useful if server policies reject RX camping)",
    )
    scan.add_argument("--center-hz", type=float, required=True, help="Center frequency (Hz)")
    scan.add_argument("--span-hz", type=float, default=12000.0, help="Waterfall span (Hz)")
    scan.add_argument(
        "--threshold-db",
        type=float,
        default=10.0,
        help="Detect bins above median noise + threshold (dB)",
    )
    scan.add_argument(
        "--rx-wait-timeout-s",
        type=float,
        default=0.0,
        help="When --rx is set and the Kiwi rejects camping, retry until this timeout (0 = retry forever)",
    )
    scan.add_argument(
        "--rx-wait-interval-s",
        type=float,
        default=2.0,
        help="Seconds to wait between camp retries when --rx is set",
    )
    scan.add_argument(
        "--rx-wait-max-retries",
        type=int,
        default=0,
        help="Max camp retries when --rx is set (0 = retry forever)",
    )
    scan.add_argument(
        "--status-hold-s",
        type=float,
        default=0.0,
        help="Keep the tuned RX / waterfall connection alive this many seconds so it appears on the KiwiSDR status page",
    )
    scan.add_argument("--min-width-bins", type=int, default=1)
    scan.add_argument(
        "--min-width-hz",
        type=float,
        default=0.0,
        help="Only emit detections whose estimated occupied bandwidth >= this many Hz",
    )
    scan.add_argument(
        "--ssb-detect",
        action="store_true",
        help="Detect wideband SSB-like energy using bandpower instead of narrow peaks",
    )
    scan.add_argument("--required-hits", type=int, default=3)
    scan.add_argument("--tolerance-bins", type=float, default=2.0)
    scan.add_argument("--expiry-frames", type=int, default=8)
    scan.add_argument("--max-frames", type=int, default=None)
    scan.add_argument("--show", action="store_true", help="Print per-frame top peaks")
    scan.add_argument("--show-top", type=int, default=5, help="How many per-frame peaks to show")
    scan.add_argument("--spanbar", action="store_true", help="Print a bar spanning the scanned frequency window")
    scan.add_argument("--spanbar-width", type=int, default=80)
    scan.add_argument("--spanbar-scale", choices=["frame", "raw"], default="frame")
    scan.add_argument("--spanbar-color", action="store_true")
    scan.add_argument("--sparkline", action="store_true", help="Print an ASCII sparkline per frame")
    scan.add_argument("--spark-width", type=int, default=80)
    scan.add_argument("--spark-clip-db", type=float, default=25.0)
    scan.add_argument(
        "--spark-charset",
        choices=["block", "ascii"],
        default="block",
        help="Sparkline character set",
    )
    scan.add_argument(
        "--spark-color",
        action="store_true",
        help="Enable ANSI-colored sparkline (terminal must support ANSI)",
    )
    scan.add_argument(
        "--spark-bucket",
        default="p90",
        choices=["max", "mean", "p50", "p75", "p90"],
        help="How each spark bucket summarizes bins",
    )
    scan.add_argument(
        "--spark-auto-clip",
        action="store_true",
        help="Auto-scale sparkline clip range per frame for more contrast",
    )
    scan.add_argument(
        "--signalbar",
        action="store_true",
        help="Add a colored strength bar next to DETECT lines",
    )
    scan.add_argument(
        "--signalbar-width",
        type=int,
        default=18,
        help="Width of the colored strength bar",
    )
    scan.add_argument("--debug", action="store_true", help="Enable connection debug logging")
    scan.add_argument(
        "--debug-messages",
        action="store_true",
        help="Log raw websocket messages (noisy; helps debug missing waterfall frames)",
    )
    scan.add_argument(
        "--record",
        action="store_true",
        help="On first persistent detection, run kiwirecorder to save a WAV snippet",
    )
    scan.add_argument("--record-seconds", type=int, default=30)
    scan.add_argument("--record-mode", default="usb")
    scan.add_argument("--record-out", type=Path, default=Path("recordings"))
    scan.add_argument(
        "--jsonl",
        type=Path,
        default=None,
        help="Optional JSONL output path for detections",
    )
    scan.add_argument(
        "--json-events",
        type=Path,
        default=None,
        help="Optional JSONL event output (filtered by --min-s)",
    )
    scan.add_argument(
        "--json-report",
        type=Path,
        default=None,
        help="Write a brief JSON report with peak frequency and estimated S",
    )
    scan.add_argument(
        "--phone-only",
        action="store_true",
        help="Only emit detections within the 40m Phone segment (bandplan-based)",
    )
    scan.add_argument(
        "--bandplan-region",
        choices=("region2", "non_region2"),
        default="region2",
        help="Select bandplan region variant (affects Phone ranges on 40m)",
    )
    scan.add_argument("--min-s", type=float, default=1.0, help="Minimum estimated S-unit to include in --json-events")
    scan.add_argument("--s1-db", type=float, default=12.0, help="rel_db that corresponds to S1 (calibration)")
    scan.add_argument("--db-per-s", type=float, default=6.0, help="dB per S-unit (approx)")

    sweep = sub.add_parser("sweep", help="Sweep a range by stepping center frequency")
    sweep.add_argument("--host", required=True, help="KiwiSDR host")
    sweep.add_argument("--port", type=int, default=8073, help="KiwiSDR port")
    sweep.add_argument("--password", default=None, help="KiwiSDR password (if any)")
    sweep.add_argument("--user", default="kiwi-scan", help="Client user name")
    sweep.add_argument(
        "--rx",
        type=int,
        default=0,
        help="Receiver index to tune (default: 0). Use --no-rx to disable explicit RX selection.",
    )
    sweep.add_argument(
        "--no-rx",
        action="store_true",
        help="Do not tune/camp a specific RX channel (useful if server policies reject RX camping)",
    )
    sweep.add_argument("--start-hz", type=float, default=None)
    sweep.add_argument("--end-hz", type=float, default=None)
    sweep.add_argument("--span-hz", type=float, default=12000.0)
    sweep.add_argument("--overlap", type=float, default=0.25)
    sweep.add_argument("--dwell-frames", type=int, default=30)
    sweep.add_argument("--threshold-db", type=float, default=10.0)
    sweep.add_argument("--min-width-bins", type=int, default=1)
    sweep.add_argument(
        "--min-width-hz",
        type=float,
        default=0.0,
        help="Only emit detections whose estimated occupied bandwidth >= this many Hz",
    )
    sweep.add_argument(
        "--rx-wait-timeout-s",
        type=float,
        default=0.0,
        help="When --rx is set and the Kiwi rejects camping, retry until this timeout (0 = retry forever)",
    )
    sweep.add_argument(
        "--rx-wait-interval-s",
        type=float,
        default=2.0,
        help="Seconds to wait between camp retries when --rx is set",
    )
    sweep.add_argument(
        "--rx-wait-max-retries",
        type=int,
        default=0,
        help="Max camp retries when --rx is set (0 = retry forever)",
    )
    sweep.add_argument(
        "--status-hold-s",
        type=float,
        default=0.0,
        help="Keep the tuned RX / waterfall connection alive this many seconds so it appears on the KiwiSDR status page",
    )
    sweep.add_argument(
        "--ssb-detect",
        action="store_true",
        help="Detect wideband SSB-like energy using bandpower instead of narrow peaks",
    )
    sweep.add_argument("--required-hits", type=int, default=3)
    sweep.add_argument("--tolerance-bins", type=float, default=2.0)
    sweep.add_argument("--expiry-frames", type=int, default=8)
    sweep.add_argument("--cache-ttl-s", type=float, default=60.0)
    sweep.add_argument("--cache-quantize-hz", type=float, default=25.0)
    sweep.add_argument("--show", action="store_true", help="Print per-frame top peaks")
    sweep.add_argument("--show-top", type=int, default=5)
    sweep.add_argument("--spanbar", action="store_true")
    sweep.add_argument("--spanbar-width", type=int, default=80)
    sweep.add_argument("--spanbar-scale", choices=["frame", "raw"], default="frame")
    sweep.add_argument("--spanbar-color", action="store_true")
    sweep.add_argument("--sparkline", action="store_true")
    sweep.add_argument("--spark-width", type=int, default=80)
    sweep.add_argument("--spark-clip-db", type=float, default=25.0)
    sweep.add_argument("--spark-charset", choices=["block", "ascii"], default="block")
    sweep.add_argument("--spark-color", action="store_true")
    sweep.add_argument("--spark-bucket", default="p90", choices=["max", "mean", "p50", "p75", "p90"])
    sweep.add_argument("--spark-auto-clip", action="store_true")
    sweep.add_argument(
        "--signalbar",
        action="store_true",
        help="Add a colored strength bar next to DETECT lines",
    )
    sweep.add_argument(
        "--signalbar-width",
        type=int,
        default=18,
        help="Width of the colored strength bar",
    )
    sweep.add_argument("--debug", action="store_true")
    sweep.add_argument("--debug-messages", action="store_true")
    sweep.add_argument("--record", action="store_true")
    sweep.add_argument("--record-seconds", type=int, default=30)
    sweep.add_argument("--record-mode", default="usb")
    sweep.add_argument("--record-out", type=Path, default=Path("recordings"))
    sweep.add_argument("--jsonl", type=Path, default=None)
    sweep.add_argument("--json-events", type=Path, default=None)
    sweep.add_argument("--json-report", type=Path, default=None)
    sweep.add_argument(
        "--json-top",
        type=Path,
        default=None,
        help="Write minimal JSON report of top-N strongest frequencies",
    )
    sweep.add_argument(
        "--json-activity",
        type=Path,
        default=None,
        help="Write JSON report of active frequencies (filtered/quantized)",
    )
    sweep.add_argument("--top-n", type=int, default=5)
    sweep.add_argument("--top-quantize-hz", type=float, default=25.0)
    sweep.add_argument("--min-s", type=float, default=1.0)
    sweep.add_argument("--s1-db", type=float, default=12.0)
    sweep.add_argument("--db-per-s", type=float, default=6.0)
    sweep.add_argument(
        "--phone-only",
        action="store_true",
        help="Only emit detections within the 40m Phone segment (bandplan-based)",
    )
    sweep.add_argument(
        "--bandplan-region",
        choices=("region2", "non_region2"),
        default="region2",
        help="Select bandplan region variant (affects Phone ranges on 40m)",
    )

    ft8 = sub.add_parser("ft8", help="Probe FT8 watering-hole frequencies per band")
    ft8.add_argument("--host", required=True, help="KiwiSDR host")
    ft8.add_argument("--port", type=int, default=8073, help="KiwiSDR port")
    ft8.add_argument("--password", default=None, help="KiwiSDR password (if any)")
    ft8.add_argument("--user", default="kiwi-ft8", help="Client user name")
    ft8.add_argument(
        "--rx",
        type=int,
        default=0,
        help="Receiver index to use (default: 0). Use --no-rx to let the server choose.",
    )
    ft8.add_argument(
        "--no-rx",
        action="store_true",
        help="Do not request a specific RX channel (useful if server policies reject RX camping)",
    )
    ft8.add_argument(
        "--bands",
        default=None,
        help="Comma-separated subset of bands to probe (e.g. 40m,20m). Default: all.",
    )
    ft8.add_argument(
        "--dwell-s",
        type=float,
        default=20.0,
        help="Seconds to dwell on each FT8 frequency (default: 20)",
    )
    ft8.add_argument(
        "--span-hz",
        type=float,
        default=3000.0,
        help="Waterfall span in Hz (default: 3000)",
    )
    ft8.add_argument(
        "--threshold-db",
        type=float,
        default=6.0,
        help="dB above noise floor to count a hit (default: 6)",
    )
    ft8.add_argument(
        "--fps",
        type=float,
        default=2.0,
        help="Expected waterfall frames per second (default: 2)",
    )
    ft8.add_argument(
        "--min-score",
        type=float,
        default=0.20,
        help="Mark band ACTIVE when score >= this value (default: 0.20)",
    )
    ft8.add_argument(
        "--sort",
        choices=("band", "score"),
        default="band",
        help="Sort output by band or score (default: band)",
    )
    ft8.add_argument("--json", action="store_true", help="Print JSON results to stdout")
    ft8.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Write JSON results to this file",
    )
    ft8.add_argument(
        "--jsonl-out",
        type=Path,
        default=None,
        help="Append JSONL (one record per band measurement) to this file",
    )
    ft8.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of cycles to run (0 = run forever). Default: 1",
    )
    ft8.add_argument(
        "--cycle-sleep-s",
        type=float,
        default=1.0,
        help="Seconds to sleep between cycles when --repeat != 1 (default: 1.0)",
    )
    ft8.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress human-readable output (useful with --jsonl-out)",
    )
    ft8.add_argument("--debug", action="store_true", help="Enable connection debug logging")
    ft8.add_argument(
        "--debug-messages",
        action="store_true",
        help="Log raw websocket messages (noisy)",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()
    if args.version:
        v = "0.1.0"
        if version is not None:
            try:
                v = str(version("kiwi-scan"))
            except PackageNotFoundError:
                pass
        print(f"kiwi-scan {v}")
        return 0

    if args.cmd == "scan":
        rx_chan = None if bool(getattr(args, "no_rx", False)) else args.rx
        return run_scan(
            host=args.host,
            port=args.port,
            password=args.password,
            user=args.user,
            rx_chan=rx_chan,
            center_freq_hz=args.center_hz,
            span_hz=args.span_hz,
            threshold_db=args.threshold_db,
            min_width_bins=args.min_width_bins,
            min_width_hz=args.min_width_hz,
            ssb_detect=args.ssb_detect,
            required_hits=args.required_hits,
            tolerance_bins=args.tolerance_bins,
            expiry_frames=args.expiry_frames,
            max_frames=args.max_frames,
            jsonl_path=args.jsonl,
            jsonl_events_path=args.json_events,
            json_report_path=args.json_report,
            min_s=args.min_s,
            s1_db=args.s1_db,
            db_per_s=args.db_per_s,
            phone_only=args.phone_only,
            bandplan_region=args.bandplan_region,
            record=args.record,
            record_seconds=args.record_seconds,
            record_mode=args.record_mode,
            record_out=args.record_out,
            show=args.show,
            show_top=args.show_top,
            spanbar=args.spanbar,
            spanbar_width=args.spanbar_width,
            spanbar_scale=args.spanbar_scale,
            spanbar_color=args.spanbar_color,
            spark=args.sparkline,
            spark_width=args.spark_width,
            spark_clip_db=args.spark_clip_db,
            spark_charset=args.spark_charset,
            spark_color=args.spark_color,
            spark_bucket=args.spark_bucket,
            spark_auto_clip=args.spark_auto_clip,
            signalbar=args.signalbar,
            signalbar_width=args.signalbar_width,
            debug=args.debug,
            debug_messages=args.debug_messages,
            rx_wait_timeout_s=args.rx_wait_timeout_s,
            rx_wait_interval_s=args.rx_wait_interval_s,
            rx_wait_max_retries=args.rx_wait_max_retries,
            status_hold_s=args.status_hold_s,
        )

    if args.cmd == "sweep":
        rx_chan = None if bool(getattr(args, "no_rx", False)) else args.rx
        return run_sweep(
            host=args.host,
            port=args.port,
            password=args.password,
            user=args.user,
            rx_chan=rx_chan,
            start_hz=args.start_hz,
            end_hz=args.end_hz,
            span_hz=args.span_hz,
            overlap=args.overlap,
            dwell_frames=args.dwell_frames,
            threshold_db=args.threshold_db,
            min_width_bins=args.min_width_bins,
            min_width_hz=args.min_width_hz,
            ssb_detect=args.ssb_detect,
            required_hits=args.required_hits,
            tolerance_bins=args.tolerance_bins,
            expiry_frames=args.expiry_frames,
            cache_ttl_s=args.cache_ttl_s,
            cache_quantize_hz=args.cache_quantize_hz,
            show=args.show,
            show_top=args.show_top,
            spanbar=args.spanbar,
            spanbar_width=args.spanbar_width,
            spanbar_scale=args.spanbar_scale,
            spanbar_color=args.spanbar_color,
            spark=args.sparkline,
            spark_width=args.spark_width,
            spark_clip_db=args.spark_clip_db,
            spark_charset=args.spark_charset,
            spark_color=args.spark_color,
            spark_bucket=args.spark_bucket,
            spark_auto_clip=args.spark_auto_clip,
            signalbar=args.signalbar,
            signalbar_width=args.signalbar_width,
            debug=args.debug,
            debug_messages=args.debug_messages,
            record=args.record,
            record_seconds=args.record_seconds,
            record_mode=args.record_mode,
            record_out=args.record_out,
            jsonl_path=args.jsonl,
            jsonl_events_path=args.json_events,
            json_report_path=args.json_report,
            json_topn_path=args.json_top,
            json_activity_path=args.json_activity,
            top_n=args.top_n,
            top_quantize_hz=args.top_quantize_hz,
            min_s=args.min_s,
            s1_db=args.s1_db,
            db_per_s=args.db_per_s,
            phone_only=args.phone_only,
            bandplan_region=args.bandplan_region,
            rx_wait_timeout_s=args.rx_wait_timeout_s,
            rx_wait_interval_s=args.rx_wait_interval_s,
            rx_wait_max_retries=args.rx_wait_max_retries,
            status_hold_s=args.status_hold_s,
        )

    if args.cmd == "ft8":
        rx_chan = None if bool(getattr(args, "no_rx", False)) else args.rx

        freqs = dict(FT8_WATERHOLES)
        if args.bands:
            requested = [b.strip() for b in str(args.bands).split(",") if b.strip()]
            unknown = [b for b in requested if b not in freqs]
            if unknown:
                print("Unknown band(s):", ", ".join(unknown))
                print("Known bands:", ", ".join(sorted(freqs.keys())))
                return 2
            freqs = {b: freqs[b] for b in requested}

        w = DiscoveryWorker(
            host=args.host,
            port=args.port,
            password=args.password,
            user=args.user,
            rx_chan=rx_chan,
            dwell_s=args.dwell_s,
            span_hz=args.span_hz,
            threshold_db=args.threshold_db,
            frames_per_second=args.fps,
            debug=bool(args.debug),
        )

        # If we're asked to loop and/or emit JSONL, measure band-by-band so we can
        # stream results incrementally.
        want_loop = (int(args.repeat) == 0) or (int(args.repeat) > 1)
        want_jsonl = args.jsonl_out is not None
        if want_loop or want_jsonl:
            jsonl_fh = None
            try:
                if args.jsonl_out is not None:
                    try:
                        args.jsonl_out.parent.mkdir(parents=True, exist_ok=True)
                    except Exception:
                        pass
                    jsonl_fh = args.jsonl_out.open("a", encoding="utf-8")

                cycles_left = int(args.repeat)
                cycle_idx = 0
                while True:
                    cycle_idx += 1
                    if cycles_left > 0 and cycle_idx > cycles_left:
                        break

                    for band, f in freqs.items():
                        try:
                            res = w.measure_freq(band, f)
                        except KiwiClientUnavailable as e:
                            print(f"Kiwi client unavailable connecting to {args.host}:{args.port}: {e}")
                            return 1

                        rec = asdict(res)
                        now = datetime.now(timezone.utc)
                        rec.update(
                            {
                                "ts": now.timestamp(),
                                "iso": now.isoformat().replace("+00:00", "Z"),
                                "host": args.host,
                                "port": int(args.port),
                                "user": args.user,
                                "rx_chan": rx_chan,
                                "dwell_s": float(args.dwell_s),
                                "span_hz": float(args.span_hz),
                                "threshold_db": float(args.threshold_db),
                                "fps": float(args.fps),
                                "cycle": cycle_idx,
                            }
                        )

                        if jsonl_fh is not None:
                            jsonl_fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
                            jsonl_fh.flush()

                        if not bool(args.quiet):
                            mhz = res.freq_hz / 1e6
                            state = "ACTIVE" if float(res.score) >= float(args.min_score) else "-"
                            print(
                                f"cycle={cycle_idx:03d} {res.band:5s} {mhz:8.4f} MHz  score={res.score:0.2f}  hits={res.hits:3d}/{res.frames_sampled:3d}  {state}"
                            )

                    if int(args.repeat) == 0 or int(args.repeat) > 1:
                        try:
                            time.sleep(float(args.cycle_sleep_s))
                        except KeyboardInterrupt:
                            break
                        except Exception:
                            pass

                    if int(args.repeat) == 1:
                        break

            except KeyboardInterrupt:
                return 0
            finally:
                try:
                    if jsonl_fh is not None:
                        jsonl_fh.close()
                except Exception:
                    pass
            return 0

        # One-shot, in-memory mode (print and/or JSON dump)
        try:
            results = w.discover(freqs)
        except KiwiClientUnavailable as e:
            print(f"Kiwi client unavailable connecting to {args.host}:{args.port}: {e}")
            return 1

        if args.sort == "score":
            results = sorted(results, key=lambda r: (r.score, r.band), reverse=True)
        else:
            results = sorted(results, key=lambda r: r.band)

        active = [r for r in results if float(r.score) >= float(args.min_score)]

        payload = [asdict(r) for r in results]
        if args.json_out is not None:
            args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        if args.json:
            print(json.dumps(payload, indent=2))
        elif not bool(args.quiet):
            for r in results:
                mhz = r.freq_hz / 1e6
                camp = "?"
                if r.camp_ok is True:
                    camp = f"ok rx={r.camp_rx}" if r.camp_rx is not None else "ok"
                elif r.camp_ok is False:
                    camp = "reject"
                state = "ACTIVE" if float(r.score) >= float(args.min_score) else "-"
                print(
                    f"{r.band:5s} {mhz:8.4f} MHz  score={r.score:0.2f}  hits={r.hits:3d}/{r.frames_sampled:3d}  camp={camp:8s}  {state}"
                )
            if active:
                print("ACTIVE:", ", ".join([f"{r.band}({r.score:0.2f})" for r in active]))
            else:
                print("ACTIVE: none")
        return 0

    print("No command specified. Try 'kiwi-scan --help' or 'kiwi-scan scan --help'.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
