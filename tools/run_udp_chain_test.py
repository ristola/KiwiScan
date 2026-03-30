#!/usr/bin/env python3

import argparse
import pathlib
import subprocess
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ft8modem plus af2udp replay with ft8modem stdin held open.")
    parser.add_argument("raw_path")
    parser.add_argument("out_dir")
    parser.add_argument("udp_port", type=int)
    parser.add_argument("--ft8modem", default="ft8modem")
    parser.add_argument("--af2udp", default="af2udp")
    parser.add_argument("--python", default="python3")
    parser.add_argument("--mode", default="FT8")
    parser.add_argument("--rate", type=int, default=48000)
    parser.add_argument("--settle-seconds", type=float, default=8.0)
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    replay_helper = pathlib.Path(__file__).with_name("replay_raw.py")

    modem_log = out_dir / "modem.log"
    replay_log = out_dir / "replay.log"

    with modem_log.open("w", encoding="utf-8") as modem_fp, replay_log.open("w", encoding="utf-8") as replay_fp:
        modem = subprocess.Popen(
            [args.ft8modem, "-t", str(out_dir), "-r", str(args.rate), args.mode, f"udp:{args.udp_port}"],
            stdin=subprocess.PIPE,
            stdout=modem_fp,
            stderr=modem_fp,
        )
        try:
            replay = subprocess.run(
                [args.python, str(replay_helper), args.raw_path, str(args.udp_port), "--af2udp", args.af2udp],
                stdout=replay_fp,
                stderr=replay_fp,
                check=False,
            )
            replay_fp.write(f"replay_rc={replay.returncode}\n")
            replay_fp.flush()
            time.sleep(args.settle_seconds)
        finally:
            modem.terminate()
            try:
                modem.wait(timeout=3)
            except subprocess.TimeoutExpired:
                modem.kill()
                modem.wait()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())