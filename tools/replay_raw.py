#!/usr/bin/env python3

import argparse
import subprocess
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay 16-bit mono raw audio to af2udp in real time.")
    parser.add_argument("raw_path", help="Path to raw signed 16-bit mono audio")
    parser.add_argument("udp_port", type=int, help="Destination UDP port for af2udp")
    parser.add_argument("--af2udp", default="af2udp", help="Path to af2udp executable")
    parser.add_argument("--sample-rate", type=int, default=48000, help="Raw audio sample rate")
    parser.add_argument("--chunk-bytes", type=int, default=9600, help="Chunk size written per interval")
    args = parser.parse_args()

    bytes_per_second = args.sample_rate * 2
    delay_per_chunk = float(args.chunk_bytes) / float(bytes_per_second)

    proc = subprocess.Popen([args.af2udp, str(args.udp_port)], stdin=subprocess.PIPE)
    try:
        assert proc.stdin is not None
        with open(args.raw_path, "rb") as raw_fp:
            while True:
                chunk = raw_fp.read(args.chunk_bytes)
                if not chunk:
                    break
                proc.stdin.write(chunk)
                proc.stdin.flush()
                time.sleep(delay_per_chunk)
        proc.stdin.close()
        return proc.wait()
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    raise SystemExit(main())