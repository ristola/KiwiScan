#!/usr/bin/env python3

import argparse
import socket
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="Count UDP packets and bytes on a local port.")
    parser.add_argument("port", type=int, help="UDP port to listen on")
    parser.add_argument("--seconds", type=float, default=25.0, help="How long to listen")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", args.port))
    sock.settimeout(0.5)

    deadline = time.time() + args.seconds
    packets = 0
    total_bytes = 0
    while time.time() < deadline:
        try:
            data, _addr = sock.recvfrom(65535)
        except socket.timeout:
            continue
        packets += 1
        total_bytes += len(data)

    print(f"packets={packets} bytes={total_bytes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())