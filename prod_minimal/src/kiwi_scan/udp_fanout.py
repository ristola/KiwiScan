#!/usr/bin/env python3
import socket
import sys
from typing import List


def main(argv: List[str]) -> int:
    if len(argv) < 3:
        return 2
    host = argv[1]
    ports = []
    for raw in argv[2:]:
        try:
            ports.append(int(raw))
        except ValueError:
            return 2
    if not ports:
        return 2
    sockets = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM) for _ in ports]
    addrs = [(host, p) for p in ports]
    buf = sys.stdin.buffer
    try:
        for chunk in iter(lambda: buf.read(4096), b""):
            for sock, addr in zip(sockets, addrs):
                try:
                    sock.sendto(chunk, addr)
                except Exception:
                    pass
    finally:
        for sock in sockets:
            try:
                sock.close()
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
