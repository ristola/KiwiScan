#!/usr/bin/env python3
"""Print a single band's status from the local dashboard.

This avoids the common failure mode where `curl -s` returns empty/non-JSON and
`json.load()` throws a confusing exception.

Usage:
    .venv-py3/bin/python tools/show_band_status.py 40m
    .venv-py3/bin/python tools/show_band_status.py 10m --url http://127.0.0.1:4020/status
"""

import argparse
import json
import sys
import urllib.error
import urllib.request


def fetch_json(url: str, timeout_s: float) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = getattr(resp, "status", None)
            body = resp.read()
            if status is not None and int(status) >= 400:
                raise RuntimeError(f"HTTP {status}: {body[:200]!r}")
    except urllib.error.HTTPError as e:
        body = e.read()
        raise RuntimeError(f"HTTP {e.code}: {body[:200]!r}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e}")

    if not body:
        raise RuntimeError("Empty response body")

    try:
        return json.loads(body.decode("utf-8", errors="replace"))
    except Exception as e:
        preview = body[:200]
        raise RuntimeError(f"Non-JSON response ({e}): {preview!r}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("band", help="Band key, e.g. 40m, 20m, 10m")
    p.add_argument("--url", default="http://127.0.0.1:4020/status")
    p.add_argument("--timeout", type=float, default=5.0)
    args = p.parse_args()

    try:
        status = fetch_json(args.url, timeout_s=float(args.timeout))
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    results = status.get("results") or {}
    band_obj = results.get(args.band) or {}

    print(json.dumps(band_obj, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
