from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.request
from typing import Iterable

import websockets


BADP_OK = 0
BADP_TRY_AGAIN = 1
BADP_STILL_DETERMINING_LOCAL_IP = 2
BADP_NOT_ALLOWED_FROM_IP = 3
BADP_NO_ADMIN_PWD_SET = 4
BADP_NO_MULTIPLE_CONNS = 5
BADP_DATABASE_UPDATE_IN_PROGRESS = 6
BADP_ADMIN_CONN_ALREADY_OPEN = 7


class KiwiAdminError(RuntimeError):
    pass


class KiwiAdminBusyError(KiwiAdminError):
    pass


def _admin_ws_url(host: str, port: int) -> str:
    ts = int(time.time() * 1000)
    return f"ws://{host}:{int(port)}/ws/kiwi/{ts}/admin"


def _decode_message(message: object) -> str:
    if isinstance(message, bytes):
        return message.decode("utf-8", errors="ignore")
    return str(message)


def _format_debug_message(text: str) -> str:
    if text.startswith("MSG load_adm="):
        return "MSG load_adm=<redacted>"
    return text


def _extract_badp(text: str) -> int | None:
    if not text.startswith("MSG badp="):
        return None
    try:
        return int(text.split("=", 1)[1].strip())
    except (TypeError, ValueError):
        return None


def _badp_error(host: str, code: int | None) -> KiwiAdminError:
    if code == BADP_OK:
        return KiwiAdminError(f"{host}: unexpected badp=0 handling path")
    if code == BADP_ADMIN_CONN_ALREADY_OPEN:
        return KiwiAdminBusyError(f"{host}: another Kiwi admin connection is already open")
    if code == BADP_NOT_ALLOWED_FROM_IP:
        return KiwiAdminError(f"{host}: admin connection not allowed from this IP address")
    if code == BADP_NO_ADMIN_PWD_SET:
        return KiwiAdminError(f"{host}: no admin password set; only same-LAN clients may connect")
    if code == BADP_NO_MULTIPLE_CONNS:
        return KiwiAdminError(f"{host}: multiple connections from this IP are not allowed")
    if code == BADP_DATABASE_UPDATE_IN_PROGRESS:
        return KiwiAdminError(f"{host}: Kiwi database update in progress")
    if code == BADP_STILL_DETERMINING_LOCAL_IP:
        return KiwiAdminError(f"{host}: Kiwi is still determining local interface address")
    if code == BADP_TRY_AGAIN:
        return KiwiAdminError(f"{host}: admin authentication rejected; retry required")
    return KiwiAdminError(f"{host}: admin websocket rejected connection with badp={code}")


async def _recv_until_quiet(ws: websockets.WebSocketClientProtocol, *, host: str, verbose: bool, quiet_seconds: float, max_wait_seconds: float) -> None:
    deadline = time.time() + max_wait_seconds
    quiet_deadline = time.time() + quiet_seconds
    while time.time() < deadline:
        timeout = max(0.1, min(0.5, quiet_deadline - time.time()))
        try:
            message = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except TimeoutError:
            if time.time() >= quiet_deadline:
                return
            continue
        text = _decode_message(message)
        if verbose:
            print(f"recv {_format_debug_message(text)[:200]}")
        badp = _extract_badp(text)
        if badp is not None:
            raise _badp_error(host, badp)
        quiet_deadline = time.time() + quiet_seconds


async def _connect_admin(host: str, port: int, *, user: str, password: str, verbose: bool) -> websockets.WebSocketClientProtocol:
    url = _admin_ws_url(host, port)
    if verbose:
        print(f"connecting {url}")
    pwd = str(password or "").strip() or "#"
    ws = await websockets.connect(url, open_timeout=5, close_timeout=2, ping_interval=None)
    try:
        await ws.send(f"SET auth t=admin p={pwd}")
        saw_init = False
        deadline = time.time() + 10.0
        while time.time() < deadline:
            timeout = 0.5 if saw_init else 2.0
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except TimeoutError:
                if saw_init:
                    return ws
                continue
            text = _decode_message(message)
            if verbose:
                print(f"recv {_format_debug_message(text)[:200]}")
            badp = _extract_badp(text)
            if badp == BADP_OK:
                continue
            if badp == BADP_OK:
                continue
            if badp is not None:
                raise _badp_error(host, badp)
            if not (text.startswith("MSG ") or text.startswith("ADM ")):
                continue
            if "init=" not in text or saw_init:
                continue
            saw_init = True
            await ws.send(f"SET ident_user={user}")
            await ws.send("SET browser=KiwiScanAdminKick")
            await ws.send("SET GET_USERS")
            if verbose:
                print("sent GET_USERS")
            await _recv_until_quiet(ws, host=host, verbose=verbose, quiet_seconds=0.75, max_wait_seconds=3.0)
            return ws
        raise KiwiAdminError("admin websocket never reached command-ready state")
    except Exception:
        await ws.close()
        raise


async def _kick_other_admin(host: str, port: int, *, password: str, verbose: bool) -> bool:
    url = _admin_ws_url(host, port)
    if verbose:
        print(f"connecting {url} for admin takeover")
    pwd = str(password or "").strip() or "#"
    async with websockets.connect(url, open_timeout=5, close_timeout=2, ping_interval=None) as ws:
        await ws.send(f"SET auth t=admin p={pwd}")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            message = await asyncio.wait_for(ws.recv(), timeout=2.0)
            text = _decode_message(message)
            if verbose:
                print(f"recv {_format_debug_message(text)[:200]}")
            badp = _extract_badp(text)
            if badp == BADP_OK:
                continue
            if badp == BADP_ADMIN_CONN_ALREADY_OPEN:
                await ws.send("SET kick_admins")
                if verbose:
                    print("sent kick_admins")
                await asyncio.sleep(1.0)
                return True
            if badp is not None:
                raise _badp_error(host, badp)
        raise KiwiAdminError("admin takeover websocket never reached decision state")


async def _send_kicks(
    host: str,
    port: int,
    *,
    user: str,
    password: str,
    kick_targets: Iterable[int],
    verbose: bool,
    take_admin: bool,
) -> int:
    try:
        ws = await _connect_admin(host, port, user=user, password=password, verbose=verbose)
    except KiwiAdminBusyError:
        if not take_admin:
            raise
        if not await _kick_other_admin(host, port, password=password, verbose=verbose):
            raise
        ws = await _connect_admin(host, port, user=user, password=password, verbose=verbose)

    try:
        for target in kick_targets:
            await ws.send(f"SET user_kick={int(target)}")
            if verbose:
                print(f"sent user_kick={int(target)}")
            await asyncio.sleep(0.05)
        await _recv_until_quiet(ws, host=host, verbose=verbose, quiet_seconds=0.5, max_wait_seconds=2.0)
        return 0
    finally:
        await ws.close()


def _fetch_users(host: str, port: int) -> list[dict]:
    with urllib.request.urlopen(f"http://{host}:{int(port)}/users?json=1", timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    return payload if isinstance(payload, list) else []


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kick KiwiSDR users via the admin websocket.")
    parser.add_argument("--host", required=True, help="KiwiSDR hostname or IP")
    parser.add_argument("--port", type=int, default=8073, help="KiwiSDR port")
    parser.add_argument("--password", default="", help="Admin password if required")
    parser.add_argument("--user", default="KiwiScanAdminKick", help="Admin session name")
    parser.add_argument(
        "--kick",
        type=int,
        action="append",
        dest="kicks",
        help="User index to kick. Repeat for multiple users.",
    )
    parser.add_argument(
        "--kick-all",
        action="store_true",
        help="Kick all active users (equivalent to SET user_kick=-1).",
    )
    parser.add_argument(
        "--take-admin",
        action="store_true",
        help="If another admin session is open, send SET kick_admins, reconnect, and retry.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable info logging from the Kiwi client")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    kick_targets = [-1] if args.kick_all or not args.kicks else [int(value) for value in args.kicks]
    try:
        before = _fetch_users(str(args.host), int(args.port)) if args.verbose else []
        if args.verbose and before:
            print("users before:")
            for row in before:
                print(row.get("i"), row.get("n"), row.get("f"), row.get("t"))
        asyncio.run(
            _send_kicks(
                str(args.host),
                int(args.port),
                user=str(args.user),
                password=str(args.password),
                kick_targets=kick_targets,
                verbose=bool(args.verbose),
                take_admin=bool(args.take_admin),
            )
        )
        print(f"sent kick targets: {','.join(str(value) for value in kick_targets)}")
        if args.verbose:
            after = _fetch_users(str(args.host), int(args.port))
            print("users after:")
            for row in after:
                print(row.get("i"), row.get("n"), row.get("f"), row.get("t"))
        return 0
    except Exception as exc:
        print(f"unexpected error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())