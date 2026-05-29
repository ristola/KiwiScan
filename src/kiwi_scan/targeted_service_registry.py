from __future__ import annotations

import threading
from typing import Callable, Dict, Generic, TypeVar

T = TypeVar("T")


def normalize_target_service_key(target: object | None = None) -> str:
    if isinstance(target, dict):
        kiwi_key = str(target.get("kiwi_key") or "").strip().lower()
        if kiwi_key:
            return kiwi_key
        host = str(target.get("host") or "").strip().lower()
        try:
            port = int(target.get("port") or 0)
        except Exception:
            port = 0
        if host and 1 <= port <= 65535:
            return f"{host}:{port}"
    return "default"


def resolve_targeted_service(service_or_registry: object, *, target: object | None = None) -> object:
    resolver = getattr(service_or_registry, "resolve_for_target", None)
    if callable(resolver):
        return resolver(target=target)
    return service_or_registry


class TargetedServiceRegistry(Generic[T]):
    def __init__(self, *, factory: Callable[[dict[str, object]], T]) -> None:
        self._factory = factory
        self._lock = threading.Lock()
        self._services: Dict[str, T] = {}

    def resolve_for_target(self, *, target: object | None = None) -> T:
        target_payload = dict(target) if isinstance(target, dict) else {}
        service_key = normalize_target_service_key(target_payload)
        with self._lock:
            service = self._services.get(service_key)
            if service is None:
                service = self._factory(dict(target_payload))
                self._services[service_key] = service
            return service

    def snapshot(self) -> dict[str, T]:
        with self._lock:
            return dict(self._services)

    def status(self) -> dict[str, object]:
        statuses: dict[str, dict[str, object]] = {}
        for service_key, service in self.snapshot().items():
            method = getattr(service, "status", None)
            if not callable(method):
                continue
            try:
                status = method()
            except Exception:
                continue
            if isinstance(status, dict):
                statuses[service_key] = dict(status)
        if not statuses:
            return {"running": False, "activating": False, "mode_active": False}
        if len(statuses) == 1:
            return next(iter(statuses.values()))
        return {
            "running": any(bool(status.get("running")) for status in statuses.values()),
            "activating": any(bool(status.get("activating")) for status in statuses.values()),
            "mode_active": any(bool(status.get("mode_active")) for status in statuses.values()),
            "targets": statuses,
        }

    def health_channels(self) -> dict[str, object]:
        services = self.snapshot()
        merged: dict[str, object] = {}
        prefix_keys = len(services) > 1
        for service_key, service in services.items():
            method = getattr(service, "health_channels", None)
            if not callable(method):
                continue
            try:
                channels = method()
            except Exception:
                continue
            if not isinstance(channels, dict):
                continue
            for channel_key, channel in channels.items():
                merged_key = f"{service_key}:{channel_key}" if prefix_keys else str(channel_key)
                merged[merged_key] = dict(channel) if isinstance(channel, dict) else channel
        return merged

    def start(self) -> dict[str, object]:
        return self._call_all("start")

    def stop(self) -> dict[str, object]:
        return self._call_all("stop")

    def deactivate(self) -> dict[str, object]:
        return self._call_all("deactivate")

    def stop_all(self, *, method_name: str = "stop") -> None:
        with self._lock:
            services = list(self._services.values())
        for service in services:
            method = getattr(service, method_name, None)
            if callable(method):
                try:
                    method()
                except Exception:
                    pass

    def _call_all(self, method_name: str) -> dict[str, object]:
        results: dict[str, object] = {}
        for service_key, service in self.snapshot().items():
            method = getattr(service, method_name, None)
            if not callable(method):
                continue
            try:
                results[service_key] = method()
            except Exception:
                continue
        if not results:
            return {"ok": True}
        if len(results) == 1:
            only_result = next(iter(results.values()))
            if isinstance(only_result, dict):
                return dict(only_result)
        return {"ok": True, "targets": results}