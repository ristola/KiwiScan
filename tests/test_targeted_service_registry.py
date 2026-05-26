from __future__ import annotations

from kiwi_scan.targeted_service_registry import TargetedServiceRegistry


class _ServiceStub:
    def __init__(self, *, name: str) -> None:
        self.name = name
        self.deactivate_calls = 0

    def status(self) -> dict[str, object]:
        return {
            "running": self.name == "kiwi-b.local:8074",
            "mode_active": self.name == "kiwi-a.local:8073",
            "service_name": self.name,
        }

    def health_channels(self) -> dict[str, dict[str, object]]:
        return {
            "0": {
                "display_name": self.name,
            }
        }

    def deactivate(self) -> dict[str, object]:
        self.deactivate_calls += 1
        return {"ok": True, "service_name": self.name}


def test_targeted_service_registry_reuses_instances_per_target() -> None:
    created_for: list[str] = []

    registry = TargetedServiceRegistry(
        factory=lambda target: created_for.append(str(target.get("kiwi_key") or "")) or {"target": target.get("kiwi_key")}
    )

    kiwi_a_first = registry.resolve_for_target(target={"kiwi_key": "kiwi-a.local:8073", "host": "kiwi-a.local", "port": 8073})
    kiwi_a_second = registry.resolve_for_target(target={"kiwi_key": "kiwi-a.local:8073", "host": "kiwi-a.local", "port": 8073})
    kiwi_b = registry.resolve_for_target(target={"kiwi_key": "kiwi-b.local:8074", "host": "kiwi-b.local", "port": 8074})

    assert kiwi_a_first is kiwi_a_second
    assert kiwi_a_first is not kiwi_b
    assert created_for == ["kiwi-a.local:8073", "kiwi-b.local:8074"]


def test_targeted_service_registry_aggregates_status_health_and_deactivate() -> None:
    registry = TargetedServiceRegistry(factory=lambda target: _ServiceStub(name=str(target.get("kiwi_key") or "default")))

    kiwi_a = registry.resolve_for_target(target={"kiwi_key": "kiwi-a.local:8073", "host": "kiwi-a.local", "port": 8073})
    kiwi_b = registry.resolve_for_target(target={"kiwi_key": "kiwi-b.local:8074", "host": "kiwi-b.local", "port": 8074})

    status = registry.status()
    health_channels = registry.health_channels()
    deactivate_result = registry.deactivate()

    assert status["running"] is True
    assert status["mode_active"] is True
    assert status["targets"]["kiwi-a.local:8073"]["service_name"] == "kiwi-a.local:8073"
    assert set(health_channels) == {"kiwi-a.local:8073:0", "kiwi-b.local:8074:0"}
    assert kiwi_a.deactivate_calls == 1
    assert kiwi_b.deactivate_calls == 1
    assert deactivate_result["ok"] is True