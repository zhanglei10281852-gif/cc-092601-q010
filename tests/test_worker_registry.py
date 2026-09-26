from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError, NotFoundError
from app.database import get_connection, init_db
from app.workers.service import WorkerRegistryService

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def register(client, key: str = "worker-1", capabilities=("solver-a",), concurrency: int = 2, version: str = "1.2.0") -> dict:
    response = client.post(
        "/api/workers",
        json={"worker_key": key, "software_version": version, "capabilities": list(capabilities), "max_concurrency": concurrency},
    )
    assert response.status_code == 201, response.text
    return response.json()


def claim(client, key: str, session: str, capabilities=("solver-a",)):
    return client.post(
        "/api/compute/tasks/claim",
        json={"worker_id": key, "session_id": session, "capabilities": list(capabilities), "lease_seconds": 60},
    )


def make_services() -> tuple[ComputeOperationsService, WorkerRegistryService, FrozenClock]:
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    return ComputeOperationsService(get_connection(), clock), WorkerRegistryService(get_connection(), clock), clock


def test_registration_heartbeat_and_offline_detection(client):
    _, registry, clock = make_services()
    worker = registry.register({"worker_key": "worker-1", "software_version": "1.2.0", "capabilities": ["solver-a"], "max_concurrency": 2})
    assert worker["status"] == "active"
    assert worker["online"] is True
    assert worker["restarted"] is False
    session = worker["session_id"]

    clock.advance(seconds=121)
    assert registry.get_worker("worker-1")["online"] is False

    refreshed = registry.heartbeat("worker-1", session)
    assert refreshed["online"] is True
    assert refreshed["last_heartbeat_at"] > worker["last_heartbeat_at"]

    with pytest.raises(ConflictError):
        registry.heartbeat("worker-1", "outdated-session")
    with pytest.raises(NotFoundError):
        registry.heartbeat("ghost", "whatever")


def test_restart_keeps_identity_and_blocks_stale_session(client):
    service, registry, clock = make_services()
    service.create_template(TEMPLATE, "administrator")
    worker = registry.register({"worker_key": "worker-1", "software_version": "1.2.0", "capabilities": ["solver-a"], "max_concurrency": 2})
    old_session = worker["session_id"]
    task = service.submit(submit_payload("restart-000001"))
    claimed = service.claim("worker-1", old_session, ["solver-a"], 30)
    assert claimed and claimed["lease_owner"].endswith(old_session)

    restarted = registry.register({"worker_key": "worker-1", "software_version": "1.2.1", "capabilities": ["solver-a"], "max_concurrency": 2})
    assert restarted["restarted"] is True
    assert restarted["session_id"] != old_session
    assert restarted["software_version"] == "1.2.1"

    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-1", old_session, {"value": 1}, {})
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-1", old_session, 30)
    with pytest.raises(ConflictError):
        registry.heartbeat("worker-1", old_session)

    clock.advance(seconds=31)
    recovered = service.recover_expired()
    assert recovered["recovered"] == [task["id"]]
    reclaimed = service.claim("worker-1", restarted["session_id"], ["solver-a"], 30)
    assert reclaimed and reclaimed["id"] == task["id"]
    done = service.complete(task["id"], "worker-1", restarted["session_id"], {"value": 1}, {})
    assert done["status"] == "succeeded"
    view = registry.get_worker("worker-1")
    assert view["total_completed"] == 1
    assert view["failure_rate"] == 0.0


def test_claim_requires_registered_current_and_capable_worker(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("gate-000001"))

    missing = client.post("/api/compute/tasks/claim", json={"worker_id": "ghost", "session_id": "s", "capabilities": [], "lease_seconds": 60})
    assert missing.status_code == 404

    worker = register(client)
    stale = client.post("/api/compute/tasks/claim", json={"worker_id": "worker-1", "session_id": "not-the-session", "capabilities": [], "lease_seconds": 60})
    assert stale.status_code == 409

    overclaimed = client.post(
        "/api/compute/tasks/claim",
        json={"worker_id": "worker-1", "session_id": worker["session_id"], "capabilities": ["solver-a", "gpu-v100"], "lease_seconds": 60},
    )
    assert overclaimed.status_code == 422
    assert overclaimed.json()["error"]["context"]["unregistered"] == ["gpu-v100"]

    ok = claim(client, "worker-1", worker["session_id"])
    assert ok.status_code == 200 and ok.json()["task"] is not None


def test_version_policy_blocks_outdated_workers(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("version-000001"))
    worker = register(client, version="1.2.0")

    policy = client.put(
        "/api/workers/policy",
        json={"min_supported_version": "2.0.0", "quarantine_failure_threshold": 3, "offline_after_seconds": 120, "actor": "administrator"},
    )
    assert policy.status_code == 200
    assert policy.json()["min_supported_version"] == "2.0.0"

    blocked = claim(client, "worker-1", worker["session_id"])
    assert blocked.status_code == 409
    assert "版本" in blocked.json()["error"]["message"]

    outdated = client.post("/api/workers", json={"worker_key": "worker-2", "software_version": "1.9.0", "capabilities": ["solver-a"], "max_concurrency": 1})
    assert outdated.status_code == 422

    upgraded = register(client, version="2.1.0")
    assert claim(client, "worker-1", upgraded["session_id"]).status_code == 200

    invalid = client.put(
        "/api/workers/policy",
        json={"min_supported_version": "two-dot-oh", "quarantine_failure_threshold": 3, "offline_after_seconds": 120, "actor": "administrator"},
    )
    assert invalid.status_code == 422


def test_concurrency_slots_limit_claims(client):
    create_template(client)
    first = client.post("/api/compute/tasks", json=submit_payload("slot-000001")).json()
    second = client.post("/api/compute/tasks", json=submit_payload("slot-000002")).json()
    worker = register(client, concurrency=1)
    session = worker["session_id"]

    assert claim(client, "worker-1", session).json()["task"]["id"] == first["id"]
    full = claim(client, "worker-1", session)
    assert full.status_code == 409
    assert "槽位" in full.json()["error"]["message"]

    completed = client.post(
        f"/api/compute/tasks/{first['id']}/complete",
        json={"worker_id": "worker-1", "session_id": session, "result": {"value": 1}, "metrics": {}},
    )
    assert completed.status_code == 200
    again = claim(client, "worker-1", session)
    assert again.status_code == 200 and again.json()["task"]["id"] == second["id"]


def test_automatic_quarantine_after_repeated_failures_and_release_audit(client):
    create_template(client)
    policy = client.put(
        "/api/workers/policy",
        json={"min_supported_version": "0.0.0", "quarantine_failure_threshold": 2, "offline_after_seconds": 120, "actor": "administrator"},
    )
    assert policy.status_code == 200
    worker = register(client)
    session = worker["session_id"]

    for key in ("quarantine-000001", "quarantine-000002"):
        task = client.post("/api/compute/tasks", json=submit_payload(key)).json()
        leased = claim(client, "worker-1", session).json()["task"]
        assert leased["id"] == task["id"]
        failed = client.post(
            f"/api/compute/tasks/{task['id']}/fail",
            json={"worker_id": "worker-1", "session_id": session, "error_code": "unsupported_algorithm", "message": "算法不支持", "retryable": False},
        )
        assert failed.status_code == 200

    view = client.get("/api/workers/worker-1").json()
    assert view["status"] == "quarantined"
    assert "连续失败 2 次" in view["quarantine_reason"]
    assert view["total_failures"] == 2
    assert view["failure_rate"] == 1.0
    quarantined_event = next(event for event in view["events"] if event["action"] == "quarantined")
    assert quarantined_event["trigger_source"] == "automatic"
    assert quarantined_event["detail"]["error_code"] == "unsupported_algorithm"

    blocked = claim(client, "worker-1", session)
    assert blocked.status_code == 409
    assert "隔离" in blocked.json()["error"]["message"]

    missing_reason = client.post("/api/workers/worker-1/release", json={"actor": "administrator", "reason": ""})
    assert missing_reason.status_code == 422

    released = client.post("/api/workers/worker-1/release", json={"actor": "administrator", "reason": "已修复算法声明并复核"})
    assert released.status_code == 200
    assert released.json()["status"] == "active"
    assert released.json()["consecutive_failures"] == 0

    view = client.get("/api/workers/worker-1").json()
    assert view["total_failures"] == 2
    assert view["quarantine_reason"]
    actions = [event["action"] for event in view["events"]]
    assert actions[:2] == ["released", "quarantined"]
    release_event = view["events"][0]
    assert release_event["reason"] == "已修复算法声明并复核"
    assert release_event["actor"] == "administrator"

    client.post("/api/compute/tasks", json=submit_payload("quarantine-000003"))
    assert claim(client, "worker-1", session).status_code == 200


def test_disable_and_enable_lifecycle(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("disable-000001"))
    worker = register(client)
    session = worker["session_id"]

    disabled = client.post("/api/workers/worker-1/disable", json={"actor": "operator", "reason": "能力声明不实，人工停用"})
    assert disabled.status_code == 200 and disabled.json()["status"] == "disabled"

    blocked = claim(client, "worker-1", session)
    assert blocked.status_code == 409 and "停用" in blocked.json()["error"]["message"]

    enabled = client.post("/api/workers/worker-1/enable", json={"actor": "operator"})
    assert enabled.status_code == 200 and enabled.json()["status"] == "active"
    assert claim(client, "worker-1", session).status_code == 200

    missing = client.post("/api/workers/ghost/disable", json={"actor": "operator", "reason": "不存在"})
    assert missing.status_code == 404


def test_manual_quarantine_requires_reason(client):
    register(client)
    invalid = client.post("/api/workers/worker-1/quarantine", json={"actor": "operator", "reason": ""})
    assert invalid.status_code == 422
    quarantined = client.post("/api/workers/worker-1/quarantine", json={"actor": "operator", "reason": "异常流量排查"})
    assert quarantined.status_code == 200
    assert quarantined.json()["status"] == "quarantined"
    assert quarantined.json()["quarantine_reason"] == "异常流量排查"


def test_worker_directory_shows_leases_failure_rate_and_events(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("view-000001")).json()
    worker = register(client)
    register(client, key="worker-2", capabilities=("other",))

    leased = claim(client, "worker-1", worker["session_id"])
    assert leased.status_code == 200

    detail = client.get("/api/workers/worker-1").json()
    assert detail["active_lease_count"] == 1
    assert detail["active_leases"][0]["task_id"] == task["id"]
    assert detail["failure_rate"] == 0.0
    assert detail["online"] is True
    assert detail["offline_after_seconds"] == 120
    assert detail["capabilities"] == ["solver-a"]
    assert detail["events"][0]["action"] == "registered"

    listing = client.get("/api/workers").json()["items"]
    assert {item["worker_key"] for item in listing} == {"worker-1", "worker-2"}
    quarantined = client.get("/api/workers", params={"status": "quarantined"}).json()["items"]
    assert quarantined == []

    missing = client.get("/api/workers/ghost")
    assert missing.status_code == 404


def test_failure_rate_tracks_completed_and_failed_outcomes(client):
    service, registry, _ = make_services()
    service.create_template(TEMPLATE, "administrator")
    worker = registry.register({"worker_key": "worker-1", "software_version": "1.0.0", "capabilities": ["solver-a"], "max_concurrency": 3})
    session = worker["session_id"]

    service.submit(submit_payload("rate-000001"))
    service.submit(submit_payload("rate-000002"))
    leased = service.claim("worker-1", session, ["solver-a"], 60)
    service.complete(leased["id"], "worker-1", session, {"value": 1}, {})
    leased = service.claim("worker-1", session, ["solver-a"], 60)
    service.fail(leased["id"], "worker-1", session, "numeric_error", "数值不收敛", False)

    view = registry.get_worker("worker-1")
    assert view["total_completed"] == 1
    assert view["total_failures"] == 1
    assert view["failure_rate"] == 0.5
    assert view["consecutive_failures"] == 1
    assert view["status"] == "active"


def test_registration_validation(client):
    bad_version = client.post("/api/workers", json={"worker_key": "worker-9", "software_version": "v1.x", "capabilities": ["solver-a"], "max_concurrency": 1})
    assert bad_version.status_code == 422
    empty_caps = client.post("/api/workers", json={"worker_key": "worker-9", "software_version": "1.0.0", "capabilities": [], "max_concurrency": 1})
    assert empty_caps.status_code == 422
    bad_key = client.post("/api/workers", json={"worker_key": "bad key!", "software_version": "1.0.0", "capabilities": ["solver-a"], "max_concurrency": 1})
    assert bad_key.status_code == 422
