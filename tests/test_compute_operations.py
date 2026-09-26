from __future__ import annotations

from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, transaction


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "tolerance": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {"tolerance": 0.001},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
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


def register_worker(client, worker_id: str, *, instance_id: str | None = None, version: str = "1.2.0", capabilities=None, max_concurrency: int = 4) -> dict:
    payload = {
        "worker_id": worker_id,
        "instance_id": instance_id or f"{worker_id}-inst-1",
        "software_version": version,
        "capabilities": ["solver-a"] if capabilities is None else capabilities,
        "max_concurrency": max_concurrency,
    }
    response = client.post("/api/compute/workers/register", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def claim_body(worker_id: str, instance_id: str, capabilities=None) -> dict:
    return {
        "worker_id": worker_id,
        "instance_id": instance_id,
        "capabilities": ["solver-a"] if capabilities is None else capabilities,
        "lease_seconds": 60,
    }


def test_template_submission_idempotency_and_parameter_validation(client):
    create_template(client)
    first = client.post("/api/compute/tasks", json=submit_payload("request-000001"))
    second = client.post("/api/compute/tasks", json=submit_payload("request-000001"))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    invalid = submit_payload("request-000002")
    invalid["parameters"]["iterations"] = 20000
    rejected = client.post("/api/compute/tasks", json=invalid)
    assert rejected.status_code == 422


def test_priority_capability_claim_and_result_version(client):
    create_template(client)
    low = client.post("/api/compute/tasks", json=submit_payload("priority-low", priority=10)).json()
    high = client.post("/api/compute/tasks", json=submit_payload("priority-high", priority=90)).json()
    # 未注册工作者不能领取
    unregistered = client.post("/api/compute/tasks/claim", json=claim_body("w0", "w0-inst", ["other"]))
    assert unregistered.status_code == 404
    # 已注册但能力不匹配
    register_worker(client, "w-mismatch", instance_id="w-mismatch-inst", capabilities=["other"])
    no_match = client.post("/api/compute/tasks/claim", json=claim_body("w-mismatch", "w-mismatch-inst", ["other"]))
    assert no_match.status_code == 200 and no_match.json()["task"] is None
    register_worker(client, "w1", instance_id="w1-inst")
    claimed = client.post("/api/compute/tasks/claim", json=claim_body("w1", "w1-inst"))
    assert claimed.status_code == 200
    assert claimed.json()["task"]["id"] == high["id"]
    completed = client.post(
        f"/api/compute/tasks/{high['id']}/complete",
        json={"worker_id": "w1", "instance_id": "w1-inst", "result": {"value": 3.14}, "metrics": {"seconds": 2}},
    )
    assert completed.status_code == 200
    details = client.get(f"/api/compute/task-details/{high['id']}").json()
    assert details["status"] == "succeeded"
    assert details["current_result_version"] == 1
    assert len(details["results"]) == 1
    assert low["status"] == "queued"


def test_quota_cancel_retry_priority_and_batch_interventions(client):
    create_template(client)
    quota = client.put(
        "/api/compute/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "limited", "max_queued": 1, "max_running": 1, "daily_submissions": 2},
    )
    assert quota.status_code == 200
    one = client.post("/api/compute/tasks", json=submit_payload("quota-one", user="limited")).json()
    blocked = client.post("/api/compute/tasks", json=submit_payload("quota-two", user="limited"))
    assert blocked.status_code == 409
    cancelled = client.post(f"/api/compute/tasks/{one['id']}/cancel", json={"actor": "administrator", "reason": "项目暂停"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    retried = client.post(f"/api/compute/tasks/{one['id']}/retry", json={"actor": "administrator", "reason": "项目恢复", "priority": 95})
    assert retried.status_code == 200 and retried.json()["priority"] == 95
    other = client.post("/api/compute/tasks", json=submit_payload("batch-other", user="other-user")).json()
    batch = client.post(
        "/api/compute/tasks/batch",
        json={"task_ids": [one["id"], other["id"]], "operation": "priority", "actor": "administrator", "reason": "紧急算例", "priority": 99},
    )
    assert batch.status_code == 200
    assert len(batch.json()["succeeded"]) == 2
    details = client.get(f"/api/compute/task-details/{one['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel", "retry", "priority"]


def test_failure_backoff_and_expired_lease_recovery(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    service.register_worker(
        {"worker_id": "worker-a", "instance_id": "worker-a-inst-1", "software_version": "1.2.0", "capabilities": ["solver-a"], "max_concurrency": 4}
    )
    first = service.submit(submit_payload("failure-000001"))
    claimed = service.claim("worker-a", "worker-a-inst-1", ["solver-a"], 10)
    assert claimed and claimed["id"] == first["id"]
    failed = service.fail(first["id"], "worker-a", "worker-a-inst-1", "numeric_error", "数值不收敛", True)
    assert failed["status"] == "queued"
    assert failed["available_at"] > failed["updated_at"]
    clock.advance(seconds=2)
    claimed_again = service.claim("worker-a", "worker-a-inst-1", ["solver-a"], 10)
    assert claimed_again and claimed_again["attempt_count"] == 2
    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["exhausted"] == [first["id"]]
    details = service.get_task(first["id"])
    assert details["status"] == "failed"
    assert details["interventions"][-1]["action"] == "lease_recovery"


def test_worker_registry_http_flow_auto_quarantine_and_release(client):
    create_template(client)
    register_worker(client, "w-http", instance_id="w-http-inst-1", max_concurrency=2)
    # 版本不兼容被拒绝
    old = client.post(
        "/api/compute/workers/register",
        json={"worker_id": "w-old", "instance_id": "w-old-inst", "software_version": "0.9.0", "capabilities": ["solver-a"], "max_concurrency": 1},
    )
    assert old.status_code == 201
    rejected = client.post("/api/compute/tasks/claim", json=claim_body("w-old", "w-old-inst"))
    assert rejected.status_code == 409
    # 连续失败达到阈值自动隔离（阈值默认 3）
    items = [client.post("/api/compute/tasks", json=submit_payload(f"http-fail-{index}")).json() for index in range(3)]
    for item in items:
        claimed = client.post("/api/compute/tasks/claim", json=claim_body("w-http", "w-http-inst-1"))
        assert claimed.status_code == 200 and claimed.json()["task"]["id"] == item["id"]
        failed = client.post(
            f"/api/compute/tasks/{item['id']}/fail",
            json={"worker_id": "w-http", "instance_id": "w-http-inst-1", "error_code": "unsupported", "message": "不支持该算法", "retryable": False},
        )
        assert failed.status_code == 200
    blocked = client.post("/api/compute/tasks/claim", json=claim_body("w-http", "w-http-inst-1"))
    assert blocked.status_code == 409
    detail = client.get("/api/compute/workers/w-http").json()
    assert detail["status"] == "quarantined"
    assert detail["quarantine_basis"]["actor"] == "system:auto-quarantine"
    assert detail["failure_rate"] == 1.0
    assert len(detail["active_leases"]) == 0
    # 解除隔离必须有原因；历史不清除
    missing_reason = client.post(
        "/api/compute/workers/w-http/release",
        json={"actor": "admin", "reason": "  "},
    )
    assert missing_reason.status_code == 422
    released = client.post(
        "/api/compute/workers/w-http/release",
        json={"actor": "admin", "reason": "已现场核验真实能力"},
    )
    assert released.status_code == 200
    body = released.json()
    assert body["status"] == "active"
    assert body["failed_count"] == 3
    assert [event["action"] for event in body["events"]] == ["quarantined", "released"]
    # 重启沿用逻辑身份：新实例领取后，旧实例会话不能回执
    client.post(
        "/api/compute/workers/register",
        json={"worker_id": "w-http", "instance_id": "w-http-inst-2", "software_version": "1.2.0", "capabilities": ["solver-a"], "max_concurrency": 2},
    )
    fresh_task = client.post("/api/compute/tasks", json=submit_payload("http-after-restart")).json()
    claimed = client.post("/api/compute/tasks/claim", json=claim_body("w-http", "w-http-inst-2"))
    assert claimed.status_code == 200 and claimed.json()["task"]["id"] == fresh_task["id"]
    stale = client.post(
        f"/api/compute/tasks/{fresh_task['id']}/complete",
        json={"worker_id": "w-http", "instance_id": "w-http-inst-1", "result": {}, "metrics": {}},
    )
    assert stale.status_code == 409
    current = client.get("/api/compute/workers/w-http").json()
    assert current["session"]["instance_id"] == "w-http-inst-2"
    assert current["free_slots"] == 1
