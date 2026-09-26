from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.compute.service import AUTO_QUARANTINE_ACTOR, ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import close_connection, get_connection, init_db


@pytest.fixture(autouse=True)
def isolated_database(tmp_path: Path):
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(tmp_path / "workers.db")
    close_connection()
    yield
    close_connection()


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
    "max_attempts": 5,
}

START = datetime(2026, 9, 26, 2, 0, tzinfo=UTC)


def _submit(service: ComputeOperationsService, key: str) -> dict:
    return service.submit(
        {
            "template_code": "solver-a",
            "project_code": "project-a",
            "requested_by": "researcher-1",
            "parameters": {"iterations": 100, "mode": "accurate"},
            "priority": 50,
            "idempotency_key": key,
        }
    )


def _register(service: ComputeOperationsService, worker_id="w1", instance_id="w1-inst-1", *, version="1.2.0", capabilities=None, max_concurrency=4):
    return service.register_worker(
        {
            "worker_id": worker_id,
            "instance_id": instance_id,
            "software_version": version,
            "capabilities": ["solver-a"] if capabilities is None else capabilities,
            "max_concurrency": max_concurrency,
        }
    )


def _service(*, min_version="1.0.0", threshold=3, offline_seconds=90) -> tuple[ComputeOperationsService, FrozenClock]:
    init_db()
    clock = FrozenClock(START)
    service = ComputeOperationsService(
        get_connection(),
        clock,
        min_worker_version=min_version,
        failure_threshold=threshold,
        offline_seconds=offline_seconds,
    )
    service.create_template(TEMPLATE, "administrator")
    return service, clock


def test_registry_persists_identity_version_capabilities_and_slots():
    service, _ = _service()
    worker = _register(service)
    assert worker["worker_id"] == "w1"
    assert worker["software_version"] == "1.2.0"
    assert worker["capabilities"] == ["solver-a"]
    assert worker["max_concurrency"] == 4
    assert worker["status"] == "active"
    assert worker["current_instance_id"] == "w1-inst-1"
    assert worker["last_heartbeat_at"]
    assert worker["active_lease_count"] == 0
    assert worker["free_slots"] == 4
    assert worker["failure_rate"] == 0.0
    assert worker["online"] is True


def test_unregistered_worker_cannot_claim():
    service, _ = _service()
    _submit(service, "task-000001")
    try:
        service.claim("ghost", "ghost-inst", ["solver-a"], 30)
        assert False, "未注册工作者应当被拒绝"
    except Exception as exc:
        assert exc.status_code == 404


def test_claim_enforces_concurrency_slots():
    service, _ = _service()
    _register(service, max_concurrency=2)
    t1 = _submit(service, "task-000001")
    t2 = _submit(service, "task-000002")
    t3 = _submit(service, "task-000003")
    first = service.claim("w1", "w1-inst-1", ["solver-a"], 30)
    second = service.claim("w1", "w1-inst-1", ["solver-a"], 30)
    assert first["id"] == t1["id"]
    assert second["id"] == t2["id"]
    # 槽位用尽：领取返回空而不是超发
    assert service.claim("w1", "w1-inst-1", ["solver-a"], 30) is None
    view = service.get_worker("w1")
    assert {lease["id"] for lease in view["active_leases"]} == {t1["id"], t2["id"]}
    assert view["active_lease_count"] == 2
    assert view["free_slots"] == 0
    # 完成一个任务后槽位释放
    service.complete(t1["id"], "w1", "w1-inst-1", {"value": 1}, {})
    assert service.get_worker("w1")["free_slots"] == 1
    third = service.claim("w1", "w1-inst-1", ["solver-a"], 30)
    assert third and third["id"] == t3["id"]


def test_undeclared_capability_in_claim_is_rejected():
    service, _ = _service()
    _register(service, capabilities=["solver-a"])
    _submit(service, "task-000001")
    try:
        service.claim("w1", "w1-inst-1", ["solver-a", "fake-algorithm"], 30)
        assert False, "注册表之外的能力声明应当被拒绝"
    except Exception as exc:
        assert exc.status_code == 409
        assert "fake-algorithm" in exc.context["capabilities"]


def test_incompatible_version_cannot_claim_and_shows_in_query():
    service, _ = _service(min_version="2.0.0")
    _register(service, version="1.9.0")
    _submit(service, "task-000001")
    view = service.get_worker("w1")
    assert view["version_compatible"] is False
    try:
        service.claim("w1", "w1-inst-1", ["solver-a"], 30)
        assert False, "版本不兼容应当被拒绝"
    except Exception as exc:
        assert exc.status_code == 409
        assert exc.context["minimum_version"] == "2.0.0"


def test_quarantined_worker_cannot_claim_or_reregister():
    service, _ = _service()
    _register(service)
    service.quarantine_worker("w1", "admin", "虚假声明算法能力")
    _submit(service, "task-000001")
    try:
        service.claim("w1", "w1-inst-1", ["solver-a"], 30)
        assert False, "隔离工作者不能领取"
    except Exception as exc:
        assert exc.status_code == 409
    # 重启换实例也不能绕过隔离
    try:
        _register(service, instance_id="w1-inst-2")
        assert False, "隔离状态不能通过重新注册清除"
    except Exception as exc:
        assert exc.status_code == 409


def test_consecutive_failures_trigger_automatic_quarantine():
    service, _ = _service(threshold=3)
    _register(service)
    tasks = [_submit(service, f"task-00000{i}") for i in range(1, 4)]
    for task in tasks:
        claimed = service.claim("w1", "w1-inst-1", ["solver-a"], 60)
        assert claimed["id"] == task["id"]
        before = service.get_worker("w1")
        if task["id"] != tasks[-1]["id"]:
            assert before["status"] == "active"
        service.fail(task["id"], "w1", "w1-inst-1", "unsupported", "声称支持但实际不支持", False)
    view = service.get_worker("w1")
    assert view["status"] == "quarantined"
    assert view["consecutive_failures"] == 3
    assert view["failure_rate"] == 1.0
    basis = view["quarantine_basis"]
    assert basis["action"] == "quarantined"
    assert basis["actor"] == AUTO_QUARANTINE_ACTOR
    assert basis["basis"]["source"] == "auto_policy"
    assert basis["basis"]["threshold"] == 3
    assert basis["basis"]["consecutive_failures"] == 3
    assert [f["error_code"] for f in basis["basis"]["recent_failures"]] == ["unsupported"] * 3


def test_success_resets_consecutive_failure_streak():
    service, _ = _service(threshold=3)
    _register(service)
    for index, outcome in enumerate(["fail", "fail", "success", "fail", "fail"], start=1):
        task = _submit(service, f"task-0000{index}")
        service.claim("w1", "w1-inst-1", ["solver-a"], 60)
        if outcome == "fail":
            service.fail(task["id"], "w1", "w1-inst-1", "numeric_error", "不收敛", False)
        else:
            service.complete(task["id"], "w1", "w1-inst-1", {"ok": True}, {})
    view = service.get_worker("w1")
    # 成功打断了连续失败计数，2 次未达阈值
    assert view["status"] == "active"
    assert view["consecutive_failures"] == 2
    assert view["failed_count"] == 4
    assert view["succeeded_count"] == 1
    assert view["failure_rate"] == 0.8


def test_release_quarantine_requires_reason_and_keeps_history():
    service, _ = _service(threshold=1)
    _register(service)
    task = _submit(service, "task-000001")
    service.claim("w1", "w1-inst-1", ["solver-a"], 60)
    service.fail(task["id"], "w1", "w1-inst-1", "boom", "崩溃", False)
    assert service.get_worker("w1")["status"] == "quarantined"
    try:
        service.release_worker("w1", "admin", "   ")
        assert False, "解除隔离必须记录原因"
    except Exception as exc:
        assert exc.status_code == 422
    released = service.release_worker("w1", "admin", "现场已升级补丁并核验能力")
    assert released["status"] == "active"
    # 历史统计不清除
    assert released["failed_count"] == 1
    assert released["failure_rate"] == 1.0
    assert released["consecutive_failures"] == 0
    actions = [event["action"] for event in released["events"]]
    assert actions == ["quarantined", "released"]
    release_event = released["events"][-1]
    assert release_event["reason"] == "现场已升级补丁并核验能力"
    assert release_event["actor"] == "admin"
    assert release_event["basis"]["kept_history"] is True
    # 隔离依据仍然可查
    assert released["quarantine_basis"]["action"] == "quarantined"


def test_restart_reuses_logical_identity_but_old_session_cannot_ack():
    service, clock = _service()
    _register(service)
    task = _submit(service, "task-000001")
    claimed = service.claim("w1", "w1-inst-1", ["solver-a"], 60)
    assert claimed["lease_instance_id"] == "w1-inst-1"
    # 实例重启：逻辑身份不变，新实例标识产生新会话
    restarted = _register(service, instance_id="w1-inst-2")
    assert restarted["current_instance_id"] == "w1-inst-2"
    assert restarted["status"] == "active"
    assert restarted["failed_count"] == 0
    # 旧实例会话已结束，不能继续回执
    try:
        service.complete(task["id"], "w1", "w1-inst-1", {"value": 9}, {})
        assert False, "旧实例会话不能继续回执"
    except Exception as exc:
        assert exc.status_code == 409
    try:
        service.fail(task["id"], "w1", "w1-inst-1", "x", "y", False)
        assert False, "旧实例会话不能继续上报失败"
    except Exception as exc:
        assert exc.status_code == 409
    # 旧实例也不能再领取
    try:
        service.claim("w1", "w1-inst-1", ["solver-a"], 60)
        assert False, "旧实例会话不能继续领取"
    except Exception as exc:
        assert exc.status_code == 409
    # 租约过期后由恢复流程回收，新实例可以重新领取
    clock.advance(seconds=61)
    service.recover_expired()
    again = service.claim("w1", "w1-inst-2", ["solver-a"], 60)
    assert again and again["id"] == task["id"]
    done = service.complete(task["id"], "w1", "w1-inst-2", {"value": 9}, {})
    assert done["status"] == "succeeded"


def test_instance_id_cannot_be_shared_between_logical_identities():
    service, _ = _service()
    _register(service, worker_id="w1", instance_id="shared-inst")
    try:
        _register(service, worker_id="w2", instance_id="shared-inst")
        assert False, "同一实例标识不能属于两个逻辑身份"
    except Exception as exc:
        assert exc.status_code == 409


def test_repeat_registration_is_idempotent_but_ended_instance_cannot_be_reused():
    service, _ = _service()
    first = _register(service, worker_id="w1", instance_id="w1-inst-1", version="1.2.0")
    again = _register(service, worker_id="w1", instance_id="w1-inst-1", version="1.3.0")
    # 相同实例重复报到：刷新资料，不产生第二张会话
    assert again["software_version"] == "1.3.0"
    assert again["session"]["instance_id"] == "w1-inst-1"
    # 重启到新实例后，旧实例标识已经结束，不能再复用
    _register(service, worker_id="w1", instance_id="w1-inst-2")
    try:
        _register(service, worker_id="w1", instance_id="w1-inst-1")
        assert False, "已结束会话的实例标识不能复用，防止旧进程冒充新会话"
    except Exception as exc:
        assert exc.status_code == 409


def test_query_shows_active_leases_failure_rate_and_quarantine_basis():
    service, _ = _service(threshold=2)
    _register(service, max_concurrency=4)
    t1 = _submit(service, "task-000001")
    t2 = _submit(service, "task-000002")
    service.claim("w1", "w1-inst-1", ["solver-a"], 60)
    service.fail(t1["id"], "w1", "w1-inst-1", "e1", "错误1", False)
    service.claim("w1", "w1-inst-1", ["solver-a"], 60)
    view = service.get_worker("w1")
    active = view["active_leases"]
    assert len(active) == 1
    assert active[0]["id"] == t2["id"]
    assert active[0]["task_algorithm"] == "solver-a"
    assert active[0]["lease_instance_id"] == "w1-inst-1"
    assert active[0]["attempt_count"] == 1
    assert active[0]["lease_expires_at"] > START.isoformat()
    assert view["failure_rate"] == 1.0
    assert view["active_lease_count"] == 1
    # 第二次失败触发自动隔离，隔离依据包含活动租约信息
    service.fail(t2["id"], "w1", "w1-inst-1", "e2", "错误2", False)
    quarantined = service.get_worker("w1")
    assert quarantined["status"] == "quarantined"
    basis = quarantined["quarantine_basis"]
    assert basis["basis"]["recent_failures"][0]["task_id"] == t1["id"]
    assert basis["basis"]["recent_failures"][1]["task_id"] == t2["id"]
    listing = service.list_workers(status="quarantined")
    assert [item["worker_id"] for item in listing["items"]] == ["w1"]


def test_fixed_clock_determines_offline_state():
    service, clock = _service(offline_seconds=90)
    _register(service)
    assert service.get_worker("w1")["online"] is True
    clock.advance(seconds=90)
    assert service.get_worker("w1")["online"] is False
    # 工作者心跳恢复在线
    service.worker_heartbeat("w1", "w1-inst-1")
    assert service.get_worker("w1")["online"] is True
    clock.advance(seconds=91)
    assert service.get_worker("w1")["online"] is False


def test_stopped_worker_cannot_claim_and_can_be_reenabled():
    service, _ = _service()
    _register(service)
    service.set_worker_enabled("w1", "admin", "机房检修", False)
    _submit(service, "task-000001")
    try:
        service.claim("w1", "w1-inst-1", ["solver-a"], 30)
        assert False, "停用工作者不能领取"
    except Exception as exc:
        assert exc.status_code == 409
    try:
        service.set_worker_enabled("w1", "admin", "   ", True)
        assert False, "启停必须记录原因"
    except Exception as exc:
        assert exc.status_code == 422
    service.set_worker_enabled("w1", "admin", "检修完成", True)
    claimed = service.claim("w1", "w1-inst-1", ["solver-a"], 30)
    assert claimed is not None
    events = [event["action"] for event in service.get_worker("w1")["events"]]
    assert events == ["disabled", "enabled"]


def test_expired_lease_recovery_counts_failure_and_can_quarantine():
    service, clock = _service(threshold=2)
    _register(service)
    t1 = _submit(service, "task-000001")
    t2 = _submit(service, "task-000002")
    for task in (t1, t2):
        service.claim("w1", "w1-inst-1", ["solver-a"], 10)
        clock.advance(seconds=11)
    result = service.recover_expired()
    assert set(result["recovered"]) == {t1["id"], t2["id"]}
    assert result["exhausted"] == []
    assert result["quarantined_workers"] == ["w1"]
    view = service.get_worker("w1")
    assert view["status"] == "quarantined"
    assert [f["error_code"] for f in view["quarantine_basis"]["basis"]["recent_failures"]] == ["lease_expired", "lease_expired"]
