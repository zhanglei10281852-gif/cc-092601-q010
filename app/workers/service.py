from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage, utc_now
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction

SCHEMA = """
CREATE TABLE IF NOT EXISTS compute_workers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_key TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    software_version TEXT NOT NULL,
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    max_concurrency INTEGER NOT NULL CHECK(max_concurrency > 0),
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled','quarantined')),
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    total_failures INTEGER NOT NULL DEFAULT 0,
    total_completed INTEGER NOT NULL DEFAULT 0,
    quarantine_reason TEXT NOT NULL DEFAULT '',
    quarantined_at TEXT NOT NULL DEFAULT '',
    last_heartbeat_at TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS compute_worker_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_key TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    trigger_source TEXT NOT NULL DEFAULT 'manual' CHECK(trigger_source IN ('automatic','manual')),
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_compute_worker_events_key ON compute_worker_events(worker_key, id);
CREATE TABLE IF NOT EXISTS compute_worker_policy (
    id INTEGER PRIMARY KEY CHECK(id=1),
    min_supported_version TEXT NOT NULL,
    quarantine_failure_threshold INTEGER NOT NULL CHECK(quarantine_failure_threshold > 0),
    offline_after_seconds INTEGER NOT NULL CHECK(offline_after_seconds > 0),
    updated_by TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

DEFAULT_POLICY = {
    "min_supported_version": "0.0.0",
    "quarantine_failure_threshold": 3,
    "offline_after_seconds": 120,
}


def ensure_schema() -> None:
    connection = get_connection()
    connection.executescript(SCHEMA)
    connection.execute(
        "INSERT OR IGNORE INTO compute_worker_policy(id,min_supported_version,quarantine_failure_threshold,offline_after_seconds,updated_by,updated_at) VALUES(1,?,?,?,?,?)",
        (DEFAULT_POLICY["min_supported_version"], DEFAULT_POLICY["quarantine_failure_threshold"], DEFAULT_POLICY["offline_after_seconds"], "system", to_storage(utc_now())),
    )


def parse_version(value: str) -> tuple[int, ...]:
    parts = value.strip().split(".")
    if any(not part.isdigit() for part in parts):
        raise ValidationError("软件版本号格式不合法", context={"software_version": value})
    return tuple(int(part) for part in parts)


def version_compatible(version: str, minimum: str) -> bool:
    current, floor = parse_version(version), parse_version(minimum)
    width = max(len(current), len(floor))
    current += (0,) * (width - len(current))
    floor += (0,) * (width - len(floor))
    return current >= floor


def lease_owner(worker_key: str, session_id: str) -> str:
    """租约归属标识：逻辑身份加会话号，实例重启后旧会话的租约自然失效。"""
    return f"{worker_key}#{session_id}"


def get_policy_row(connection: sqlite3.Connection) -> sqlite3.Row:
    row = connection.execute("SELECT * FROM compute_worker_policy WHERE id=1").fetchone()
    if row is None:
        raise NotFoundError("工作者策略未初始化")
    return row


def add_event(connection: sqlite3.Connection, *, worker_key: str, action: str, actor: str, reason: str, trigger: str, detail: dict[str, Any], now: str) -> None:
    connection.execute(
        "INSERT INTO compute_worker_events(worker_key,action,actor,reason,trigger_source,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
        (worker_key, action, actor, reason, trigger, json.dumps(detail, ensure_ascii=False, sort_keys=True), now),
    )


def require_current_session(connection: sqlite3.Connection, worker_key: str, session_id: str) -> sqlite3.Row:
    row = connection.execute("SELECT * FROM compute_workers WHERE worker_key=?", (worker_key,)).fetchone()
    if row is None:
        raise NotFoundError("工作者未注册")
    if row["session_id"] != session_id:
        raise ConflictError("工作者会话已失效，请重新注册后再回执")
    return row


def assert_claimable(connection: sqlite3.Connection, now: str, worker_key: str, session_id: str) -> sqlite3.Row:
    """领取前校验：当前会话、启停与隔离状态、版本兼容性和剩余并发槽位。"""
    row = require_current_session(connection, worker_key, session_id)
    if row["status"] == "quarantined":
        raise ConflictError("工作者已被隔离，禁止领取任务", context={"quarantine_reason": row["quarantine_reason"], "quarantined_at": row["quarantined_at"]})
    if row["status"] == "disabled":
        raise ConflictError("工作者已停用，禁止领取任务")
    policy = get_policy_row(connection)
    if not version_compatible(row["software_version"], policy["min_supported_version"]):
        raise ConflictError("工作者软件版本低于平台最低要求", context={"software_version": row["software_version"], "min_supported_version": policy["min_supported_version"]})
    active = connection.execute(
        "SELECT COUNT(*) FROM compute_tasks WHERE lease_owner=? AND status='running' AND lease_expires_at<>'' AND lease_expires_at>?",
        (lease_owner(worker_key, session_id), now),
    ).fetchone()[0]
    if int(active) >= int(row["max_concurrency"]):
        raise ConflictError("工作者并发槽位已满", context={"max_concurrency": int(row["max_concurrency"]), "active_leases": int(active)})
    return row


def record_outcome(connection: sqlite3.Connection, now: str, worker_key: str, *, succeeded: bool, task_id: int, error_code: str = "") -> None:
    """按回执结果累计工作者统计，连续失败达到策略阈值时自动隔离。"""
    row = connection.execute("SELECT * FROM compute_workers WHERE worker_key=?", (worker_key,)).fetchone()
    if row is None:
        return
    if succeeded:
        connection.execute(
            "UPDATE compute_workers SET total_completed=total_completed+1, consecutive_failures=0, updated_at=? WHERE worker_key=?",
            (now, worker_key),
        )
        return
    consecutive = int(row["consecutive_failures"]) + 1
    threshold = int(get_policy_row(connection)["quarantine_failure_threshold"])
    if row["status"] == "active" and consecutive >= threshold:
        reason = f"连续失败 {consecutive} 次，达到策略阈值 {threshold}"
        connection.execute(
            "UPDATE compute_workers SET total_failures=total_failures+1, consecutive_failures=?, status='quarantined', quarantine_reason=?, quarantined_at=?, updated_at=?, version=version+1 WHERE worker_key=?",
            (consecutive, reason, now, now, worker_key),
        )
        add_event(connection, worker_key=worker_key, action="quarantined", actor="system", reason=reason, trigger="automatic", detail={"task_id": task_id, "error_code": error_code, "consecutive_failures": consecutive, "threshold": threshold}, now=now)
    else:
        connection.execute(
            "UPDATE compute_workers SET total_failures=total_failures+1, consecutive_failures=?, updated_at=? WHERE worker_key=?",
            (consecutive, now, worker_key),
        )


class WorkerRegistryService:
    """工作者注册、心跳、启停、隔离解除和版本策略管理。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        ensure_schema()
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()

    def register(self, payload: dict[str, Any]) -> dict[str, Any]:
        parse_version(payload["software_version"])
        capabilities = sorted({item.strip() for item in payload["capabilities"] if item.strip()})
        if not capabilities:
            raise ValidationError("工作者至少声明一项算法能力")
        now_value = self.clock.now()
        now = to_storage(now_value)
        session_id = secrets.token_hex(8)
        with transaction(immediate=True) as connection:
            policy = get_policy_row(connection)
            if not version_compatible(payload["software_version"], policy["min_supported_version"]):
                raise ValidationError("软件版本低于平台最低要求", context={"software_version": payload["software_version"], "min_supported_version": policy["min_supported_version"]})
            existing = connection.execute("SELECT * FROM compute_workers WHERE worker_key=?", (payload["worker_key"],)).fetchone()
            detail = {"software_version": payload["software_version"], "capabilities": capabilities, "max_concurrency": payload["max_concurrency"]}
            if existing is None:
                connection.execute(
                    "INSERT INTO compute_workers(worker_key,session_id,software_version,capabilities_json,max_concurrency,status,last_heartbeat_at,registered_at,updated_at) VALUES(?,?,?,?,?,'active',?,?,?)",
                    (payload["worker_key"], session_id, payload["software_version"], json.dumps(capabilities, ensure_ascii=False), payload["max_concurrency"], now, now, now),
                )
                action, restarted = "registered", False
            else:
                # 重启沿用逻辑身份：更换会话号并刷新元数据，但保留状态、统计和隔离依据。
                connection.execute(
                    "UPDATE compute_workers SET session_id=?, software_version=?, capabilities_json=?, max_concurrency=?, last_heartbeat_at=?, updated_at=?, version=version+1 WHERE worker_key=?",
                    (session_id, payload["software_version"], json.dumps(capabilities, ensure_ascii=False), payload["max_concurrency"], now, now, payload["worker_key"]),
                )
                action, restarted = "restarted", True
            add_event(connection, worker_key=payload["worker_key"], action=action, actor=payload["worker_key"], reason="", trigger="manual", detail=detail, now=now)
            view = self._view(connection, payload["worker_key"], now_value)
            view["restarted"] = restarted
            return view

    def heartbeat(self, worker_key: str, session_id: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            require_current_session(connection, worker_key, session_id)
            connection.execute("UPDATE compute_workers SET last_heartbeat_at=?, updated_at=? WHERE worker_key=?", (now, now, worker_key))
            return self._view(connection, worker_key, now_value)

    def disable(self, worker_key: str, actor: str, reason: str) -> dict[str, Any]:
        return self._set_status(worker_key, actor=actor, action="disabled", target="disabled", sources={"active"}, reason=reason)

    def enable(self, worker_key: str, actor: str) -> dict[str, Any]:
        return self._set_status(worker_key, actor=actor, action="enabled", target="active", sources={"disabled"}, reason="")

    def quarantine(self, worker_key: str, actor: str, reason: str) -> dict[str, Any]:
        return self._set_status(worker_key, actor=actor, action="quarantined", target="quarantined", sources={"active"}, reason=reason)

    def release(self, worker_key: str, actor: str, reason: str) -> dict[str, Any]:
        """解除隔离：必须记录操作者和原因，历史事件与统计数据全部保留。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            row = self._require(connection, worker_key)
            if row["status"] != "quarantined":
                raise ConflictError("工作者未处于隔离状态")
            connection.execute(
                "UPDATE compute_workers SET status='active', consecutive_failures=0, updated_at=?, version=version+1 WHERE worker_key=?",
                (now, worker_key),
            )
            add_event(connection, worker_key=worker_key, action="released", actor=actor, reason=reason, trigger="manual", detail={"quarantine_reason": row["quarantine_reason"]}, now=now)
            return self._view(connection, worker_key, now_value)

    def get_worker(self, worker_key: str) -> dict[str, Any]:
        now_value = self.clock.now()
        view = self._view(self.connection, worker_key, now_value)
        owner = lease_owner(worker_key, view["session_id"])
        leases = self.connection.execute(
            "SELECT id AS task_id, status, lease_expires_at, started_at FROM compute_tasks WHERE lease_owner=? AND status='running' AND lease_expires_at<>'' AND lease_expires_at>? ORDER BY id",
            (owner, to_storage(now_value)),
        ).fetchall()
        view["active_leases"] = [dict(row) for row in leases]
        events = self.connection.execute(
            "SELECT action, actor, reason, trigger_source, detail_json, created_at FROM compute_worker_events WHERE worker_key=? ORDER BY id DESC LIMIT 50",
            (worker_key,),
        ).fetchall()
        view["events"] = [
            {"action": event["action"], "actor": event["actor"], "reason": event["reason"], "trigger_source": event["trigger_source"], "detail": json.loads(event["detail_json"]), "created_at": event["created_at"]}
            for event in events
        ]
        return view

    def list_workers(self, status: str | None = None) -> list[dict[str, Any]]:
        now_value = self.clock.now()
        rows = self.connection.execute("SELECT worker_key FROM compute_workers ORDER BY worker_key").fetchall()
        views = [self._view(self.connection, row["worker_key"], now_value) for row in rows]
        if status:
            views = [view for view in views if view["status"] == status]
        return views

    def get_policy(self) -> dict[str, Any]:
        row = get_policy_row(self.connection)
        return {
            "min_supported_version": row["min_supported_version"],
            "quarantine_failure_threshold": int(row["quarantine_failure_threshold"]),
            "offline_after_seconds": int(row["offline_after_seconds"]),
            "updated_by": row["updated_by"],
            "updated_at": row["updated_at"],
        }

    def update_policy(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        parse_version(payload["min_supported_version"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE compute_worker_policy SET min_supported_version=?, quarantine_failure_threshold=?, offline_after_seconds=?, updated_by=?, updated_at=? WHERE id=1",
                (payload["min_supported_version"], payload["quarantine_failure_threshold"], payload["offline_after_seconds"], actor, now),
            )
        return self.get_policy()

    def _set_status(self, worker_key: str, *, actor: str, action: str, target: str, sources: set[str], reason: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            row = self._require(connection, worker_key)
            if row["status"] not in sources:
                raise ConflictError(f"当前状态 {row['status']} 不允许该操作")
            quarantine_reason = reason if target == "quarantined" else row["quarantine_reason"]
            quarantined_at = now if target == "quarantined" else row["quarantined_at"]
            connection.execute(
                "UPDATE compute_workers SET status=?, quarantine_reason=?, quarantined_at=?, updated_at=?, version=version+1 WHERE worker_key=?",
                (target, quarantine_reason, quarantined_at, now, worker_key),
            )
            add_event(connection, worker_key=worker_key, action=action, actor=actor, reason=reason, trigger="manual", detail={}, now=now)
            return self._view(connection, worker_key, now_value)

    def _view(self, connection: sqlite3.Connection, worker_key: str, now_value: datetime) -> dict[str, Any]:
        row = self._require(connection, worker_key)
        policy = get_policy_row(connection)
        last_heartbeat = from_storage(row["last_heartbeat_at"])
        online = last_heartbeat is not None and now_value - last_heartbeat <= timedelta(seconds=int(policy["offline_after_seconds"]))
        completed = int(row["total_completed"])
        failures = int(row["total_failures"])
        outcomes = completed + failures
        active = connection.execute(
            "SELECT COUNT(*) FROM compute_tasks WHERE lease_owner=? AND status='running' AND lease_expires_at<>'' AND lease_expires_at>?",
            (lease_owner(worker_key, row["session_id"]), to_storage(now_value)),
        ).fetchone()[0]
        return {
            "worker_key": row["worker_key"],
            "session_id": row["session_id"],
            "software_version": row["software_version"],
            "capabilities": json.loads(row["capabilities_json"]),
            "max_concurrency": int(row["max_concurrency"]),
            "status": row["status"],
            "online": online,
            "offline_after_seconds": int(policy["offline_after_seconds"]),
            "last_heartbeat_at": row["last_heartbeat_at"],
            "registered_at": row["registered_at"],
            "updated_at": row["updated_at"],
            "consecutive_failures": int(row["consecutive_failures"]),
            "total_failures": failures,
            "total_completed": completed,
            "failure_rate": round(failures / outcomes, 4) if outcomes else 0.0,
            "quarantine_reason": row["quarantine_reason"],
            "quarantined_at": row["quarantined_at"],
            "active_lease_count": int(active),
            "version": int(row["version"]),
        }

    @staticmethod
    def _require(connection: sqlite3.Connection, worker_key: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM compute_workers WHERE worker_key=?", (worker_key,)).fetchone()
        if row is None:
            raise NotFoundError("工作者未注册")
        return row
