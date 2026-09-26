from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.config import Settings
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction

_RECENT_FAILURE_LIMIT = 10
_VERSION_SEGMENT = re.compile(r"^\d+$")
# 自动隔离动作的执行者标识，便于与人手工隔离区分。
AUTO_QUARANTINE_ACTOR = "system:auto-quarantine"


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def parse_version(value: str) -> tuple[int, ...]:
    """把 1.2.3 形式的版本号解析为可比较的元组，非法输入抛 ValidationError。"""
    text = (value or "").strip()
    parts = text.split(".") if text else []
    if not parts or any(not _VERSION_SEGMENT.match(part) for part in parts):
        raise ValidationError("软件版本号必须是点分数字，例如 1.4.2")
    return tuple(int(part) for part in parts)


def version_compatible(actual: str, minimum: str) -> bool:
    try:
        return parse_version(actual) >= parse_version(minimum)
    except ValidationError:
        return False


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(
        self,
        connection: sqlite3.Connection | None = None,
        clock: Clock | None = None,
        *,
        min_worker_version: str | None = None,
        failure_threshold: int | None = None,
        offline_seconds: int | None = None,
    ) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)
        settings = Settings.load()
        self.min_worker_version = min_worker_version or settings.worker_min_version
        self.failure_threshold = (
            settings.worker_failure_threshold if failure_threshold is None else failure_threshold
        )
        self.offline_seconds = settings.worker_offline_seconds if offline_seconds is None else offline_seconds

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, instance_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            worker = self._require_registered_worker(repository, worker_id)
            self._ensure_current_instance(repository, worker, instance_id)
            if worker["status"] == "quarantined":
                raise ConflictError("工作者已被隔离，不能领取任务")
            if worker["status"] == "stopped":
                raise ConflictError("工作者已被停用，不能领取任务")
            if not version_compatible(worker["software_version"], self.min_worker_version):
                raise ConflictError(
                    "工作者版本不兼容",
                    context={"software_version": worker["software_version"], "minimum_version": self.min_worker_version},
                )
            registered_capabilities = set(json.loads(worker["capabilities_json"]))
            undeclared = sorted(set(capabilities) - registered_capabilities)
            if undeclared:
                raise ConflictError("工作者声明了注册表之外的能力", context={"capabilities": undeclared})
            if repository.active_lease_count(worker_id) >= int(worker["max_concurrency"]):
                return None
            candidate = repository.queued_candidate(sorted(registered_capabilities), now)
            if candidate is None:
                self._touch_worker_heartbeat(connection, worker_id, now)
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_instance_id=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, instance_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            self._touch_worker_heartbeat(connection, worker_id, now)
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, instance_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            self._authorize_lease(repository, task, worker_id, instance_id)
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=? AND lease_instance_id=?",
                (expires, now, task_id, worker_id, instance_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            self._touch_worker_heartbeat(connection, worker_id, now)
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, instance_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            self._authorize_lease(repository, task, worker_id, instance_id)
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_instance_id='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            connection.execute(
                "UPDATE compute_workers SET succeeded_count=succeeded_count+1,consecutive_failures=0,updated_at=? WHERE worker_id=?",
                (now, worker_id),
            )
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, instance_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            self._authorize_lease(repository, task, worker_id, instance_id)
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_instance_id='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            self._record_worker_failure(
                repository,
                worker_id=worker_id,
                failure={"task_id": task_id, "error_code": error_code, "at": now},
                reason_detail=message[:2000],
                now=now,
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        quarantined: list[str] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_instance_id='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                owner = task["lease_owner"]
                if owner:
                    self._record_worker_failure(
                        repository,
                        worker_id=owner,
                        failure={"task_id": int(task["id"]), "error_code": "lease_expired", "at": now},
                        reason_detail="工作者租约已过期",
                        now=now,
                    )
                    worker = repository.worker_by_id(owner)
                    if worker is not None and worker["status"] == "quarantined":
                        quarantined.append(owner)
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "quarantined_workers": sorted(set(quarantined))}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    # ----- 工作者注册表 -----

    def register_worker(self, payload: dict[str, Any]) -> dict[str, Any]:
        """注册或重启报到：沿用逻辑身份，每次重启产生新的实例会话。

        隔离状态不能通过重启绕过；既有计数与事件历史始终保留。
        """
        worker_id = payload["worker_id"]
        instance_id = payload["instance_id"]
        software_version = payload["software_version"].strip()
        parse_version(software_version)
        capabilities = sorted(set(payload["capabilities"]))
        max_concurrency = int(payload["max_concurrency"])
        if max_concurrency <= 0:
            raise ValidationError("并发上限必须大于 0")
        now = to_storage(self.clock.now())
        capabilities_json = json.dumps(capabilities, ensure_ascii=False, sort_keys=True)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            worker = repository.worker_by_id(worker_id)
            if worker is not None and worker["status"] == "quarantined":
                raise ConflictError("工作者处于隔离状态，必须由管理员解除隔离后才能重新报到")
            if self._instance_known_elsewhere(repository, worker_id, instance_id):
                raise ConflictError("实例标识已经属于另一个工作者身份")
            prior_session = repository.session_by_instance(instance_id)
            if prior_session is not None and prior_session["ended_at"]:
                raise ConflictError("实例标识已随上一次会话结束，重启必须使用新的实例标识")
            if worker is None:
                connection.execute(
                    "INSERT INTO compute_workers(worker_id,current_instance_id,software_version,capabilities_json,max_concurrency,status,last_heartbeat_at,registered_at,updated_at) VALUES(?,?,?,?,?, 'active', ?, ?, ?)",
                    (worker_id, instance_id, software_version, capabilities_json, max_concurrency, now, now, now),
                )
                self._insert_session(connection, worker_id, instance_id, software_version, now)
            else:
                open_session = repository.open_session_for_worker(worker_id)
                if open_session is not None and open_session["instance_id"] != instance_id:
                    # 重启换新实例：关闭旧会话，旧实例此后不能再领取或回执
                    self._close_open_session(connection, worker_id, "superseded_by_restart", now)
                    self._insert_session(connection, worker_id, instance_id, software_version, now)
                elif open_session is None:
                    self._insert_session(connection, worker_id, instance_id, software_version, now)
                # open_session 实例标识相同则视为重复报到，幂等刷新
                connection.execute(
                    "UPDATE compute_workers SET current_instance_id=?,software_version=?,capabilities_json=?,max_concurrency=?,last_heartbeat_at=?,updated_at=? WHERE worker_id=?",
                    (instance_id, software_version, capabilities_json, max_concurrency, now, now, worker_id),
                )
            self._touch_worker_heartbeat(connection, worker_id, now)
            return self._worker_view(repository, worker_id)

    def worker_heartbeat(self, worker_id: str, instance_id: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            worker = self._require_registered_worker(repository, worker_id)
            self._ensure_current_instance(repository, worker, instance_id)
            if worker["status"] == "quarantined":
                raise ConflictError("工作者已被隔离，心跳被拒绝")
            if worker["status"] == "stopped":
                raise ConflictError("工作者已被停用，心跳被拒绝")
            self._touch_worker_heartbeat(connection, worker_id, now)
            return self._worker_view(repository, worker_id)

    def get_worker(self, worker_id: str) -> dict[str, Any]:
        with transaction() as connection:
            repository = ComputeRepository(connection)
            worker = repository.worker_by_id(worker_id)
            if worker is None:
                raise NotFoundError("工作者不存在")
            return self._worker_view(repository, worker_id)

    def list_workers(self, *, status: str | None = None, limit: int = 100) -> dict[str, Any]:
        with transaction() as connection:
            repository = ComputeRepository(connection)
            rows = repository.list_workers(status=status, limit=max(1, min(limit, 500)))
            return {"items": [self._worker_view(repository, row["worker_id"]) for row in rows]}

    def quarantine_worker(self, worker_id: str, actor: str, reason: str) -> dict[str, Any]:
        if not reason.strip():
            raise ValidationError("隔离必须填写原因")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            worker = repository.worker_by_id(worker_id)
            if worker is None:
                raise NotFoundError("工作者不存在")
            if worker["status"] == "quarantined":
                raise ConflictError("工作者已经处于隔离状态")
            self._quarantine_locked(
                repository,
                worker_id,
                reason=reason.strip(),
                basis={"source": "manual", "operator": actor},
                actor=actor,
                now=now,
            )
            return self._worker_view(repository, worker_id)

    def release_worker(self, worker_id: str, actor: str, reason: str) -> dict[str, Any]:
        """管理员解除隔离：必须记录原因；事件与累计统计等历史不清除。"""
        if not reason.strip():
            raise ValidationError("解除隔离必须填写原因")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            worker = repository.worker_by_id(worker_id)
            if worker is None:
                raise NotFoundError("工作者不存在")
            if worker["status"] != "quarantined":
                raise ConflictError("只有隔离中的工作者可以解除隔离")
            connection.execute(
                "UPDATE compute_workers SET status='active',consecutive_failures=0,updated_at=? WHERE worker_id=?",
                (now, worker_id),
            )
            repository.add_worker_event(
                worker_id=worker_id,
                action="released",
                reason=reason.strip(),
                basis={"source": "manual", "operator": actor, "kept_history": True},
                actor=actor,
                now=now,
            )
            return self._worker_view(repository, worker_id)

    def set_worker_enabled(self, worker_id: str, actor: str, reason: str, enabled: bool) -> dict[str, Any]:
        if not reason.strip():
            raise ValidationError("调整启停状态必须填写原因")
        now = to_storage(self.clock.now())
        target = "active" if enabled else "stopped"
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            worker = repository.worker_by_id(worker_id)
            if worker is None:
                raise NotFoundError("工作者不存在")
            if worker["status"] == "quarantined":
                raise ConflictError("隔离状态必须先解除隔离，不能直接启停")
            if worker["status"] == target:
                raise ConflictError("工作者已经处于目标状态")
            connection.execute("UPDATE compute_workers SET status=?,updated_at=? WHERE worker_id=?", (target, now, worker_id))
            repository.add_worker_event(
                worker_id=worker_id,
                action="enabled" if enabled else "disabled",
                reason=reason.strip(),
                basis={"source": "manual", "operator": actor},
                actor=actor,
                now=now,
            )
            return self._worker_view(repository, worker_id)

    def _require_registered_worker(self, repository: ComputeRepository, worker_id: str) -> sqlite3.Row:
        worker = repository.worker_by_id(worker_id)
        if worker is None:
            raise NotFoundError("工作者尚未注册，不能参与任务调度")
        return worker

    @staticmethod
    def _instance_known_elsewhere(repository: ComputeRepository, worker_id: str, instance_id: str) -> bool:
        session = repository.session_by_instance(instance_id)
        return session is not None and session["worker_id"] != worker_id

    @staticmethod
    def _ensure_current_instance(repository: ComputeRepository, worker: sqlite3.Row, instance_id: str) -> None:
        session = repository.open_session_for_worker(worker["worker_id"])
        if session is None:
            raise ConflictError("工作者当前没有有效会话，请重新注册报到")
        if session["instance_id"] != instance_id:
            raise ConflictError("实例标识与当前会话不符，旧实例已失效，请使用新实例重新注册报到")

    def _authorize_lease(
        self,
        repository: ComputeRepository,
        task: sqlite3.Row | None,
        worker_id: str,
        instance_id: str,
    ) -> sqlite3.Row:
        if task is None:
            raise NotFoundError("计算任务不存在")
        if task["status"] != "running" or task["lease_owner"] != worker_id:
            raise ConflictError("任务未由当前工作者持有")
        worker = self._require_registered_worker(repository, worker_id)
        self._ensure_current_instance(repository, worker, instance_id)
        if task["lease_instance_id"] != instance_id:
            raise ConflictError("任务租约属于已结束的旧实例会话，重启后不能继续回执")
        if worker["status"] == "quarantined":
            raise ConflictError("工作者已被隔离，不能继续回执或心跳，租约等待恢复")
        if worker["status"] == "stopped":
            raise ConflictError("工作者已被停用，不能继续回执或心跳")
        return worker

    @staticmethod
    def _insert_session(connection: sqlite3.Connection, worker_id: str, instance_id: str, software_version: str, now: str) -> None:
        connection.execute(
            "INSERT INTO compute_worker_sessions(worker_id,instance_id,software_version,started_at,last_heartbeat_at) VALUES(?,?,?,?,?)",
            (worker_id, instance_id, software_version, now, now),
        )

    @staticmethod
    def _close_open_session(connection: sqlite3.Connection, worker_id: str, reason: str, now: str) -> None:
        connection.execute(
            "UPDATE compute_worker_sessions SET ended_at=?,end_reason=? WHERE worker_id=? AND ended_at=''",
            (now, reason, worker_id),
        )

    @staticmethod
    def _touch_worker_heartbeat(connection: sqlite3.Connection, worker_id: str, now: str) -> None:
        connection.execute("UPDATE compute_workers SET last_heartbeat_at=? WHERE worker_id=?", (now, worker_id))
        connection.execute(
            "UPDATE compute_worker_sessions SET last_heartbeat_at=? WHERE worker_id=? AND ended_at=''",
            (now, worker_id),
        )

    def _record_worker_failure(
        self,
        repository: ComputeRepository,
        *,
        worker_id: str,
        failure: dict[str, Any],
        reason_detail: str,
        now: str,
    ) -> None:
        worker = repository.worker_by_id(worker_id)
        if worker is None:
            return
        recent = json.loads(worker["recent_failures_json"])
        recent.append(failure)
        recent = recent[-_RECENT_FAILURE_LIMIT:]
        connection = repository.connection
        connection.execute(
            "UPDATE compute_workers SET failed_count=failed_count+1,consecutive_failures=consecutive_failures+1,recent_failures_json=?,updated_at=? WHERE worker_id=?",
            (json.dumps(recent, ensure_ascii=False, sort_keys=True), now, worker_id),
        )
        updated = repository.worker_by_id(worker_id)
        if updated["status"] == "active" and int(updated["consecutive_failures"]) >= self.failure_threshold:
            self._quarantine_locked(
                repository,
                worker_id,
                reason=f"连续 {updated['consecutive_failures']} 次任务失败，达到隔离阈值 {self.failure_threshold}",
                basis={
                    "source": "auto_policy",
                    "threshold": self.failure_threshold,
                    "consecutive_failures": int(updated["consecutive_failures"]),
                    "recent_failures": recent,
                    "last_error": reason_detail,
                },
                actor=AUTO_QUARANTINE_ACTOR,
                now=now,
            )

    @staticmethod
    def _quarantine_locked(
        repository: ComputeRepository,
        worker_id: str,
        *,
        reason: str,
        basis: dict[str, Any],
        actor: str,
        now: str,
    ) -> None:
        cursor = repository.connection.execute(
            "UPDATE compute_workers SET status='quarantined',updated_at=? WHERE worker_id=? AND status<>'quarantined'",
            (now, worker_id),
        )
        if cursor.rowcount != 1:
            return
        repository.add_worker_event(
            worker_id=worker_id,
            action="quarantined",
            reason=reason,
            basis=basis,
            actor=actor,
            now=now,
        )

    def _worker_view(self, repository: ComputeRepository, worker_id: str) -> dict[str, Any]:
        worker = repository.worker_by_id(worker_id)
        if worker is None:
            raise NotFoundError("工作者不存在")
        data = dict(worker)
        data["capabilities"] = json.loads(data.pop("capabilities_json"))
        recent = json.loads(data.pop("recent_failures_json"))
        data["recent_failures"] = recent
        succeeded = int(worker["succeeded_count"])
        failed = int(worker["failed_count"])
        finished = succeeded + failed
        data["failure_rate"] = round(failed / finished, 4) if finished else 0.0
        leases = repository.active_leases(worker_id)
        data["active_leases"] = leases
        data["active_lease_count"] = len(leases)
        data["free_slots"] = max(0, int(worker["max_concurrency"]) - len(leases))
        events = repository.worker_events(worker_id)
        for event in events:
            event["basis"] = json.loads(event.pop("basis_json"))
        data["events"] = events
        quarantine = next((event for event in reversed(events) if event["action"] == "quarantined"), None)
        data["quarantine_basis"] = quarantine
        session = repository.open_session_for_worker(worker_id)
        data["session"] = dict(session) if session is not None else None
        heartbeat = worker["last_heartbeat_at"]
        if heartbeat:
            hb_time = datetime.fromisoformat(heartbeat)
            online = self.clock.now() - hb_time < timedelta(seconds=self.offline_seconds)
        else:
            online = False
        data["online"] = online
        data["offline_threshold_seconds"] = self.offline_seconds
        data["version_compatible"] = version_compatible(worker["software_version"], self.min_worker_version)
        data["minimum_version"] = self.min_worker_version
        return data

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
