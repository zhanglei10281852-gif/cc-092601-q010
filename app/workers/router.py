from __future__ import annotations

from fastapi import APIRouter

from app.workers.schemas import WorkerDisable, WorkerEnable, WorkerHeartbeat, WorkerPolicyUpdate, WorkerQuarantine, WorkerRegister, WorkerRelease
from app.workers.service import WorkerRegistryService

router = APIRouter(prefix="/api/workers", tags=["计算工作者注册表"])


def service() -> WorkerRegistryService:
    return WorkerRegistryService()


@router.post("", status_code=201)
def register_worker(payload: WorkerRegister):
    return service().register(payload.model_dump())


@router.get("")
def list_workers(status: str | None = None):
    return {"items": service().list_workers(status=status)}


@router.get("/policy")
def get_policy():
    return service().get_policy()


@router.put("/policy")
def update_policy(payload: WorkerPolicyUpdate):
    data = payload.model_dump()
    actor = data.pop("actor")
    return service().update_policy(data, actor)


@router.get("/{worker_key}")
def get_worker(worker_key: str):
    return service().get_worker(worker_key)


@router.post("/{worker_key}/heartbeat")
def heartbeat(worker_key: str, payload: WorkerHeartbeat):
    return service().heartbeat(worker_key, payload.session_id)


@router.post("/{worker_key}/disable")
def disable_worker(worker_key: str, payload: WorkerDisable):
    return service().disable(worker_key, payload.actor, payload.reason)


@router.post("/{worker_key}/enable")
def enable_worker(worker_key: str, payload: WorkerEnable):
    return service().enable(worker_key, payload.actor)


@router.post("/{worker_key}/quarantine")
def quarantine_worker(worker_key: str, payload: WorkerQuarantine):
    return service().quarantine(worker_key, payload.actor, payload.reason)


@router.post("/{worker_key}/release")
def release_worker(worker_key: str, payload: WorkerRelease):
    return service().release(worker_key, payload.actor, payload.reason)
