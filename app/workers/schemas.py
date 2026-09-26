from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

Capability = Annotated[str, StringConstraints(min_length=1, max_length=120)]


class WorkerRegister(BaseModel):
    worker_key: str = Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    software_version: str = Field(min_length=1, max_length=40)
    capabilities: list[Capability] = Field(min_length=1, max_length=100)
    max_concurrency: int = Field(ge=1, le=1000)


class WorkerHeartbeat(BaseModel):
    session_id: str = Field(min_length=1, max_length=120)


class WorkerDisable(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class WorkerEnable(BaseModel):
    actor: str = Field(min_length=1, max_length=120)


class WorkerQuarantine(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class WorkerRelease(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class WorkerPolicyUpdate(BaseModel):
    min_supported_version: str = Field(min_length=1, max_length=40)
    quarantine_failure_threshold: int = Field(ge=1, le=100)
    offline_after_seconds: int = Field(ge=1, le=86400)
    actor: str = Field(min_length=1, max_length=120)
