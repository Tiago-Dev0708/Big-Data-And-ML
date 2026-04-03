import uuid
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Enum para os status possíveis de um job."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Job(BaseModel):
    """Modelo base para um job no sistema."""
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: JobStatus = JobStatus.PENDING
    result: Optional[Any] = None
    error: Optional[str] = None


class JobCreate(BaseModel):
    """Modelo para a criação de um novo job de predição."""
    input_path: str


class JobStatusResponse(BaseModel):
    """Modelo de resposta para o status de um job."""
    job_id: str
    status: JobStatus
    error: Optional[str] = None
