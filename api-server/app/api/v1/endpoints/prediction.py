from fastapi import (APIRouter, BackgroundTasks, Depends, HTTPException, status)

from app.api.deps import get_api_key
from app.models.job import JobCreate, JobStatusResponse
from app.models.prediction import EChartsHeatmapResponse
from app.services.prediction_service import prediction_service
from app.store.job_store import job_store

router = APIRouter()


@router.post(
    "/start",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobStatusResponse,
    summary="Inicia um job de predição assíncrono",
)
def start_prediction(
    job_create: JobCreate,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(get_api_key),
) -> JobStatusResponse:
    """
    Inicia um job de predição em background.

    - **input_path**: Caminho do arquivo de entrada no HDFS.

    A API responde imediatamente com um `job_id` e status `PENDING`.
    O processamento real ocorre de forma assíncrona.
    """
    job = prediction_service.start_prediction_job(input_path=job_create.input_path)
    background_tasks.add_task(
        prediction_service.run_prediction_in_background, job.job_id, job_create.input_path
    )
    return job


@router.get(
    "/status/{job_id}",
    response_model=JobStatusResponse,
    summary="Verifica o status de um job de predição",
)
def get_prediction_status(
    job_id: str,
    api_key: str = Depends(get_api_key),
) -> JobStatusResponse:
    """
    Retorna o status atual de um job (`PENDING`, `RUNNING`, `SUCCESS`, `FAILED`).
    Use o `job_id` recebido do endpoint `/start`.
    """
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job não encontrado.")
    return job


@router.get(
    "/result/{job_id}",
    response_model=EChartsHeatmapResponse,
    summary="Obtém o resultado de um job de predição bem-sucedido",
)
def get_prediction_result(
    job_id: str,
    api_key: str = Depends(get_api_key),
) -> EChartsHeatmapResponse:
    """
    Retorna o resultado de um job que foi concluído com status `SUCCESS`.
    Se o job ainda não terminou ou falhou, retorna um erro.
    """
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job não encontrado.")
    if job.status != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"O Job ainda não foi concluído com sucesso. Status atual: {job.status}",
        )
    if not job.result:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resultado não encontrado para este job.")
         
    return job.result
