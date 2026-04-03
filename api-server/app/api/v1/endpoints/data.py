from typing import List, Dict

from fastapi import (APIRouter, Depends, File, Query, UploadFile, status)

from app.api.deps import get_api_key
from app.services.hdfs_service import hdfs_service

router = APIRouter()


@router.post(
    "/upload",
    status_code=status.HTTP_201_CREATED,
    summary="Upload de arquivo para o HDFS",
    response_description="Confirmação do upload com nome e caminho do arquivo."
)
def upload_data(
    hdfs_path: str = Query(
        ...,
        description="Caminho completo no HDFS onde o arquivo será salvo.",
        example="/data/raw/poços_brasil.csv"
    ),
    file: UploadFile = File(..., description="Arquivo CSV a ser enviado."),
    api_key: str = Depends(get_api_key),
) -> Dict[str, str]:
    """
    Envia um arquivo (multipart/form-data) para um caminho especificado no HDFS.
    Este endpoint é protegido e requer uma `x-api-key` válida.
    """
    return hdfs_service.upload_file(file=file, hdfs_path=hdfs_path)


@router.get(
    "/files",
    summary="Lista arquivos em um diretório do HDFS",
    response_description="Uma lista de arquivos com seus nomes e tamanhos."
)
def list_data_files(
    hdfs_path: str = Query(
        "/data",
        description="Diretório no HDFS para listar os arquivos.",
        example="/data/raw"
    ),
    api_key: str = Depends(get_api_key),
) -> List[Dict[str, object]]:
    """
    Lista todos os arquivos em um diretório específico do HDFS, retornando
    seus nomes e tamanhos em bytes.
    Este endpoint é protegido e requer uma `x-api-key` válida.
    """
    return hdfs_service.list_files(hdfs_path)
