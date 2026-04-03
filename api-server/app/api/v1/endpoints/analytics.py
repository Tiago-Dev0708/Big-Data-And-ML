from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_api_key
from app.models.analytics import (HistoricalSuccessResponse,
                                  SituationDistributionItem)
from app.services.analytics_service import analytics_service

router = APIRouter()


@router.get(
    "/historical-success",
    response_model=HistoricalSuccessResponse,
    summary="Calcula a taxa de sucesso de perfuração histórica vs. IA",
)
def get_historical_success(
    bacia: str = Query(..., description="Nome da bacia a ser analisada.", example="Potiguar"),
    api_key: str = Depends(get_api_key),
) -> HistoricalSuccessResponse:
    """
    Compara a taxa de sucesso histórica (poços produtores / total) de uma
    bacia com uma taxa de sucesso simulada de um método de IA.
    """
    return analytics_service.get_historical_success(bacia=bacia)


@router.get(
    "/situation-distribution",
    response_model=List[SituationDistributionItem],
    summary="Retorna a distribuição de poços por situação",
)
def get_situation_distribution(
    bacia: Optional[str] = Query(
        None, description="(Opcional) Filtra a distribuição por uma bacia específica.", example="Potiguar"
    ),
    api_key: str = Depends(get_api_key),
) -> List[SituationDistributionItem]:
    """
    Conta e retorna o número de poços para cada 'SITUACAO'
    (ex: 'PRODUTOR', 'SECO SEM INDÍCIOS').
    Pode ser filtrado por bacia.
    """
    return analytics_service.get_situation_distribution(bacia=bacia)
