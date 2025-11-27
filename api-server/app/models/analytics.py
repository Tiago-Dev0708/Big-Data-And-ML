from typing import List
from pydantic import BaseModel


class HistoricalSuccessDataItem(BaseModel):
    """Item de dados individual para o gráfico de sucesso histórico."""
    value: float
    name: str


class HistoricalSuccessResponse(BaseModel):
    """Modelo de resposta completo para o gráfico de sucesso histórico."""
    bacia: str
    categories: List[str]
    data: List[HistoricalSuccessDataItem]


class SituationDistributionItem(BaseModel):
    """Item de dados para o gráfico de distribuição por situação."""
    value: int
    name: str
