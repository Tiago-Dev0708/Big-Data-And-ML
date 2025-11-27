from typing import List, Tuple
from pydantic import BaseModel


class EChartsHeatmapResponse(BaseModel):
    """
    Modelo de resposta formatado para o gráfico de mapa de calor (heatmap) do ECharts.
    """
    data: List[Tuple[float, float, float]]
    max_value: float
    min_value: float
