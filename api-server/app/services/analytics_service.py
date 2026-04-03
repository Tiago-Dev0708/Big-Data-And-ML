import logging
import traceback
from pyspark.sql.functions import col, count, when
from pyspark.sql.utils import AnalysisException
from fastapi import HTTPException, status
from app.core.config import settings
from app.services.spark_service import spark_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalyticsService:
    """
    Contém a lógica de negócio para os endpoints de análise de dados.
    Utiliza o Spark para processar os dados armazenados no HDFS.
    """

    def _get_full_hdfs_path(self, hdfs_path: str) -> str:
        return f"{settings.HDFS_BASE_URL}{hdfs_path}"

    def get_historical_success(self, bacia: str, hdfs_path: str = "/data/poços_brasil.csv"):
        """
        Calcula a taxa de sucesso histórica para uma bacia e a compara com uma
        taxa prevista por um "Método IA".
        """
        spark = spark_service.get_spark_session()
        full_path = self._get_full_hdfs_path(hdfs_path)
        logger.info(f"Calculando sucesso histórico para bacia '{bacia}' usando dados de '{full_path}'.")

        try:
            df = spark.read.csv(full_path, header=True, inferSchema=True)
            filtered_df = df.filter((col("BACIA") == bacia) & col("SITUACAO").isNotNull())
            agg_df = filtered_df.agg(
                count("*").alias("total"),
                count(when(col("SITUACAO") == 'PRODUTOR', True)).alias("success_count")
            ).first()

            if not agg_df or agg_df["total"] == 0:
                historical_rate = 0.0
            else:
                historical_rate = (agg_df["success_count"] / agg_df["total"]) * 100
            ia_predicted_rate = min(historical_rate * 1.5, 95.0)

            return {
                "bacia": bacia,
                "categories": ["Método Convencional (Histórico)", "Método IA (Previsto)"],
                "data": [
                    {"value": round(historical_rate, 1), "name": "Método Convencional (Histórico)"},
                    {"value": round(ia_predicted_rate, 1), "name": "Método IA (Previsto)"},
                ],
            }
        except AnalysisException as e:
            logger.error(f"Erro de análise do Spark (arquivo não encontrado): {e}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Arquivo de dados não encontrado em {hdfs_path}")
        except Exception as e:
            logger.error(f"Erro inesperado ao calcular sucesso histórico: {traceback.format_exc()}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    def get_situation_distribution(self, bacia: str = None, hdfs_path: str = "/data/poços_brasil.csv"):
        """
        Calcula a distribuição de poços pela sua situação (e.g., PRODUTOR, SECO).
        Pode ser filtrado opcionalmente por bacia.
        """
        spark = spark_service.get_spark_session()
        full_path = self._get_full_hdfs_path(hdfs_path)
        log_msg = f"Calculando distribuição de situação usando dados de '{full_path}'"
        if bacia:
            log_msg += f" para a bacia '{bacia}'."
        logger.info(log_msg)
        
        try:
            df = spark.read.csv(full_path, header=True, inferSchema=True)

            if bacia:
                df = df.filter(col("BACIA") == bacia)

            distribution_df = df.groupBy("SITUACAO").count().orderBy(col("count").desc())
            results = distribution_df.collect()
            return [{"value": row["count"], "name": row["SITUACAO"]} for row in results]
            
        except AnalysisException as e:
            logger.error(f"Erro de análise do Spark (arquivo não encontrado): {e}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Arquivo de dados não encontrado em {hdfs_path}")
        except Exception as e:
            logger.error(f"Erro inesperado ao calcular distribuição: {traceback.format_exc()}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


analytics_service = AnalyticsService()
