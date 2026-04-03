from fastapi import APIRouter

from app.services.hdfs_service import hdfs_service
from app.services.spark_service import spark_service

router = APIRouter()


@router.get(
    "/health",
    summary="Verifica a saúde do Cluster",
    response_description="Status dos serviços HDFS e Spark"
)
def get_cluster_health():
    """
    Verifica e retorna o status de conectividade dos principais serviços
    do cluster de Big Data:
    - **HDFS**: Confirma se o NameNode está acessível.
    - **Spark**: Confirma se a sessão Spark pode ser estabelecida com o Master.
    """
    hdfs_status = hdfs_service.check_hdfs_status()
    spark_status = "UNKNOWN"
    try:
        spark = spark_service.get_spark_session()
        if spark.sparkContext.uiWebUrl:
            spark_status = "ONLINE"
        else:
            spark_status = "OFFLINE"
    except Exception:
        spark_status = "OFFLINE"

    return {"hdfs_status": hdfs_status, "spark_status": spark_status}
