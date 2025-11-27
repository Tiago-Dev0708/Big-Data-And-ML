import logging
from pyspark.sql import SparkSession
from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkService:
    """
    Gerencia a sessão Spark como um singleton para reutilização em toda a aplicação.
    """
    _spark: SparkSession = None

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        """
        Retorna a sessão Spark ativa ou cria uma nova se não existir.
        Configura a sessão para se conectar ao HDFS e ao Master do Spark.
        """
        if cls._spark is None:
            logger.info("Nenhuma sessão Spark ativa encontrada. Criando uma nova...")
            try:
                cls._spark = (
                    SparkSession.builder.appName("PetroPredictAPI")
                    .master(settings.SPARK_MASTER_URL)
                    .config("spark.hadoop.fs.defaultFS", settings.HDFS_BASE_URL)
                    .config("spark.hadoop.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
                    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
                    .config("spark.driver.host", "api-server") 
                    .config("spark.driver.bindAddress", "0.0.0.0")
                    .config("spark.blockManager.port", "10025")
                    .config("spark.driver.port", "10026")
                    .config("spark.driver.blockManager.port", "10027") 
                    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1")
                    .getOrCreate()
                )
                logger.info("Sessão Spark criada com sucesso.")
            except Exception as e:
                logger.error(f"Falha ao criar a sessão Spark: {e}")
                raise
        return cls._spark

    @classmethod
    def stop_spark_session(cls):
        """
        Encerra a sessão Spark ativa, liberando os recursos no cluster.
        """
        if cls._spark:
            logger.info("Encerrando a sessão Spark...")
            cls._spark.stop()
            cls._spark = None
            logger.info("Sessão Spark encerrada com sucesso.")

spark_service = SparkService()
