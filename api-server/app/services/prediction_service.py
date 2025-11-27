import logging
import traceback
import lightgbm as lgb
import pandas as pd
import numpy as np
from app.models.job import Job, JobStatus
from app.store.job_store import job_store
from app.core.config import settings
from app.services.spark_service import spark_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PredictionService:
    """
    Orquestra a execução de jobs de predição de forma assíncrona.
    """

    def start_prediction_job(self, input_path: str) -> Job:
        """
        Cria um novo job, armazena-o e o retorna.
        """
        new_job = Job()
        job_store[new_job.job_id] = new_job
        logger.info(f"Job de predição '{new_job.job_id}' criado para o arquivo '{input_path}'.")
        return new_job

    def run_prediction_in_background(self, job_id: str, input_path: str):
        """
        Executa a lógica de predição em background.
        Atualiza o status do job conforme o progresso.
        """
        job = job_store.get(job_id)
        if not job:
            logger.error(f"Job '{job_id}' não foi encontrado para execução em background.")
            return

        job.status = JobStatus.RUNNING
        logger.info(f"Job '{job_id}' iniciado. Status: RUNNING.")

        try:
            logger.info(f"Iniciando processamento Spark para o job '{job_id}'...")
            
            spark = spark_service.get_spark_session()
            hdfs_full_path = f"{settings.HDFS_BASE_URL}{input_path}"
            logger.info(f"Lendo dados do HDFS: {hdfs_full_path}")
            
            df_spark = spark.read.csv(hdfs_full_path, header=True, inferSchema=True)
            
            df_pandas = df_spark.toPandas()
            logger.info(f"Dados carregados: {len(df_pandas)} linhas.")
            
            feature_cols = ['LAT_POCO', 'LONG_POCO', 'PROFUNDIDADE', 'MAG_VALOR', 'THC', 'UC', 'KC']
            target_col = 'TARGET'
            
            available_features = [c for c in feature_cols if c in df_pandas.columns]
            
            if not available_features:
                raise ValueError(f"Colunas de features esperadas não encontradas. Disponíveis: {df_pandas.columns}")
                
            df_pandas[available_features] = df_pandas[available_features].fillna(0)
            
            X = df_pandas[available_features]
            
            if target_col in df_pandas.columns:
                logger.info("Coluna TARGET encontrada. Treinando modelo...")
                y = df_pandas[target_col].fillna(0)
                
                model = lgb.LGBMClassifier(verbose=-1)
                model.fit(X, y)
                
                probs = model.predict_proba(X)[:, 1]
            else:
                logger.info("Coluna TARGET não encontrada. Pulando treinamento...")
                probs = [0.5] * len(X) 
            
            result_data = []
            
            if 'LAT_POCO' in df_pandas.columns and 'LONG_POCO' in df_pandas.columns:
                lat_data = df_pandas['LAT_POCO'].values
                long_data = df_pandas['LONG_POCO'].values
                
                for i in range(len(df_pandas)):
                    try:
                        lat = float(lat_data[i])
                        lon = float(long_data[i])
                        val = float(probs[i])
                        result_data.append((lon, lat, val)) 
                    except ValueError:
                        continue
            else:
                raise ValueError("Colunas de coordenadas (LAT_POCO, LONG_POCO) necessárias para o resultado.")

            if not result_data:
                raise ValueError("Nenhum dado válido gerado para o resultado.")

            max_val = float(np.max(probs)) if len(probs) > 0 else 0.0
            min_val = float(np.min(probs)) if len(probs) > 0 else 0.0

            prediction_result = {
                "data": result_data,
                "max_value": max_val,
                "min_value": min_val,
            }

            job.result = prediction_result
            job.status = JobStatus.SUCCESS
            logger.info(f"Job '{job_id}' concluído com SUCESSO.")

        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(f"Erro crítico durante a execução do job '{job_id}': {e}\n{error_details}")
            job.status = JobStatus.FAILED
            job.error = str(e)


prediction_service = PredictionService()
