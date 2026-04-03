from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configurações da aplicação carregadas a partir de variáveis de ambiente.
    """
    API_KEY: str
    HADOOP_NAMENODE_URL: str
    SPARK_MASTER_URL: str
    HDFS_BASE_URL: str

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding='utf-8', extra='ignore'
    )


settings = Settings()
