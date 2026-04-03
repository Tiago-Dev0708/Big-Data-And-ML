import logging
import os
from hdfs import InsecureClient
from fastapi import UploadFile, HTTPException, status
from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HDFSService:
    """
    Encapsula a lógica de interação com o Hadoop Distributed File System (HDFS).
    """

    def __init__(self):
        """
        Inicializa o cliente HDFS usando a URL do NameNode das configurações.
        """
        try:
            self.client = InsecureClient(settings.HADOOP_NAMENODE_URL, user='root')
        except Exception as e:
            logger.error(f"Não foi possível conectar ao HDFS em {settings.HADOOP_NAMENODE_URL}. Erro: {e}")
            self.client = None

    def check_hdfs_status(self) -> str:
        """Verifica se o HDFS está acessível."""
        if not self.client:
            return "OFFLINE"
        try:
            self.client.status('/')
            logger.info("Conexão com HDFS verificada com sucesso.")
            return "ONLINE"
        except Exception as e:
            logger.error(f"Falha ao verificar status do HDFS: {e}")
            return "OFFLINE"

    def upload_file(self, file: UploadFile, hdfs_path: str) -> dict:
        """
        Faz upload de um arquivo para um caminho específico no HDFS.
        """
        if not self.client:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Serviço HDFS indisponível.")

        try:
            directory = os.path.dirname(hdfs_path)
            if not self.client.status(directory, strict=False):
                self.client.makedirs(directory)
                logger.info(f"Diretório HDFS '{directory}' criado.")

            self.client.write(hdfs_path, data=file.file.read(), overwrite=True)
            
            logger.info(f"Arquivo '{file.filename}' enviado para '{hdfs_path}' com sucesso.")
            return {"filename": file.filename, "path": hdfs_path}

        except Exception as e:
            logger.error(f"Erro ao fazer upload do arquivo para o HDFS: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro no HDFS: {e}")

    def list_files(self, hdfs_path: str) -> list:
        """
        Lista os arquivos e diretórios em um caminho específico no HDFS.
        """
        if not self.client:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Serviço HDFS indisponível.")
            
        try:
            if not self.client.status(hdfs_path, strict=False):
                return []

            files_status = self.client.list(hdfs_path, status=True)
            files = [
                {"filename": status['pathSuffix'], "size": status['length']}
                for _, status in files_status
            ]
            
            logger.info(f"Listagem de arquivos em '{hdfs_path}' bem-sucedida.")
            return files

        except Exception as e:
            logger.error(f"Erro ao listar arquivos do HDFS em '{hdfs_path}': {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro no HDFS: {e}")


hdfs_service = HDFSService()
