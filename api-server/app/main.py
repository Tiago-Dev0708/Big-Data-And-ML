from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.router import api_router
from app.services.spark_service import spark_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gerenciador de ciclo de vida da aplicação FastAPI.
    - Na inicialização (`startup`): Inicia a sessão Spark.
    - No encerramento (`shutdown`): Encerra a sessão Spark para liberar recursos.
    """
    print("Iniciando a aplicação e a sessão Spark...")
    spark_service.get_spark_session()
    yield
    print("Encerrando a aplicação e a sessão Spark...")
    spark_service.stop_spark_session()


app = FastAPI(
    title="PetroPredict API",
    description="API para orquestrar jobs de predição e análise de dados de poços de petróleo em um cluster Big Data.",
    version="1.0.0",
    lifespan=lifespan,
    contact={
        "name": "Engenharia de Dados",
        "url": "http://example.com/contact",
        "email": "dados@example.com",
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")


@app.get("/", tags=["Root"])
def read_root():
    """
    Endpoint raiz para verificar se a API está online.
    """
    return {"message": "Bem-vindo à PetroPredict API"}
