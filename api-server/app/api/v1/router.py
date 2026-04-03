from fastapi import APIRouter
from app.api.v1.endpoints import analytics, cluster, data, prediction

api_router = APIRouter()

api_router.include_router(cluster.router, prefix="/cluster", tags=["Cluster"])
api_router.include_router(data.router, prefix="/data", tags=["Data Management"])
api_router.include_router(prediction.router, prefix="/predict", tags=["Prediction"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])
