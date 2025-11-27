from fastapi import HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from app.core.config import settings

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


async def get_api_key(api_key_header_value: str = Security(api_key_header)):
    """
    Dependência do FastAPI para validar a chave de API.
    Compara a chave recebida no header com a chave definida nas configurações.
    """
    if api_key_header_value and api_key_header_value == settings.API_KEY:
        return api_key_header_value
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Chave de API inválida ou ausente.",
        )
