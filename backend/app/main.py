from fastapi import FastAPI

from backend.app.routes import router
from kalbot.settings import get_settings

settings = get_settings()

app = FastAPI(
    title="Kalbot API",
    version="0.1.0",
    summary="Kalshi weather prediction and execution API",
)

app.include_router(router)
