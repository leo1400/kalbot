from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.routes import router
from kalbot.settings import get_settings

settings = get_settings()

app = FastAPI(
    title="Kalbot API",
    version="0.1.0",
    summary="Kalshi weather prediction and execution API",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
