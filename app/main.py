import logging
import os

from fastapi import FastAPI

from app.core.db import build_engine, build_session_factory
from app.jobs.full_pipeline_job import run_full_pipeline
from app.services.operadoras_query_service import OperadorasQueryService

# Routers
from app.api.v1.routers.health import router as health_router
from app.api.v1.routers.operadoras import router as operadoras_router

logger = logging.getLogger("app")

app = FastAPI(title="TestStagio API", version="1.0.0")


@app.on_event("startup")
def startup_job() -> None:
    # 1) Configura DB
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        # Falha explícita: API depende do DB agora
        raise RuntimeError(
            "DATABASE_URL não configurado. Exemplo: "
            "postgresql+psycopg://postgres:postgres@localhost:5432/ans"
        )

    engine = build_engine(database_url)
    session_factory = build_session_factory(engine)

    app.state.engine = engine
    app.state.session_factory = session_factory

    # Cache TTL do /api/estatisticas (default: 300s)
    stats_cache_ttl = int(os.getenv("STATS_CACHE_TTL_SECONDS", "300"))
    app.state.operadoras_service = OperadorasQueryService(stats_cache_ttl_seconds=stats_cache_ttl)

    # 2) Registra routers
    # Mantém o health existente e adiciona as rotas do Item 4
    app.include_router(health_router)
    app.include_router(operadoras_router)

    # 3) Mantém o comportamento original: roda pipeline no startup
    # Se quiser desligar futuramente, basta exportar RUN_PIPELINE_ON_STARTUP=0
    run_pipeline = os.getenv("RUN_PIPELINE_ON_STARTUP", "1").strip().lower() not in {"0", "false", "no"}
    if run_pipeline:
        try:
            run_full_pipeline()
        except Exception:
            logger.exception("Full pipeline failed on startup")
