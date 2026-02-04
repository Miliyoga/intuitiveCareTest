# app/api/v1/routers/operadoras.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app.api.v1.schemas.operadoras import (
    DespesaHistoricoItem,
    EstatisticasResponse,
    OperadoraDespesasResponse,
    OperadoraDetailResponse,
    OperadorasListResponse,
    OperadoraItem,
    TopOperadoraItem,
)
from app.core.db import db_session
from app.services.operadoras_query_service import OperadorasQueryService


router = APIRouter(prefix="/api", tags=["Operadoras"])


def _get_service(request: Request) -> OperadorasQueryService:
    svc = getattr(request.app.state, "operadoras_service", None)
    if svc is None:
        raise RuntimeError("OperadorasQueryService não configurado em app.state.operadoras_service")
    return svc


def _get_session_factory(request: Request):
    factory = getattr(request.app.state, "session_factory", None)
    if factory is None:
        raise RuntimeError("Session factory não configurada em app.state.session_factory")
    return factory


@router.get("/operadoras", response_model=OperadorasListResponse)
def list_operadoras(
    request: Request,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    q: str | None = Query(None, description="Busca por razão social (ILIKE) ou CNPJ (digits)."),
):
    svc = _get_service(request)
    session_factory = _get_session_factory(request)

    with db_session(session_factory) as session:
        result = svc.list_operadoras(session=session, page=page, limit=limit, search=q)

    return OperadorasListResponse(
        data=[OperadoraItem(**item) for item in result.data],
        total=result.total,
        page=result.page,
        limit=result.limit,
    )


@router.get("/operadoras/{cnpj}", response_model=OperadoraDetailResponse)
def get_operadora(
    request: Request,
    cnpj: str,
):
    svc = _get_service(request)
    session_factory = _get_session_factory(request)

    with db_session(session_factory) as session:
        op = svc.get_operadora(session=session, cnpj=cnpj)

    if not op:
        raise HTTPException(status_code=404, detail="Operadora não encontrada.")

    return OperadoraDetailResponse(**op)


@router.get("/operadoras/{cnpj}/despesas", response_model=OperadoraDespesasResponse)
def get_operadora_despesas(
    request: Request,
    cnpj: str,
):
    svc = _get_service(request)
    session_factory = _get_session_factory(request)

    with db_session(session_factory) as session:
        op = svc.get_operadora(session=session, cnpj=cnpj)
        if not op:
            raise HTTPException(status_code=404, detail="Operadora não encontrada.")

        despesas = svc.get_despesas_operadora(session=session, cnpj=cnpj)

    return OperadoraDespesasResponse(
        cnpj=op["cnpj"],
        razao_social=op.get("razao_social"),
        uf=op.get("uf"),
        despesas=[
            DespesaHistoricoItem(
                ano=int(item["ano"]),
                trimestre=int(item["trimestre"]),
                valor_despesas=float(item["valor_despesas"]),
                razao_social_snapshot=item.get("razao_social_snapshot"),
            )
            for item in despesas
        ],
    )


@router.get("/estatisticas", response_model=EstatisticasResponse)
def get_estatisticas(request: Request):
    svc = _get_service(request)
    session_factory = _get_session_factory(request)

    with db_session(session_factory) as session:
        stats = svc.get_estatisticas(session=session)

    return EstatisticasResponse(
        total_despesas=float(stats["total_despesas"]),
        media_despesas=float(stats["media_despesas"]),
        top_5_operadoras=[TopOperadoraItem(**item) for item in stats["top_5_operadoras"]],
    )
