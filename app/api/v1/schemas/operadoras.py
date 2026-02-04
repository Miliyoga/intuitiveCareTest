# app/api/v1/schemas/operadoras.py
from __future__ import annotations

from pydantic import BaseModel, Field


class OperadoraItem(BaseModel):
    cnpj: str = Field(..., description="CNPJ somente dígitos (14).")
    razao_social: str
    registro_ans: str | None = None
    modalidade: str | None = None
    uf: str | None = None


class OperadorasListResponse(BaseModel):
    data: list[OperadoraItem]
    total: int
    page: int
    limit: int


class OperadoraDetailResponse(OperadoraItem):
    pass


class DespesaHistoricoItem(BaseModel):
    ano: int
    trimestre: int
    valor_despesas: float
    razao_social_snapshot: str | None = None


class OperadoraDespesasResponse(BaseModel):
    cnpj: str
    razao_social: str | None = None
    uf: str | None = None
    despesas: list[DespesaHistoricoItem]


class TopOperadoraItem(BaseModel):
    razao_social: str
    uf: str | None = None
    total_despesas: float


class EstatisticasResponse(BaseModel):
    total_despesas: float
    media_despesas: float
    top_5_operadoras: list[TopOperadoraItem]
