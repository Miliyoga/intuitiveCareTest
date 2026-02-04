# app/services/operadoras_query_service.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def _digits_only(value: str) -> str:
    return re.sub(r"\D+", "", value or "")


@dataclass
class PaginatedResult:
    data: list[dict[str, Any]]
    total: int
    page: int
    limit: int


class OperadorasQueryService:
    """
    Base atual (schema único 'ans'):
      - ans.operadoras_cadop
      - ans.consolidated_validated
      - ans.despesas_agregadas

    Mantém compatibilidade com o startup do app:
      OperadorasQueryService(stats_cache_ttl_seconds=...)
    """

    def __init__(self, stats_cache_ttl_seconds: int = 300) -> None:
        self.stats_cache_ttl_seconds = int(stats_cache_ttl_seconds)
        self._stats_cache: dict[str, Any] | None = None
        self._stats_cache_ts: float = 0.0

    def list_operadoras(self, session: Session, page: int, limit: int, search: str | None) -> PaginatedResult:
        offset = (page - 1) * limit
        search = (search or "").strip()
        digits = _digits_only(search)

        where_sql = "WHERE 1=1"
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if digits:
            where_sql += " AND btrim(cnpj) LIKE :cnpj_like"
            params["cnpj_like"] = f"%{digits}%"
        elif search:
            where_sql += " AND razao_social ILIKE :razao_like"
            params["razao_like"] = f"%{search}%"

        total_q = text(f"""
            SELECT COUNT(*)::int AS total
            FROM ans.operadoras_cadop
            {where_sql}
        """)

        data_q = text(f"""
            SELECT
              btrim(cnpj) AS cnpj,
              razao_social,
              registro_operadora AS registro_ans,
              modalidade,
              uf
            FROM ans.operadoras_cadop
            {where_sql}
            ORDER BY razao_social ASC NULLS LAST
            LIMIT :limit OFFSET :offset
        """)

        total = session.execute(total_q, params).mappings().one()["total"]
        rows = session.execute(data_q, params).mappings().all()

        return PaginatedResult(
            data=[dict(r) for r in rows],
            total=total,
            page=page,
            limit=limit,
        )

    def get_operadora(self, session: Session, cnpj: str) -> dict[str, Any] | None:
        cnpj_digits = _digits_only(cnpj)

        q = text("""
            SELECT
              btrim(cnpj) AS cnpj,
              razao_social,
              registro_operadora AS registro_ans,
              modalidade,
              uf
            FROM ans.operadoras_cadop
            WHERE btrim(cnpj) = :cnpj
            LIMIT 1
        """)

        row = session.execute(q, {"cnpj": cnpj_digits}).mappings().first()
        return dict(row) if row else None

    def get_despesas_operadora(self, session: Session, cnpj: str) -> list[dict[str, Any]]:
        cnpj_digits = _digits_only(cnpj)

        q = text("""
            SELECT
              ano,
              trimestre,
              valor_despesas,
              razao_social AS razao_social_snapshot
            FROM ans.consolidated_validated
            WHERE btrim(cnpj) = :cnpj
            ORDER BY ano ASC, trimestre ASC
        """)

        rows = session.execute(q, {"cnpj": cnpj_digits}).mappings().all()
        return [dict(r) for r in rows]

    def get_estatisticas(self, session: Session) -> dict[str, Any]:
        # Cache simples em memória (por worker)
        now = time.time()
        if self._stats_cache and (now - self._stats_cache_ts) < self.stats_cache_ttl_seconds:
            return self._stats_cache

        agg_q = text("""
            SELECT
              COALESCE(SUM(valor_despesas), 0)::numeric AS total_despesas,
              COALESCE(AVG(valor_despesas), 0)::numeric AS media_despesas
            FROM ans.consolidated_validated
        """)

        top_q = text("""
            WITH top AS (
              SELECT
                btrim(v.cnpj) AS cnpj,
                MAX(v.razao_social) AS razao_social,
                SUM(v.valor_despesas)::numeric AS total_despesas
              FROM ans.consolidated_validated v
              GROUP BY btrim(v.cnpj)
              ORDER BY SUM(v.valor_despesas) DESC
              LIMIT 5
            )
            SELECT
              top.razao_social,
              c.uf,
              top.total_despesas
            FROM top
            LEFT JOIN ans.operadoras_cadop c
              ON btrim(c.cnpj) = top.cnpj
            ORDER BY top.total_despesas DESC
        """)

        agg = session.execute(agg_q).mappings().one()
        top = session.execute(top_q).mappings().all()

        result = {
            "total_despesas": agg["total_despesas"],
            "media_despesas": agg["media_despesas"],
            "top_5_operadoras": [dict(r) for r in top],
        }

        self._stats_cache = result
        self._stats_cache_ts = now
        return result
