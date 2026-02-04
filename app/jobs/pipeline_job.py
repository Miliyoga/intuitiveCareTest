from __future__ import annotations

import logging
from pathlib import Path

from app.clients.ans_registry_client import AnsRegistryClient
from app.services.ans_enrichment_service import AnsEnrichmentService
from app.services.ans_aggregation_service import AnsAggregationService

logger = logging.getLogger("pipeline")


def run_data_pipeline() -> None:
    """
    Pipeline (2.2 + 2.3):
    - baixa cadastro de operadoras ativas
    - enriquece o consolidado por CNPJ
    - gera agregações por RazaoSocial/UF
    """

    # Entradas
    consolidated_csv = Path("data/processed/consolidated.csv")

    # Raw
    registry_csv = Path("data/raw/ans/operadoras_ativas/Relatorio_cadop.csv")

    # Processed
    enriched_csv = Path("data/processed/enriched.csv")
    unmatched_csv = Path("data/processed/unmatched_cnpj.csv")
    conflicts_csv = Path("data/processed/registry_conflicts.csv")

    # Aggregated
    aggregated_csv = Path("data/aggregated/despesas_por_operadora_uf.csv")

    registry_client = AnsRegistryClient()

    enrichment = AnsEnrichmentService(
        registry_client=registry_client,
        consolidated_csv_path=consolidated_csv,
        registry_csv_path=registry_csv,
        out_enriched_csv_path=enriched_csv,
        out_unmatched_csv_path=unmatched_csv,
        out_registry_conflicts_csv_path=conflicts_csv,
    )

    agg = AnsAggregationService(
        enriched_csv_path=enriched_csv,
        out_aggregated_csv_path=aggregated_csv,
    )

    logger.info("Pipeline started")
    enrichment.run()
    agg.run()
    logger.info("Pipeline finished")
