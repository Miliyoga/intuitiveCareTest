from pathlib import Path
import logging

from app.clients.ans_client import AnsClient
from app.clients.ans_registry_client import AnsRegistryClient
from app.services.ans_sync_service import AnsSyncService
from app.services.ans_registry_sync_service import AnsRegistrySyncService
from app.services.ans_consolidation_service import AnsConsolidationService
from app.services.ans_validation_service import AnsValidationService
from app.services.ans_enrichment_service import AnsEnrichmentService
from app.services.ans_aggregation_service import AnsAggregationService
from app.core.zip_utils import zip_single_file

logger = logging.getLogger("full_pipeline")

def run_full_pipeline() -> None:
    logger.info("Full pipeline started")

    base = Path(".")
    raw_demo_dir = base / "data/raw/ans/demonstracoes_contabeis"
    raw_registry_dir = base / "data/raw/ans/operadoras_ativas"
    raw_registry_csv = raw_registry_dir / "Relatorio_cadop.csv"

    processed_dir = base / "data/processed"
    aggregated_dir = base / "data/aggregated"
    artifacts_dir = base / "data/artifacts"

    consolidated_csv = processed_dir / "consolidated_despesas.csv"
    consolidated_zip = artifacts_dir / "consolidado_despesas.zip"

    validated_csv = processed_dir / "consolidated_validated.csv"
    invalid_rows_csv = processed_dir / "consolidated_invalid_rows.csv"

    enriched_csv = processed_dir / "enriched.csv"
    unmatched_csv = processed_dir / "unmatched_cnpj.csv"
    conflicts_csv = processed_dir / "registry_conflicts.csv"

    aggregated_csv = aggregated_dir / "despesas_agregadas.csv"
    final_zip = artifacts_dir / "Teste_Mili.zip"

    # 1) Baixar os 3 últimos trimestres (ZIPs das demonstrações)
    AnsSyncService(
        client=AnsClient(),
        out_dir=raw_demo_dir,
        last_n_quarters=3,
    ).sync()

    # 2) Baixar cadastro CADOP (operadoras ativas)
    AnsRegistrySyncService(
        client=AnsRegistryClient(),
        out_dir=raw_registry_dir,
    ).sync()

    # 3) Consolidar e já gerar no formato exigido (CNPJ, RazaoSocial, Trimestre, Ano, ValorDespesas)
    AnsConsolidationService(
        raw_dir=raw_demo_dir,
        registry_csv_path=raw_registry_csv,
        out_consolidated_csv=consolidated_csv,
    ).run()

    # 4) Compactar consolidado (1.3)
    zip_single_file(consolidated_csv, consolidated_zip, arcname="consolidated_despesas.csv")

    # 5) Validar (2.1)
    AnsValidationService(
        consolidated_csv_path=consolidated_csv,
        out_validated_csv_path=validated_csv,
        out_invalid_rows_csv_path=invalid_rows_csv,
    ).run()

    # 6) Enriquecer via join por CNPJ (2.2)
    AnsEnrichmentService(
        consolidated_csv_path=validated_csv,
        registry_csv_path=raw_registry_csv,
        out_enriched_csv_path=enriched_csv,
        out_unmatched_csv_path=unmatched_csv,
        out_registry_conflicts_csv_path=conflicts_csv,
    ).run()

    # 7) Agregar e ordenar (2.3)
    AnsAggregationService(
        enriched_csv_path=enriched_csv,
        out_aggregated_csv_path=aggregated_csv,
    ).run()

    # 8) ZIP final do teste
    zip_single_file(aggregated_csv, final_zip, arcname="despesas_agregadas.csv")

    logger.info("Full pipeline finished successfully")
