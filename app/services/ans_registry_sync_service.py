from pathlib import Path
import logging

from app.clients.ans_registry_client import AnsRegistryClient

logger = logging.getLogger("ans_registry_sync")


class AnsRegistrySyncService:
    def __init__(self, client: AnsRegistryClient, out_dir: Path) -> None:
        self.client = client
        self.out_dir = out_dir

    def sync(self) -> Path:
        """
        Baixa o CSV de operadoras ativas (CADOP) e salva em:
        data/raw/ans/operadoras_ativas/Relatorio_cadop.csv
        """
        logger.info("Registry sync started")

        self.out_dir.mkdir(parents=True, exist_ok=True)

        out_path = self.out_dir / "Relatorio_cadop.csv"

        self.client.download_registry_csv(out_path)

        logger.info("Registry sync finished: %s", out_path)
        return out_path
