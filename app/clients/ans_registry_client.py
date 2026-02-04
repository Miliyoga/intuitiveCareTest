from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urljoin

import httpx

logger = logging.getLogger("ans_registry")

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
DEFAULT_FILENAME = "Relatorio_cadop.csv"


class AnsRegistryClient:
    def __init__(self, timeout_seconds: int = 60) -> None:
        self.timeout_seconds = timeout_seconds

    def download_registry_csv(self, out_path: Path) -> Path:
        out_path.parent.mkdir(parents=True, exist_ok=True)

        url = urljoin(BASE_URL, DEFAULT_FILENAME)
        logger.info("Downloading registry CSV: %s", url)

        with httpx.Client(timeout=self.timeout_seconds, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)

        logger.info("Registry CSV saved: %s (bytes=%d)", out_path, out_path.stat().st_size)
        return out_path
