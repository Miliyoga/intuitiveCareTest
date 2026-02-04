from __future__ import annotations

import logging
from pathlib import Path

from app.clients.ans_client import AnsClient

logger = logging.getLogger("ans_sync")


class AnsSyncService:
    """
    Orquestra a sincronização:
    - descobre últimos N trimestres
    - baixa/atualiza os arquivos localmente
    - não derruba a aplicação se falhar (resiliência)
    """

    def __init__(self, client: AnsClient, out_dir: Path, last_n_quarters: int = 3) -> None:
        self.client = client
        self.out_dir = out_dir
        self.last_n_quarters = last_n_quarters

    def sync(self) -> list[Path]:
        logger.info("ANS sync started (last_n_quarters=%s)", self.last_n_quarters)

        artifacts = self.client.discover_latest_quarter_artifacts(last_n_quarters=self.last_n_quarters)
        if not artifacts:
            logger.warning("ANS sync finished: no artifacts found")
            return []

        for a in artifacts:
            logger.info("Selected: %s/%sT -> %s", a.key.year, a.key.quarter, a.filename)

        saved = self.client.download_artifacts(artifacts, out_dir=self.out_dir)

        logger.info("ANS sync finished: %d file(s)", len(saved))
        return saved
