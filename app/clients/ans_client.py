from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

YEAR_DIR_RE = re.compile(r'href="(\d{4})/"', re.IGNORECASE)
HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)

# Padrão mais comum: 1T2025.zip, 2T2025.zip, etc.
ZIP_QUARTER_RE = re.compile(r"^(?P<q>[1-4])T(?P<y>\d{4})\.zip$", re.IGNORECASE)

# Variações de diretório dentro do ano:
# - /YYYY/01/  (01..04)
# - /YYYY/1T2025/
DIR_QQ_RE = re.compile(r"^0[1-4]$", re.IGNORECASE)
DIR_TRIMESTRE_RE = re.compile(r"^(?P<q>[1-4])T(?P<y>\d{4})/?$", re.IGNORECASE)


@dataclass(frozen=True, order=True)
class QuarterKey:
    year: int
    quarter: int  # 1..4


@dataclass(frozen=True)
class Artifact:
    key: QuarterKey
    url: str
    filename: str


class AnsClient:
    """
    Client responsável por:
    - listar diretórios/arquivos na fonte da ANS
    - descobrir quais são os últimos N trimestres disponíveis
    - baixar os arquivos selecionados
    """

    def __init__(self, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/") + "/"

    def _fetch_text(self, client: httpx.Client, url: str) -> str:
        r = client.get(url, follow_redirects=True, timeout=60.0)
        r.raise_for_status()
        return r.text

    def _list_hrefs(self, html: str) -> list[str]:
        return [m.group(1) for m in HREF_RE.finditer(html)]

    def _discover_years(self, client: httpx.Client) -> list[int]:
        html = self._fetch_text(client, self.base_url)
        years = sorted({int(m.group(1)) for m in YEAR_DIR_RE.finditer(html)}, reverse=True)
        return years

    def _collect_zip_artifacts_from_year(self, client: httpx.Client, year: int) -> list[Artifact]:
        """Coleta .zip diretamente dentro de /YYYY/."""
        url = f"{self.base_url}{year}/"
        html = self._fetch_text(client, url)
        hrefs = self._list_hrefs(html)

        artifacts: list[Artifact] = []
        for href in hrefs:
            name = href.strip("/")
            m = ZIP_QUARTER_RE.match(name)
            if not m:
                continue

            q = int(m.group("q"))
            y = int(m.group("y"))
            artifacts.append(Artifact(key=QuarterKey(year=y, quarter=q), url=f"{url}{name}", filename=name))

        return artifacts

    def _collect_zip_artifacts_from_quarter_dirs(self, client: httpx.Client, year: int) -> list[Artifact]:
        """Coleta .zip dentro de subpastas do ano (01..04 ou 1TYYYY)."""
        year_url = f"{self.base_url}{year}/"
        html = self._fetch_text(client, year_url)
        hrefs = self._list_hrefs(html)

        quarter_dirs: list[tuple[QuarterKey, str]] = []

        for href in hrefs:
            d = href.strip("/")

            # /YYYY/01/ .. /YYYY/04/
            if DIR_QQ_RE.fullmatch(d):
                q = int(d)  # 1..4
                quarter_dirs.append((QuarterKey(year=year, quarter=q), f"{year_url}{d}/"))
                continue

            # /YYYY/1TYYYY/
            m = DIR_TRIMESTRE_RE.match(d)
            if m and int(m.group("y")) == year:
                q = int(m.group("q"))
                quarter_dirs.append((QuarterKey(year=year, quarter=q), f"{year_url}{d}/"))
                continue

        artifacts: list[Artifact] = []
        for key, qdir_url in quarter_dirs:
            qhtml = self._fetch_text(client, qdir_url)
            qhrefs = self._list_hrefs(qhtml)
            for href in qhrefs:
                name = href.strip("/")
                if name.lower().endswith(".zip"):
                    artifacts.append(Artifact(key=key, url=f"{qdir_url}{name}", filename=name))

        return artifacts

    def discover_latest_quarter_artifacts(self, last_n_quarters: int = 3) -> list[Artifact]:
        """
        Descobre os artefatos (zips) dos últimos N trimestres.
        Se um trimestre tiver múltiplos arquivos, retorna todos.
        """
        with httpx.Client() as client:
            years = self._discover_years(client)

            all_artifacts: list[Artifact] = []
            for year in years:
                all_artifacts.extend(self._collect_zip_artifacts_from_year(client, year))
                all_artifacts.extend(self._collect_zip_artifacts_from_quarter_dirs(client, year))

            by_quarter: dict[QuarterKey, list[Artifact]] = {}
            for a in all_artifacts:
                by_quarter.setdefault(a.key, []).append(a)

            quarter_keys = sorted(by_quarter.keys(), reverse=True)[:last_n_quarters]

            latest: list[Artifact] = []
            for k in quarter_keys:
                latest.extend(sorted(by_quarter[k], key=lambda x: x.filename.lower()))

            return latest

    def download_artifacts(self, artifacts: Iterable[Artifact], out_dir: Path) -> list[Path]:
        out_dir.mkdir(parents=True, exist_ok=True)
        saved: list[Path] = []

        with httpx.Client() as client:
            for a in artifacts:
                target_dir = out_dir / str(a.key.year) / f"{a.key.quarter}T"
                target_dir.mkdir(parents=True, exist_ok=True)
                target_file = target_dir / a.filename

                # evita re-download
                if target_file.exists() and target_file.stat().st_size > 0:
                    saved.append(target_file)
                    continue

                with client.stream("GET", a.url, follow_redirects=True, timeout=120.0) as r:
                    r.raise_for_status()
                    with open(target_file, "wb") as f:
                        for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)

                saved.append(target_file)

        return saved
