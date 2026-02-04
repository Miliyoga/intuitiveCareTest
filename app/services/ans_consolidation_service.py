from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile
import pandas as pd
import logging

logger = logging.getLogger("ans_consolidation")

@dataclass
class ConsolidationResult:
    consolidated_csv: Path
    rows: int

class AnsConsolidationService:
    def __init__(
        self,
        raw_dir: Path,
        registry_csv_path: Path,
        out_consolidated_csv: Path,
    ) -> None:
        self.raw_dir = raw_dir
        self.registry_csv_path = registry_csv_path
        self.out_consolidated_csv = out_consolidated_csv

    def _read_registry(self) -> pd.DataFrame:
        # CADOP (Relatorio_cadop.csv) normalmente é ; e utf-8
        df = pd.read_csv(self.registry_csv_path, sep=";", encoding="utf-8", dtype=str, low_memory=False)
        # normaliza nomes
        df = df.rename(
            columns={
                "REGISTRO_OPERADORA": "RegistroANS",
                "Razao_Social": "RazaoSocial",
            }
        )
        for c in ["RegistroANS", "CNPJ", "RazaoSocial"]:
            if c not in df.columns:
                df[c] = ""
        df["RegistroANS"] = df["RegistroANS"].astype(str).str.strip()
        df["CNPJ"] = df["CNPJ"].astype(str).str.replace(r"\D", "", regex=True).str.strip()
        df["RazaoSocial"] = df["RazaoSocial"].astype(str).str.strip()
        return df[["RegistroANS", "CNPJ", "RazaoSocial"]].drop_duplicates()

    def _parse_quarter_from_path(self, zip_path: Path) -> tuple[int, str]:
        # .../2025/1T/1T2025.zip -> (2025, "1")
        parts = zip_path.parts
        year = None
        quarter = None
        for i, p in enumerate(parts):
            if p.isdigit() and len(p) == 4:
                year = int(p)
                if i + 1 < len(parts) and parts[i + 1].endswith("T"):
                    quarter = parts[i + 1].replace("T", "")
        if year is None or quarter is None:
            raise RuntimeError(f"Não consegui inferir ano/trimestre do path: {zip_path}")
        return year, quarter

    def _read_zip_csv(self, zip_path: Path) -> pd.DataFrame:
        with ZipFile(zip_path, "r") as zf:
            csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csvs:
                return pd.DataFrame()

            # pega o primeiro csv (no seu caso é tipo 1T2025.csv)
            name = csvs[0]
            with zf.open(name) as fh:
                df = pd.read_csv(fh, sep=";", encoding="utf-8", dtype=str, low_memory=False)
            return df

    def run(self) -> ConsolidationResult:
        logger.info("Consolidation started")

        registry = self._read_registry()

        zips = sorted(self.raw_dir.rglob("*.zip"))
        if not zips:
            raise RuntimeError(f"Nenhum ZIP encontrado em {self.raw_dir}")

        rows_out = []
        for zip_path in zips:
            year, quarter = self._parse_quarter_from_path(zip_path)
            logger.info(f"Reading ZIP: {zip_path} (Ano={year} Trimestre={quarter}T)")

            df = self._read_zip_csv(zip_path)
            if df.empty:
                continue

            # normalização esperada do arquivo atual
            expected_cols = {"REG_ANS", "CD_CONTA_CONTABIL", "VL_SALDO_FINAL"}
            if not expected_cols.issubset(set(df.columns)):
                # resiliente: se mudar, você pode mapear aqui
                logger.warning(f"ZIP {zip_path} sem colunas esperadas. Columns={list(df.columns)}")
                continue

            df["REG_ANS"] = df["REG_ANS"].astype(str).str.strip()
            df["CD_CONTA_CONTABIL"] = df["CD_CONTA_CONTABIL"].astype(str).str.strip()


            # filtro: SOMENTE conta 411
            eventos = df[df["CD_CONTA_CONTABIL"] == "411"].copy()


            # valor: aceita vírgula decimal
            v = eventos["VL_SALDO_FINAL"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
            eventos["_valor"] = pd.to_numeric(v, errors="coerce").fillna(0.0)

            grouped = (
                eventos.groupby("REG_ANS", as_index=False)["_valor"]
                .sum()
                .rename(columns={"REG_ANS": "RegistroANS", "_valor": "ValorDespesas"})
            )

            grouped["Ano"] = str(year)
            grouped["Trimestre"] = str(quarter)

            rows_out.append(grouped)

        if not rows_out:
            raise RuntimeError("Consolidação não gerou linhas. Verifique conteúdo e formato dos ZIPs.")

        consolidated = pd.concat(rows_out, ignore_index=True)

        # join para obter CNPJ e RazaoSocial (para cumprir o 1.3)
        consolidated["RegistroANS"] = consolidated["RegistroANS"].astype(str).str.zfill(6)
        registry["RegistroANS"] = registry["RegistroANS"].astype(str).str.zfill(6)

        consolidated = consolidated.merge(registry, on="RegistroANS", how="left")

        # normaliza e garante colunas finais exigidas pelo enunciado (1.3)
        consolidated["CNPJ"] = consolidated["CNPJ"].fillna("").astype(str)
        consolidated["RazaoSocial"] = consolidated["RazaoSocial"].fillna("").astype(str)
        consolidated["ValorDespesas"] = consolidated["ValorDespesas"].astype(float).map(lambda x: f"{x:.2f}")

        out_df = consolidated[["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]].copy()

        self.out_consolidated_csv.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(self.out_consolidated_csv, index=False)

        logger.info(f"Consolidation finished: {self.out_consolidated_csv} (rows={len(out_df)})")
        return ConsolidationResult(consolidated_csv=self.out_consolidated_csv, rows=len(out_df))
