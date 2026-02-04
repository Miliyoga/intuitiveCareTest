from dataclasses import dataclass
from pathlib import Path
import pandas as pd

@dataclass
class AggregationResult:
    aggregated_csv: Path
    rows: int

class AnsAggregationService:
    def __init__(self, enriched_csv_path: Path, out_aggregated_csv_path: Path) -> None:
        self.enriched_csv_path = enriched_csv_path
        self.out_aggregated_csv_path = out_aggregated_csv_path

    def run(self) -> AggregationResult:
        df = pd.read_csv(self.enriched_csv_path, dtype=str, low_memory=False)

        for c in ["RazaoSocial", "UF", "Ano", "Trimestre", "ValorDespesas"]:
            if c not in df.columns:
                raise RuntimeError(f"Agregação: coluna faltando {c}. Encontrado: {list(df.columns)}")

        df["RazaoSocial"] = df["RazaoSocial"].astype(str).fillna("").str.strip()
        df["UF"] = df["UF"].astype(str).fillna("").str.strip()

        v = df["ValorDespesas"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        df["_valor"] = pd.to_numeric(v, errors="coerce").fillna(0.0)

        g = df.groupby(["RazaoSocial", "UF"], as_index=False)["_valor"].agg(
            TotalDespesas="sum",
            MediaTrimestral="mean",
            DesvioPadraoTrimestral="std",
            QtdeTrimestres="count",
        )

        g["DesvioPadraoTrimestral"] = g["DesvioPadraoTrimestral"].fillna(0.0)
        g = g.sort_values("TotalDespesas", ascending=False)

        # formata com 2 casas
        for col in ["TotalDespesas", "MediaTrimestral", "DesvioPadraoTrimestral"]:
            g[col] = g[col].astype(float).map(lambda x: f"{x:.2f}")

        self.out_aggregated_csv_path.parent.mkdir(parents=True, exist_ok=True)
        g.to_csv(self.out_aggregated_csv_path, index=False)

        return AggregationResult(aggregated_csv=self.out_aggregated_csv_path, rows=len(g))
