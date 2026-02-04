from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger("ans_enrichment")

@dataclass
class EnrichmentResult:
    enriched_csv: Path
    unmatched_csv: Path
    conflicts_csv: Path
    rows: int
    matched_rows: int

class AnsEnrichmentService:
    def __init__(
        self,
        consolidated_csv_path: Path,
        registry_csv_path: Path,
        out_enriched_csv_path: Path,
        out_unmatched_csv_path: Path,
        out_registry_conflicts_csv_path: Path,
    ) -> None:
        self.consolidated_csv_path = consolidated_csv_path
        self.registry_csv_path = registry_csv_path
        self.out_enriched_csv_path = out_enriched_csv_path
        self.out_unmatched_csv_path = out_unmatched_csv_path
        self.out_registry_conflicts_csv_path = out_registry_conflicts_csv_path

    def _load_consolidated(self) -> pd.DataFrame:
        df = pd.read_csv(self.consolidated_csv_path, dtype=str, low_memory=False)
        required = ["CNPJ", "RazaoSocial", "Ano", "Trimestre", "ValorDespesas"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"Consolidado: colunas faltando {missing}. Encontrado: {list(df.columns)}")
        df["CNPJ"] = df["CNPJ"].astype(str).str.replace(r"\D", "", regex=True).str.strip()
        return df

    def _load_registry(self) -> pd.DataFrame:
        reg = pd.read_csv(self.registry_csv_path, sep=";", encoding="utf-8", dtype=str, low_memory=False)
        reg = reg.rename(columns={"REGISTRO_OPERADORA": "RegistroANS", "Razao_Social": "RazaoSocial"})
        reg["CNPJ"] = reg["CNPJ"].astype(str).str.replace(r"\D", "", regex=True).str.strip()
        reg["RegistroANS"] = reg["RegistroANS"].astype(str).str.zfill(6)
        reg["Modalidade"] = reg.get("Modalidade", "").astype(str)
        reg["UF"] = reg.get("UF", "").astype(str)

        # reduz para colunas relevantes
        return reg[["CNPJ", "RegistroANS", "Modalidade", "UF"]].copy()

    def _detect_conflicts(self, reg: pd.DataFrame) -> pd.DataFrame:
        # conflito: mesmo CNPJ com mais de um RegistroANS/Modalidade/UF distintos
        g = reg.groupby("CNPJ").agg(
            registros=("RegistroANS", lambda s: "|".join(sorted(set(s.dropna().astype(str))))),
            modalidades=("Modalidade", lambda s: "|".join(sorted(set(s.dropna().astype(str))))),
            ufs=("UF", lambda s: "|".join(sorted(set(s.dropna().astype(str))))),
            n=("CNPJ", "size"),
        ).reset_index()

        conflict = g[
            (g["registros"].str.contains(r"\|", regex=True)) |
            (g["modalidades"].str.contains(r"\|", regex=True)) |
            (g["ufs"].str.contains(r"\|", regex=True))
        ].copy()

        conflict = conflict.rename(columns={
            "registros": "RegistroANS_distintos",
            "modalidades": "Modalidade_distintas",
            "ufs": "UFs_distintas",
            "n": "QtdeLinhasCadastro",
        })
        return conflict

    def run(self) -> EnrichmentResult:
        logger.info("Enrichment started")

        consolidated = self._load_consolidated()
        registry = self._load_registry()

        conflicts = self._detect_conflicts(registry)
        self.out_registry_conflicts_csv_path.parent.mkdir(parents=True, exist_ok=True)
        conflicts.to_csv(self.out_registry_conflicts_csv_path, index=False)

        # estratégia para conflitos:
        # - escolhe a primeira ocorrência por CNPJ (determinística pelo sort)
        registry_sorted = registry.sort_values(["CNPJ", "RegistroANS", "UF", "Modalidade"], na_position="last")
        registry_dedup = registry_sorted.drop_duplicates(subset=["CNPJ"], keep="first")

        enriched = consolidated.merge(registry_dedup, on="CNPJ", how="left")

        # unmatched = linhas do consolidado que não tiveram RegistroANS no cadastro
        unmatched = enriched[enriched["RegistroANS"].isna() | (enriched["RegistroANS"].astype(str).str.strip() == "")]
        self.out_unmatched_csv_path.parent.mkdir(parents=True, exist_ok=True)
        unmatched.to_csv(self.out_unmatched_csv_path, index=False)

        # salvar enriquecido
        self.out_enriched_csv_path.parent.mkdir(parents=True, exist_ok=True)
        enriched.to_csv(self.out_enriched_csv_path, index=False)

        matched_rows = len(enriched) - len(unmatched)

        logger.info(f"Enrichment finished: {self.out_enriched_csv_path} (rows={len(enriched)})")
        return EnrichmentResult(
            enriched_csv=self.out_enriched_csv_path,
            unmatched_csv=self.out_unmatched_csv_path,
            conflicts_csv=self.out_registry_conflicts_csv_path,
            rows=len(enriched),
            matched_rows=matched_rows,
        )
