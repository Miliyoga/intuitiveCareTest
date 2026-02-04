from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from app.core.cnpj import is_valid_cnpj, normalize_cnpj

@dataclass
class ValidationResult:
    validated_csv: Path
    invalid_rows_csv: Path
    rows_valid: int
    rows_invalid: int

class AnsValidationService:
    def __init__(
        self,
        consolidated_csv_path: Path,
        out_validated_csv_path: Path,
        out_invalid_rows_csv_path: Path,
    ) -> None:
        self.consolidated_csv_path = consolidated_csv_path
        self.out_validated_csv_path = out_validated_csv_path
        self.out_invalid_rows_csv_path = out_invalid_rows_csv_path

    def run(self) -> ValidationResult:
        df = pd.read_csv(self.consolidated_csv_path, dtype=str, low_memory=False)

        required = ["CNPJ", "RazaoSocial", "Ano", "Trimestre", "ValorDespesas"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"Validação: colunas faltando {missing}. Encontrado: {list(df.columns)}")

        df["CNPJ"] = df["CNPJ"].apply(normalize_cnpj)
        df["RazaoSocial"] = df["RazaoSocial"].astype(str).fillna("").str.strip()

        # ValorDespesas como float
        v = df["ValorDespesas"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        df["_valor"] = pd.to_numeric(v, errors="coerce")

        reasons: list[str] = []
        invalid_mask = pd.Series(False, index=df.index)

        cnpj_invalid = ~df["CNPJ"].apply(is_valid_cnpj)
        rs_empty = df["RazaoSocial"].eq("")
        val_invalid = df["_valor"].isna() | (df["_valor"] <= 0)

        invalid_mask = cnpj_invalid | rs_empty | val_invalid

        def build_reason(row_idx: int) -> str:
            parts = []
            if bool(cnpj_invalid.loc[row_idx]): parts.append("CNPJ_INVALIDO")
            if bool(rs_empty.loc[row_idx]): parts.append("RAZAO_SOCIAL_VAZIA")
            if bool(val_invalid.loc[row_idx]): parts.append("VALOR_INVALIDO_OU_NAO_POSITIVO")
            return "|".join(parts) if parts else "OK"

        df["_motivo_invalido"] = [build_reason(i) for i in df.index]

        invalid_df = df[invalid_mask].copy()
        valid_df = df[~invalid_mask].copy()

        # limpar coluna auxiliar e padronizar ValorDespesas com ponto
        valid_df["ValorDespesas"] = valid_df["_valor"].astype(float).map(lambda x: f"{x:.2f}")
        valid_df = valid_df[required]

        invalid_df["ValorDespesas"] = invalid_df["_valor"]
        cols_invalid = required + ["_motivo_invalido"]
        for c in cols_invalid:
            if c not in invalid_df.columns:
                invalid_df[c] = ""
        invalid_df = invalid_df[cols_invalid]

        self.out_validated_csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.out_invalid_rows_csv_path.parent.mkdir(parents=True, exist_ok=True)

        valid_df.to_csv(self.out_validated_csv_path, index=False)
        invalid_df.to_csv(self.out_invalid_rows_csv_path, index=False)

        return ValidationResult(
            validated_csv=self.out_validated_csv_path,
            invalid_rows_csv=self.out_invalid_rows_csv_path,
            rows_valid=len(valid_df),
            rows_invalid=len(invalid_df),
        )
