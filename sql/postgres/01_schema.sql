BEGIN;

CREATE SCHEMA IF NOT EXISTS ans;

-- =========================
-- Operadoras (CADOP)
-- =========================
DROP TABLE IF EXISTS ans.operadoras_cadop;

CREATE TABLE ans.operadoras_cadop (
  registro_operadora          TEXT,
  cnpj                        CHAR(14),
  razao_social                TEXT,
  nome_fantasia               TEXT,
  modalidade                  TEXT,
  logradouro                  TEXT,
  numero                      TEXT,
  complemento                 TEXT,
  bairro                      TEXT,
  cidade                      TEXT,
  uf                          CHAR(2),
  cep                         TEXT,
  ddd                         TEXT,
  telefone                    TEXT,
  fax                         TEXT,
  endereco_eletronico         TEXT,
  representante               TEXT,
  cargo_representante         TEXT,
  regiao_de_comercializacao   INTEGER,
  data_registro_ans           DATE
);

CREATE INDEX idx_operadoras_cadop_cnpj ON ans.operadoras_cadop (cnpj);
CREATE INDEX idx_operadoras_cadop_uf ON ans.operadoras_cadop (uf);

-- =========================
-- Despesas por trimestre
-- =========================
DROP TABLE IF EXISTS ans.consolidated_validated;

CREATE TABLE ans.consolidated_validated (
  cnpj             CHAR(14)      NOT NULL,
  razao_social     TEXT          NOT NULL,
  ano              SMALLINT      NOT NULL,
  trimestre        SMALLINT      NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
  valor_despesas   NUMERIC(20,2) NOT NULL
);

CREATE INDEX idx_consolidated_validated_cnpj ON ans.consolidated_validated (cnpj);
CREATE INDEX idx_consolidated_validated_periodo ON ans.consolidated_validated (ano, trimestre);

-- =========================
-- Despesas agregadas
-- =========================
DROP TABLE IF EXISTS ans.despesas_agregadas;

CREATE TABLE ans.despesas_agregadas (
  razao_social              TEXT          NOT NULL,
  uf                        CHAR(2),
  total_despesas            NUMERIC(20,2) NOT NULL,
  media_trimestral          NUMERIC(20,2) NOT NULL,
  desvio_padrao_trimestral  NUMERIC(20,2) NOT NULL,
  qtde_trimestres           INTEGER       NOT NULL
);

CREATE INDEX idx_despesas_agregadas_razao ON ans.despesas_agregadas (razao_social);
CREATE INDEX idx_despesas_agregadas_uf ON ans.despesas_agregadas (uf);

COMMIT;
