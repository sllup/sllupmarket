-- DDL v1 - Camada Canônica (Postgres)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Mantemos tabelas no schema padrão; o search_path tratará de resolver para SllupMarket
CREATE TABLE IF NOT EXISTS dim_cliente (
  cod_cliente    TEXT PRIMARY KEY,
  razao_social   TEXT,
  cnpj           TEXT,
  segmento       TEXT,
  cidade         TEXT,
  uf             TEXT,
  status_crm     TEXT,
  criado_em      DATE
);

CREATE TABLE IF NOT EXISTS dim_produto (
  sku            TEXT PRIMARY KEY,
  produto        TEXT,
  familia        TEXT,
  sub_familia    TEXT,
  marca          TEXT,
  cor            TEXT
);

CREATE TABLE IF NOT EXISTS dim_tempo (
  data           DATE PRIMARY KEY,
  ano            INT,
  mes            INT,
  semana         INT,
  trimestre      INT,
  yyyymm         INT
);

CREATE TABLE IF NOT EXISTS fato_venda (
  id_fato_venda        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  data                 DATE NOT NULL,
  cod_cliente          TEXT NOT NULL,
  sku                  TEXT NOT NULL,
  tam                  TEXT,
  qtde                 NUMERIC(18,4) NOT NULL,
  preco_unit           NUMERIC(18,4),
  total_venda          NUMERIC(18,4),
  total_custo          NUMERIC(18,4),
  margem               NUMERIC(18,4),
  documento_fiscal     TEXT,
  carga_dt             TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_venda_nf_item ON fato_venda (
  COALESCE(documento_fiscal,''), COALESCE(sku,''), COALESCE(tam,'')
);

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.raw_vendas_achatado (
  data            TEXT,
  produto         TEXT,
  sku             TEXT,
  familia         TEXT,
  sub_familia     TEXT,
  cor             TEXT,
  tam             TEXT,
  marca           TEXT,
  cod_cliente     TEXT,
  razao_social    TEXT,
  qtde            TEXT,
  preco_unit      TEXT,
  total_venda     TEXT,
  total_custo     TEXT,
  margem          TEXT,
  documento_fiscal TEXT,
  carga_dt        TIMESTAMPTZ DEFAULT now()
);