-- DDL base (use SET search_path TO "SllupMarket", public; antes de rodar)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS dim_cliente (
  cod_cliente    TEXT PRIMARY KEY,
  razao_social   TEXT
);

CREATE TABLE IF NOT EXISTS dim_produto (
  sku            TEXT PRIMARY KEY,
  produto        TEXT,
  familia        TEXT,
  sub_familia    TEXT,
  marca          TEXT,
  cor            TEXT
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