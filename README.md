# B2B Engajamento – MVP v1

Conteúdo:
- **/sql/ddl.sql** – Tabelas canônicas + staging.
- **/dbt_project** – Esqueleto dbt (staging/core/marts com RFM).
- **/apps/api** – API FastAPI (`/health`, `/clientes/{cod}/visao`, `/insights/churn`).
- **/apps/dashboard** – Streamlit (visão geral, RFM, vendas).

## Passo a passo

### 1) Banco
psql "$DATABASE_URL" -f sql/ddl.sql

### 2) Carga da planilha achatada (staging)
CSV UTF-8 com cabeçalhos:
data,produto,sku,familia,sub_familia,cor,tam,marca,cod_cliente,razao_social,qtde,preco_unit,total_venda,total_custo,margem,documento_fiscal

Importe:
COPY staging.raw_vendas_achatado FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

### 3) dbt
export DBT_HOST=...
export DBT_USER=...
export DBT_PASSWORD=...
export DBT_PORT=5432
export DBT_DBNAME=...
cd dbt_project && dbt run

### 4) API (Render)
Vars: DATABASE_URL
cd apps/api && uvicorn main:app --host 0.0.0.0 --port 10000

### 5) Dashboard (Render)
Vars: DATABASE_URL
cd apps/dashboard && streamlit run streamlit_app.py --server.port 10000 --server.address 0.0.0.0

## Próximos
- Conectores (n8n) alimentando staging.
- Marts: ABC/XYZ, clusters de sortimento, elasticidade.
- Alertas de engajamento e risco.