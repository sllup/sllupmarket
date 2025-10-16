# B2B Engajamento – Full Stack v3 (Render Free) • Importar por URL

## Componentes
- **engajamento-api** (FastAPI): `/ingest/url` baixa CSV/CSV.GZ em streaming, filtra e faz COPY em `staging.raw_vendas_achatado`. Endpoints de leitura (visão cliente, churn).
- **engajamento-dashboard** (Streamlit): abas de KPIs, RFM, Vendas e **Importar por URL** (sem upload local).
- **dbt_project**: `stg_vendas`, `fato_venda`, `mart_rfm` no schema `"SllupMarket"`.
- **dbt-runner** (FastAPI): endpoint manual para `dbt build`.

## Variáveis
- API/Dashboard: `DATABASE_URL`
- Dashboard: `API_BASE_URL` (ex.: `https://engajamento-api-<hash>.onrender.com`)
- dbt-runner: `DBT_HOST, DBT_PORT=5432, DBT_DBNAME, DBT_USER, DBT_PASSWORD, DBT_SSLMODE=require`, `DBT_RUN_TOKEN` (opcional)

## Setup
1. Suba o repositório no GitHub. No Render → **New → Web Service (From Git)** (o `render.yaml` cria tudo).
2. No banco, rode:
   ```sql
   SET search_path TO "SllupMarket", public;
   \i sql/ddl.sql
   ```
3. Dashboard → **Importar por URL**: cole URL pública `.csv` ou `.csv.gz` (S3 presigned, Dropbox `?dl=1`, GDrive direto), escolha **Full** ou **Append**.
4. (Opcional) Rode `dbt build` via serviço `dbt-runner` (curl).

## Notas
- Python 3.12.6 e wheels binárias onde necessário.
- O dashboard não lê arquivo nenhum → sem OOM em 512MB.
- A API faz download/transform em `/tmp` e `COPY` para máxima performance.