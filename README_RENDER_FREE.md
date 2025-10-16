# Deploy no Render (Free) – SllupMarket patch

## Serviços (via render.yaml)
- engajamento-api (FastAPI) – força `SET search_path TO "SllupMarket", public`
- engajamento-dashboard (Streamlit) – idem
- dbt-runner (FastAPI) – dispara `dbt build` manualmente

## Variáveis
- API/Dashboard: `DATABASE_URL`
- dbt-runner: `DBT_HOST, DBT_PORT, DBT_DBNAME, DBT_USER, DBT_PASSWORD, DBT_SSLMODE=require` (+ opcional `DBT_RUN_TOKEN`)
- Todas usam `PYTHON_VERSION=3.12.6`

## Build
- Dashboard instala binários de numpy/pandas para evitar compilação (Py 3.12).

## Dicas
- Se quiser embutir o schema via URL: `?options=-c%20search_path%3D%22SllupMarket%22%2Cpublic`
- dbt: `cd dbt_project && dbt run` (via dbt-runner: `POST /dbt/build`).