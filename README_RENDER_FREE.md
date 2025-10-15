# Deploy no Render (Free) – Build do dbt manual via Web Service

## Serviços criados pelo `render.yaml`
- **engajamento-api**: FastAPI (dados/insights)
- **engajamento-dashboard**: Streamlit
- **dbt-runner**: FastAPI para disparar `dbt build` manualmente

## Variáveis
- API/Dashboard: `DATABASE_URL`
- dbt-runner: `DBT_HOST`, `DBT_USER`, `DBT_PASSWORD`, `DBT_PORT=5432`, `DBT_DBNAME`
- (Opcional) `DBT_RUN_TOKEN` para proteger o endpoint

## Como rodar o dbt manualmente
1. Depois do deploy, acesse o serviço **dbt-runner**:
   - **POST** `https://<HOST-DBT-RUNNER>/dbt/build`
     - Header opcional: `X-Token: <seu token>` (se `DBT_RUN_TOKEN` estiver setado)
   - Resposta: `run_id`, `returncode`, `tail` (últimas linhas do log)
2. Para ver logs completos:
   - **GET** `https://<HOST-DBT-RUNNER>/dbt/logs/{run_id}`

> Importante: o Render Free pode encerrar requisições muito longas. Por isso, retornamos o *tail* e gravamos o log em `/tmp/dbt_logs`. Se a chamada cair por timeout, você ainda consegue consultar depois com o `run_id`.

## Fluxo sugerido
- Suba CSV para `staging.raw_vendas_achatado`.
- Dispare `POST /dbt/build` no **dbt-runner**.
- Consulte o dashboard **engajamento-dashboard** e as APIs da **engajamento-api**.