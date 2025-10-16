# B2B Engajamento – Full Stack (Render Free) • SllupMarket + Upload Streaming

## Serviços (render.yaml)
- **engajamento-api** (FastAPI) — `SET search_path TO "SllupMarket", public`
- **engajamento-dashboard** (Streamlit) — mesma conexão + **Upload CSV streaming** (.csv ou .csv.gz)
- **dbt-runner** (FastAPI) — endpoint manual para `dbt build`

## Variáveis
- API/Dashboard: `DATABASE_URL`
- Dashboard (opcionais): `DBT_RUNNER_URL`, `DBT_RUNNER_TOKEN`
- dbt-runner: `DBT_HOST, DBT_PORT=5432, DBT_DBNAME, DBT_USER, DBT_PASSWORD, DBT_SSLMODE=require`, `DBT_RUN_TOKEN` (opcional)

## Passo-a-passo
1. **Suba o repo** com este conteúdo. No Render, crie via **New → Web Service (From Git)**; ele lerá o `render.yaml`.
2. **Banco**: rode `sql/ddl.sql` com `SET search_path TO "SllupMarket", public;` antes, para criar tabelas.
3. **Upload**: na aba **Upload CSV** do dashboard, envie seu arquivo (UTF-8, cabeçalho padrão). Suporta `.csv.gz`.
4. **Build**: marque a opção de disparar o dbt ou chame manualmente:
   ```bash
   curl -X POST https://<seu-dbt-runner>/dbt/build -H "X-Token: <token_se_configurado>"
   ```
5. **Validar**:
   ```sql
   SET search_path TO "SllupMarket", public;
   SELECT count(*) FROM staging.raw_vendas_achatado;
   SELECT * FROM mart_rfm LIMIT 10;
   ```

## Observações
- Deploys fixam Python 3.12.6 e usam wheels binárias de numpy/pandas para evitar compilação no Free plan.
- O upload **streaming** evita estourar 512 MB de RAM do Render Free.