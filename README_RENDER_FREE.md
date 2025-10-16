# B2B Engajamento – Full Stack (Render Free) • v2 • SllupMarket + Upload Streaming

## Serviços (render.yaml)
- **engajamento-api** (FastAPI) — `SET search_path TO "SllupMarket", public`
- **engajamento-dashboard** (Streamlit) — **Upload CSV streaming, disco** (.csv ou .csv.gz)
- **dbt-runner** (FastAPI) — endpoint manual para `dbt build`

## Variáveis
- API/Dashboard: `DATABASE_URL`
- Dashboard (opcionais): `DBT_RUNNER_URL`, `DBT_RUNNER_TOKEN`
- dbt-runner: `DBT_HOST, DBT_PORT=5432, DBT_DBNAME, DBT_USER, DBT_PASSWORD, DBT_SSLMODE=require`, `DBT_RUN_TOKEN` (opcional)

## Passos
1. Suba este repo e crie os 3 serviços (Render lê `render.yaml`).
2. Banco: rode `sql/ddl.sql` com `SET search_path TO "SllupMarket", public;` antes.
3. Upload: na aba **Upload CSV** do dashboard, envie seu `.csv` ou `.csv.gz` (cabeçalho padrão).
4. Build: marque o checkbox ou chame:
   ```bash
   curl -X POST https://<seu-dbt-runner>/dbt/build -H "X-Token: <token>"
   ```
5. Validar:
   ```sql
   SET search_path TO "SllupMarket", public;
   SELECT count(*) FROM staging.raw_vendas_achatado;
   SELECT * FROM mart_rfm LIMIT 10;
   ```

## Por que não estoura RAM
- Upload → **/tmp (chunks de 1MB)**, nunca em RAM.
- Transformação → **/tmp filtrado**.
- COPY → lê do disco.
- Limpeza de temporários e `gc.collect()` após cada etapa.
