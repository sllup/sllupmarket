# B2B Engajamento – Full Stack v3+ (Render Free) • Importar por URL + DBT Trigger + Validador de Data

## Novidades
- Dashboard com **botão para rodar `dbt build`** no serviço dbt-runner.
- API `/ingest/url` aceita **`date_format`**: `"YYYY-MM-DD"` (padrão) ou `"DD/MM/YYYY"` — converte a coluna `data` on-the-fly.
- Upload continua 100% **server-side** (sem OOM no Streamlit).

## Variáveis
- API/Dashboard: `DATABASE_URL`
- Dashboard: `API_BASE_URL`, `DBT_RUNNER_URL`, `DBT_RUNNER_TOKEN` (opcional)
- dbt-runner: `DBT_HOST, DBT_PORT=5432, DBT_DBNAME, DBT_USER, DBT_PASSWORD, DBT_SSLMODE=require`, `DBT_RUN_TOKEN` (opcional)

## Fluxo
1. Suba os serviços via `render.yaml`.
2. Rode `sql/ddl.sql` com `SET search_path TO "SllupMarket", public;`.
3. Dashboard → **Importar por URL**: informe URL do CSV/CSV.GZ, escolha **Full/Append** e **formato da data**, execute.
4. Clique em **Rodar dbt build** na mesma aba (informe `DBT_RUNNER_URL` e token se necessário).
5. Confira KPIs/RFM/Vendas nas abas iniciais.

## Observação
O conversor de data é simples e eficiente: transforma `DD/MM/YYYY` → `YYYY-MM-DD` durante a geração do CSV filtrado em `/tmp` (sem memória alta).