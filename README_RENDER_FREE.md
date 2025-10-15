# Deploy no Render (Free)

## Passo 1) Banco
Crie um Postgres (pode ser Render Free ou Supabase). Copie a `DATABASE_URL`.
Rode o DDL localmente ou num admin (pode ser via container psql) usando:
```
psql "$DATABASE_URL" -f sql/ddl.sql
```

## Passo 2) Subir o repositório
- Faça upload deste ZIP para um repositório Git (GitHub).
- Conecte o repo ao Render e selecione o `render.yaml` automaticamente.

## Serviços criados
- **engajamento-api (web)**: FastAPI em Free — dorme quando ocioso, acorda ao 1º acesso.
- **engajamento-dashboard (web)**: Streamlit em Free.
- **dbt-build (cron)**: roda diariamente `dbt build` no Free.

## Variáveis obrigatórias
- Em **engajamento-api** e **engajamento-dashboard**: `DATABASE_URL`
- No **cron dbt-build**: `DBT_HOST, DBT_USER, DBT_PASSWORD, DBT_PORT (5432), DBT_DBNAME`

## Carga de dados
1. Converta seu Excel para CSV UTF-8 com cabeçalho conforme `README.md`.
2. Importe para `staging.raw_vendas_achatado` (via psql Adminer/pgAdmin).
3. O cron `dbt-build` criará `fato_venda` e `mart_rfm` diariamente (ou rode manualmente na aba **Manual Run** do cron).

## Dica
- Free dorme após inatividade; no 1º acesso pode demorar alguns segundos para acordar.