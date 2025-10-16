import os
import pandas as pd
import streamlit as st
import psycopg
from psycopg.rows import dict_row
import requests

st.set_page_config(page_title="Engajamento B2B", layout="wide")
st.title("📊 Engajamento B2B – v3+ (URL Ingest + DBT Trigger)")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    st.error("Defina a variável de ambiente DATABASE_URL para conectar no Postgres.")
    st.stop()

@st.cache_data(ttl=300)
def run_query(sql, params=None):
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur0:
            cur0.execute('SET search_path TO "SllupMarket", public;')
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return pd.DataFrame(rows)

tab1, tab2, tab3, tab4 = st.tabs(["Visão Geral", "Clientes (RFM)", "Explorar Vendas", "Importar por URL"])

with tab1:
    st.subheader("Indicadores")
    try:
        df_counts = run_query("""
            select 
              (select count(*) from dim_cliente) as clientes,
              (select count(*) from dim_produto) as skus,
              (select count(*) from fato_venda)  as vendas
        """ )
        c1, c2, c3 = st.columns(3)
        c1.metric("Clientes", int(df_counts.iloc[0]["clientes"]))
        c2.metric("SKUs", int(df_counts.iloc[0]["skus"]))
        c3.metric("Registros de Venda", int(df_counts.iloc[0]["vendas"]))
    except Exception as e:
        st.warning("Não foi possível ler os indicadores. Verifique se as tabelas existem no schema SllupMarket.")
        st.exception(e)

with tab2:
    st.subheader("Ranking RFM (Top 200)")
    try:
        df_rfm = run_query("""
            select cod_cliente, ultima_compra::date, freq, valor, r_score, f_score, m_score, 
                   (r_score + f_score + m_score) as rfm_score
            from mart_rfm
            order by rfm_score desc
            limit 200
        """ )
        st.dataframe(df_rfm, use_container_width=True)
        cod = st.text_input("Buscar cliente (cod_cliente)")
        if cod:
            df_v = run_query("""
                with rfm as (select * from mart_rfm where cod_cliente = %s)
                select c.cod_cliente, c.razao_social, rfm.*
                from dim_cliente c
                left join rfm on rfm.cod_cliente = c.cod_cliente
                where c.cod_cliente = %s
            """, (cod, cod))
            st.dataframe(df_v, use_container_width=True)
    except Exception as e:
        st.warning("⚠️ Não encontrei `mart_rfm`. Rode o dbt build no mesmo banco do dashboard.")
        st.exception(e)

with tab3:
    st.subheader("Vendas recentes")
    try:
        df_vendas = run_query("""
            select data, cod_cliente, sku, tam, qtde, total_venda, total_custo, margem, documento_fiscal
            from fato_venda
            order by data desc
            limit 500
        """ )
        st.dataframe(df_vendas, use_container_width=True)
    except Exception as e:
        st.warning("Não foi possível carregar as vendas recentes. Verifique se `fato_venda` existe.")
        st.exception(e)

with tab4:
    st.subheader("Importar para staging.raw_vendas_achatado via URL (CSV/CSV.GZ)")
    st.caption("Use uma URL de download direto (S3 presigned, Dropbox ?dl=1, GDrive link direto).")

    api_base = st.text_input("Base URL da API (engajamento-api)", value=os.getenv("API_BASE_URL", ""))
    csv_url = st.text_input("URL do arquivo (csv ou csv.gz)")
    mode = st.radio("Modo de carga", ["Full replace (TRUNCATE + INSERT)", "Append"], index=0)
    date_fmt = st.selectbox("Formato da data (coluna 'data')", ["YYYY-MM-DD", "DD/MM/YYYY"], index=0)

    c1, c2 = st.columns(2)
    with c1:
        run = st.button("Importar do URL")
    with c2:
        st.write("")

    if run:
        if not api_base or not csv_url:
            st.error("Preencha a API Base URL e a URL do arquivo.")
        else:
            try:
                payload = {"url": csv_url, "mode": "full" if mode.startswith("Full") else "append", "date_format": date_fmt}
                resp = requests.post(api_base.rstrip("/") + "/ingest/url", json=payload, timeout=900)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("ok"):
                        st.success(f"Ingest concluído ✅ Linhas ~{data.get('rows')} (date_format={data.get('date_format')})")
                        with st.expander("Preview (até 5 linhas)"):
                            st.code("\n".join([",".join(data.get("preview_header", []))] + [",".join(r) for r in data.get("preview_rows", [])]), language="csv")
                    else:
                        st.error(f"Falhou: {data}")
                else:
                    st.error(f"API respondeu {resp.status_code}: {resp.text}")
            except Exception as e:
                st.exception(e)

    st.divider()
    st.subheader("DBT Runner")
    st.caption("Dispare o `dbt build` manualmente após a ingestão.")
    colA, colB = st.columns(2)
    with colA:
        dbt_url = st.text_input("DBT Runner URL", value=os.getenv("DBT_RUNNER_URL", ""))
    with colB:
        dbt_token = st.text_input("DBT Runner Token (X-Token)", type="password", value=os.getenv("DBT_RUNNER_TOKEN", ""))

    if st.button("Rodar dbt build agora"):
        if not dbt_url:
            st.warning("Informe a URL do dbt-runner.")
        else:
            try:
                headers = {"X-Token": dbt_token} if dbt_token else {}
                r = requests.post(dbt_url.rstrip("/") + "/dbt/build", headers=headers, timeout=240)
                st.code((r.json().get("tail","") or "")[-2000:], language="bash")
            except Exception as e:
                st.exception(e)