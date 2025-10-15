import os
import pandas as pd
import streamlit as st
import psycopg
from psycopg.rows import dict_row

st.set_page_config(page_title="Engajamento B2B", layout="wide")
st.title("📊 Engajamento B2B – v1 (MVP)")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    st.error("Defina a variável de ambiente DATABASE_URL para conectar no Postgres.")
    st.stop()

@st.cache_data(ttl=300)
def run_query(sql, params=None):
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return pd.DataFrame(rows)

tab1, tab2, tab3 = st.tabs(["Visão Geral", "Clientes (RFM)", "Explorar Vendas"])

with tab1:
    st.subheader("Indicadores")
    df_counts = run_query("""
        select 
          (select count(*) from dim_cliente) as clientes,
          (select count(*) from dim_produto) as skus,
          (select count(*) from fato_venda)  as vendas
    """)
    c1, c2, c3 = st.columns(3)
    c1.metric("Clientes", int(df_counts.iloc[0]["clientes"]))
    c2.metric("SKUs", int(df_counts.iloc[0]["skus"]))
    c3.metric("Registros de Venda", int(df_counts.iloc[0]["vendas"]))

with tab2:
    st.subheader("Ranking RFM (Top 200)")
    df_rfm = run_query("""
        select cod_cliente, ultima_compra::date, freq, valor, r_score, f_score, m_score, 
               (r_score + f_score + m_score) as rfm_score
        from mart_rfm
        order by rfm_score desc
        limit 200
    """)
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

with tab3:
    st.subheader("Vendas recentes")
    df_vendas = run_query("""
        select data, cod_cliente, sku, tam, qtde, total_venda, total_custo, margem, documento_fiscal
        from fato_venda
        order by data desc
        limit 500
    """)
    st.dataframe(df_vendas, use_container_width=True)