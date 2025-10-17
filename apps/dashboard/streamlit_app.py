
import os, json, requests
import pandas as pd
import streamlit as st
import psycopg
from psycopg.rows import dict_row

st.set_page_config(page_title="Engajamento B2B", layout="wide")
st.title("📊 Engajamento B2B – v5.3 (DBT autodiscovery + debug)")

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

tabs = st.tabs(["DBT"])
(tab_dbt,) = tabs

DEFAULT_API = os.getenv("API_BASE_URL", "https://engajamento-api.onrender.com")

with tab_dbt:
    st.subheader("DBT – Build via API (autodiscovery)")
    api_base_dbt = st.text_input("Base URL da API", value=DEFAULT_API, key="api_base_url_dbt")
    colA, colB = st.columns([1,1])
    if colA.button("Ver /health da API", key="btn_health_api"):
        try:
            r = requests.get(api_base_dbt.rstrip("/") + "/health", timeout=30)
            st.code(r.text, language="json")
        except Exception as e:
            st.exception(e)
    if colB.button("Rodar dbt build (API)", key="btn_dbt_via_api"):
        if not api_base_dbt:
            st.error("Preencha a API Base URL")
        else:
            try:
                r = requests.post(api_base_dbt.rstrip("/") + "/dbt/run", timeout=900)
                st.write(f"Status: {r.status_code}")
                st.code(r.text[:4000], language="json")
            except Exception as e:
                st.exception(e)
