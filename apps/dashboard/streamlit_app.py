
import os, json, requests
import pandas as pd
import streamlit as st
import psycopg
from psycopg.rows import dict_row

st.set_page_config(page_title="Engajamento B2B", layout="wide")
st.title("📊 Engajamento B2B – v4 (aliases de header, URL & Upload, DBT via API)")

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

tabs = st.tabs(["Visão Geral","Clientes (RFM)","Explorar Vendas","Importar por URL","Upload CSV","DBT"])
tab1, tab2, tab3, tab4, tab5, tab6 = tabs

with tab1:
    st.subheader("Indicadores")
    try:
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
        """)
        st.dataframe(df_rfm, use_container_width=True)
        cod = st.text_input("Buscar cliente (cod_cliente)", key="rfm_busca_cod")
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
        """)
        st.dataframe(df_vendas, use_container_width=True)
    except Exception as e:
        st.warning("Não foi possível carregar as vendas recentes. Verifique se `fato_venda` existe.")
        st.exception(e)

DEFAULT_API = os.getenv("API_BASE_URL", "https://engajamento-api.onrender.com")

def parse_header_map_json(txt):
    if not txt:
        return None
    try:
        j = json.loads(txt)
        if isinstance(j, dict):
            return j
        st.error("header_map deve ser um objeto JSON (ex.: {'data':'Data','sku':'SKU'})")
    except Exception as e:
        st.error(f"JSON inválido: {e}")
    return None

with tab4:
    st.subheader("Importar por URL (CSV/CSV.GZ) – recomendado p/ arquivos grandes")
    st.caption("Se os nomes das colunas diferirem, use o campo 'header_map' para mapear. Exemplo: {'data':'Data','cod_cliente':'Cód. Cliente'}")
    api_base = st.text_input("Base URL da API", value=DEFAULT_API, key="api_base_url_url")
    csv_url = st.text_input("URL do arquivo (csv ou csv.gz)", key="csv_url_field")
    header_map_txt = st.text_area("header_map (JSON opcional)", height=100, key="header_map_url")
    mode = st.radio("Modo de carga", ["Full replace (TRUNCATE + INSERT)", "Append"], index=0, key="modo_url")
    date_fmt = st.selectbox("Formato da data (coluna 'data')", ["YYYY-MM-DD", "DD/MM/YYYY"], index=0, key="datefmt_url")

    if st.button("Importar do URL", key="btn_import_url"):
        hm = parse_header_map_json(header_map_txt)
        if not api_base or not csv_url:
            st.error("Preencha a API Base URL e a URL do arquivo.")
        else:
            try:
                payload = {"url": csv_url, "mode": "full" if mode.startswith("Full") else "append", "date_format": date_fmt, "header_map": hm}
                resp = requests.post(api_base.rstrip("/") + "/ingest/url", json=payload, timeout=900)
                if resp.status_code == 200:
                    data = resp.json()
                    st.success(f"Ingest concluído ✅ Linhas ~{data.get('rows')} | Dialect: {data.get('dialect')}")
                    with st.expander("Preview (até 5 linhas) + Mapeamento usado", expanded=False):
                        st.code("\n".join([",".join(data.get("preview_header", []))] + [",".join(r) for r in data.get("preview_rows", [])]), language="csv")
                        # opcional: st.json(data)
                else:
                    st.error(f"API respondeu {resp.status_code}: {resp.text}")
            except Exception as e:
                st.exception(e)

with tab5:
    st.subheader("Upload de CSV (até 64MB) – para arquivos pequenos")
    st.caption("Se os nomes das colunas diferirem, use 'header_map' (JSON). Ex.: {'data':'Data','cod_cliente':'Cód. Cliente'}")
    api_base_up = st.text_input("Base URL da API", value=DEFAULT_API, key="api_base_url_upload")
    uploaded = st.file_uploader("Escolha um arquivo .csv ou .csv.gz", type=["csv", "gz"], key="uploader_csv")
    header_map_up = st.text_area("header_map (JSON opcional)", height=100, key="header_map_upload")
    mode_up = st.radio("Modo de carga (upload)", ["Full replace (TRUNCATE + INSERT)", "Append"], index=0, key="modo_upload")
    date_fmt_up = st.selectbox("Formato da data (upload)", ["YYYY-MM-DD", "DD/MM/YYYY"], index=0, key="datefmt_upload")

    if st.button("Enviar upload", key="btn_upload"):
        if not api_base_up or not uploaded:
            st.error("Informe a API Base URL e selecione um arquivo.")
        else:
            try:
                files = {"file": (uploaded.name, uploaded, "application/octet-stream")}
                data = {"mode": "full" if mode_up.startswith("Full") else "append", "date_format": date_fmt_up}
                hm = parse_header_map_json(header_map_up)
                if hm is not None:
                    data["header_map_json"] = json.dumps(hm, ensure_ascii=False)
                resp = requests.post(api_base_up.rstrip("/") + "/ingest/upload", files=files, data=data, timeout=900)
                if resp.status_code == 200:
                    data = resp.json()
                    st.success(f"Ingest concluído ✅ Linhas ~{data.get('rows')} | Dialect: {data.get('dialect')}")
                    with st.expander("Preview (até 5 linhas) + Mapeamento usado", expanded=False):
                        st.code("\n".join([",".join(data.get("preview_header", []))] + [",".join(r) for r in data.get("preview_rows", [])]), language="csv")
                else:
                    st.error(f"API respondeu {resp.status_code}: {resp.text}")
            except Exception as e:
                st.exception(e)

with tab6:
    st.subheader("DBT – Build via API")
    st.caption("Aciona o DBT Runner através da API (sem expor token no front).")
    api_base_dbt = st.text_input("Base URL da API", value=DEFAULT_API, key="api_base_url_dbt")
    if st.button("Rodar dbt build (API)", key="btn_dbt_via_api"):
        if not api_base_dbt:
            st.error("Preencha a API Base URL")
        else:
            try:
                r = requests.post(api_base_dbt.rstrip("/") + "/dbt/run", timeout=600)
                if r.status_code == 200:
                    j = r.json()
                    tail = j.get("tail") or j.get("text") or str(j)
                    st.code(str(tail)[-2000:], language="bash")
                    if not j.get("ok", False):
                        st.warning(f"dbt runner respondeu status {j.get('status')}")
                else:
                    st.error(f"API respondeu {r.status_code}: {r.text}")
            except Exception as e:
                st.exception(e)
