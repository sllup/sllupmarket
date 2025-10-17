
import os, json, requests, time
import pandas as pd
import streamlit as st
import psycopg
from psycopg.rows import dict_row

st.set_page_config(page_title="Engajamento B2B", layout="wide")
st.title("📊 Engajamento B2B – v5.5 (painel de status + abas completas)")

DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_API = os.getenv("API_BASE_URL", "https://engajamento-api.onrender.com")

# ===== Painel de Status =====
def check_db():
    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10) as conn:
            with conn.cursor() as cur0:
                cur0.execute('SET search_path TO "SllupMarket", public;')
            with conn.cursor() as cur:
                cur.execute("select 1 as ok")
                cur.fetchall()
        return True, None
    except Exception as e:
        return False, str(e)

def check_api(base):
    try:
        r = requests.get(base.rstrip("/") + "/health", timeout=10)
        if r.status_code == 200:
            return True, r.json()
        return False, {"status_code": r.status_code, "text": r.text[:500]}
    except Exception as e:
        return False, str(e)

st.markdown("### 🔌 Status")
c1, c2, c3 = st.columns([1,2,2])

# DB status
ok_db, info_db = check_db()
c1.metric("Postgres", "OK" if ok_db else "Falhou", delta=None)
if not ok_db:
    c1.caption(str(info_db))

# API health
api_base = st.text_input("Base URL da API", value=DEFAULT_API, key="api_base_status")
ok_api, info_api = check_api(api_base)
c2.metric("API /health", "OK" if ok_api else "Falhou", delta=None)
if ok_api and isinstance(info_api, dict):
    c2.caption(f"staging_table: {info_api.get('staging_table')} | fallback_delete: {info_api.get('fallback_delete')}")
else:
    c2.caption(str(info_api))

# DBT runner (exibido a partir do /health da API)
if ok_api and isinstance(info_api, dict):
    runner = info_api.get("dbt_runner_url")
else:
    runner = None
c3.metric("DBT Runner URL", "configurado" if runner else "vazio", delta=None)
if runner:
    c3.caption(runner)

st.divider()

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
    st.caption("Use header_map se os nomes de colunas forem diferentes.")
    api_base = st.text_input("Base URL da API", value=DEFAULT_API, key="api_base_url_url")
    csv_url = st.text_input("URL do arquivo (csv ou csv.gz)", key="csv_url_field")
    header_map_txt = st.text_area("header_map (JSON opcional)", height=100, key="header_map_url")
    mode = st.radio("Modo de carga", ["Full replace (TRUNCATE/DELETE + INSERT)", "Append"], index=0, key="modo_url")
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
                    st.success(f"Ingest concluído ✅ Linhas ~{data.get('rows')} | Dialect: {data.get('dialect')} | Staging: {data.get('staging_table')}")
                    with st.expander("Preview (até 5 linhas)", expanded=False):
                        st.code("\n".join([",".join(data.get("preview_header", []))] + [",".join(r) for r in data.get("preview_rows", [])]), language="csv")
                else:
                    st.error(f"API respondeu {resp.status_code}: {resp.text}")
            except Exception as e:
                st.exception(e)

with tab5:
    st.subheader("Upload de CSV (até 64MB) – para arquivos pequenos")
    api_base_up = st.text_input("Base URL da API", value=DEFAULT_API, key="api_base_url_upload")
    uploaded = st.file_uploader("Escolha um arquivo .csv ou .csv.gz", type=["csv", "gz"], key="uploader_csv")
    header_map_up = st.text_area("header_map (JSON opcional)", height=100, key="header_map_upload")
    mode_up = st.radio("Modo de carga (upload)", ["Full replace (TRUNCATE/DELETE + INSERT)", "Append"], index=0, key="modo_upload")
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
                    st.success(f"Ingest concluído ✅ Linhas ~{data.get('rows')} | Dialect: {data.get('dialect')} | Staging: {data.get('staging_table')}")
                    with st.expander("Preview (até 5 linhas)", expanded=False):
                        st.code("\n".join([",".join(data.get("preview_header", []))] + [",".join(r) for r in data.get("preview_rows", [])]), language="csv")
                else:
                    st.error(f"API respondeu {resp.status_code}: {resp.text}")
            except Exception as e:
                st.exception(e)

with tab6:
    st.subheader("DBT – Build via API (autodiscovery)")
    st.caption("A API testa múltiplas rotas e headers; veja detalhes abaixo.")
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
