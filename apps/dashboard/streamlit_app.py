
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
        # 🔧 Garante schema correto em toda conexão
        with conn.cursor() as cur0:
            cur0.execute('SET search_path TO "SllupMarket", public;')
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return pd.DataFrame(rows)

tab1, tab2, tab3 = st.tabs(["Visão Geral", "Clientes (RFM)", "Explorar Vendas"])

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
        """)
        st.dataframe(df_vendas, use_container_width=True)
    except Exception as e:
        st.warning("Não foi possível carregar as vendas recentes. Verifique se `fato_venda` existe.")
        st.exception(e)

# -------------------------
# 📤 Upload CSV -> staging
# -------------------------
import io
import csv

try:
    import requests  # para acionar o dbt-runner (opcional)
except Exception:
    requests = None

tab4, = st.tabs(["Upload CSV"])

with tab4:
    st.subheader("Upload para staging.raw_vendas_achatado")
    st.caption("Cabeçalho esperado: data,produto,sku,familia,sub_familia,cor,tam,marca,cod_cliente,razao_social,qtde,preco_unit,total_venda,total_custo,margem,documento_fiscal")

    file = st.file_uploader("Selecione o CSV (UTF-8, com HEADER)", type=["csv"])
    mode = st.radio("Modo de carga", ["Full replace (TRUNCATE + INSERT)", "Append (inserir no que já existe)"], index=0)
    do_dbt = st.checkbox("Disparar dbt build após carga (via dbt-runner)")
    col1, col2 = st.columns(2)
    with col1:
        dbt_url = st.text_input("DBT Runner URL (ex: https://seu-runner.onrender.com)", value=os.getenv("DBT_RUNNER_URL", ""))
    with col2:
        dbt_token = st.text_input("DBT Runner Token (X-Token, se configurado)", type="password", value=os.getenv("DBT_RUNNER_TOKEN", ""))

    if file is not None:
        # Preview (5 linhas)
        try:
            content = file.read()
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            st.error("Falha ao ler como UTF-8. Reexporte o CSV em UTF-8.")
            st.stop()

        sample = "\n".join(text.splitlines()[:6])
        st.code(sample, language="csv")

        # Validação de cabeçalhos
        req_cols = ["data","produto","sku","familia","sub_familia","cor","tam","marca","cod_cliente","razao_social","qtde","preco_unit","total_venda","total_custo","margem","documento_fiscal"]
        reader = csv.reader(io.StringIO(text))
        header = next(reader, None)
        if header is None:
            st.error("CSV sem cabeçalho."); st.stop()
        miss = [c for c in req_cols if c not in header]
        extra = [c for c in header if c not in req_cols]
        if miss:
            st.error(f"Colunas faltantes: {miss}"); st.stop()
        if extra:
            st.warning(f"Colunas extras serão ignoradas: {extra}")

        if st.button("Carregar no banco"):
            try:
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    # staging é outro schema; COPY qualificado
                    with conn.cursor() as cur:
                        # Modo full-replace
                        if mode.startswith("Full"):
                            cur.execute('TRUNCATE TABLE staging.raw_vendas_achatado;')

                        # Prepara CSV apenas com colunas requeridas e na ordem certa
                        out = io.StringIO()
                        w = csv.writer(out, lineterminator="\n")
                        w.writerow(req_cols)
                        idx = [header.index(c) for c in req_cols]
                        for row in reader:
                            if not row:
                                continue
                            padded = row + [""] * (len(header) - len(row))
                            w.writerow([padded[i] for i in idx])

                        out.seek(0)
                        copy_sql = """
                        COPY staging.raw_vendas_achatado
                        (data,produto,sku,familia,sub_familia,cor,tam,marca,cod_cliente,razao_social,qtde,preco_unit,total_venda,total_custo,margem,documento_fiscal)
                        FROM STDIN WITH (FORMAT csv, HEADER true)
                        """
                        cur.copy(copy_sql, out)
                    conn.commit()
                st.success("Carga concluída na staging.raw_vendas_achatado ✅")

                # Disparar dbt-runner (opcional)
                if do_dbt:
                    if not dbt_url:
                        st.warning("Informe a URL do dbt-runner para disparar o build.")
                    elif requests is None:
                        st.warning("Instale 'requests' no requirements.txt para usar o disparo do dbt.")
                    else:
                        try:
                            headers = {}
                            if dbt_token:
                                headers["X-Token"] = dbt_token
                            resp = requests.post(dbt_url.rstrip("/") + "/dbt/build", headers=headers, timeout=120)
                            if resp.status_code == 200:
                                data = resp.json()
                                st.info(f"dbt build disparado. run_id={data.get('run_id')} returncode={data.get('returncode')}")
                                st.code((data.get("tail","") or "")[-2000:], language="bash")
                            else:
                                st.warning(f"dbt-runner respondeu {resp.status_code}: {resp.text}")
                        except Exception as e:
                            st.error(f"Falha ao chamar dbt-runner: {e}")

            except Exception as e:
                st.exception(e)
