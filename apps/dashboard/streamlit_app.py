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

# -------------------------
# 📤 Upload CSV -> staging (streaming, low-mem)
# -------------------------
import csv, io, tempfile, gzip

try:
    import requests  # opcional: acionar dbt-runner
except Exception:
    requests = None

tab4, = st.tabs(["Upload CSV"])

with tab4:
    st.subheader("Upload para staging.raw_vendas_achatado (streaming)")
    st.caption("Cabeçalho esperado: data,produto,sku,familia,sub_familia,cor,tam,marca,cod_cliente,razao_social,qtde,preco_unit,total_venda,total_custo,margem,documento_fiscal")

    file = st.file_uploader("Selecione o CSV (UTF-8) — também aceita .csv.gz", type=["csv", "gz"])
    mode = st.radio("Modo de carga", ["Full replace (TRUNCATE + INSERT)", "Append (inserir no que já existe)"], index=0)
    do_dbt = st.checkbox("Disparar dbt build após carga (via dbt-runner)")
    col1, col2 = st.columns(2)
    with col1:
        dbt_url = st.text_input("DBT Runner URL", value=os.getenv("DBT_RUNNER_URL", ""))
    with col2:
        dbt_token = st.text_input("DBT Runner Token (X-Token)", type="password", value=os.getenv("DBT_RUNNER_TOKEN", ""))

    if file is not None:
        # Detecta gzip pelo nome
        is_gz = (file.name or "").lower().endswith(".gz")

        # Abre o arquivo em modo streaming (sem carregar tudo na RAM)
        def open_text_stream(uploaded_file, gz=False):
            uploaded_file.seek(0)
            if gz:
                return io.TextIOWrapper(gzip.GzipFile(fileobj=uploaded_file, mode="rb"), encoding="utf-8", newline="")
            else:
                return io.TextIOWrapper(uploaded_file, encoding="utf-8", newline="")

        try:
            text_stream = open_text_stream(file, gz=is_gz)
            reader = csv.reader(text_stream)
            header = next(reader, None)
        except Exception as e:
            st.error(f"Falha ao ler o cabeçalho: {e}")
            st.stop()

        req_cols = ["data","produto","sku","familia","sub_familia","cor","tam","marca","cod_cliente","razao_social","qtde","preco_unit","total_venda","total_custo","margem","documento_fiscal"]

        if not header:
            st.error("CSV sem cabeçalho.")
            st.stop()

        miss = [c for c in req_cols if c not in header]
        extra = [c for c in header if c not in req_cols]
        if miss:
            st.error(f"Colunas faltantes: {miss}")
            st.stop()
        if extra:
            st.info(f"Colunas extras serão ignoradas: {extra}")

        # Preview leve (5 linhas)
        preview_rows = []
        for _ in range(5):
            row = next(reader, None)
            if row is None:
                break
            preview_rows.append(row)
        st.code("\n".join([",".join(header)] + [",".join(r) for r in preview_rows]), language="csv")

        # Reposiciona o stream para começar de novo
        file.seek(0)
        text_stream = open_text_stream(file, gz=is_gz)
        reader = csv.reader(text_stream)
        _ = next(reader, None)  # pula cabeçalho

        if st.button("Carregar no banco (streaming)"):
            tmp_path = None
            try:
                # Cria arquivo temporário no disco (não usa RAM)
                with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmp:
                    tmp_path = tmp.name
                    writer = csv.writer(tmp, lineterminator="\n")
                    writer.writerow(req_cols)
                    idx = [header.index(c) for c in req_cols]

                    # Escreve no temp à medida que lê (linha a linha)
                    count = 0
                    for row in reader:
                        if not row:
                            continue
                        if len(row) < len(header):
                            row = row + [""] * (len(header) - len(row))
                        writer.writerow([row[i] for i in idx])
                        count += 1
                        if count % 50000 == 0:
                            tmp.flush()  # descarrega para disco periodicamente

                # COPY FROM STDIN lendo do arquivo temporário (stream)
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    with conn.cursor() as cur:
                        if mode.startswith("Full"):
                            cur.execute('TRUNCATE TABLE staging.raw_vendas_achatado;')

                        copy_sql = """
                        COPY staging.raw_vendas_achatado
                        (data,produto,sku,familia,sub_familia,cor,tam,marca,cod_cliente,razao_social,qtde,preco_unit,total_venda,total_custo,margem,documento_fiscal)
                        FROM STDIN WITH (FORMAT csv, HEADER true)
                        """
                        with open(tmp_path, "r", encoding="utf-8") as f:
                            cur.copy(copy_sql, f)
                    conn.commit()

                st.success("Carga concluída na staging.raw_vendas_achatado ✅")

                # Disparar dbt build (opcional)
                if do_dbt:
                    if not dbt_url:
                        st.warning("Informe a URL do dbt-runner para disparar o build.")
                    elif requests is None:
                        st.warning("Instale 'requests' no requirements.txt para usar o disparo do dbt.")
                    else:
                        try:
                            headers = {"X-Token": dbt_token} if dbt_token else {}
                            import requests as _req
                            resp = _req.post(dbt_url.rstrip("/") + "/dbt/build", headers=headers, timeout=120)
                            if resp.status_code == 200:
                                data = resp.json()
                                st.info(f"dbt build disparado. run_id={data.get('run_id')} returncode={data.get('returncode')}")
                                st.code((data.get('tail','') or '')[-2000:], language="bash")
                            else:
                                st.warning(f"dbt-runner respondeu {resp.status_code}: {resp.text}")
                        except Exception as e:
                            st.error(f"Falha ao chamar dbt-runner: {e}")

            except Exception as e:
                st.exception(e)
            finally:
                try:
                    if tmp_path and os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass