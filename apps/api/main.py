import os, tempfile, csv, gzip
from typing import Optional
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
import requests
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")
app = FastAPI(title="B2B Engajamento API")

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado")
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute('SET search_path TO "SllupMarket", public;')
    return conn

@app.get("/health")
def health():
    return {"status":"ok"}

class ClienteVisao(BaseModel):
    cod_cliente: str
    razao_social: Optional[str] = None
    r_score: Optional[int] = None
    f_score: Optional[int] = None
    m_score: Optional[int] = None
    ultima_compra: Optional[str] = None
    freq: Optional[int] = None
    valor: Optional[float] = None

@app.get("/clientes/{cod}/visao", response_model=ClienteVisao)
def visao_cliente(cod: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                  with rfm as (select * from mart_rfm where cod_cliente = %s)
                  select
                    c.cod_cliente,
                    coalesce(max(c.razao_social),'') as razao_social,
                    max(rfm.r_score) as r_score,
                    max(rfm.f_score) as f_score,
                    max(rfm.m_score) as m_score,
                    max(rfm.ultima_compra)::text as ultima_compra,
                    max(rfm.freq) as freq,
                    max(rfm.valor) as valor
                  from dim_cliente c
                  left join rfm on rfm.cod_cliente = c.cod_cliente
                  where c.cod_cliente = %s
                  group by c.cod_cliente
                """, (cod, cod))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Cliente não encontrado")
                return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/insights/churn")
def churn_top(n: int = 20):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                  select cod_cliente, ultima_compra::date, freq, valor, (r_score + f_score + m_score) as rfm_score
                  from mart_rfm
                  order by r_score asc, valor asc
                  limit %s
                """, (n,))
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------
# Ingestão por URL (streaming, low-mem) -> staging.raw_vendas_achatado
# ----------------------
REQ_COLS = ["data","produto","sku","familia","sub_familia","cor","tam","marca",
            "cod_cliente","razao_social","qtde","preco_unit","total_venda",
            "total_custo","margem","documento_fiscal"]

def _open_text_reader(path):
    if path.endswith(".gz"):
        f = gzip.open(path, mode="rt", encoding="utf-8", newline="")
    else:
        f = open(path, mode="rt", encoding="utf-8", newline="")
    return f, csv.reader(f)

@app.post("/ingest/url")
def ingest_from_url(url: str = Body(..., embed=True),
                    mode: str = Body("full", embed=True)):
    """Baixa CSV (.csv ou .csv.gz) via streaming -> /tmp, reordena/filtra colunas -> /tmp
    e faz COPY em staging.raw_vendas_achatado. mode: "full" (TRUNCATE) ou "append"."""
    # 1) download streaming para disco
    try:
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            suffix = ".csv.gz" if url.lower().endswith(".gz") else ".csv"
            with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=suffix) as tmp_in:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        tmp_in.write(chunk)
                in_path = tmp_in.name
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha no download: {e}")

    out_path = None
    count = 0
    try:
        fprev, rprev = _open_text_reader(in_path)
        header = next(rprev, None)
        if not header:
            fprev.close()
            raise HTTPException(status_code=400, detail="CSV sem cabeçalho")

        miss = [c for c in REQ_COLS if c not in header]
        if miss:
            fprev.close()
            raise HTTPException(status_code=400, detail=f"Colunas faltantes: {miss}")
        idx = [header.index(c) for c in REQ_COLS]

        # preview até 5 linhas
        preview = []
        for _ in range(5):
            row = next(rprev, None)
            if row is None: break
            preview.append(row)
        fprev.close()

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding="utf-8", newline="") as tmp_out:
            out_path = tmp_out.name
            w = csv.writer(tmp_out, lineterminator="\n")
            w.writerow(REQ_COLS)

            fin, rin = _open_text_reader(in_path)
            _ = next(rin, None)  # pula cabeçalho original
            for row in rin:
                if not row:
                    continue
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))
                w.writerow([row[i] for i in idx])
                count += 1
                if count % 50000 == 0:
                    tmp_out.flush()
            fin.close()

        # COPY no Postgres
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    if mode.lower().startswith("full"):
                        cur.execute('TRUNCATE TABLE staging.raw_vendas_achatado;')
                    copy_sql = """
                    COPY staging.raw_vendas_achatado
                    (data,produto,sku,familia,sub_familia,cor,tam,marca,
                     cod_cliente,razao_social,qtde,preco_unit,total_venda,
                     total_custo,margem,documento_fiscal)
                    FROM STDIN WITH (FORMAT csv, HEADER true)
                    """
                    with open(out_path, "r", encoding="utf-8") as fcsv:
                        cur.copy(copy_sql, fcsv)
                conn.commit()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro no COPY: {e}")

        return {"ok": True, "rows": count, "mode": mode, "preview_header": header, "preview_rows": preview}
    finally:
        try:
            if out_path and os.path.exists(out_path): os.remove(out_path)
        except Exception: pass
        try:
            if in_path and os.path.exists(in_path): os.remove(in_path)
        except Exception: pass