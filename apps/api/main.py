import os, tempfile, csv, gzip, re
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Body, UploadFile, File, Form
import requests
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")
DBT_RUNNER_URL = os.getenv("DBT_RUNNER_URL")
DBT_RUNNER_TOKEN = os.getenv("DBT_RUNNER_TOKEN")

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

REQ_COLS = ["data","produto","sku","familia","sub_familia","cor","tam","marca",
            "cod_cliente","razao_social","qtde","preco_unit","total_venda",
            "total_custo","margem","documento_fiscal"]

NUMERIC_COLS = {"qtde","preco_unit","total_venda","total_custo","margem"}
decimal_re = re.compile(r"^-?\d{1,3}(\.\d{3})*,\d+$|^-?\d+,\d+$")

def normalize_decimal(val: str) -> str:
    if val is None or val == "":
        return val
    s = val.strip()
    if decimal_re.match(s):
        s = s.replace(".", "").replace(",", ".")
        return s
    return s

def detect_dialect(sample_text: str):
    try:
        import csv as _csv
        sniffer = _csv.Sniffer()
        dialect = sniffer.sniff(sample_text, delimiters=[",",";","\t","|"])
        return dialect
    except Exception:
        if sample_text.count(";") > sample_text.count(","):
            class Semi(csv.Dialect):
                delimiter = ";"; quotechar = '"'; doublequote=True; skipinitialspace=True; lineterminator="\n"; quoting=csv.QUOTE_MINIMAL
            return Semi
        else:
            return csv.excel

def _open_text_reader(path, detected=None):
    if path.endswith(".gz"):
        f = gzip.open(path, mode="rt", encoding="utf-8", newline="")
    else:
        f = open(path, mode="rt", encoding="utf-8", newline="")
    if detected:
        return f, csv.reader(f, dialect=detected)
    return f, csv.reader(f)

def process_to_filtered_csv(in_path: str, date_format: str) -> Dict[str, Any]:
    if in_path.endswith(".gz"):
        with gzip.open(in_path, "rt", encoding="utf-8", newline="") as ftxt:
            sample_text = ftxt.read(10000)
    else:
        with open(in_path, "rt", encoding="utf-8", newline="") as ftxt:
            sample_text = ftxt.read(10000)
    dialect = detect_dialect(sample_text)

    fin, rin = _open_text_reader(in_path, dialect)
    header = next(rin, None)
    if not header:
        fin.close()
        raise HTTPException(status_code=400, detail="CSV sem cabeçalho")
    miss = [c for c in REQ_COLS if c not in header]
    if miss:
        fin.close()
        raise HTTPException(status_code=400, detail=f"Colunas faltantes: {miss}")
    idx = [header.index(c) for c in REQ_COLS]

    preview = []
    for _ in range(5):
        row = next(rin, None)
        if row is None: break
        preview.append(row)
    fin.close()

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding="utf-8", newline="") as tmp_out:
        out_path = tmp_out.name
        w = csv.writer(tmp_out, lineterminator="\n")
        w.writerow(REQ_COLS)

        fin, rin = _open_text_reader(in_path, dialect)
        _ = next(rin, None)
        h_idx = {c: header.index(c) for c in header}
        d_idx = h_idx.get("data", None)

        count = 0
        for row in rin:
            if not row:
                continue
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            rlist = list(row)
            if d_idx is not None:
                dd = rlist[d_idx]
                if date_format == "DD/MM/YYYY" and "/" in dd:
                    parts = dd.split("/")
                    if len(parts)==3 and all(parts):
                        dd = f"{parts[2]}-{int(parts[1]):02d}-{int(parts[0]):02d}"
                rlist[d_idx] = dd
            for c in NUMERIC_COLS:
                ci = h_idx.get(c)
                if ci is not None:
                    rlist[ci] = normalize_decimal(rlist[ci])
            w.writerow([rlist[i] for i in idx])
            count += 1
            if count % 50000 == 0:
                tmp_out.flush()

    return {"out_path": out_path, "header": header, "preview": preview, "count": count, "dialect": getattr(dialect,'__name__', str(dialect))}

def copy_into_db(out_path: str, mode: str):
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

@app.post("/ingest/url")
def ingest_from_url(
    url: str = Body(..., embed=True),
    mode: str = Body("full", embed=True),
    date_format: str = Body("YYYY-MM-DD", embed=True)
):
    try:
        with requests.get(url, stream=True, timeout=900) as r:
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
    try:
        proc = process_to_filtered_csv(in_path, date_format)
        out_path = proc["out_path"]
        copy_into_db(out_path, mode)
        return {"ok": True, "rows": proc["count"], "mode": mode, "date_format": date_format,
                "dialect": proc["dialect"], "preview_header": proc["header"], "preview_rows": proc["preview"]}
    finally:
        try:
            if out_path and os.path.exists(out_path): os.remove(out_path)
        except Exception: pass
        try:
            if in_path and os.path.exists(in_path): os.remove(in_path)
        except Exception: pass

@app.post("/ingest/upload")
async def ingest_upload(
    file: UploadFile = File(...),
    mode: str = Form("full"),
    date_format: str = Form("YYYY-MM-DD")
):
    try:
        suffix = ".csv.gz" if file.filename.lower().endswith(".gz") else ".csv"
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=suffix) as tmp_in:
            while True:
                chunk = await file.read(1024*1024)
                if not chunk: break
                tmp_in.write(chunk)
            in_path = tmp_in.name
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha ao receber upload: {e}")

    out_path = None
    try:
        proc = process_to_filtered_csv(in_path, date_format)
        out_path = proc["out_path"]
        copy_into_db(out_path, mode)
        return {"ok": True, "rows": proc["count"], "mode": mode, "date_format": date_format,
                "dialect": proc["dialect"], "preview_header": proc["header"], "preview_rows": proc["preview"]}
    finally:
        try:
            if out_path and os.path.exists(out_path): os.remove(out_path)
        except Exception: pass
        try:
            if in_path and os.path.exists(in_path): os.remove(in_path)
        except Exception: pass

@app.post("/dbt/run")
def dbt_run():
    if not DBT_RUNNER_URL:
        raise HTTPException(status_code=500, detail="DBT_RUNNER_URL não configurado na API")
    try:
        headers = {"X-Token": DBT_RUNNER_TOKEN} if DBT_RUNNER_TOKEN else {}
        r = requests.post(DBT_RUNNER_URL.rstrip("/") + "/dbt/build", headers=headers, timeout=900)
        payload = {"ok": r.ok, "status": r.status_code}
        try:
            j = r.json()
            if isinstance(j, dict):
                payload["tail"] = j.get("tail")
            else:
                payload["raw"] = j
        except Exception:
            payload["text"] = r.text[:5000]
        return payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao acionar DBT runner: {e}")
