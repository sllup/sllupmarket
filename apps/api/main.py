
import os, tempfile, csv, gzip, re, json
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, HTTPException, Body, UploadFile, File, Form
import requests
import psycopg
from psycopg.rows import dict_row
from unidecode import unidecode

DATABASE_URL = os.getenv("DATABASE_URL")
DBT_RUNNER_URL = os.getenv("DBT_RUNNER_URL")
DBT_RUNNER_TOKEN = os.getenv("DBT_RUNNER_TOKEN")
STAGING_TABLE = os.getenv("STAGING_TABLE", "staging.raw_vendas_achatado")
FALLBACK_DELETE = os.getenv("FALLBACK_DELETE_ON_TRUNCATE_ERROR", "true").lower() in ("1","true","yes","y")

app = FastAPI(title="B2B Engajamento API v5.7")

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado")
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute('SET search_path TO "SllupMarket", public;')
    return conn

@app.get("/health")
def health():
    return {"status":"ok","staging_table":STAGING_TABLE,"fallback_delete":FALLBACK_DELETE,"dbt_runner_url": DBT_RUNNER_URL}

# ------- Ingest helpers (same as v5.6, omitted for brevity in this cell) -------
REQ_COLS = ["data","produto","sku","familia","sub_familia","cor","tam","marca","cod_cliente","razao_social","qtde","preco_unit","total_venda","total_custo","margem","documento_fiscal"]
ALIASES = {
    "data": ["data","dt","data_venda","dt_venda","date","dt_emissao","emissao","data_emissao"],
    "produto": ["produto","produto_desc","nome_produto","descricao_produto","produto_nome"],
    "sku": ["sku","cod_sku","codigo_sku","id_sku","produto_id","item_id"],
    "familia": ["familia","linha","grupo","departamento"],
    "sub_familia": ["sub_familia","subfamilia","categoria","subcategoria","grupo2"],
    "cor": ["cor","color"],
    "tam": ["tam","tamanho","size"],
    "marca": ["marca","brand"],
    "cod_cliente": ["cod_cliente","cliente_id","id_cliente","codigo_cliente","cod_cli"],
    "razao_social": ["razao_social","cliente","nome_cliente","fantasia","razaosocial"],
    "qtde": ["qtde","qtd","quantidade","quant"],
    "preco_unit": ["preco_unit","preco","preco_unitario","valor_unit","vl_unit"],
    "total_venda": ["total_venda","valor_venda","vl_total","venda_total","faturamento","vl_venda"],
    "total_custo": ["total_custo","custo_total","vl_custo","custo"],
    "margem": ["margem","margem_total","lucro","markup","margem_%","margem_perc"],
    "documento_fiscal": ["documento_fiscal","nf","nfe","nota","pedido","doc_fiscal","num_doc"],
}
NUMERIC_COLS = {"qtde","preco_unit","total_venda","total_custo","margem"}
decimal_re = re.compile(r"^-?\d{1,3}(\.\d{3})*,\d+$|^-?\d+,\d+$")
def normalize_decimal(val: str) -> str:
    if val is None or val == "": return val
    s = val.strip()
    if decimal_re.match(s): s = s.replace(".", "").replace(",", ".")
    return s
def slug(s: str) -> str:
    s = unidecode((s or "").strip().lower()); out=[]; prev=False
    for ch in s:
        if ch.isalnum(): out.append(ch); prev=False
        else:
            if not prev: out.append("_"); prev=True
    res="".join(out).strip("_")
    while "__" in res: res=res.replace("__","_")
    return res
def detect_dialect(sample_text: str):
    try:
        import csv as _csv; sniffer=_csv.Sniffer()
        return sniffer.sniff(sample_text, delimiters=[",",";","\t","|"])
    except Exception:
        if sample_text.count(";")>sample_text.count(","):
            class Semi(csv.Dialect):
                delimiter=";"; quotechar='"'; doublequote=True; skipinitialspace=True; lineterminator="\n"; quoting=csv.QUOTE_MINIMAL
            return Semi
        else: return csv.excel
def _open_text_reader(path, detected=None):
    if path.endswith(".gz"): f=gzip.open(path, mode="rt", encoding="utf-8", newline="")
    else: f=open(path, mode="rt", encoding="utf-8", newline="")
    if detected: return f, csv.reader(f, dialect=detected)
    return f, csv.reader(f)
def build_alias_map(header: List[str]):
    norm={slug(h):i for i,h in enumerate(header)}; matched={}
    for req,alist in ALIASES.items():
        for a in alist:
            key=slug(a)
            if key in norm: matched[req]=norm[key]; break
    return matched
def apply_user_header_map(header: List[str], header_map: Optional[Dict[str,str]]):
    if not header_map: return {}
    norm_index={slug(h):i for i,h in enumerate(header)}; out={}
    for req_col, provided_name in header_map.items():
        if provided_name in header: idx = header.index(provided_name)
        else: idx = norm_index.get(slug(provided_name))
        if idx is None: raise HTTPException(status_code=400, detail=f"header_map aponta '{provided_name}' que não existe no CSV")
        out[req_col]=idx
    return out
def process_to_filtered_csv(in_path: str, date_format: str, header_map: Optional[Dict[str,str]]):
    if in_path.endswith(".gz"): ftxt=gzip.open(in_path,"rt",encoding="utf-8",newline=""); sample_text=ftxt.read(10000); ftxt.close()
    else:
        with open(in_path,"rt",encoding="utf-8",newline="") as ftxt: sample_text=ftxt.read(10000)
    dialect=detect_dialect(sample_text)
    fin, rin=_open_text_reader(in_path,dialect); header=next(rin,None)
    if not header: fin.close(); raise HTTPException(status_code=400, detail="CSV sem cabeçalho")
    idx_map={**build_alias_map(header), **apply_user_header_map(header, header_map)}
    miss=[c for c in REQ_COLS if c not in idx_map]
    if miss:
        fin.close()
        raise HTTPException(status_code=400, detail={"erro":"Colunas faltantes","faltantes":miss,"cabecalho_disponivel":header,"cabecalho_normalizado":[slug(h) for h in header]})
    preview=[]; 
    for _ in range(5):
        row=next(rin,None)
        if row is None: break
        preview.append(row)
    fin.close()
    with tempfile.NamedTemporaryFile(mode="w",delete=False,suffix=".csv",encoding="utf-8",newline="") as tmp_out:
        out_path=tmp_out.name; w=csv.writer(tmp_out,lineterminator="\n"); w.writerow(REQ_COLS)
        fin, rin=_open_text_reader(in_path,dialect); _=next(rin,None)
        count=0
        for row in rin:
            if not row: continue
            if len(row)<len(header): row+=[""]*(len(header)-len(row))
            r={ col: row[idx_map[col]] for col in REQ_COLS }
            dd=r["data"]
            if isinstance(dd,str) and date_format=="DD/MM/YYYY" and "/" in dd:
                p=dd.split("/")
                if len(p)==3 and all(p): dd=f"{p[2]}-{int(p[1]):02d}-{int(p[0]):02d}"
            r["data"]=dd
            for c in NUMERIC_COLS: r[c]=normalize_decimal(r[c])
            w.writerow([r[c] for c in REQ_COLS]); count+=1
            if count%50000==0: tmp_out.flush()
    return {"out_path":out_path,"header":header,"preview":preview,"count":count,"dialect":getattr(dialect,'__name__',str(dialect)),"staging_table":STAGING_TABLE}
def copy_into_db(out_path: str, mode: str):
    with get_conn() as conn:
        try:
            with conn.cursor() as cur:
                if mode.lower().startswith("full"):
                    try: cur.execute(f'TRUNCATE TABLE {STAGING_TABLE};')
                    except Exception:
                        if FALLBACK_DELETE:
                            conn.rollback()
                            with conn.cursor() as cur2: cur2.execute(f'DELETE FROM {STAGING_TABLE};')
                        else: raise
                copy_sql = f"""COPY {STAGING_TABLE}
                (data,produto,sku,familia,sub_familia,cor,tam,marca,
                 cod_cliente,razao_social,qtde,preco_unit,total_venda,
                 total_custo,margem,documento_fiscal)
                FROM STDIN WITH (FORMAT csv, HEADER true)"""
                with open(out_path,"r",encoding="utf-8") as fcsv: cur.copy(copy_sql, fcsv)
            conn.commit()
        except Exception:
            conn.rollback(); raise

@app.post("/ingest/url")
def ingest_from_url(url: str = Body(..., embed=True), mode: str = Body("full", embed=True), date_format: str = Body("YYYY-MM-DD", embed=True), header_map: Optional[Dict[str,str]] = Body(None, embed=True)):
    try:
        with requests.get(url, stream=True, timeout=900) as r:
            r.raise_for_status()
            suffix = ".csv.gz" if url.lower().endswith(".gz") else ".csv"
            with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=suffix) as tmp_in:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk: tmp_in.write(chunk)
                in_path = tmp_in.name
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha no download: {e}")
    out_path=None
    try:
        proc=process_to_filtered_csv(in_path,date_format,header_map); out_path=proc["out_path"]; copy_into_db(out_path,mode)
        return {"ok":True,"rows":proc["count"],"mode":mode,"date_format":date_format,"dialect":proc["dialect"],"preview_header":proc["header"],"preview_rows":proc["preview"],"staging_table":proc["staging_table"]}
    finally:
        try:
            if out_path and os.path.exists(out_path): os.remove(out_path)
        except Exception: pass
        try:
            if in_path and os.path.exists(in_path): os.remove(in_path)
        except Exception: pass

@app.post("/ingest/upload")
async def ingest_upload(file: UploadFile = File(...), mode: str = Form("full"), date_format: str = Form("YYYY-MM-DD"), header_map_json: Optional[str] = Form(None)):
    header_map=None
    if header_map_json:
        try:
            header_map=json.loads(header_map_json)
            if not isinstance(header_map, dict): raise ValueError("header_map_json deve ser um objeto JSON")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"header_map_json inválido: {e}")
    try:
        suffix = ".csv.gz" if file.filename.lower().endswith(".gz") else ".csv"
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=suffix) as tmp_in:
            while True:
                chunk=await file.read(1024*1024)
                if not chunk: break
                tmp_in.write(chunk)
            in_path=tmp_in.name
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha ao receber upload: {e}")
    out_path=None
    try:
        proc=process_to_filtered_csv(in_path,date_format,header_map); out_path=proc["out_path"]; copy_into_db(out_path,mode)
        return {"ok":True,"rows":proc["count"],"mode":mode,"date_format":date_format,"dialect":proc["dialect"],"preview_header":proc["header"],"preview_rows":proc["preview"],"staging_table":proc["staging_table"]}
    finally:
        try:
            if out_path and os.path.exists(out_path): os.remove(out_path)
        except Exception: pass
        try:
            if in_path and os.path.exists(in_path): os.remove(in_path)
        except Exception: pass

# Aliases
@app.post("/upload")
async def upload_alias(file: UploadFile = File(...), mode: str = Form("full"), date_format: str = Form("YYYY-MM-DD"), header_map_json: Optional[str] = Form(None)):
    return await ingest_upload(file=file, mode=mode, date_format=date_format, header_map_json=header_map_json)
@app.post("/ingest/file")
async def ingest_file_alias(file: UploadFile = File(...), mode: str = Form("full"), date_format: str = Form("YYYY-MM-DD"), header_map_json: Optional[str] = Form(None)):
    return await ingest_upload(file=file, mode=mode, date_format=date_format, header_map_json=header_map_json)
@app.post("/api/ingest/upload")
async def api_ingest_upload_alias(file: UploadFile = File(...), mode: str = Form("full"), date_format: str = Form("YYYY-MM-DD"), header_map_json: Optional[str] = Form(None)):
    return await ingest_upload(file=file, mode=mode, date_format=date_format, header_map_json=header_map_json)

# ---------- DBT autodiscovery v2 (reads openapi.json to derive paths) ----------
def _dbt_headers_variants():
    vs=[]
    if DBT_RUNNER_TOKEN:
        vs.append({"X-Token": DBT_RUNNER_TOKEN})
        vs.append({"Authorization": f"Bearer {DBT_RUNNER_TOKEN}"})
        vs.append({"X-API-Key": DBT_RUNNER_TOKEN})
    vs.append({})
    return vs

def _dbt_payload_variants():
    return [
        ({"Content-Type":"application/json"}, {}),
        ({"Content-Type":"application/json"}, {"action":"build"}),
        ({"Content-Type":"application/json"}, {"cmd":"dbt build"}),
        ({}, {}),
    ]

def _fetch_openapi_paths(base: str):
    paths=[]
    for path in ["/openapi.json","/docs/openapi.json","/api/openapi.json","/v1/openapi.json"]:
        try:
            r=requests.get(base.rstrip("/")+path,timeout=10)
            if r.ok:
                j=r.json()
                if isinstance(j, dict) and "paths" in j:
                    paths = list(j["paths"].keys())
                    if paths: return paths
        except Exception:
            pass
    return []

@app.post("/dbt/run")
def dbt_run():
    if not DBT_RUNNER_URL:
        raise HTTPException(status_code=500, detail="DBT_RUNNER_URL não configurado na API")
    base = DBT_RUNNER_URL.rstrip("/")
    tried=[]
    # Candidates from openapi.json if available
    dyn_paths = _fetch_openapi_paths(base)
    api_guess = [p for p in dyn_paths if any(k in p.lower() for k in ["build","run"])]
    # Static candidates
    static = ["/dbt/build","/dbt/run","/build","/run","/api/dbt/build","/api/build","/v1/build","/v1/run","/-/build","/-/run"]
    candidates = api_guess + static
    last=None
    for path in candidates:
        url = base + path
        for hdr_auth in _dbt_headers_variants():
            for hdr_ct, body in _dbt_payload_variants():
                headers = {**hdr_ct, **hdr_auth}
                try:
                    r = requests.post(url, headers=headers, json=body if hdr_ct.get("Content-Type")=="application/json" else None, timeout=900)
                    meta={"url":url,"headers":list(headers.keys()),"status":r.status_code,"body":None}
                    try: j=r.json()
                    except Exception: j=None
                    if r.ok:
                        resp={"ok":True,"status":r.status_code,"used_url":url,"used_headers":list(headers.keys())}
                        if j: resp["tail"] = j.get("tail") or j
                        else: resp["text"]=r.text[:5000]
                        return resp
                    else:
                        meta["body"] = (j if j else r.text[:500])
                        tried.append(meta); last=(r.status_code, meta["body"])
                except Exception as e:
                    tried.append({"url":url,"headers":list(headers.keys()),"error":str(e)}); last=(0,str(e))
    detail={"message":"Nenhum endpoint DBT aceitou a chamada","tried":tried[-10:]}
    raise HTTPException(status_code=404 if (last and isinstance(last[0], int) and last[0]==404) else 502, detail=detail)
