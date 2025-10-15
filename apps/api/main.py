import os
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")
app = FastAPI(title="B2B Engajamento API")

def get_conn():
  if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado")
  return psycopg.connect(DATABASE_URL, row_factory=dict_row)

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
      cur = conn.cursor()
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
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))

@app.get("/insights/churn")
def churn_top(n: int = 20):
  try:
    with get_conn() as conn:
      cur = conn.cursor()
      cur.execute("""
        select cod_cliente, ultima_compra::date, freq, valor, (r_score + f_score + m_score) as rfm_score
        from mart_rfm
        order by r_score asc, valor asc
        limit %s
      """, (n,))
      return cur.fetchall()
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))