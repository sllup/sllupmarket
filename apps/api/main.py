# minimal api for v6.1 placeholder; user already has full v6.0 main.py in earlier zip.
from fastapi import FastAPI
app=FastAPI(title='Engajamento API v6.1')
@app.get('/health')
def h(): return {'status':'ok','dbt_runner_url':'LOCAL'}
