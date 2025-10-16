import os, subprocess, time
from fastapi import FastAPI, Header, HTTPException
from typing import Optional
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent.parent.parent  # repo root
DBT_DIR = APP_ROOT / "dbt_project"
LOGS_DIR = Path(os.getenv("DBT_LOG_DIR", "/tmp/dbt_logs"))
LOGS_DIR.mkdir(parents=True, exist_ok=True)
EXPECTED_TOKEN = os.getenv("DBT_RUN_TOKEN")  # opcional

app = FastAPI(title="DBT Runner")

@app.get("/health")
def health():
    return {"status":"ok"}

@app.post("/dbt/build")
def dbt_build(x_token: Optional[str] = Header(default=None)):
    if EXPECTED_TOKEN and x_token != EXPECTED_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    run_id = str(int(time.time()))
    log_path = LOGS_DIR / f"dbt_build_{run_id}.log"

    if not DBT_DIR.exists():
        raise HTTPException(status_code=500, detail=f"dbt_project não encontrado em {DBT_DIR}")

    env = os.environ.copy()
    cmd = f"cd dbt_project && dbt deps || true && dbt build --fail-fast | tee -a '{log_path}'"

    try:
        proc = subprocess.run(cmd, shell=True, cwd=APP_ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=540)
        output = proc.stdout[-4000:]
        return {"run_id": run_id, "returncode": proc.returncode, "tail": output}
    except subprocess.TimeoutExpired as e:
        partial = e.stdout[-4000:] if isinstance(e.stdout, str) else ""
        return {"run_id": run_id, "returncode": None, "tail": partial, "timeout": True}

@app.get("/dbt/logs/{run_id}")
def dbt_logs(run_id: str):
    path = LOGS_DIR / f"dbt_build_{run_id}.log"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Log não encontrado")
    return {"run_id": run_id, "log": path.read_text()[-10000:]}