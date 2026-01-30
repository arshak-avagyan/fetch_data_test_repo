from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import subprocess
import sys
import time
import os

app = FastAPI()

SECRET = os.getenv("JOB_SECRET")

@app.get("/run")
def run_job(x_token: str = Header(None)):
    if x_token != SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    start = time.time()

    result = subprocess.run(
        [sys.executable, "job.py"],
        capture_output=True,
        text=True
    )

    return {
        "status": "ok" if result.returncode == 0 else "error",
        "runtime_sec": round(time.time() - start, 2),
        "stdout": result.stdout,
        "stderr": result.stderr,
    }





BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

@app.get("/files/{filename}")
def get_file(filename: str, x_token: str = Header(None)):
    if x_token != os.getenv("JOB_SECRET"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    file_path = DATA_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(file_path)
