from fastapi import FastAPI, Header, HTTPException
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
