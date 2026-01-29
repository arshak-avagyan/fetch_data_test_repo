import subprocess
import sys
import os

SCRIPTS = [
    "main.py",
    "ratio.py",
    "snapshot_signals.py",
    "merge_signals.py",
]

LOCK_FILE = "job.lock"

# Prevent overlapping runs
if os.path.exists(LOCK_FILE):
    print("Job already running. Skipping.")
    sys.exit(0)

open(LOCK_FILE, "w").close()

try:
    for script in SCRIPTS:
        print(f"Running {script}...")

        result = subprocess.run(
            [sys.executable, script],
            capture_output=True,
            text=True
        )

        print(result.stdout)

        if result.returncode != 0:
            print(f"ERROR in {script}")
            print(result.stderr)
            sys.exit(1)

    print("All scripts completed successfully.")

finally:
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
