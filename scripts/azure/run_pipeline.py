"""
Trigger and monitor the Prime Capital Bank full pipeline on Databricks.
Polls until completion and reports status for each task.
"""

import os
import time
import sys
import requests
from datetime import datetime

DATABRICKS_HOST  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
JOB_ID           = os.environ.get("JOB_ID_PIPELINE", "")

POLL_INTERVAL = 30  # seconds

def api(method: str, path: str, payload: dict = None) -> dict:
    url = f"{DATABRICKS_HOST}/api/2.1{path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    resp = requests.request(method, url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json() if resp.text else {}

def trigger_run(job_id: str) -> str:
    result = api("POST", "/jobs/run-now", {"job_id": int(job_id)})
    run_id = str(result["run_id"])
    print(f"  Triggered run_id: {run_id}")
    print(f"  Monitor: {DATABRICKS_HOST}/#job/{job_id}/run/{run_id}")
    return run_id

def get_run_state(run_id: str) -> dict:
    return api("GET", f"/jobs/runs/get?run_id={run_id}")

def print_task_status(run: dict):
    tasks = run.get("tasks", [])
    print(f"\n  {'TASK':<35} {'STATUS':<20} {'DURATION'}")
    print(f"  {'-'*35} {'-'*20} {'-'*12}")
    for t in tasks:
        name     = t.get("task_key", "?")
        state    = t.get("state", {})
        life     = state.get("life_cycle_state", "PENDING")
        result   = state.get("result_state", "")
        status   = f"{life}/{result}" if result else life
        start    = t.get("start_time", 0)
        end      = t.get("end_time", 0)
        duration = f"{(end - start) / 1000:.0f}s" if end and start else "running..."
        icon     = "✓" if result == "SUCCESS" else ("✗" if result == "FAILED" else "○")
        print(f"  {icon} {name:<34} {status:<20} {duration}")

def main():
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars.")
        sys.exit(1)
    if not JOB_ID:
        print("ERROR: Set JOB_ID_PIPELINE env var (get from schedule_pipeline.py output).")
        sys.exit(1)

    print(f"\nPrime Capital Bank — Pipeline Runner")
    print(f"Workspace : {DATABRICKS_HOST}")
    print(f"Job ID    : {JOB_ID}")
    print(f"Started   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    run_id = trigger_run(JOB_ID)
    start_time = time.time()

    while True:
        run = get_run_state(run_id)
        state = run.get("state", {})
        life  = state.get("life_cycle_state", "")
        result = state.get("result_state", "")

        elapsed = int(time.time() - start_time)
        print(f"\n[{elapsed:>4}s] Pipeline state: {life}/{result or 'in_progress'}")
        print_task_status(run)

        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            final = result or life
            print(f"\n{'='*60}")
            print(f"Pipeline finished: {final}")
            print(f"Total duration   : {elapsed}s ({elapsed//60}m {elapsed%60}s)")
            print(f"{'='*60}\n")
            sys.exit(0 if final == "SUCCESS" else 1)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
