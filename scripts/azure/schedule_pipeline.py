"""
Create and schedule the full Prime Capital Bank Databricks Workflow.
Creates a multi-task job with proper dependencies and cron schedules.
"""

import os
import json
import requests
from pathlib import Path

DATABRICKS_HOST  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
CONFIG_FILE      = Path(__file__).parent.parent.parent / "pipeline" / "pipeline_config.json"

def api(method: str, path: str, payload: dict = None) -> dict:
    url = f"{DATABRICKS_HOST}/api/2.1{path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    resp = requests.request(method, url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json() if resp.text else {}

def get_or_create_cluster() -> str:
    """Return existing cluster ID or create a new one."""
    clusters = api("GET", "/clusters/list").get("clusters", [])
    for c in clusters:
        if c.get("cluster_name") == "prime-capital-compute" and c.get("state") != "TERMINATED":
            print(f"  Reusing cluster: {c['cluster_id']}")
            return c["cluster_id"]

    result = api("POST", "/clusters/create", {
        "cluster_name": "prime-capital-compute",
        "spark_version": "14.3.x-ml-scala2.12",
        "node_type_id": "Standard_DS4_v2",
        "autoscale": {"min_workers": 2, "max_workers": 16},
        "auto_termination_minutes": 30,
        "spark_conf": {
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
        },
        "custom_tags": {"project": "prime-capital-bank", "env": "prod"}
    })
    cluster_id = result["cluster_id"]
    print(f"  Created cluster: {cluster_id}")
    return cluster_id

def notebook_task(name: str, path: str, depends_on: list = None) -> dict:
    task = {
        "task_key": name,
        "notebook_task": {
            "notebook_path": f"/Prime Capital Bank/{path}",
            "source": "WORKSPACE"
        },
        "job_cluster_key": "main_cluster",
        "timeout_seconds": 7200,
        "max_retries": 2,
        "retry_on_timeout": True
    }
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    return task

def create_full_pipeline_job() -> int:
    job_def = {
        "name": "PCB — Full Daily Pipeline",
        "tags": {"project": "prime-capital-bank", "env": "prod"},
        "job_clusters": [{
            "job_cluster_key": "main_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-ml-scala2.12",
                "node_type_id": "Standard_DS4_v2",
                "autoscale": {"min_workers": 2, "max_workers": 16},
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true"
                }
            }
        }],
        "tasks": [
            notebook_task("setup_unity_catalog", "00_setup_unity_catalog"),
            notebook_task("bronze_ingestion",    "01_bronze_ingestion",      ["setup_unity_catalog"]),
            notebook_task("silver_transform",    "02_silver_transformation", ["bronze_ingestion"]),
            notebook_task("gold_build",          "03_gold_star_schema",      ["silver_transform"]),
            notebook_task("ml_credit_scoring",   "04_ml_credit_scoring",     ["gold_build"]),
            notebook_task("fraud_detection",     "05_fraud_detection",       ["silver_transform"]),
            notebook_task("aml_scoring",         "06_aml_risk_scoring",      ["silver_transform"]),
            notebook_task("regulatory_reports",  "07_regulatory_reporting",  ["gold_build", "ml_credit_scoring"]),
            notebook_task("orchestrator",        "08_pipeline_orchestrator", [
                "ml_credit_scoring", "fraud_detection", "aml_scoring", "regulatory_reports"
            ])
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",  # 02:00 SAST daily
            "timezone_id": "Africa/Johannesburg",
            "pause_status": "UNPAUSED"
        },
        "email_notifications": {
            "on_failure": [os.environ.get("ALERT_EMAIL", "anthony@the-spot.tech")],
            "on_success": [],
            "no_alert_for_skipped_runs": True
        },
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK"
    }

    result = api("POST", "/jobs/create", job_def)
    job_id = result["job_id"]
    print(f"  ✓ Full pipeline job created: {job_id}")
    return job_id

def create_fraud_streaming_job() -> int:
    """Separate lightweight job for real-time fraud scoring (runs every 15 min)."""
    result = api("POST", "/jobs/create", {
        "name": "PCB — Fraud Detection (Real-Time)",
        "tags": {"project": "prime-capital-bank", "type": "streaming"},
        "job_clusters": [{
            "job_cluster_key": "stream_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-ml-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2
            }
        }],
        "tasks": [notebook_task("fraud_stream", "05_fraud_detection")],
        "schedule": {
            "quartz_cron_expression": "0 0/15 * * * ?",  # every 15 min
            "timezone_id": "Africa/Johannesburg",
            "pause_status": "UNPAUSED"
        },
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK"
    })
    job_id = result["job_id"]
    print(f"  ✓ Fraud streaming job created: {job_id}")
    return job_id

def main():
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        raise ValueError("Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")

    print(f"\nScheduling Prime Capital Bank pipelines on {DATABRICKS_HOST}")
    print("=" * 60)

    pipeline_job_id = create_full_pipeline_job()
    fraud_job_id    = create_fraud_streaming_job()

    print(f"\n✓ All jobs scheduled.")
    print(f"  Full pipeline job ID : {pipeline_job_id}")
    print(f"  Fraud streaming job ID: {fraud_job_id}")
    print(f"  View jobs: {DATABRICKS_HOST}/#job/list")

if __name__ == "__main__":
    main()
