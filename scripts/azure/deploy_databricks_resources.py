"""
Prime Capital Bank — Databricks Resource Deployment
====================================================
Uses the Databricks REST API to:
  - Upload all notebooks to /Prime Capital Bank/ workspace folder
  - Create cluster prime-capital-compute (14.3 LTS ML, 8-32 nodes, Standard_DS4_v2)
  - Create individual Databricks Jobs for each notebook with schedules
  - Create a multi-task Databricks Workflow for the full pipeline
  - Set library dependencies on the cluster
  - Configure cluster init scripts

Usage:
    python deploy_databricks_resources.py \\
        --workspace-url https://adb-XXXX.azuredatabricks.net \\
        --token dapi... \\
        --notebooks-dir /path/to/notebooks
"""

import argparse
import base64
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("deploy_databricks")

# ── Constants ──────────────────────────────────────────────────────────────────
WORKSPACE_FOLDER  = "/Prime Capital Bank"
CLUSTER_NAME      = "prime-capital-compute"
DBR_RUNTIME       = "14.3.x-scala2.12"        # LTS ML runtime
DBR_ML_RUNTIME    = "14.3.x-cpu-ml-scala2.12" # ML flavour for notebooks 04-06
NODE_TYPE         = "Standard_DS4_v2"
MIN_WORKERS       = 8
MAX_WORKERS       = 32
AUTO_TERM_MINUTES = 30

NOTEBOOK_JOBS = [
    # (display_name, notebook_basename, cron_expression, is_ml_cluster)
    ("00 Unity Catalog Setup",   "00_setup_unity_catalog",   None,                    False),
    ("01 Bronze Ingestion",      "01_bronze_ingestion",       "0 0 2 * * ?",           False),
    ("02 Silver Transformation", "02_silver_transformation",  "0 30 2 * * ?",          False),
    ("03 Gold Star Schema",      "03_gold_star_schema",       "0 0 3 * * ?",           False),
    ("04 ML Credit Scoring",     "04_ml_credit_scoring",      "0 0 22 ? * SUN",        True),
    ("05 Fraud Detection",       "05_fraud_detection",        "0 0 * * * ?",           True),
    ("06 AML Risk Scoring",      "06_aml_risk_scoring",       "0 30 3 * * ?",          True),
    ("07 Regulatory Reporting",  "07_regulatory_reporting",   "0 0 4 1 * ?",           False),
    ("08 Pipeline Orchestrator", "08_pipeline_orchestrator",  "0 0 2 * * ?",           False),
]

PYPI_LIBRARIES = [
    {"pypi": {"package": "lightgbm==4.3.0"}},
    {"pypi": {"package": "shap==0.45.0"}},
    {"pypi": {"package": "mlflow==2.13.0"}},
    {"pypi": {"package": "imbalanced-learn==0.12.0"}},
    {"pypi": {"package": "sendgrid==6.11.0"}},
    {"pypi": {"package": "scipy==1.12.0"}},
    {"pypi": {"package": "python-dateutil==2.9.0"}},
    {"pypi": {"package": "requests==2.31.0"}},
]

# ── REST Client ────────────────────────────────────────────────────────────────

class DatabricksClient:
    def __init__(self, workspace_url: str, token: str):
        self.base = workspace_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        })

    def get(self, path: str, params: dict = None) -> Any:
        resp = self.session.get(f"{self.base}{path}", params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, payload: dict) -> Any:
        resp = self.session.post(f"{self.base}{path}", json=payload, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def delete(self, path: str, payload: dict = None) -> Any:
        resp = self.session.delete(f"{self.base}{path}", json=payload or {}, timeout=30)
        resp.raise_for_status()
        return resp.json() if resp.content else {}


# ── Step 1: Create Workspace Folder ───────────────────────────────────────────

def create_workspace_folder(client: DatabricksClient, folder: str):
    log.info(f"Creating workspace folder: {folder}")
    try:
        client.post("/api/2.0/workspace/mkdirs", {"path": folder})
        log.info(f"Folder created: {folder}")
    except requests.HTTPError as e:
        if e.response.status_code == 400:
            log.info(f"Folder already exists: {folder}")
        else:
            raise


# ── Step 2: Upload Notebooks ───────────────────────────────────────────────────

def upload_notebooks(client: DatabricksClient, notebooks_dir: Path):
    """Base64-encode each .py file and import it to the workspace."""
    py_files = sorted(notebooks_dir.glob("*.py"))
    if not py_files:
        raise FileNotFoundError(f"No .py notebooks found in {notebooks_dir}")

    log.info(f"Found {len(py_files)} notebooks to upload")
    uploaded = []

    for nb_file in py_files:
        nb_name  = nb_file.stem
        nb_path  = f"{WORKSPACE_FOLDER}/{nb_name}"
        content  = nb_file.read_bytes()
        encoded  = base64.b64encode(content).decode("utf-8")

        log.info(f"Uploading: {nb_file.name} -> {nb_path}")
        client.post("/api/2.0/workspace/import", {
            "path":      nb_path,
            "language":  "PYTHON",
            "format":    "SOURCE",
            "overwrite": True,
            "content":   encoded,
        })
        log.info(f"Uploaded: {nb_path}")
        uploaded.append(nb_path)

    return uploaded


# ── Step 3: Create Cluster ─────────────────────────────────────────────────────

def create_cluster(client: DatabricksClient, init_script_path: Optional[str] = None) -> str:
    """Create prime-capital-compute cluster. Returns cluster_id."""
    log.info(f"Creating cluster: {CLUSTER_NAME}")

    spark_conf = {
        "spark.databricks.delta.preview.enabled":          "true",
        "spark.databricks.io.cache.enabled":               "true",
        "spark.databricks.io.cache.maxDiskUsage":          "200g",
        "spark.databricks.delta.autoCompact.enabled":      "true",
        "spark.databricks.delta.optimizeWrite.enabled":    "true",
        "spark.sql.extensions":                            "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":                 "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.unityCatalog.enabled":           "true",
    }

    cluster_cfg = {
        "cluster_name":            CLUSTER_NAME,
        "spark_version":           DBR_ML_RUNTIME,
        "node_type_id":            NODE_TYPE,
        "driver_node_type_id":     NODE_TYPE,
        "autoscale": {
            "min_workers": MIN_WORKERS,
            "max_workers": MAX_WORKERS,
        },
        "autotermination_minutes": AUTO_TERM_MINUTES,
        "spark_conf":              spark_conf,
        "azure_attributes": {
            "availability":        "SPOT_WITH_FALLBACK_AZURE",
            "first_on_demand":     1,
            "spot_bid_max_price":  -1,
        },
        "libraries":               PYPI_LIBRARIES,
        "custom_tags": {
            "project":     "prime-capital-bank",
            "environment": "prod",
            "managed-by":  "deploy_databricks_resources.py",
        },
        "enable_elastic_disk": True,
        "data_security_mode": "SINGLE_USER",
    }

    if init_script_path:
        cluster_cfg["init_scripts"] = [
            {"dbfs": {"destination": f"dbfs:/prime-capital/init/{init_script_path}"}}
        ]

    result     = client.post("/api/2.0/clusters/create", cluster_cfg)
    cluster_id = result["cluster_id"]
    log.info(f"Cluster created: {CLUSTER_NAME} (ID: {cluster_id})")
    return cluster_id


# ── Step 4: Create Individual Jobs ────────────────────────────────────────────

def create_notebook_jobs(client: DatabricksClient, cluster_id: str) -> dict:
    """Create one Databricks Job per notebook. Returns {name: job_id}."""
    job_ids = {}

    for display_name, nb_basename, cron_expr, is_ml in NOTEBOOK_JOBS:
        nb_path = f"{WORKSPACE_FOLDER}/{nb_basename}"
        job_cfg = {
            "name": f"PCB — {display_name}",
            "tasks": [
                {
                    "task_key":        nb_basename,
                    "notebook_task": {
                        "notebook_path": nb_path,
                        "base_parameters": {
                            "pipeline_run_id": "{{job.run_id}}",
                            "env":             "prod",
                        },
                    },
                    "existing_cluster_id": cluster_id,
                    "timeout_seconds":     7200,
                    "libraries":          PYPI_LIBRARIES if is_ml else [],
                }
            ],
            "email_notifications": {
                "on_failure":         ["data-engineering@primecapital.co.za"],
                "no_alert_for_skipped_runs": False,
            },
            "max_retries":       1,
            "min_retry_interval_millis": 60000,
            "max_concurrent_runs":       1,
            "tags": {"project": "prime-capital-bank"},
        }

        if cron_expr:
            job_cfg["schedule"] = {
                "quartz_cron_expression": cron_expr,
                "timezone_id":            "Africa/Johannesburg",
                "pause_status":           "UNPAUSED",
            }

        result = client.post("/api/2.1/jobs/create", job_cfg)
        job_id = result["job_id"]
        job_ids[nb_basename] = job_id
        log.info(f"Job created: {display_name} (ID: {job_id})")

    return job_ids


# ── Step 5: Create Multi-Task Workflow ────────────────────────────────────────

def create_pipeline_workflow(client: DatabricksClient, cluster_id: str) -> int:
    """
    Create a single multi-task Databricks Workflow that runs the full pipeline
    with task dependencies reflecting the notebook dependency graph.
    """
    log.info("Creating full pipeline multi-task workflow...")

    tasks = [
        {
            "task_key":        "setup_unity_catalog",
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/00_setup_unity_catalog",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 1800,
        },
        {
            "task_key":        "bronze_ingestion",
            "depends_on":      [{"task_key": "setup_unity_catalog"}],
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/01_bronze_ingestion",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 10800,
            "max_retries":     2,
            "min_retry_interval_millis": 60000,
        },
        {
            "task_key":        "silver_transformation",
            "depends_on":      [{"task_key": "bronze_ingestion"}],
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/02_silver_transformation",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 7200,
            "max_retries":     1,
        },
        {
            "task_key":        "gold_star_schema",
            "depends_on":      [{"task_key": "silver_transformation"}],
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/03_gold_star_schema",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 5400,
            "max_retries":     1,
        },
        {
            "task_key":        "ml_credit_scoring",
            "depends_on":      [{"task_key": "gold_star_schema"}],
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/04_ml_credit_scoring",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 7200,
            "max_retries":     1,
            "libraries":       PYPI_LIBRARIES,
        },
        {
            "task_key":        "fraud_detection",
            "depends_on":      [{"task_key": "gold_star_schema"}],
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/05_fraud_detection",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 5400,
            "max_retries":     1,
            "libraries":       PYPI_LIBRARIES,
        },
        {
            "task_key":        "aml_risk_scoring",
            "depends_on":      [{"task_key": "silver_transformation"}],
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/06_aml_risk_scoring",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 5400,
            "max_retries":     1,
            "libraries":       PYPI_LIBRARIES,
        },
        {
            "task_key":        "regulatory_reporting",
            "depends_on":      [
                {"task_key": "gold_star_schema"},
                {"task_key": "ml_credit_scoring"},
            ],
            "notebook_task":   {"notebook_path": f"{WORKSPACE_FOLDER}/07_regulatory_reporting",
                                "base_parameters": {"env": "prod"}},
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 3600,
        },
    ]

    workflow_cfg = {
        "name": "PCB — Full Pipeline (Daily)",
        "tasks": tasks,
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",
            "timezone_id":            "Africa/Johannesburg",
            "pause_status":           "UNPAUSED",
        },
        "email_notifications": {
            "on_failure":    ["data-engineering@primecapital.co.za", "it-ops@primecapital.co.za"],
            "on_success":    ["data-engineering@primecapital.co.za"],
            "no_alert_for_skipped_runs": True,
        },
        "max_concurrent_runs": 1,
        "tags": {"project": "prime-capital-bank", "type": "full-pipeline"},
    }

    result      = client.post("/api/2.1/jobs/create", workflow_cfg)
    workflow_id = result["job_id"]
    log.info(f"Pipeline workflow created (Job ID: {workflow_id})")
    return workflow_id


# ── Step 6: Upload Init Script ─────────────────────────────────────────────────

def upload_init_script(client: DatabricksClient):
    """Upload a cluster init script to DBFS."""
    init_content = b"""#!/bin/bash
# Prime Capital Bank — Cluster Init Script
# Installs GraphFrames and custom packages

# Install GraphFrames
/databricks/python/bin/pip install --quiet graphframes

# Set Java options for large heap
echo "JAVA_OPTS='-Xmx8g -Xms4g'" >> /databricks/spark/conf/spark-env.sh

echo "Init script completed"
"""
    encoded = base64.b64encode(init_content).decode("utf-8")

    try:
        client.post("/api/2.0/dbfs/put", {
            "path":      "dbfs:/prime-capital/init/cluster_init.sh",
            "contents":  encoded,
            "overwrite": True,
        })
        log.info("Init script uploaded to DBFS")
    except Exception as e:
        log.warning(f"Init script upload failed (non-fatal): {e}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deploy Databricks resources for Prime Capital Bank")
    parser.add_argument("--workspace-url", required=True, help="Databricks workspace URL")
    parser.add_argument("--token",         required=True, help="Databricks PAT token")
    parser.add_argument("--notebooks-dir",
                        default=str(Path(__file__).parent.parent / "notebooks"),
                        help="Path to notebooks directory")
    parser.add_argument("--skip-cluster",    action="store_true", help="Reuse existing cluster ID")
    parser.add_argument("--cluster-id",      default=None, help="Existing cluster ID (if --skip-cluster)")
    args = parser.parse_args()

    client         = DatabricksClient(args.workspace_url, args.token)
    notebooks_dir  = Path(args.notebooks_dir)

    if not notebooks_dir.exists():
        log.error(f"Notebooks directory not found: {notebooks_dir}")
        sys.exit(1)

    try:
        # 1. Workspace folder
        create_workspace_folder(client, WORKSPACE_FOLDER)

        # 2. Upload notebooks
        uploaded = upload_notebooks(client, notebooks_dir)
        log.info(f"Uploaded {len(uploaded)} notebooks")

        # 3. Init script
        upload_init_script(client)

        # 4. Cluster
        if args.skip_cluster and args.cluster_id:
            cluster_id = args.cluster_id
            log.info(f"Using existing cluster: {cluster_id}")
        else:
            cluster_id = create_cluster(client, init_script_path="cluster_init.sh")

        # 5. Individual jobs
        job_ids = create_notebook_jobs(client, cluster_id)

        # 6. Full pipeline workflow
        workflow_id = create_pipeline_workflow(client, cluster_id)

        # ── Summary ──────────────────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("  Databricks Deployment — COMPLETE")
        print("=" * 60)
        print(f"  Workspace Folder : {WORKSPACE_FOLDER}")
        print(f"  Notebooks        : {len(uploaded)}")
        print(f"  Cluster ID       : {cluster_id}")
        print(f"  Individual Jobs  : {len(job_ids)}")
        print(f"  Pipeline Workflow: Job ID {workflow_id}")
        print(f"\n  Job IDs:")
        for name, jid in job_ids.items():
            print(f"    {name:<35} {jid}")
        print("=" * 60)

        # Save deployment state
        deploy_state = {
            "workspace_url": args.workspace_url,
            "cluster_id":    cluster_id,
            "workflow_id":   workflow_id,
            "job_ids":       job_ids,
            "notebooks":     uploaded,
        }
        state_path = Path(__file__).parent / "pipeline" / "deployment_state.json"
        state_path.parent.mkdir(exist_ok=True)
        state_path.write_text(json.dumps(deploy_state, indent=2))
        log.info(f"Deployment state saved: {state_path}")

    except requests.HTTPError as e:
        log.error(f"Databricks API error: {e.response.status_code} — {e.response.text}")
        sys.exit(1)
    except Exception as e:
        log.exception(f"Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
