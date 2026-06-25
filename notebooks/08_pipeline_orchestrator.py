# Databricks notebook source
# Prime Capital Bank — Pipeline Orchestrator
# Notebook: 08_pipeline_orchestrator.py
# Author:   Prime Capital Bank — Data Engineering
# Purpose:  Master orchestrator. Sequences all pipeline notebooks via Databricks
#           Workflows API. Logs run status to control table. Sends email alerts
#           on failure via SendGrid. Reports SLA compliance.


# COMMAND ----------

import uuid, logging, time, json, requests
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline_orchestrator")

# ── Configuration ──────────────────────────────────────────────────────────────
BRONZE           = "bronze"
RUN_ID           = str(uuid.uuid4())
PIPELINE_START   = datetime.now(timezone.utc)

# Databricks workspace config (read from secrets or environment)
try:
    WORKSPACE_URL    = dbutils.secrets.get("prime-capital-secrets", "databricks-workspace-url")
    DATABRICKS_TOKEN = dbutils.secrets.get("prime-capital-secrets", "databricks-token")
except Exception:
    # Widget fallback for manual runs
    WORKSPACE_URL    = dbutils.widgets.get("workspace_url") if "workspace_url" in \
                       [w.name for w in dbutils.widgets.getAll()] else "https://adb-XXXX.azuredatabricks.net"
    DATABRICKS_TOKEN = "REPLACE_WITH_PAT"

CLUSTER_ID       = dbutils.widgets.get("cluster_id") if "cluster_id" in \
                   [w.name for w in dbutils.widgets.getAll()] else "REPLACE_WITH_CLUSTER_ID"

# SendGrid alert config
try:
    SENDGRID_API_KEY = dbutils.secrets.get("prime-capital-secrets", "sendgrid-api-key")
except Exception:
    SENDGRID_API_KEY = None

ALERT_FROM       = "databricks-pipeline@primecapital.co.za"
ALERT_TO         = ["data-engineering@primecapital.co.za", "it-ops@primecapital.co.za"]
SLA_HOURS        = 4.0    # full pipeline must complete within 4 hours

# ── Notebook registry ─────────────────────────────────────────────────────────
# (name, notebook_path, timeout_minutes, retries, depends_on)
PIPELINE = [
    ("00_setup",         "/Prime Capital Bank/00_setup_unity_catalog",  30,  0, []),
    ("01_bronze",        "/Prime Capital Bank/01_bronze_ingestion",     180, 2, ["00_setup"]),
    ("02_silver",        "/Prime Capital Bank/02_silver_transformation", 120, 1, ["01_bronze"]),
    ("03_gold",          "/Prime Capital Bank/03_gold_star_schema",      90, 1, ["02_silver"]),
    ("04_credit_scoring","/Prime Capital Bank/04_ml_credit_scoring",    120, 1, ["03_gold"]),
    ("05_fraud",         "/Prime Capital Bank/05_fraud_detection",       90, 1, ["03_gold"]),
    ("06_aml",           "/Prime Capital Bank/06_aml_risk_scoring",      90, 1, ["02_silver"]),
    ("07_regulatory",    "/Prime Capital Bank/07_regulatory_reporting",  60, 0, ["03_gold","04_credit_scoring"]),
]

# COMMAND ----------

# -- ## Helper — Databricks Jobs REST Client

# COMMAND ----------

class DatabricksJobsClient:
    def __init__(self, workspace_url: str, token: str):
        self.base = workspace_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }

    def _post(self, path: str, payload: dict) -> dict:
        resp = requests.post(f"{self.base}{path}", headers=self.headers,
                             json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _get(self, path: str) -> dict:
        resp = requests.get(f"{self.base}{path}", headers=self.headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def run_notebook(self, notebook_path: str, cluster_id: str,
                     timeout_s: int = 7200, params: dict = None) -> int:
        payload = {
            "run_name":        f"PCB_{notebook_path.split('/')[-1]}_{RUN_ID[:8]}",
            "existing_cluster_id": cluster_id,
            "timeout_seconds": timeout_s,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": params or {},
            },
        }
        result = self._post("/api/2.1/jobs/runs/submit", payload)
        return result["run_id"]

    def get_run_state(self, run_id: int) -> dict:
        return self._get(f"/api/2.1/jobs/runs/get?run_id={run_id}")

    def wait_for_run(self, run_id: int, poll_interval_s: int = 30,
                     timeout_s: int = 86400) -> str:
        """Poll until run completes. Returns life_cycle_state."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            state = self.get_run_state(run_id)
            lc    = state["state"]["life_cycle_state"]
            rs    = state["state"].get("result_state", "")
            log.info(f"Run {run_id}: {lc} / {rs}")
            if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                return rs or lc
            time.sleep(poll_interval_s)
        return "TIMEOUT"

# COMMAND ----------

# -- ## Helper — SendGrid Email Alert

# COMMAND ----------

def send_alert(subject: str, body: str, is_failure: bool = True):
    if not SENDGRID_API_KEY:
        log.warning("SendGrid API key not configured — skipping email alert")
        return
    try:
        payload = {
            "personalizations": [{"to": [{"email": e} for e in ALERT_TO]}],
            "from":    {"email": ALERT_FROM, "name": "Prime Capital Bank Pipeline"},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type":  "application/json",
            },
            json=payload, timeout=30,
        )
        resp.raise_for_status()
        log.info(f"Alert email sent: {subject}")
    except Exception as e:
        log.error(f"Failed to send alert email: {e}")

# COMMAND ----------

# -- ## Helper — Control Table Logging

# COMMAND ----------

def log_step(step_name: str, status: str, run_id_db: Optional[int] = None,
             error: str = None, duration_s: float = None):
    err_val = f"'{error[:500]}'" if error else "NULL"
    dur_val = str(round(duration_s, 1)) if duration_s else "NULL"
    spark.sql(f"""
        MERGE INTO `{BRONZE}`.`pipeline_control` AS t
        USING (SELECT
            '{RUN_ID}'            AS run_id,
            '08_orchestrator'     AS notebook_name,
            '{step_name}'         AS table_name,
            '{status}'            AS run_status,
            {run_id_db or "NULL"} AS rows_ingested,
            NULL                  AS schema_changes,
            {err_val}             AS error_message,
            current_timestamp()   AS started_at,
            current_timestamp()   AS completed_at
        ) AS s
        ON t.run_id = s.run_id AND t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            t.run_status = s.run_status,
            t.error_message = s.error_message,
            t.completed_at = s.completed_at
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# -- ## Main — Execute Pipeline

# COMMAND ----------

client    = DatabricksJobsClient(WORKSPACE_URL, DATABRICKS_TOKEN)
completed = {}   # step_name -> result_state
failed    = []

for step_name, nb_path, timeout_min, retries, depends_on in PIPELINE:

    # Check dependencies
    dep_ok = all(completed.get(d) == "SUCCESS" for d in depends_on)
    if depends_on and not dep_ok:
        failed_deps = [d for d in depends_on if completed.get(d) != "SUCCESS"]
        log.warning(f"Skipping {step_name} — dependencies failed: {failed_deps}")
        completed[step_name] = "SKIPPED"
        log_step(step_name, "SKIPPED", error=f"Dependencies failed: {failed_deps}")
        continue

    step_start = time.time()
    attempt    = 0
    result     = None

    while attempt <= retries:
        try:
            log.info(f"Launching {step_name} (attempt {attempt+1}/{retries+1})")
            db_run_id = client.run_notebook(
                nb_path, CLUSTER_ID,
                timeout_s = timeout_min * 60,
                params    = {"pipeline_run_id": RUN_ID},
            )
            log_step(step_name, "RUNNING", run_id_db=db_run_id)
            result = client.wait_for_run(db_run_id, timeout_s=timeout_min * 60 + 300)

            if result == "SUCCESS":
                break
            elif attempt < retries:
                wait_s = 60 * (attempt + 1)
                log.warning(f"{step_name} failed ({result}) — retrying in {wait_s}s")
                time.sleep(wait_s)
        except Exception as e:
            result = "ERROR"
            log.error(f"{step_name} attempt {attempt+1} exception: {e}")
            if attempt < retries:
                time.sleep(60)
        attempt += 1

    duration = time.time() - step_start
    completed[step_name] = result if result else "UNKNOWN"

    if result == "SUCCESS":
        log_step(step_name, "SUCCESS", duration_s=duration)
        log.info(f"{step_name} SUCCESS in {duration/60:.1f} min")
    else:
        error_msg = f"Step {step_name} failed after {retries+1} attempt(s): {result}"
        log_step(step_name, "FAILED", error=error_msg, duration_s=duration)
        failed.append(step_name)
        send_alert(
            subject=f"[ALERT] Prime Capital Bank Pipeline — {step_name} FAILED",
            body=(
                f"Pipeline Run ID : {RUN_ID}\n"
                f"Failed Step     : {step_name}\n"
                f"Result State    : {result}\n"
                f"Duration        : {duration/60:.1f} min\n"
                f"Timestamp       : {datetime.now(timezone.utc).isoformat()}\n\n"
                f"Please review Databricks job run for details.\n"
                f"Workspace: {WORKSPACE_URL}"
            ),
        )

# COMMAND ----------

# -- ## SLA Reporting

# COMMAND ----------

PIPELINE_END     = datetime.now(timezone.utc)
PIPELINE_ELAPSED = (PIPELINE_END - PIPELINE_START).total_seconds() / 3600.0
SLA_MET          = PIPELINE_ELAPSED <= SLA_HOURS and len(failed) == 0

print("\n" + "=" * 65)
print(f"  PRIME CAPITAL BANK — PIPELINE ORCHESTRATION SUMMARY")
print("=" * 65)
print(f"  Run ID          : {RUN_ID}")
print(f"  Start           : {PIPELINE_START.strftime('%Y-%m-%d %H:%M UTC')}")
print(f"  End             : {PIPELINE_END.strftime('%Y-%m-%d %H:%M UTC')}")
print(f"  Elapsed         : {PIPELINE_ELAPSED:.2f} hours")
print(f"  SLA ({SLA_HOURS:.0f}h target) : {'MET' if SLA_ELAPSED := PIPELINE_ELAPSED <= SLA_HOURS else 'BREACHED'}")
print(f"  Steps total     : {len(PIPELINE)}")
print(f"  Steps succeeded : {sum(1 for v in completed.values() if v == 'SUCCESS')}")
print(f"  Steps failed    : {len(failed)}")
print(f"  Overall Status  : {'SUCCESS' if not failed else 'FAILED — ' + str(failed)}")
print("=" * 65)

for step_name, _, _, _, _ in PIPELINE:
    status = completed.get(step_name, "NOT_RUN")
    icon   = "OK" if status == "SUCCESS" else ("--" if status == "SKIPPED" else "FAIL")
    print(f"  [{icon}] {step_name:<25} {status}")

if failed:
    send_alert(
        subject=f"[PIPELINE FAILED] Prime Capital Bank — {len(failed)} step(s) failed",
        body=(
            f"Run ID   : {RUN_ID}\n"
            f"Failed   : {failed}\n"
            f"Elapsed  : {PIPELINE_ELAPSED:.2f} hours\n"
            f"SLA Met  : {SLA_MET}\n"
            f"Time     : {PIPELINE_END.isoformat()}"
        ),
    )
    raise RuntimeError(f"Pipeline failed on: {failed}")
elif not SLA_MET:
    send_alert(
        subject=f"[SLA BREACH] Prime Capital Bank pipeline exceeded {SLA_HOURS}h SLA",
        body=f"Elapsed: {PIPELINE_ELAPSED:.2f}h | Run ID: {RUN_ID}",
        is_failure=False,
    )

print(f"\n08_pipeline_orchestrator — COMPLETE")
