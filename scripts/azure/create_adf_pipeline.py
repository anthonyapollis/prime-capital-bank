"""
Create all ADF pipelines for Prime Capital Bank:
  1. pl_ingest_raw_to_adls   — Copy raw CSV files into ADLS containers
  2. pl_bronze_to_silver      — Trigger Databricks bronze + silver notebooks
  3. pl_silver_to_gold        — Trigger gold star schema + views
  4. pl_ml_models             — Trigger ML credit, fraud, AML notebooks (parallel)
  5. pl_regulatory_reports    — Trigger regulatory reporting notebook
  6. pl_master_orchestrator   — Master pipeline that calls all sub-pipelines in sequence
"""

import os
import json
import requests

FACTORY   = "adf-prime-capital-prod"
RG        = "rg-prime-capital-prod"
SUB       = "cea67e6f-62b2-4b2f-83e8-9af31093d8c8"
DBX_HOST  = "https://adb-7405610635095106.6.azuredatabricks.net"

MGMT_TOKEN = os.environ.get("MGMT_TOKEN", "")

BASE_URL = f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{FACTORY}"
HEADERS  = {
    "Authorization": f"Bearer {MGMT_TOKEN}",
    "Content-Type": "application/json"
}

def put_pipeline(name: str, body: dict):
    url = f"{BASE_URL}/pipelines/{name}?api-version=2018-06-01"
    r = requests.put(url, headers=HEADERS, json=body, timeout=60)
    if r.status_code in (200, 201):
        print(f"  ✓ Pipeline '{name}' created/updated")
    else:
        print(f"  ✗ Pipeline '{name}' failed: {r.status_code} — {r.text[:200]}")
    return r.json()

def notebook_activity(name: str, notebook_path: str, depends_on: list = None) -> dict:
    act = {
        "name": name,
        "type": "DatabricksNotebook",
        "linkedServiceName": {
            "referenceName": "ls_databricks_primecapital",
            "type": "LinkedServiceReference"
        },
        "typeProperties": {
            "notebookPath": f"/Prime Capital Bank/{notebook_path}"
        },
        "dependsOn": [],
        "policy": {
            "timeout": "02:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60
        }
    }
    if depends_on:
        act["dependsOn"] = [
            {"activity": d, "dependencyConditions": ["Succeeded"]}
            for d in depends_on
        ]
    return act

def pipeline_activity(name: str, pipeline_name: str, depends_on: list = None) -> dict:
    act = {
        "name": name,
        "type": "ExecutePipeline",
        "typeProperties": {
            "pipeline": {
                "referenceName": pipeline_name,
                "type": "PipelineReference"
            },
            "waitOnCompletion": True
        },
        "dependsOn": []
    }
    if depends_on:
        act["dependsOn"] = [
            {"activity": d, "dependencyConditions": ["Succeeded"]}
            for d in depends_on
        ]
    return act


def main():
    print("\nCreating ADF Pipelines for Prime Capital Bank")
    print("=" * 55)

    # 1. Setup + Bronze ingestion pipeline
    put_pipeline("pl_01_bronze_ingestion", {
        "properties": {
            "description": "Run Unity Catalog setup and Bronze Auto Loader ingestion",
            "activities": [
                notebook_activity("setup_unity_catalog", "00_setup_unity_catalog"),
                notebook_activity("bronze_ingestion", "01_bronze_ingestion", ["setup_unity_catalog"])
            ]
        }
    })

    # 2. Silver transformation pipeline
    put_pipeline("pl_02_silver_transformation", {
        "properties": {
            "description": "Cleanse, deduplicate and enrich raw data into Silver layer",
            "activities": [
                notebook_activity("silver_transformation", "02_silver_transformation")
            ]
        }
    })

    # 3. Gold star schema pipeline
    put_pipeline("pl_03_gold_star_schema", {
        "properties": {
            "description": "Build SCD2 dimensions and partitioned fact tables (Gold layer)",
            "activities": [
                notebook_activity("gold_star_schema", "03_gold_star_schema")
            ]
        }
    })

    # 4. ML models pipeline (credit + fraud + AML run in parallel)
    put_pipeline("pl_04_ml_models", {
        "properties": {
            "description": "Train/score PD, LGD, Fraud Detection and AML models (parallel)",
            "activities": [
                notebook_activity("ml_credit_scoring", "04_ml_credit_scoring"),
                notebook_activity("fraud_detection",   "05_fraud_detection"),
                notebook_activity("aml_risk_scoring",  "06_aml_risk_scoring")
            ]
        }
    })

    # 5. Regulatory reporting pipeline
    put_pipeline("pl_05_regulatory_reports", {
        "properties": {
            "description": "Generate SARB BA700, IFRS9 ECL, Basel III RWA reports",
            "activities": [
                notebook_activity("regulatory_reporting", "07_regulatory_reporting")
            ]
        }
    })

    # 6. Master orchestrator pipeline
    put_pipeline("pl_00_master_orchestrator", {
        "properties": {
            "description": "Master pipeline: Bronze → Silver → Gold → ML → Regulatory (sequential)",
            "activities": [
                pipeline_activity("run_bronze",      "pl_01_bronze_ingestion"),
                pipeline_activity("run_silver",      "pl_02_silver_transformation", ["run_bronze"]),
                pipeline_activity("run_gold",        "pl_03_gold_star_schema",      ["run_silver"]),
                pipeline_activity("run_ml",          "pl_04_ml_models",             ["run_gold"]),
                pipeline_activity("run_regulatory",  "pl_05_regulatory_reports",    ["run_ml"])
            ]
        }
    })

    print("\n✓ All 6 ADF pipelines created.")
    print(f"  View in Azure Portal:")
    print(f"  https://adf.azure.com/en/authoring/pipeline")

if __name__ == "__main__":
    main()
