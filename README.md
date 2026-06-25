# Prime Capital Bank — Data Intelligence Platform

**Enterprise Azure Databricks | Medallion Architecture | Delta Lake | Unity Catalog**

---

## Overview

A production-ready commercial bank data platform built on Azure Databricks with:
- 22 dimension tables + 16 fact tables (Gold star schema)
- 60+ banking products across 6 categories
- 4 ML models: PD scoring, LGD, fraud detection (real-time), AML scoring
- Full regulatory coverage: IFRS 9, Basel III, SARB BA700, POPIA/FICA
- 14M+ rows of synthetic SA banking data across 7 datasets
- Complete dbt transformation layer with staging → intermediate → mart models
- Automated Azure provisioning (Terraform + PowerShell)
- Databricks Workflow orchestration with cron scheduling

---

## Project Structure

```
Prime Capital Bank/
├── sql/ddl/                    # Bronze → Silver → Gold DDL
│   ├── 01_bronze_tables.sql    # 14 raw ingestion tables
│   ├── 02_silver_tables.sql    # 11 cleansed/enriched tables
│   ├── 03_gold_star_schema.sql # 22 dims + 16 facts
│   └── 04_gold_views.sql       # 15 analytical views
│
├── dbt/                        # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/            # 8 staging models
│   │   ├── intermediate/       # 5 intermediate models
│   │   └── marts/              # 6 mart models
│   ├── macros/                 # exchange_rate, risk_segment, income_band
│   └── tests/                  # custom data quality tests
│
├── notebooks/                  # Databricks Python notebooks
│   ├── 00_setup_unity_catalog.py
│   ├── 01_bronze_ingestion.py  # Auto Loader / cloudFiles
│   ├── 02_silver_transformation.py
│   ├── 03_gold_star_schema.py  # SCD2 merges, Z-ordering
│   ├── 04_ml_credit_scoring.py # LightGBM PD/LGD + MLflow
│   ├── 05_fraud_detection.py   # Real-time streaming + ensemble
│   ├── 06_aml_risk_scoring.py  # GraphFrames + SAR scoring
│   ├── 07_regulatory_reporting.py # SARB BA700 + IFRS9 reports
│   └── 08_pipeline_orchestrator.py
│
├── scripts/
│   ├── azure/
│   │   ├── provision_infrastructure.ps1  # Full Azure provisioning
│   │   ├── deploy_databricks_resources.py
│   │   ├── upload_notebooks.py           # REST API notebook upload
│   │   ├── schedule_pipeline.py          # Create Databricks Workflow
│   │   └── run_pipeline.py               # Trigger + monitor pipeline
│   └── data_gen/
│       ├── generate_bank_data.py         # 14M+ row synthetic data generator
│       └── upload_to_adls.py             # Upload CSV → ADLS Gen2
│
├── data/                       # Generated synthetic datasets
│   ├── customers.csv           # 500K SA customers
│   ├── accounts.csv            # 1.2M accounts
│   ├── transactions.csv        # 10M transactions
│   ├── loans.csv               # 300K loans
│   ├── card_transactions.csv   # 2M card transactions
│   ├── payments.csv            # 500K payments
│   └── fraud_alerts.csv        # 50K fraud alerts
│
├── terraform/
│   └── main.tf                 # IaC: ADLS, Databricks, SQL, Key Vault
│
├── pipeline/
│   └── pipeline_config.json    # Master config (schedules, tables, MLflow)
│
├── docs/
│   ├── ebook/
│   │   ├── index.html          # 12-chapter technical ebook
│   │   └── assets/
│   │       ├── PCB_COMPLETE_ERD.html     # Full ERD (22 dims, 16 facts)
│   │       ├── architecture_diagram.svg  # Azure architecture
│   │       └── erd_gold_schema.svg       # Star schema ERD
│   ├── data_dictionary/
│   │   └── data_dictionary.md  # 40-table column-level documentation
│   └── pdf_exports/
│       └── README.md           # PDF conversion instructions
│
└── presentation/
    └── prime_capital_bank_overview.html  # 15-slide executive deck
```

---

## Quick Start

### 1. Generate Synthetic Data
```powershell
pip install pandas numpy faker
python "scripts\data_gen\generate_bank_data.py"
# Output: data\ folder, ~14M rows across 7 CSV files (~2-4 GB)
```

### 2. Provision Azure Infrastructure
```powershell
# Option A: Azure CLI script
$env:AZURE_SUBSCRIPTION_ID = "<your-subscription-id>"
.\scripts\azure\provision_infrastructure.ps1

# Option B: Terraform
cd terraform
terraform init
terraform plan -var="subscription_id=<id>" -var="admin_password=<pwd>"
terraform apply
```

### 3. Upload Data to ADLS
```powershell
$env:ADLS_ACCOUNT_KEY = "<storage-account-key>"
python "scripts\data_gen\upload_to_adls.py"
```

### 4. Upload Notebooks to Databricks
```powershell
$env:DATABRICKS_HOST  = "https://<workspace>.azuredatabricks.net"
$env:DATABRICKS_TOKEN = "<pat-token>"
python "scripts\azure\upload_notebooks.py"
```

### 5. Deploy SQL Schema
Run in Databricks SQL Editor or notebook:
```sql
-- In order:
%run /Prime Capital Bank/00_setup_unity_catalog
-- Then execute the DDL files in sql/ddl/ order
```

### 6. Schedule Pipeline
```powershell
python "scripts\azure\schedule_pipeline.py"
# Outputs job IDs — save them to pipeline_config.json
```

### 7. Run Pipeline Now
```powershell
$env:JOB_ID_PIPELINE = "<job-id-from-step-6>"
python "scripts\azure\run_pipeline.py"
```

---

## Data Model Summary

| Layer | Tables | Rows (target) | Refresh |
|-------|--------|--------------|---------|
| Bronze | 14 | 14M+ | Daily / Hourly |
| Silver | 11 | 14M+ | Daily / Hourly |
| Gold Dims | 22 | ~4M total | Daily SCD2 |
| Gold Facts | 16 | 15M+ / year | Daily / Streaming |

---

## ML Models

| Model | Algorithm | Target | AUC / Metric |
|-------|-----------|--------|--------------|
| Credit PD | LightGBM | P(default 90DPD) | AUC > 0.78 |
| Credit LGD | XGBoost | Loss given default | R² > 0.65 |
| Fraud Detection | IF + GBM ensemble | Card fraud | FPR < 0.1% |
| AML Scoring | GraphFrames + GBM | SAR candidate | Precision > 0.70 |

---

## Regulatory Coverage

| Standard | Coverage |
|----------|----------|
| IFRS 9 | Stage 1/2/3 classification, ECL computation (PD × LGD × EAD) |
| Basel III | RWA, CET1, Tier 1 ratio, leverage ratio, LCR, NSFR |
| SARB BA700 | Capital adequacy return — automated monthly export |
| POPIA | PII masking in silver layer, data retention policies |
| FICA | KYC/FICA compliance tracking in dim_kyc_status |
| AML/CFT | Transaction monitoring, SAR filing pipeline |

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Azure Databricks (Premium) |
| Storage | Azure Data Lake Storage Gen2 |
| Table Format | Delta Lake |
| Governance | Unity Catalog |
| Secrets | Azure Key Vault |
| Transformation | dbt-databricks |
| ML | MLflow + LightGBM + GraphFrames |
| Streaming | Structured Streaming (Auto Loader) |
| IaC | Terraform + Azure CLI |
| BI | Power BI (DirectQuery on Azure SQL) |
| Monitoring | Azure Log Analytics |

---

## Contact

**Anthony Apollis** · anthony@the-spot.tech  
Prime Capital Bank Data Intelligence Platform · v2.0 · 2026
