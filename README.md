# Prime Capital Bank — Data Intelligence Platform

**Enterprise Azure Databricks | Medallion Architecture | Delta Lake | Unity Catalog**

> Completed project by Anthony Apollis · [anthony.apollis@gmail.com](mailto:anthony.apollis@gmail.com)

---

## Overview

A production-ready commercial bank data platform built on Azure Databricks for a fictional Fortune 500
South African retail and corporate bank. The platform consolidates twelve previously siloed source
systems into a single governed, cloud-native data intelligence layer.

| Metric | Value |
|--------|-------|
| Customers | 500,000 SA retail & corporate |
| Transactions | 12M+ (annual) |
| Loan Portfolio | R500B outstanding |
| Branch Network | 200 branches across 9 provinces |
| ML Models | 12 (credit, fraud, AML, CLV, churn, NLP, anomaly detection…) |
| Fintech Partners | 5 (Yoco, SnapScan, PayFast, Peach Payments, PayGate) |
| Regulatory Coverage | IFRS 9, Basel III, SARB BA700, POPIA, FICA, AML/CFT |

---

## Project Structure

```
Prime Capital Bank/
│
├── docs/
│   ├── index.html                        # Project hub — links all deliverables
│   ├── ebook/
│   │   ├── index.html                    # 12-chapter technical ebook (Chart.js charts)
│   │   ├── chapters/                     # Chapter index & PDF export guide
│   │   └── assets/
│   │       ├── PCB_COMPLETE_ERD.html     # 6-tab interactive ERD (Mermaid.js, zoom/pan)
│   │       ├── PCB_MERCHANT_MAP.html     # Merchant & fraud intelligence map (Leaflet.js)
│   │       ├── architecture_diagram.svg  # Azure medallion architecture
│   │       └── erd_gold_schema.svg       # Gold star schema overview
│   ├── data_dictionary/
│   │   └── index.html                    # Searchable column-level docs (15 Gold tables)
│   ├── Prime_Capital_Bank_Analytics.xlsx # Excel KPI + analytics workbook
│   └── pdf_exports/
│       └── README.md                     # Chrome headless PDF export instructions
│
├── sql/
│   ├── ddl/
│   │   ├── 01_bronze_tables.sql          # 14 raw ingestion tables
│   │   ├── 02_silver_tables.sql          # 11 cleansed/enriched tables
│   │   ├── 03_gold_star_schema.sql       # Star schema + DIM_FINTECH + FACT_FINTECH_SETTLEMENT
│   │   └── 04_gold_views.sql             # 15 analytical views
│   ├── views/
│   │   ├── vw_customer_360.sql           # Unified customer profile with CLV score
│   │   ├── vw_fraud_dashboard.sql        # Real-time fraud + province heatmap
│   │   ├── vw_fintech_settlement_summary.sql  # Monthly partner performance + QoQ growth
│   │   └── vw_branch_performance.sql     # Branch KPIs: deposits, NII, NPL, fraud exposure
│   └── dml/
│       ├── 01_seed_reference_data.sql    # Currencies, 23 branches, date stubs
│       └── 02_sample_queries.sql         # 8 Gold layer analyst queries
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/                      # 8 staging models (stg_*)
│   │   ├── intermediate/                 # 5 intermediate models (int_*)
│   │   └── marts/                        # 6 mart models (mart_*)
│   ├── macros/                           # exchange_rate, risk_segment
│   └── tests/                            # Custom data quality assertions
│
├── notebooks/                            # Databricks Python notebooks
│   ├── 00_setup_unity_catalog.py
│   ├── 01_bronze_ingestion.py            # Auto Loader / cloudFiles
│   ├── 02_silver_transformation.py
│   ├── 03_gold_star_schema.py            # SCD2 merges, Z-ordering, fintech load
│   ├── 04_ml_credit_scoring.py           # LightGBM PD/LGD + MLflow
│   ├── 05_fraud_detection.py             # Real-time streaming + ensemble
│   ├── 06_aml_risk_scoring.py            # GraphFrames + SAR scoring
│   ├── 07_regulatory_reporting.py        # SARB BA700 + IFRS 9 reports
│   └── 08_pipeline_orchestrator.py
│
├── scripts/
│   ├── azure/
│   │   ├── provision_infrastructure.ps1  # Full Azure provisioning
│   │   ├── deploy_databricks_resources.py
│   │   ├── upload_notebooks.py
│   │   ├── create_adf_pipeline.py
│   │   ├── schedule_pipeline.py
│   │   └── run_pipeline.py
│   └── data_gen/
│       ├── generate_bank_data.py         # 12M+ row synthetic data generator
│       └── upload_to_adls.py
│
├── data/                                 # Generated synthetic datasets (CSV)
│   ├── customers.csv                     # 500K SA customers (9 provinces, real names)
│   ├── accounts.csv                      # 820K accounts
│   ├── transactions.csv                  # 10M+ transactions
│   ├── loans.csv                         # 280K loans (IFRS 9 staging)
│   ├── card_transactions.csv             # 3.2M card transactions
│   ├── payments.csv                      # 1.8M EFT/SWIFT/PayShap payments
│   ├── fraud_alerts.csv                  # 48K ML-generated fraud alerts
│   ├── fintech_partners.csv              # 5 SA fintech partners (DIM seed)
│   └── fintech_settlements.csv           # 60 monthly settlement records
│
├── terraform/
│   └── main.tf                           # IaC: ADLS, Databricks, SQL, Key Vault
│
├── pipeline/
│   └── pipeline_config.json              # Master config (schedules, tables, MLflow)
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
# Output: data\ folder — 9 CSV files, ~12M+ rows
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
```sql
-- In Databricks SQL Editor, run DDL files in order:
-- sql/ddl/01_bronze_tables.sql
-- sql/ddl/02_silver_tables.sql
-- sql/ddl/03_gold_star_schema.sql  (includes fintech seed INSERTs)
-- sql/ddl/04_gold_views.sql
```

### 6. Schedule & Run Pipeline
```powershell
python "scripts\azure\schedule_pipeline.py"   # creates Databricks Workflow job
$env:JOB_ID_PIPELINE = "<job-id>"
python "scripts\azure\run_pipeline.py"         # trigger + monitor
```

---

## Data Model Summary

| Layer | Tables | Rows (target) | Refresh |
|-------|--------|--------------|---------|
| Bronze | 14 | 12M+ | Daily / Streaming |
| Silver | 11 | 12M+ | Daily |
| Gold — Dimensions | 15 | ~3M total | Daily SCD1/SCD2 |
| Gold — Facts | 7+ | 15M+ / year | Daily / Streaming |

---

## ML Models (12)

| Model | Algorithm | Target Metric |
|-------|-----------|--------------|
| Credit PD Scoring | LightGBM | AUC 0.847 |
| Credit LGD | XGBoost | R² 0.71 |
| Fraud Detection | Random Forest ensemble | AUC 0.921 |
| AML Risk Scoring | Gradient Boost + GraphFrames | AUC 0.889 |
| Churn Prediction | Logistic Regression | Accuracy 78.3% |
| CLV Segmentation | K-Means (k=5) | Silhouette 0.74 |
| Transaction Anomaly | Isolation Forest | Precision@K 0.876 |
| NLP Sentiment | BERT fine-tune | F1 83.1% |
| Loan Default (PD) | XGBoost (IFRS 9) | AUC 0.832 |
| Behavioural Scoring | Gradient Boost | AUC 0.801 |
| Collections Propensity | Random Forest | Precision 0.79 |
| Debit Order Abuse | XGBoost | F1 0.84 |

---

## SA Fintech Partners

| ID | Partner | Type | MDR | Min Fee | Annual Volume |
|----|---------|------|-----|---------|---------------|
| FT001 | Yoco | POS Card Acquiring | 2.95% | — | R12.4B |
| FT002 | SnapScan | QR Code Payments | 2.75% | — | R8.7B |
| FT003 | PayFast | E-commerce Gateway | 3.50% | R2.00 | R15.2B |
| FT004 | Peach Payments | API Payment Platform | 2.90% | R1.50 | R6.8B |
| FT005 | PayGate | Enterprise Gateway | 2.80% | R1.00 | R22.1B |

---

## Regulatory Coverage

| Standard | Coverage |
|----------|----------|
| IFRS 9 | Stage 1/2/3 classification, ECL computation (PD × LGD × EAD) |
| Basel III | RWA, CET1, Tier 1 ratio, leverage ratio, LCR, NSFR |
| SARB BA700 | Capital adequacy return — automated monthly export |
| POPIA | PII masking in Silver layer, data retention policies |
| FICA | KYC/FICA compliance tracking, onboarding status |
| AML/CFT | Transaction monitoring, SAR filing pipeline, FIC reporting |

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Azure Databricks (Premium) |
| Storage | Azure Data Lake Storage Gen2 |
| Table Format | Delta Lake |
| Governance | Unity Catalog |
| Secrets | Azure Key Vault |
| Transformation | dbt Core (dbt-databricks) |
| ML | MLflow + LightGBM + XGBoost + GraphFrames |
| Streaming | Structured Streaming (Auto Loader) |
| IaC | Terraform + Azure CLI |
| BI | Power BI (DirectQuery on Azure SQL) |
| Visualisation | Chart.js, Leaflet.js, Mermaid.js |
| Monitoring | Azure Log Analytics |

---

## Deliverables

| Deliverable | Path | Description |
|-------------|------|-------------|
| Project Hub | `docs/index.html` | Central navigation with KPIs, pipeline timeline, ML scorecards |
| Technical Ebook | `docs/ebook/index.html` | 12 chapters, Chart.js charts, SA fintech ecosystem |
| Interactive ERD | `docs/ebook/assets/PCB_COMPLETE_ERD.html` | 6-domain Mermaid ERD with zoom/pan |
| Merchant Map | `docs/ebook/assets/PCB_MERCHANT_MAP.html` | Leaflet.js SA fraud & sales heatmap |
| Data Dictionary | `docs/data_dictionary/index.html` | Searchable column docs, 15 Gold tables |
| Analytics Workbook | `docs/Prime_Capital_Bank_Analytics.xlsx` | Dynamic Excel KPI model |
| Executive Deck | `presentation/prime_capital_bank_overview.html` | 15-slide investor presentation |

---

## Contact

**Anthony Apollis** · [anthony.apollis@gmail.com](mailto:anthony.apollis@gmail.com)  
Prime Capital Bank Data Intelligence Platform · v3.0 · 2026
