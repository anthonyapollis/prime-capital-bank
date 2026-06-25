-- =============================================================================
-- PRIME CAPITAL BANK — SILVER LAYER (Cleansed, Typed & Enriched)
-- Unity Catalog / Delta Lake — Databricks SQL
-- Layer   : Silver (validated, deduplicated, type-cast, business-logic enriched)
-- Author  : Data Engineering Team
-- Created : 2026-06-24
-- Notes   : Silver tables consume from Bronze via structured streaming or
--           scheduled batch jobs. All amounts are cast to DECIMAL(18,2) in ZAR.
--           Derived / computed columns are materialised here for downstream
--           performance. PII fields are masked or hashed where required for
--           non-privileged access patterns.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. silver_customers
--    Source: bronze_customers (deduplicated, type-cast, derived fields)
--    SCD Type 2 — full history retained via effective_date / expiry_date
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_customers;

CREATE TABLE prime_capital.silver.silver_customers (
  -- Surrogate and natural keys
  customer_sk                 BIGINT        COMMENT 'Silver surrogate key (auto-incrementing)',
  customer_id                 STRING        COMMENT 'Source CRM customer identifier (UUID)',
  legacy_customer_id          STRING        COMMENT 'Legacy core banking customer number',
  -- SCD2 control columns
  effective_date              DATE          COMMENT 'Date this version of the record became effective',
  expiry_date                 DATE          COMMENT 'Date this version expired (9999-12-31 = current)',
  is_current                  BOOLEAN       COMMENT 'True if this is the currently active record version',
  -- Personal demographics (typed)
  title                       STRING        COMMENT 'Title: Mr/Mrs/Ms/Dr/Prof',
  first_name                  STRING        COMMENT 'Legal first name (trimmed and title-cased)',
  middle_name                 STRING        COMMENT 'Middle name(s)',
  last_name                   STRING        COMMENT 'Legal surname (trimmed and title-cased)',
  full_name                   STRING        COMMENT 'Derived: CONCAT(first_name, middle_name, last_name)',
  preferred_name              STRING        COMMENT 'Preferred name',
  date_of_birth               DATE          COMMENT 'Validated date of birth (cast from source)',
  -- DERIVED: age and age bands
  age_years                   INT           COMMENT 'Derived: DATEDIFF(CURRENT_DATE, date_of_birth) / 365',
  age_band                    STRING        COMMENT 'Derived age band: 18-25/26-35/36-45/46-55/56-65/65+',
  gender                      STRING        COMMENT 'Standardised gender: MALE/FEMALE/NON_BINARY/UNKNOWN',
  nationality                 STRING        COMMENT 'ISO 3166-1 alpha-2 nationality code',
  country_of_birth            STRING        COMMENT 'ISO 3166-1 alpha-2 country of birth',
  home_language               STRING        COMMENT 'Standardised language code',
  marital_status              STRING        COMMENT 'Standardised: SINGLE/MARRIED/DIVORCED/WIDOWED/UNKNOWN',
  -- KYC / identity (validated)
  id_type                     STRING        COMMENT 'Standardised: SA_ID/PASSPORT/ASYLUM/TEMP_PERMIT',
  id_number_hashed            STRING        COMMENT 'SHA-256 hash of ID number for non-privileged access',
  tax_number_hashed           STRING        COMMENT 'SHA-256 hash of tax reference number',
  fica_status                 STRING        COMMENT 'Standardised FICA status: COMPLIANT/PENDING/EXPIRED/EXEMPTED',
  fica_expiry_date            DATE          COMMENT 'Cast and validated FICA expiry date',
  fica_verified_date          DATE          COMMENT 'Cast and validated FICA verification date',
  fica_days_to_expiry         INT           COMMENT 'Derived: days until FICA documentation expires',
  kyc_risk_rating             STRING        COMMENT 'Standardised KYC risk: LOW/MEDIUM/HIGH',
  pep_flag                    BOOLEAN       COMMENT 'Politically Exposed Person flag',
  pep_category                STRING        COMMENT 'PEP category description',
  sanctions_flag              BOOLEAN       COMMENT 'Sanctions list flag',
  -- Contact (validated)
  email_address               STRING        COMMENT 'Validated and lower-cased primary email',
  mobile_number               STRING        COMMENT 'Validated mobile number in E.164 format',
  -- Address (standardised)
  residential_address_line1   STRING        COMMENT 'Standardised street address',
  residential_address_line2   STRING        COMMENT 'Suburb',
  residential_city            STRING        COMMENT 'City',
  residential_province        STRING        COMMENT 'Standardised province code: GP/WC/KZN/EC/FS/LP/MP/NW/NC',
  residential_province_name   STRING        COMMENT 'Derived full province name',
  residential_postal_code     STRING        COMMENT 'Validated 4-digit postal code',
  residential_country         STRING        COMMENT 'Country ISO code (ZA for domestic)',
  -- Employment (validated and typed)
  employment_status           STRING        COMMENT 'Standardised: EMPLOYED/SELF_EMPLOYED/RETIRED/UNEMPLOYED/STUDENT',
  employer_name               STRING        COMMENT 'Standardised employer name (upper-cased)',
  occupation                  STRING        COMMENT 'Occupation',
  industry_code               STRING        COMMENT 'ISIC industry code',
  gross_monthly_income        DECIMAL(18,2) COMMENT 'Validated gross monthly income in ZAR',
  net_monthly_income          DECIMAL(18,2) COMMENT 'Validated net monthly income in ZAR',
  annual_income               DECIMAL(18,2) COMMENT 'Derived: gross_monthly_income * 12',
  -- DERIVED: Income bands
  income_band                 STRING        COMMENT 'Derived income band: <5K/5-15K/15-30K/30-60K/60-120K/120K+',
  -- Customer classification (derived/enriched)
  customer_type               STRING        COMMENT 'Standardised: RETAIL/CORPORATE/SME/PRIVATE_BANKING/PREMIER',
  customer_segment            STRING        COMMENT 'Standardised segment code',
  customer_status             STRING        COMMENT 'Standardised: ACTIVE/INACTIVE/DECEASED/DORMANT/BLACKLISTED',
  -- DERIVED: Risk and value segments
  risk_segment                STRING        COMMENT 'Derived risk segment: LOW_RISK/STANDARD/ELEVATED/HIGH_RISK/WATCHLIST',
  value_segment               STRING        COMMENT 'Derived value segment based on income and product holding',
  clv_tier                    STRING        COMMENT 'Customer Lifetime Value tier: BRONZE/SILVER/GOLD/PLATINUM',
  -- Dates (typed)
  onboarding_date             DATE          COMMENT 'Cast and validated onboarding date',
  days_since_onboarding       INT           COMMENT 'Derived: DATEDIFF(CURRENT_DATE, onboarding_date)',
  tenure_years                DECIMAL(5,2)  COMMENT 'Derived: days_since_onboarding / 365.25',
  tenure_band                 STRING        COMMENT 'Derived: <1yr/1-3yr/3-5yr/5-10yr/10yr+',
  last_review_date            DATE          COMMENT 'Cast and validated last review date',
  days_since_review           INT           COMMENT 'Derived: DATEDIFF(CURRENT_DATE, last_review_date)',
  review_overdue_flag         BOOLEAN       COMMENT 'Derived: True if days_since_review > 365',
  -- Consent (typed)
  marketing_consent           BOOLEAN       COMMENT 'Marketing consent flag (cast from Y/N)',
  popi_consent                BOOLEAN       COMMENT 'POPIA consent flag (cast from Y/N)',
  popi_consent_date           DATE          COMMENT 'Cast POPIA consent date',
  -- Data quality flags
  dq_email_valid              BOOLEAN       COMMENT 'Email address passes format validation',
  dq_mobile_valid             BOOLEAN       COMMENT 'Mobile number passes E.164 format check',
  dq_id_valid                 BOOLEAN       COMMENT 'ID number passes Luhn check (SA ID)',
  dq_dob_consistent           BOOLEAN       COMMENT 'DOB is consistent with ID number',
  dq_score                    INT           COMMENT 'Data quality score 0-100 (higher = better)',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch that produced this record',
  _bronze_ingested_at         TIMESTAMP     COMMENT 'When record was loaded into bronze',
  _silver_processed_at        TIMESTAMP     COMMENT 'When record was processed into silver',
  _record_hash                STRING        COMMENT 'Hash of business key fields for change detection'
)
COMMENT 'Silver cleansed and enriched customer master. Includes validated types, derived age/income/risk/tenure fields, data quality scores, and SCD2 history. PII fields are hashed. Primary source for all customer analytics.'
PARTITIONED BY (residential_province, customer_type)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  'quality.layer'                    = 'silver',
  'scd.type'                         = '2'
);

-- ---------------------------------------------------------------------------
-- 2. silver_accounts
--    Source: bronze_accounts (typed, enriched, dormancy logic applied)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_accounts;

CREATE TABLE prime_capital.silver.silver_accounts (
  account_sk                  BIGINT        COMMENT 'Silver surrogate key',
  account_id                  STRING        COMMENT 'Core banking account number',
  customer_id                 STRING        COMMENT 'Owning customer identifier',
  -- SCD2
  effective_date              DATE          COMMENT 'Effective date of this account version',
  expiry_date                 DATE          COMMENT 'Expiry date (9999-12-31 = current)',
  is_current                  BOOLEAN       COMMENT 'True if current version',
  -- Account classification
  account_type_code           STRING        COMMENT 'Standardised account type: CURRENT/SAVINGS/FIXED/MONEY_MARKET/OFFSHORE/BUSINESS',
  account_type_desc           STRING        COMMENT 'Full account type description',
  product_code                STRING        COMMENT 'Banking product code',
  product_name                STRING        COMMENT 'Banking product name',
  currency_code               STRING        COMMENT 'ISO 4217 currency code',
  is_foreign_currency         BOOLEAN       COMMENT 'Derived: True if currency_code != ZAR',
  -- Status and lifecycle
  account_status              STRING        COMMENT 'Standardised: OPEN/CLOSED/DORMANT/FROZEN/BLOCKED',
  open_date                   DATE          COMMENT 'Cast and validated open date',
  close_date                  DATE          COMMENT 'Cast and validated close date',
  account_age_days            INT           COMMENT 'Derived: DATEDIFF(CURRENT_DATE, open_date)',
  last_transaction_date       DATE          COMMENT 'Cast and validated last transaction date',
  days_since_last_tx          INT           COMMENT 'Derived: DATEDIFF(CURRENT_DATE, last_transaction_date)',
  dormancy_flag               BOOLEAN       COMMENT 'Derived: True if days_since_last_tx > 180',
  dormancy_tier               STRING        COMMENT 'Derived: ACTIVE/PRE_DORMANT/DORMANT/INACTIVE (based on days)',
  -- Balances (typed to DECIMAL)
  current_balance             DECIMAL(18,2) COMMENT 'Current ledger balance in ZAR',
  available_balance           DECIMAL(18,2) COMMENT 'Available balance after holds',
  hold_amount                 DECIMAL(18,2) COMMENT 'Total amount under hold/lien',
  overdraft_limit             DECIMAL(18,2) COMMENT 'Approved overdraft limit',
  overdraft_utilised          DECIMAL(18,2) COMMENT 'Current overdraft amount used',
  overdraft_utilisation_pct   DECIMAL(8,4)  COMMENT 'Derived: overdraft_utilised / overdraft_limit * 100',
  -- Derived balance bands
  balance_band                STRING        COMMENT 'Derived: <0/0-1K/1-10K/10-50K/50-250K/250K-1M/1M+',
  is_negative_balance         BOOLEAN       COMMENT 'Derived: True if current_balance < 0',
  -- Interest
  interest_rate               DECIMAL(8,4)  COMMENT 'Annual interest rate (cast to DECIMAL)',
  interest_accrued_ytd        DECIMAL(18,2) COMMENT 'YTD interest accrued',
  interest_paid_ytd           DECIMAL(18,2) COMMENT 'YTD interest paid',
  fees_charged_ytd            DECIMAL(18,2) COMMENT 'YTD fees charged',
  minimum_balance             DECIMAL(18,2) COMMENT 'Minimum balance requirement',
  below_minimum_flag          BOOLEAN       COMMENT 'Derived: True if current_balance < minimum_balance',
  -- Fixed / notice account fields
  maturity_date               DATE          COMMENT 'Cast maturity date for fixed deposits',
  days_to_maturity            INT           COMMENT 'Derived: DATEDIFF(maturity_date, CURRENT_DATE)',
  notice_period_days          INT           COMMENT 'Cast notice period days',
  rollover_instruction        STRING        COMMENT 'Standardised rollover instruction',
  -- Branch and relationship
  branch_code                 STRING        COMMENT 'Home branch code',
  relationship_manager_id     STRING        COMMENT 'Assigned relationship manager ID',
  linked_account_id           STRING        COMMENT 'Linked account for sweep/salary',
  -- Data quality
  dq_score                    INT           COMMENT 'Data quality score 0-100',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Ingestion batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp',
  _record_hash                STRING        COMMENT 'Business key hash'
)
COMMENT 'Silver cleansed account master with typed balances (DECIMAL), derived dormancy flags, balance bands, overdraft utilisation, and days-since-last-transaction. Used for account analytics, balance reporting, and dormancy management.'
PARTITIONED BY (account_type_code)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 3. silver_transactions
--    Source: bronze_transactions (typed, FX converted, enriched)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_transactions;

CREATE TABLE prime_capital.silver.silver_transactions (
  transaction_id              STRING        COMMENT 'Source transaction identifier',
  account_id                  STRING        COMMENT 'Account number',
  customer_id                 STRING        COMMENT 'Customer identifier',
  -- Date/time (fully typed)
  transaction_date            DATE          COMMENT 'Business date of transaction',
  transaction_datetime        TIMESTAMP     COMMENT 'Full transaction timestamp (SAST)',
  transaction_datetime_utc    TIMESTAMP     COMMENT 'Full transaction timestamp (UTC)',
  value_date                  DATE          COMMENT 'Value date for interest calculation',
  posting_date                DATE          COMMENT 'Posting date to ledger',
  -- Transaction classification
  tx_type_code                STRING        COMMENT 'Standardised transaction type code',
  tx_type_desc                STRING        COMMENT 'Standardised transaction type description',
  tx_category                 STRING        COMMENT 'Category: DEBIT/CREDIT/FEE/INTEREST/ADJUSTMENT/REVERSAL',
  tx_sub_category             STRING        COMMENT 'Sub-category: SALARY/GROCERIES/UTILITIES/ENTERTAINMENT/etc',
  debit_credit_indicator      STRING        COMMENT 'Standardised: DEBIT/CREDIT',
  is_debit                    BOOLEAN       COMMENT 'Derived: True if debit_credit_indicator = DEBIT',
  is_reversal                 BOOLEAN       COMMENT 'Derived: True if reversal_flag = Y',
  original_transaction_id     STRING        COMMENT 'Original transaction ID for reversals',
  -- Amounts (fully typed, FX-converted to ZAR)
  amount_original_currency    DECIMAL(18,2) COMMENT 'Transaction amount in original currency',
  currency_code               STRING        COMMENT 'Original transaction currency ISO code',
  exchange_rate               DECIMAL(18,6) COMMENT 'Exchange rate applied',
  amount_zar                  DECIMAL(18,2) COMMENT 'Derived: Amount in ZAR (amount * exchange_rate)',
  fee_amount_zar              DECIMAL(18,2) COMMENT 'Fee component in ZAR',
  tax_amount_zar              DECIMAL(18,2) COMMENT 'VAT component in ZAR',
  total_amount_zar            DECIMAL(18,2) COMMENT 'Derived: amount_zar + fee_amount_zar + tax_amount_zar',
  running_balance             DECIMAL(18,2) COMMENT 'Account balance after transaction in ZAR',
  -- Channel
  channel_code                STRING        COMMENT 'Standardised: ATM/MOBILE_APP/ONLINE/BRANCH/POS/EFT/USSD',
  channel_category            STRING        COMMENT 'Derived: DIGITAL/PHYSICAL/ELECTRONIC',
  is_digital                  BOOLEAN       COMMENT 'Derived: True if channel is digital (mobile/online/USSD)',
  device_id                   STRING        COMMENT 'Device identifier (ATM number or mobile device)',
  -- Counterparty
  counter_party_account       STRING        COMMENT 'Counter-party account number',
  counter_party_name          STRING        COMMENT 'Counter-party name (standardised)',
  counter_party_bank_code     STRING        COMMENT 'Counter-party bank code',
  is_internal_transfer        BOOLEAN       COMMENT 'Derived: True if counter-party is within Prime Capital',
  -- Merchant (for POS transactions)
  merchant_id                 STRING        COMMENT 'Merchant identifier',
  merchant_name               STRING        COMMENT 'Merchant name (standardised)',
  merchant_mcc                STRING        COMMENT 'Merchant Category Code',
  merchant_mcc_desc           STRING        COMMENT 'Derived MCC description (lookup)',
  merchant_city               STRING        COMMENT 'Merchant city',
  merchant_country            STRING        COMMENT 'Merchant country ISO code',
  is_international_merchant   BOOLEAN       COMMENT 'Derived: True if merchant_country != ZA',
  -- Reference
  reference_number            STRING        COMMENT 'Payment reference number',
  description                 STRING        COMMENT 'Standardised transaction description',
  authorisation_code          STRING        COMMENT 'Payment authorisation code',
  -- Fraud
  fraud_score                 DECIMAL(8,4)  COMMENT 'Cast fraud score (0-100)',
  fraud_flag                  BOOLEAN       COMMENT 'Fraud flag (cast from Y/N)',
  -- GL
  gl_account_code             STRING        COMMENT 'General ledger account code',
  cost_centre_code            STRING        COMMENT 'Cost centre code',
  branch_code                 STRING        COMMENT 'Processing branch code',
  -- Data quality
  dq_amount_valid             BOOLEAN       COMMENT 'Amount is non-null and > 0',
  dq_date_valid               BOOLEAN       COMMENT 'Transaction date is valid and not in future',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Ingestion batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver cleansed transaction ledger. All amounts cast to DECIMAL(18,2) and converted to ZAR. Transaction types standardised. Digital channel flag, MCC descriptions, and counterparty enrichment applied. Partitioned by transaction_date.'
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  'delta.dataSkippingNumIndexedCols' = '5',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 4. silver_loans
--    Source: bronze_loans (typed, enriched with NPL/delinquency logic)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_loans;

CREATE TABLE prime_capital.silver.silver_loans (
  loan_id                     STRING        COMMENT 'Unique loan account identifier',
  application_id              STRING        COMMENT 'Originating application ID',
  customer_id                 STRING        COMMENT 'Borrower customer identifier',
  snapshot_date               DATE          COMMENT 'Daily snapshot date',
  -- Loan classification
  loan_type                   STRING        COMMENT 'Standardised: PERSONAL/HOME/VEHICLE/COMMERCIAL/REVOLVING',
  loan_status                 STRING        COMMENT 'Standardised: ACTIVE/SETTLED/IN_ARREARS/DEFAULT/WRITTEN_OFF/LEGAL',
  product_code                STRING        COMMENT 'Banking product code',
  -- Key dates (typed)
  disbursement_date           DATE          COMMENT 'Cast disbursement date',
  maturity_date               DATE          COMMENT 'Cast maturity date',
  loan_age_days               INT           COMMENT 'Derived: DATEDIFF(snapshot_date, disbursement_date)',
  days_to_maturity            INT           COMMENT 'Derived: DATEDIFF(maturity_date, snapshot_date)',
  -- Amounts (typed to DECIMAL)
  original_amount             DECIMAL(18,2) COMMENT 'Original loan amount',
  outstanding_balance         DECIMAL(18,2) COMMENT 'Current outstanding principal',
  principal_paid              DECIMAL(18,2) COMMENT 'Total principal repaid to date',
  principal_paid_pct          DECIMAL(8,4)  COMMENT 'Derived: principal_paid / original_amount * 100',
  interest_paid               DECIMAL(18,2) COMMENT 'Total interest paid to date',
  interest_accrued            DECIMAL(18,2) COMMENT 'Interest accrued and unpaid',
  fees_paid                   DECIMAL(18,2) COMMENT 'Total fees paid',
  arrears_amount              DECIMAL(18,2) COMMENT 'Amount in arrears (cast)',
  -- Delinquency
  delinquency_days            INT           COMMENT 'Standardised: cast and validated arrears days',
  missed_instalments          INT           COMMENT 'Count of missed instalments (cast)',
  delinquency_band            STRING        COMMENT 'Derived: CURRENT/1-30DPD/31-60DPD/61-90DPD/91-120DPD/120+DPD',
  npl_flag                    BOOLEAN       COMMENT 'Derived: True if delinquency_days >= 90',
  npl_date                    DATE          COMMENT 'Date loan first classified as NPL',
  -- Interest rates (typed)
  interest_rate               DECIMAL(8,4)  COMMENT 'Annual interest rate (cast)',
  rate_type                   STRING        COMMENT 'FIXED/VARIABLE/PRIME_LINKED',
  prime_rate_margin           DECIMAL(6,4)  COMMENT 'Margin above/below prime rate',
  current_instalment          DECIMAL(18,2) COMMENT 'Current monthly instalment',
  -- Collateral and LTV
  collateral_type             STRING        COMMENT 'PROPERTY/VEHICLE/SURETY/CESSION/NONE',
  collateral_value            DECIMAL(18,2) COMMENT 'Current collateral value (cast)',
  ltv_ratio                   DECIMAL(8,4)  COMMENT 'Derived: outstanding_balance / collateral_value * 100',
  ltv_band                    STRING        COMMENT 'Derived: <50%/50-70%/70-80%/80-90%/90-100%/100%+',
  is_high_ltv                 BOOLEAN       COMMENT 'Derived: True if ltv_ratio > 80%',
  -- IFRS 9 provisioning
  ecl_stage                   INT           COMMENT 'IFRS 9 ECL stage: 1/2/3 (cast)',
  provision_rate              DECIMAL(8,4)  COMMENT 'Provision rate applied (cast)',
  provision_amount            DECIMAL(18,2) COMMENT 'Provision amount (cast)',
  provision_coverage_ratio    DECIMAL(8,4)  COMMENT 'Derived: provision_amount / outstanding_balance * 100',
  -- Payment history
  last_payment_date           DATE          COMMENT 'Date of most recent payment',
  last_payment_amount         DECIMAL(18,2) COMMENT 'Amount of most recent payment',
  next_payment_date           DATE          COMMENT 'Next scheduled payment date',
  next_payment_amount         DECIMAL(18,2) COMMENT 'Next scheduled payment amount',
  days_since_last_payment     INT           COMMENT 'Derived: DATEDIFF(snapshot_date, last_payment_date)',
  -- Restructure / write-off
  restructured_flag           BOOLEAN       COMMENT 'Loan has been restructured',
  restructure_date            DATE          COMMENT 'Date of most recent restructure',
  legal_action_date           DATE          COMMENT 'Date legal action commenced',
  write_off_date              DATE          COMMENT 'Date loan was written off',
  write_off_amount            DECIMAL(18,2) COMMENT 'Amount written off',
  -- Relationship
  relationship_manager_id     STRING        COMMENT 'Assigned relationship manager',
  branch_code                 STRING        COMMENT 'Administering branch',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver enriched loan book. Daily snapshot with typed DECIMAL amounts, derived LTV ratio, delinquency bands, NPL flag, IFRS 9 ECL stage and provision coverage. Primary source for credit risk and NPL reporting.'
PARTITIONED BY (snapshot_date, loan_type)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 5. silver_payments
--    Source: bronze_payments (typed, enriched with correspondent bank logic)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_payments;

CREATE TABLE prime_capital.silver.silver_payments (
  payment_id                  STRING        COMMENT 'Unique payment identifier',
  payment_reference           STRING        COMMENT 'Customer-facing payment reference',
  payment_type                STRING        COMMENT 'Standardised: EFT/RTGS/SWIFT/INTERNAL/CASH/DEBIT_ORDER',
  payment_direction           STRING        COMMENT 'OUTBOUND/INBOUND',
  payment_status              STRING        COMMENT 'Standardised: PENDING/PROCESSING/SETTLED/FAILED/RECALLED/RETURNED',
  is_settled                  BOOLEAN       COMMENT 'Derived: True if status = SETTLED',
  is_failed                   BOOLEAN       COMMENT 'Derived: True if status = FAILED or RETURNED',
  -- Timing
  submission_datetime         TIMESTAMP     COMMENT 'Cast submission datetime',
  processing_datetime         TIMESTAMP     COMMENT 'Cast processing datetime',
  settlement_datetime         TIMESTAMP     COMMENT 'Cast settlement datetime',
  settlement_date             DATE          COMMENT 'Business settlement date',
  processing_lag_minutes      INT           COMMENT 'Derived: DATEDIFF in minutes between submission and settlement',
  is_same_day_settlement      BOOLEAN       COMMENT 'Derived: True if submission_date = settlement_date',
  -- Originator
  originator_account_id       STRING        COMMENT 'Paying account number',
  originator_customer_id      STRING        COMMENT 'Paying customer ID',
  originator_name             STRING        COMMENT 'Paying party name',
  originator_bank_code        STRING        COMMENT 'Originator bank code',
  is_originator_internal      BOOLEAN       COMMENT 'Derived: True if originator is Prime Capital customer',
  -- Beneficiary
  beneficiary_account_id      STRING        COMMENT 'Receiving account number',
  beneficiary_customer_id     STRING        COMMENT 'Receiving customer ID (if internal)',
  beneficiary_name            STRING        COMMENT 'Beneficiary name',
  beneficiary_bank_code       STRING        COMMENT 'Beneficiary bank sort code',
  beneficiary_bank_name       STRING        COMMENT 'Beneficiary bank name',
  beneficiary_country         STRING        COMMENT 'Beneficiary country ISO code',
  is_beneficiary_internal     BOOLEAN       COMMENT 'Derived: True if beneficiary is Prime Capital customer',
  is_cross_border             BOOLEAN       COMMENT 'Derived: True if beneficiary_country != ZA',
  correspondent_bank          STRING        COMMENT 'Enriched: correspondent bank name for SWIFT payments',
  correspondent_bank_bic      STRING        COMMENT 'Enriched: correspondent bank BIC code',
  -- Amounts (typed)
  payment_amount              DECIMAL(18,2) COMMENT 'Payment amount in payment currency',
  payment_currency            STRING        COMMENT 'Payment currency ISO code',
  zar_equivalent              DECIMAL(18,2) COMMENT 'ZAR equivalent amount',
  exchange_rate               DECIMAL(18,6) COMMENT 'FX rate applied',
  fee_amount                  DECIMAL(18,2) COMMENT 'Transaction fee in ZAR',
  -- SWIFT specifics
  swift_message_type          STRING        COMMENT 'SWIFT message type: MT103/MT202/MT910',
  swift_uetr                  STRING        COMMENT 'SWIFT UETR for GPI tracking',
  swift_status                STRING        COMMENT 'Derived GPI status: INITIATED/CONFIRMED/COMPLETED',
  sarb_reference              STRING        COMMENT 'SARB/SAMOS reference number',
  -- Failure analysis
  reason_code                 STRING        COMMENT 'Failure/return reason code',
  failure_reason_category     STRING        COMMENT 'Enriched: INSUFFICIENT_FUNDS/INVALID_ACCOUNT/LIMIT_EXCEEDED/etc',
  channel                     STRING        COMMENT 'Standardised: ONLINE/MOBILE/BRANCH/API/BATCH',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver enriched payment data with typed amounts, settlement lag calculations, cross-border flags, correspondent bank enrichment, and SWIFT GPI tracking status. Used for payment analytics and regulatory reporting.'
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 6. silver_credit_cards
--    Source: bronze_credit_cards (typed, utilisation and risk logic)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_credit_cards;

CREATE TABLE prime_capital.silver.silver_credit_cards (
  card_id                     STRING        COMMENT 'Unique card account identifier',
  card_number_token           STRING        COMMENT 'Tokenised PAN',
  customer_id                 STRING        COMMENT 'Primary cardholder customer ID',
  supplementary_customer_id   STRING        COMMENT 'Supplementary cardholder ID',
  account_id                  STRING        COMMENT 'Linked account ID',
  snapshot_date               DATE          COMMENT 'Daily snapshot date',
  -- Card classification
  card_type                   STRING        COMMENT 'Standardised: CREDIT/DEBIT/PREPAID',
  card_scheme                 STRING        COMMENT 'VISA/MASTERCARD/AMEX',
  card_tier                   STRING        COMMENT 'STANDARD/GOLD/PLATINUM/SIGNATURE/INFINITE',
  card_status                 STRING        COMMENT 'Standardised: ACTIVE/BLOCKED/EXPIRED/CANCELLED/LOST/STOLEN',
  card_status_reason          STRING        COMMENT 'Reason for non-ACTIVE status',
  -- Dates (typed)
  issue_date                  DATE          COMMENT 'Cast issue date',
  expiry_date                 DATE          COMMENT 'Cast expiry date',
  card_age_days               INT           COMMENT 'Derived: DATEDIFF(snapshot_date, issue_date)',
  days_to_expiry              INT           COMMENT 'Derived: DATEDIFF(expiry_date, snapshot_date)',
  is_expiring_within_60_days  BOOLEAN       COMMENT 'Derived: days_to_expiry <= 60',
  -- Balances (typed)
  credit_limit                DECIMAL(18,2) COMMENT 'Approved credit limit',
  available_credit            DECIMAL(18,2) COMMENT 'Available credit',
  current_balance             DECIMAL(18,2) COMMENT 'Current outstanding balance',
  overlimit_amount            DECIMAL(18,2) COMMENT 'Amount exceeding limit (if overlimit)',
  -- Derived utilisation metrics
  utilisation_rate            DECIMAL(8,4)  COMMENT 'Derived: current_balance / credit_limit * 100',
  utilisation_band            STRING        COMMENT 'Derived: 0%/1-25%/26-50%/51-75%/76-90%/90%+',
  overlimit_flag              BOOLEAN       COMMENT 'Derived: True if current_balance > credit_limit',
  -- Payment behaviour
  minimum_payment_due         DECIMAL(18,2) COMMENT 'Minimum payment due',
  payment_due_date            DATE          COMMENT 'Cast payment due date',
  last_payment_date           DATE          COMMENT 'Cast last payment date',
  last_payment_amount         DECIMAL(18,2) COMMENT 'Last payment amount',
  days_to_payment_due         INT           COMMENT 'Derived: DATEDIFF(payment_due_date, snapshot_date)',
  minimum_payment_flag        BOOLEAN       COMMENT 'Derived: True if last_payment_amount <= minimum_payment_due * 1.1',
  full_payment_flag           BOOLEAN       COMMENT 'Derived: True if last_payment_amount >= current_balance',
  missed_payment_flag         BOOLEAN       COMMENT 'Derived: True if snapshot_date > payment_due_date AND no payment',
  -- Interest rates (typed)
  interest_rate_purchase      DECIMAL(8,4)  COMMENT 'Purchase interest rate',
  interest_rate_cash          DECIMAL(8,4)  COMMENT 'Cash advance interest rate',
  annual_fee                  DECIMAL(18,2) COMMENT 'Annual fee',
  -- Rewards
  reward_points_balance       BIGINT        COMMENT 'Current reward points balance (cast)',
  reward_points_ytd           BIGINT        COMMENT 'YTD reward points earned',
  -- Features
  contactless_enabled         BOOLEAN       COMMENT 'Contactless payments enabled',
  online_transactions_enabled BOOLEAN       COMMENT 'Online transactions enabled',
  international_enabled       BOOLEAN       COMMENT 'International transactions enabled',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver enriched credit card accounts with typed balances, derived utilisation rate and band, overlimit flag, payment behaviour flags, and days-to-expiry. Used for card analytics and credit risk reporting.'
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 7. silver_card_transactions
--    Source: bronze_card_transactions (typed, enriched with MCC and FX)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_card_transactions;

CREATE TABLE prime_capital.silver.silver_card_transactions (
  card_tx_id                  STRING        COMMENT 'Unique card transaction identifier',
  authorisation_code          STRING        COMMENT 'Card authorisation code',
  card_id                     STRING        COMMENT 'Card account identifier',
  customer_id                 STRING        COMMENT 'Cardholder customer ID',
  -- Dates/times (fully typed)
  transaction_date            DATE          COMMENT 'Business transaction date',
  transaction_datetime        TIMESTAMP     COMMENT 'Full transaction timestamp (SAST)',
  transaction_datetime_utc    TIMESTAMP     COMMENT 'Full transaction timestamp (UTC)',
  settlement_date             DATE          COMMENT 'Settlement date',
  -- Transaction classification
  transaction_type            STRING        COMMENT 'Standardised: PURCHASE/CASH_ADVANCE/REFUND/REVERSAL/FEE/INTEREST',
  transaction_status          STRING        COMMENT 'Standardised: AUTHORISED/SETTLED/DECLINED/REVERSED',
  is_declined                 BOOLEAN       COMMENT 'Derived: True if status = DECLINED',
  decline_reason              STRING        COMMENT 'Standardised decline reason',
  -- Amounts (typed, FX-converted)
  amount_original_currency    DECIMAL(18,2) COMMENT 'Amount in transaction currency',
  transaction_currency        STRING        COMMENT 'Transaction currency ISO code',
  billing_amount_zar          DECIMAL(18,2) COMMENT 'Billing amount in ZAR',
  exchange_rate               DECIMAL(18,6) COMMENT 'FX exchange rate applied',
  fx_markup_amount_zar        DECIMAL(18,2) COMMENT 'Derived: FX markup fee in ZAR',
  cashback_amount             DECIMAL(18,2) COMMENT 'Cashback amount if applicable',
  -- Merchant (enriched)
  merchant_id                 STRING        COMMENT 'Merchant identifier',
  merchant_name               STRING        COMMENT 'Standardised merchant name',
  merchant_mcc                STRING        COMMENT 'Merchant Category Code',
  mcc_category                STRING        COMMENT 'Enriched MCC category (GROCERY/FUEL/RESTAURANT/TRAVEL/etc)',
  mcc_category_group          STRING        COMMENT 'Enriched MCC group (ESSENTIALS/LIFESTYLE/TRAVEL/FINANCIAL)',
  merchant_city               STRING        COMMENT 'Merchant city',
  merchant_province           STRING        COMMENT 'Merchant province',
  merchant_country            STRING        COMMENT 'Merchant country ISO code',
  is_international            BOOLEAN       COMMENT 'True if merchant_country != ZA',
  -- Entry mode
  entry_mode                  STRING        COMMENT 'Standardised: CHIP/MAGNETIC_STRIPE/CONTACTLESS/ONLINE/CNP',
  is_contactless              BOOLEAN       COMMENT 'Derived: True if entry_mode = CONTACTLESS',
  is_card_not_present         BOOLEAN       COMMENT 'Derived: True if entry_mode = CNP or ONLINE',
  is_3ds_verified             BOOLEAN       COMMENT 'Cast 3D Secure flag',
  is_recurring                BOOLEAN       COMMENT 'Recurring/subscription indicator',
  -- Fraud scoring
  fraud_score                 DECIMAL(8,4)  COMMENT 'Cast fraud score (0-100)',
  fraud_flag                  BOOLEAN       COMMENT 'Fraud flag',
  fraud_type                  STRING        COMMENT 'Fraud type code if flagged',
  fraud_risk_tier             STRING        COMMENT 'Derived: LOW/MEDIUM/HIGH/VERY_HIGH based on fraud_score',
  -- Chargeback
  chargeback_flag             BOOLEAN       COMMENT 'Chargeback raised flag',
  chargeback_date             DATE          COMMENT 'Chargeback date',
  chargeback_reason           STRING        COMMENT 'ISO 8583 chargeback reason code',
  -- Rewards
  reward_points_earned        INT           COMMENT 'Reward points earned on transaction',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver enriched card transactions with typed amounts, ZAR-converted billing amounts, MCC category enrichment, fraud risk tiers, and international merchant flags. Partitioned by transaction_date.'
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 8. silver_fraud_alerts
--    Source: bronze_fraud_alerts (typed, enriched, disposition logic)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_fraud_alerts;

CREATE TABLE prime_capital.silver.silver_fraud_alerts (
  alert_id                    STRING        COMMENT 'Unique fraud alert identifier',
  alert_reference             STRING        COMMENT 'Human-readable alert reference',
  alert_datetime              TIMESTAMP     COMMENT 'Cast alert datetime',
  alert_date                  DATE          COMMENT 'Derived: DATE(alert_datetime)',
  alert_type                  STRING        COMMENT 'Standardised: CARD_FRAUD/ACCOUNT_TAKEOVER/IDENTITY/APPLICATION/INTERNAL',
  alert_type_category         STRING        COMMENT 'Derived grouping: EXTERNAL/INTERNAL',
  alert_source                STRING        COMMENT 'Detection rule or model',
  alert_priority              STRING        COMMENT 'Standardised: P1_CRITICAL/P2_HIGH/P3_MEDIUM/P4_LOW',
  -- Customer and account
  customer_id                 STRING        COMMENT 'Associated customer ID',
  account_id                  STRING        COMMENT 'Associated account ID',
  card_id                     STRING        COMMENT 'Associated card ID',
  transaction_id              STRING        COMMENT 'Triggering transaction ID',
  transaction_amount          DECIMAL(18,2) COMMENT 'Suspicious transaction amount (cast)',
  -- Fraud scoring
  fraud_score                 DECIMAL(10,2) COMMENT 'Cast composite fraud score',
  fraud_score_band            STRING        COMMENT 'Derived: LOW/MEDIUM/HIGH/CRITICAL based on score',
  fraud_model_version         STRING        COMMENT 'Model version',
  rule_triggered              STRING        COMMENT 'Rule code(s) triggered',
  -- Alert disposition
  alert_status                STRING        COMMENT 'Standardised: OPEN/UNDER_REVIEW/CONFIRMED/FALSE_POSITIVE/CLOSED',
  is_confirmed_fraud          BOOLEAN       COMMENT 'Derived: True if status = CONFIRMED',
  is_false_positive           BOOLEAN       COMMENT 'Derived: True if status = FALSE_POSITIVE',
  analyst_id                  STRING        COMMENT 'Fraud analyst assigned',
  disposition_date            DATE          COMMENT 'Cast disposition date',
  resolution_days             INT           COMMENT 'Derived: DATEDIFF(disposition_date, alert_date)',
  breach_sla_flag             BOOLEAN       COMMENT 'Derived: True if resolution_days > SLA threshold by priority',
  -- Financial impact
  loss_amount                 DECIMAL(18,2) COMMENT 'Confirmed fraud loss amount',
  recovery_amount             DECIMAL(18,2) COMMENT 'Amount recovered',
  net_loss_amount             DECIMAL(18,2) COMMENT 'Derived: loss_amount - recovery_amount',
  recovery_rate               DECIMAL(8,4)  COMMENT 'Derived: recovery_amount / loss_amount * 100',
  -- Context
  channel                     STRING        COMMENT 'Standardised channel where fraud occurred',
  device_id                   STRING        COMMENT 'Device involved',
  previous_alerts_count       INT           COMMENT 'Cast count of previous alerts on this customer',
  is_repeat_offender_target   BOOLEAN       COMMENT 'Derived: True if previous_alerts_count > 0',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver enriched fraud alerts with typed amounts, fraud score bands, SLA breach flags, net loss calculation, and repeat offender indicators. Used for fraud dashboard, operational reporting, and risk analytics.'
PARTITIONED BY (alert_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 9. silver_treasury_trades
--    Source: bronze_treasury_trades (typed, MTM and P&L enriched)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_treasury_trades;

CREATE TABLE prime_capital.silver.silver_treasury_trades (
  trade_id                    STRING        COMMENT 'Unique trade identifier',
  trade_reference             STRING        COMMENT 'Front-office trade reference',
  trade_date                  DATE          COMMENT 'Cast trade date',
  value_date                  DATE          COMMENT 'Cast value/settlement date',
  maturity_date               DATE          COMMENT 'Cast maturity date',
  snapshot_date               DATE          COMMENT 'EOD snapshot date',
  days_to_maturity            INT           COMMENT 'Derived: DATEDIFF(maturity_date, snapshot_date)',
  -- Classification
  trade_type                  STRING        COMMENT 'Standardised: BOND/FX_SPOT/FX_FORWARD/IRS/CDS/EQUITY/MONEY_MARKET/REPO',
  trade_type_category         STRING        COMMENT 'Derived: FIXED_INCOME/FX/DERIVATIVE/EQUITY/MONEY_MARKET',
  trade_direction             STRING        COMMENT 'BUY/SELL/LEND/BORROW',
  instrument_id               STRING        COMMENT 'Instrument ISIN / identifier',
  instrument_name             STRING        COMMENT 'Instrument description',
  instrument_type             STRING        COMMENT 'GOVERNMENT_BOND/CORPORATE_BOND/EQUITY/DERIVATIVE/FX',
  -- Amounts (typed)
  currency                    STRING        COMMENT 'Trade currency ISO code',
  notional_amount             DECIMAL(18,2) COMMENT 'Notional / face value',
  notional_amount_zar         DECIMAL(18,2) COMMENT 'Derived: notional in ZAR',
  price                       DECIMAL(18,6) COMMENT 'Trade price (cast)',
  yield                       DECIMAL(8,4)  COMMENT 'Yield (cast)',
  coupon_rate                 DECIMAL(8,4)  COMMENT 'Coupon rate for bonds',
  -- MTM and P&L
  mtm_value                   DECIMAL(18,2) COMMENT 'Mark-to-market value',
  mtm_value_zar               DECIMAL(18,2) COMMENT 'MTM value in ZAR',
  unrealised_pnl              DECIMAL(18,2) COMMENT 'Unrealised P&L',
  realised_pnl                DECIMAL(18,2) COMMENT 'Realised P&L',
  total_pnl                   DECIMAL(18,2) COMMENT 'Derived: unrealised_pnl + realised_pnl',
  accrued_interest            DECIMAL(18,2) COMMENT 'Accrued interest',
  -- Risk metrics
  duration                    DECIMAL(10,4) COMMENT 'Modified duration (cast)',
  dv01                        DECIMAL(18,6) COMMENT 'DV01 sensitivity (cast)',
  -- Counterparty
  counterparty_id             STRING        COMMENT 'Counterparty institution ID',
  counterparty_name           STRING        COMMENT 'Counterparty full name',
  counterparty_lei            STRING        COMMENT 'Counterparty LEI',
  counterparty_rating         STRING        COMMENT 'Credit rating (Moodys/S&P/Fitch)',
  counterparty_rating_tier    STRING        COMMENT 'Derived: INVESTMENT_GRADE/NON_INVESTMENT_GRADE/UNRATED',
  -- Desk / book
  trader_id                   STRING        COMMENT 'Trader employee ID',
  desk                        STRING        COMMENT 'Trading desk',
  portfolio_id                STRING        COMMENT 'Portfolio identifier',
  book_id                     STRING        COMMENT 'Book: FVTPL/AFS/HTM',
  -- Status
  trade_status                STRING        COMMENT 'Standardised: LIVE/MATURED/CANCELLED/AMENDED',
  settlement_status           STRING        COMMENT 'Standardised: UNSETTLED/SETTLED/FAILED',
  is_live                     BOOLEAN       COMMENT 'Derived: True if trade_status = LIVE',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver enriched treasury trade data with typed DECIMAL amounts, ZAR-converted MTM values, total P&L, counterparty rating tiers, and days-to-maturity. Used for treasury analytics, risk reporting, and ALCO dashboards.'
PARTITIONED BY (trade_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 10. silver_gl_entries
--     Source: bronze_gl_entries (typed, with account hierarchy enrichment)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_gl_entries;

CREATE TABLE prime_capital.silver.silver_gl_entries (
  journal_id                  STRING        COMMENT 'Unique journal entry identifier',
  journal_line_number         INT           COMMENT 'Cast line number',
  journal_reference           STRING        COMMENT 'Journal reference number',
  journal_type                STRING        COMMENT 'Standardised: MANUAL/AUTO/REVERSAL/ADJUSTMENT/OPENING/CLOSING',
  is_reversal                 BOOLEAN       COMMENT 'Cast reversal flag',
  original_journal_id         STRING        COMMENT 'Original journal for reversals',
  -- Posting periods (typed)
  posting_date                DATE          COMMENT 'Cast GL posting date',
  accounting_period           STRING        COMMENT 'Accounting period (YYYY-MM)',
  financial_year              STRING        COMMENT 'Financial year (Apr-Mar)',
  financial_quarter           STRING        COMMENT 'Derived: FY quarter (Q1/Q2/Q3/Q4)',
  month_end_flag              BOOLEAN       COMMENT 'Derived: True if posting_date is last business day of month',
  -- GL account hierarchy (5 levels enriched from COA reference)
  gl_account_code             STRING        COMMENT 'GL account code',
  gl_account_name             STRING        COMMENT 'GL account name',
  account_type                STRING        COMMENT 'ASSET/LIABILITY/EQUITY/INCOME/EXPENSE',
  account_level1              STRING        COMMENT 'Level 1: LEGAL_ENTITY',
  account_level2              STRING        COMMENT 'Level 2: SEGMENT (Retail/Corporate/Treasury)',
  account_level3              STRING        COMMENT 'Level 3: DIVISION',
  account_level4              STRING        COMMENT 'Level 4: DEPARTMENT',
  account_level5              STRING        COMMENT 'Level 5: ACCOUNT_CODE',
  -- Organisation
  cost_centre_code            STRING        COMMENT 'Cost centre code',
  cost_centre_name            STRING        COMMENT 'Cost centre name',
  cost_centre_type            STRING        COMMENT 'Derived: REVENUE/SUPPORT/SHARED_SERVICES',
  legal_entity                STRING        COMMENT 'Legal entity (Prime Capital Bank / subsidiaries)',
  business_unit               STRING        COMMENT 'Business unit code',
  source_module               STRING        COMMENT 'Source: LOANS/CARDS/TREASURY/PAYMENTS/PAYROLL/MANUAL',
  -- Amounts (typed to DECIMAL)
  debit_amount                DECIMAL(18,2) COMMENT 'Debit amount in ZAR',
  credit_amount               DECIMAL(18,2) COMMENT 'Credit amount in ZAR',
  net_amount                  DECIMAL(18,2) COMMENT 'Derived: credit_amount - debit_amount',
  transaction_currency        STRING        COMMENT 'Original transaction currency',
  transaction_amount_foreign  DECIMAL(18,2) COMMENT 'Amount in foreign currency',
  fx_rate                     DECIMAL(18,6) COMMENT 'FX rate used for conversion',
  -- Intercompany
  is_intercompany             BOOLEAN       COMMENT 'Intercompany journal flag',
  intercompany_entity         STRING        COMMENT 'Counterparty entity for intercompany',
  -- Approval
  created_by                  STRING        COMMENT 'User who created entry',
  approved_by                 STRING        COMMENT 'User who approved entry',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver GL entries enriched with 5-level account hierarchy from COA reference, typed DECIMAL amounts, financial year/quarter derivation, and cost centre type classification. Primary source for P&L and balance sheet reporting.'
PARTITIONED BY (posting_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- ---------------------------------------------------------------------------
-- 11. silver_aml_cases
--     Source: bronze_aml_alerts (deduped by case, enriched with risk scoring)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.silver.silver_aml_cases;

CREATE TABLE prime_capital.silver.silver_aml_cases (
  case_id                     STRING        COMMENT 'Unique AML case identifier (one or more alerts)',
  alert_id                    STRING        COMMENT 'Driving alert identifier',
  alert_reference             STRING        COMMENT 'Alert reference number',
  alert_datetime              TIMESTAMP     COMMENT 'Cast alert datetime',
  alert_date                  DATE          COMMENT 'Derived: DATE(alert_datetime)',
  -- Alert classification
  alert_type                  STRING        COMMENT 'Standardised AML typology',
  alert_scenario              STRING        COMMENT 'Specific scenario triggered',
  alert_priority              STRING        COMMENT 'Standardised: HIGH/MEDIUM/LOW',
  -- Customer and account
  customer_id                 STRING        COMMENT 'Associated customer ID',
  account_id                  STRING        COMMENT 'Associated account ID',
  transaction_ids             STRING        COMMENT 'Relevant transaction IDs',
  total_amount_flagged        DECIMAL(18,2) COMMENT 'Cast total suspicious amount',
  lookback_period_days        INT           COMMENT 'Cast lookback window',
  transaction_count           INT           COMMENT 'Cast count of transactions in pattern',
  -- Risk scoring
  risk_score                  DECIMAL(10,2) COMMENT 'Cast AML risk score',
  risk_score_band             STRING        COMMENT 'Derived: LOW/MEDIUM/HIGH/VERY_HIGH based on score',
  -- Case status
  case_status                 STRING        COMMENT 'Standardised: OPEN/UNDER_REVIEW/CLOSED_NO_ACTION/SAR_FILED/REFERRED',
  analyst_id                  STRING        COMMENT 'AML analyst assigned',
  review_start_date           DATE          COMMENT 'Cast review start date',
  review_end_date             DATE          COMMENT 'Cast review end date',
  review_duration_days        INT           COMMENT 'Derived: DATEDIFF(review_end_date, review_start_date)',
  breach_regulatory_sla       BOOLEAN       COMMENT 'Derived: True if review_duration_days > 30 (FICA requirement)',
  closure_reason              STRING        COMMENT 'Reason for closing without SAR',
  -- SAR details
  sar_filed_flag              BOOLEAN       COMMENT 'Cast SAR filed flag',
  sar_reference               STRING        COMMENT 'FIC SAR reference number',
  sar_amount                  DECIMAL(18,2) COMMENT 'Cast SAR reported amount',
  sar_filing_date             DATE          COMMENT 'Date SAR was filed with FIC',
  days_to_sar_filing          INT           COMMENT 'Derived: DATEDIFF(sar_filing_date, alert_date)',
  -- Audit
  _source_system              STRING        COMMENT 'Source system',
  _batch_id                   STRING        COMMENT 'Batch ID',
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp'
)
COMMENT 'Silver AML cases consolidated from alerts. Includes typed amounts, risk score bands, SLA breach flags, SAR filing status, and days-to-SAR metrics. Primary source for FIC regulatory reporting and AML operational dashboards.'
PARTITIONED BY (alert_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'silver'
);

-- =============================================================================
-- END OF SILVER LAYER DDL
-- =============================================================================
