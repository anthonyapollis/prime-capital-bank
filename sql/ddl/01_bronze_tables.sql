-- =============================================================================
-- PRIME CAPITAL BANK — BRONZE LAYER (Raw Ingestion)
-- Unity Catalog / Delta Lake — Databricks SQL
-- Layer   : Bronze (raw, unvalidated, append-only source data)
-- Author  : Data Engineering Team
-- Created : 2026-06-24
-- Notes   : All tables retain source data exactly as received.
--           No transformations are applied. Audit columns (_ingested_at,
--           _source_system, _batch_id) are appended at load time.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. bronze_customers
--    Source: CRM system (Salesforce / internal CRM export)
--    Frequency: Daily full + incremental CDC
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_customers;

CREATE TABLE prime_capital.bronze.bronze_customers (
  -- Primary identifiers
  customer_id                 STRING        COMMENT 'Source CRM customer identifier (UUID)',
  legacy_customer_id          STRING        COMMENT 'Legacy core banking customer number',
  crm_account_id              STRING        COMMENT 'CRM account object ID',
  -- Personal demographics
  title                       STRING        COMMENT 'Title: Mr / Mrs / Ms / Dr / Prof',
  first_name                  STRING        COMMENT 'Legal first name as per identity document',
  middle_name                 STRING        COMMENT 'Middle name(s)',
  last_name                   STRING        COMMENT 'Legal surname as per identity document',
  preferred_name              STRING        COMMENT 'Preferred name used by customer',
  date_of_birth               STRING        COMMENT 'DOB in source format (YYYY-MM-DD expected)',
  gender                      STRING        COMMENT 'Gender: M/F/NB/U (Unknown)',
  nationality                 STRING        COMMENT 'ISO 3166-1 alpha-2 nationality code',
  country_of_birth            STRING        COMMENT 'ISO 3166-1 alpha-2 country of birth',
  home_language               STRING        COMMENT 'Home language: EN/AF/ZU/XH/ST/TN/SS/VE/TS/NR/ND',
  marital_status              STRING        COMMENT 'Marital status: Single/Married/Divorced/Widowed',
  -- Identity documents (KYC)
  id_type                     STRING        COMMENT 'ID document type: SA_ID/PASSPORT/ASYLUM/TEMP_PERMIT',
  id_number                   STRING        COMMENT 'South African ID number or passport number',
  passport_number             STRING        COMMENT 'Passport number if foreign national',
  passport_expiry_date        STRING        COMMENT 'Passport expiry date',
  passport_country            STRING        COMMENT 'Passport issuing country ISO code',
  tax_number                  STRING        COMMENT 'SARS income tax reference number',
  vat_number                  STRING        COMMENT 'VAT registration number (for corporate-linked customers)',
  fica_status                 STRING        COMMENT 'FICA compliance status: COMPLIANT/PENDING/EXPIRED/EXEMPTED',
  fica_expiry_date            STRING        COMMENT 'Date FICA documentation expires',
  fica_verified_date          STRING        COMMENT 'Date FICA verification was completed',
  kyc_risk_rating             STRING        COMMENT 'KYC risk rating at onboarding: LOW/MEDIUM/HIGH',
  pep_flag                    STRING        COMMENT 'Politically Exposed Person flag: Y/N',
  pep_category                STRING        COMMENT 'PEP category if flagged',
  sanctions_flag              STRING        COMMENT 'OFAC/UN/EU sanctions list flag: Y/N',
  -- Contact information
  email_address               STRING        COMMENT 'Primary email address',
  email_address_secondary     STRING        COMMENT 'Secondary email address',
  mobile_number               STRING        COMMENT 'Mobile phone number (international format)',
  home_phone_number           STRING        COMMENT 'Home telephone number',
  work_phone_number           STRING        COMMENT 'Work telephone number',
  -- Residential address
  residential_address_line1   STRING        COMMENT 'Street number and street name',
  residential_address_line2   STRING        COMMENT 'Suburb',
  residential_city            STRING        COMMENT 'City / town',
  residential_province        STRING        COMMENT 'South African province code',
  residential_postal_code     STRING        COMMENT 'Postal code',
  residential_country         STRING        COMMENT 'Country ISO code',
  -- Postal address
  postal_address_line1        STRING        COMMENT 'Postal address line 1',
  postal_address_line2        STRING        COMMENT 'Postal address line 2',
  postal_city                 STRING        COMMENT 'Postal city',
  postal_province             STRING        COMMENT 'Postal province code',
  postal_code                 STRING        COMMENT 'Postal code',
  same_as_residential         STRING        COMMENT 'Flag if postal = residential: Y/N',
  -- Employment & income
  employment_status           STRING        COMMENT 'EMPLOYED/SELF_EMPLOYED/RETIRED/UNEMPLOYED/STUDENT',
  employer_name               STRING        COMMENT 'Name of current employer',
  employer_registration_no    STRING        COMMENT 'Employer company registration number',
  occupation                  STRING        COMMENT 'Occupation / job title',
  industry_code               STRING        COMMENT 'ISIC industry classification code',
  years_employed              STRING        COMMENT 'Years with current employer',
  gross_monthly_income        STRING        COMMENT 'Gross monthly income (source format)',
  net_monthly_income          STRING        COMMENT 'Net monthly income (source format)',
  income_verification_method  STRING        COMMENT 'PAYSLIP/BANK_STATEMENT/TAX_RETURN/SELF_DECLARED',
  income_verified_date        STRING        COMMENT 'Date income was last verified',
  -- Customer segmentation
  customer_type               STRING        COMMENT 'RETAIL/CORPORATE/SME/PRIVATE_BANKING/PREMIER',
  customer_segment            STRING        COMMENT 'Source system segment code',
  customer_status             STRING        COMMENT 'ACTIVE/INACTIVE/DECEASED/DORMANT/BLACKLISTED',
  -- Dates
  onboarding_date             STRING        COMMENT 'Date customer relationship commenced',
  last_review_date            STRING        COMMENT 'Date of last customer review / refresh',
  closed_date                 STRING        COMMENT 'Date customer relationship closed (if applicable)',
  -- Consent
  marketing_consent           STRING        COMMENT 'Marketing consent: Y/N',
  popi_consent                STRING        COMMENT 'POPIA consent given: Y/N',
  popi_consent_date           STRING        COMMENT 'Date POPIA consent was obtained',
  -- Audit columns
  _source_system              STRING        COMMENT 'Source system name (CRM / CORE_BANKING)',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion into bronze',
  _file_name                  STRING        COMMENT 'Source file name or API endpoint',
  _record_hash                STRING        COMMENT 'MD5 hash of record for deduplication'
)
COMMENT 'Bronze raw customer data ingested from CRM system. Contains all demographics, KYC, identity, address, and employment fields in source format. No transformations applied.'
PARTITIONED BY (_ingested_at DATE)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'CRM'
);

-- ---------------------------------------------------------------------------
-- 2. bronze_accounts
--    Source: Core Banking System (Temenos T24 / FinnOne)
--    Frequency: Daily full extract
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_accounts;

CREATE TABLE prime_capital.bronze.bronze_accounts (
  account_id                  STRING        COMMENT 'Core banking account number',
  account_uuid                STRING        COMMENT 'UUID surrogate key from source',
  customer_id                 STRING        COMMENT 'Owning customer identifier (FK to bronze_customers)',
  joint_customer_id           STRING        COMMENT 'Joint account holder customer ID',
  account_type_code           STRING        COMMENT 'Raw account type code from core banking',
  account_type_desc           STRING        COMMENT 'Account type description',
  product_code                STRING        COMMENT 'Banking product code',
  product_name                STRING        COMMENT 'Banking product name',
  currency_code               STRING        COMMENT 'Account currency ISO 4217 code',
  account_status              STRING        COMMENT 'OPEN/CLOSED/DORMANT/FROZEN/BLOCKED',
  account_status_reason       STRING        COMMENT 'Reason for non-OPEN status',
  open_date                   STRING        COMMENT 'Date account was opened',
  close_date                  STRING        COMMENT 'Date account was closed (if applicable)',
  last_transaction_date       STRING        COMMENT 'Date of most recent transaction',
  branch_code                 STRING        COMMENT 'Home branch code',
  relationship_manager_id     STRING        COMMENT 'Assigned RM employee ID',
  current_balance             STRING        COMMENT 'Current ledger balance (source format)',
  available_balance           STRING        COMMENT 'Available balance after holds',
  hold_amount                 STRING        COMMENT 'Total amount under hold/lien',
  overdraft_limit             STRING        COMMENT 'Approved overdraft limit',
  overdraft_utilised          STRING        COMMENT 'Current overdraft amount used',
  interest_rate               STRING        COMMENT 'Annual interest rate applied to account',
  interest_accrued_ytd        STRING        COMMENT 'Year-to-date interest accrued',
  interest_paid_ytd           STRING        COMMENT 'Year-to-date interest paid out',
  fees_charged_ytd            STRING        COMMENT 'Year-to-date fees charged',
  minimum_balance             STRING        COMMENT 'Contractual minimum balance requirement',
  notice_period_days          STRING        COMMENT 'Notice period for withdrawals (fixed/notice accounts)',
  maturity_date               STRING        COMMENT 'Maturity date for fixed deposit accounts',
  rollover_instruction        STRING        COMMENT 'ROLLOVER_CAPITAL/ROLLOVER_WITH_INTEREST/CLOSE',
  tier_code                   STRING        COMMENT 'Interest tier / rate band code',
  linked_account_id           STRING        COMMENT 'Linked account for sweep / salary credit',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _file_name                  STRING        COMMENT 'Source file name',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw account data from core banking system. Includes all account types: current, savings, fixed deposit, money market, offshore, and business accounts.'
PARTITIONED BY (_ingested_at DATE)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'CORE_BANKING'
);

-- ---------------------------------------------------------------------------
-- 3. bronze_transactions
--    Source: Core Banking Ledger (high volume — partitioned by date)
--    Frequency: Near-real-time / hourly batch
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_transactions;

CREATE TABLE prime_capital.bronze.bronze_transactions (
  transaction_id              STRING        COMMENT 'Unique transaction identifier from source',
  transaction_uuid            STRING        COMMENT 'UUID generated by source system',
  account_id                  STRING        COMMENT 'Account number on which transaction posted',
  customer_id                 STRING        COMMENT 'Customer who owns the account',
  transaction_date            DATE          COMMENT 'Business date on which transaction occurred',
  transaction_datetime        STRING        COMMENT 'Full timestamp including time (source format)',
  value_date                  STRING        COMMENT 'Value date for interest purposes',
  posting_date                STRING        COMMENT 'Date transaction was posted to ledger',
  transaction_type_code       STRING        COMMENT 'Source transaction type code',
  transaction_type_desc       STRING        COMMENT 'Human-readable transaction type description',
  transaction_category        STRING        COMMENT 'Categorisation code: DEBIT/CREDIT/FEE/INTEREST/ADJUSTMENT',
  amount                      STRING        COMMENT 'Transaction amount in account currency (source format)',
  amount_usd                  STRING        COMMENT 'USD equivalent amount for reporting',
  currency_code               STRING        COMMENT 'Transaction currency ISO 4217',
  exchange_rate               STRING        COMMENT 'Exchange rate applied (if FX)',
  running_balance             STRING        COMMENT 'Account balance after transaction',
  available_balance_after     STRING        COMMENT 'Available balance after transaction',
  debit_credit_indicator      STRING        COMMENT 'D=Debit, C=Credit',
  reference_number            STRING        COMMENT 'Customer or bank reference number',
  description                 STRING        COMMENT 'Transaction narration / description',
  counter_party_account       STRING        COMMENT 'Counter-party account number (if applicable)',
  counter_party_name          STRING        COMMENT 'Counter-party name',
  counter_party_bank_code     STRING        COMMENT 'Counter-party bank sort code / SWIFT BIC',
  channel_code                STRING        COMMENT 'Transaction channel: ATM/MOBILE/ONLINE/BRANCH/POS/EFT',
  device_id                   STRING        COMMENT 'ATM or device identifier',
  merchant_id                 STRING        COMMENT 'Merchant identifier (for POS transactions)',
  merchant_name               STRING        COMMENT 'Merchant name',
  merchant_mcc                STRING        COMMENT 'Merchant Category Code (ISO 18245)',
  merchant_city               STRING        COMMENT 'Merchant city',
  merchant_country            STRING        COMMENT 'Merchant country ISO code',
  card_number_masked          STRING        COMMENT 'Masked card number used (if card transaction)',
  authorisation_code          STRING        COMMENT 'Payment authorisation code',
  batch_number                STRING        COMMENT 'Batch number for batch-posted transactions',
  reversal_flag               STRING        COMMENT 'Y if this is a reversal transaction',
  original_transaction_id     STRING        COMMENT 'Original transaction ID for reversals',
  fee_amount                  STRING        COMMENT 'Fee component of transaction',
  tax_amount                  STRING        COMMENT 'VAT / tax component',
  gl_account_code             STRING        COMMENT 'General ledger account code',
  cost_centre_code            STRING        COMMENT 'Cost centre code for revenue attribution',
  branch_code                 STRING        COMMENT 'Processing branch code',
  teller_id                   STRING        COMMENT 'Teller employee ID (for branch transactions)',
  ip_address                  STRING        COMMENT 'IP address for online/mobile transactions',
  session_id                  STRING        COMMENT 'Online/mobile session identifier',
  fraud_score                 STRING        COMMENT 'Real-time fraud score from detection engine',
  fraud_flag                  STRING        COMMENT 'Y if transaction flagged by fraud engine',
  compliance_hold             STRING        COMMENT 'Y if transaction held for compliance review',
  notes                       STRING        COMMENT 'Additional notes or exception information',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _file_name                  STRING        COMMENT 'Source file or stream name',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw transaction ledger from core banking. High-volume table (500M+ rows annually). Partitioned by transaction_date for efficient querying. Contains all debit/credit postings across all account types and channels.'
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  'delta.dataSkippingNumIndexedCols' = '5',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'CORE_BANKING_LEDGER'
);

-- ---------------------------------------------------------------------------
-- 4. bronze_loan_applications
--    Source: Loan Origination System (LOS)
--    Frequency: Daily
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_loan_applications;

CREATE TABLE prime_capital.bronze.bronze_loan_applications (
  application_id              STRING        COMMENT 'Unique loan application identifier',
  application_reference       STRING        COMMENT 'Customer-facing application reference number',
  customer_id                 STRING        COMMENT 'Applicant customer identifier',
  joint_applicant_id          STRING        COMMENT 'Joint applicant customer ID (if applicable)',
  application_date            STRING        COMMENT 'Date application was submitted',
  application_channel         STRING        COMMENT 'BRANCH/ONLINE/MOBILE/BROKER/CALL_CENTRE',
  loan_type                   STRING        COMMENT 'PERSONAL/HOME/VEHICLE/COMMERCIAL/REVOLVING',
  loan_purpose                STRING        COMMENT 'Purpose of loan: HOME_PURCHASE/RENOVATION/EDUCATION/etc',
  requested_amount            STRING        COMMENT 'Amount requested by applicant',
  requested_term_months       STRING        COMMENT 'Requested loan term in months',
  requested_repayment_amount  STRING        COMMENT 'Requested monthly repayment amount',
  interest_rate_type          STRING        COMMENT 'FIXED/VARIABLE/PRIME_LINKED',
  property_value              STRING        COMMENT 'Property value (for home loans)',
  property_address            STRING        COMMENT 'Property street address',
  property_type               STRING        COMMENT 'SECTIONAL_TITLE/FREEHOLD/COMMERCIAL/AGRICULTURAL',
  vehicle_make                STRING        COMMENT 'Vehicle make (for vehicle finance)',
  vehicle_model               STRING        COMMENT 'Vehicle model',
  vehicle_year                STRING        COMMENT 'Vehicle year of manufacture',
  vehicle_price               STRING        COMMENT 'Vehicle purchase price',
  deposit_amount              STRING        COMMENT 'Customer deposit / down payment',
  applicant_gross_income      STRING        COMMENT 'Applicant gross monthly income at application',
  applicant_net_income        STRING        COMMENT 'Applicant net monthly income at application',
  existing_obligations        STRING        COMMENT 'Total existing monthly debt obligations',
  credit_score                STRING        COMMENT 'Credit bureau score at application',
  bureau_name                 STRING        COMMENT 'Credit bureau: TransUnion/Experian/Compuscan/XDS',
  bureau_enquiry_date         STRING        COMMENT 'Date of credit bureau enquiry',
  dti_ratio                   STRING        COMMENT 'Debt-to-income ratio calculated at application',
  nca_affordability_amount    STRING        COMMENT 'NCA affordability assessment result',
  application_status          STRING        COMMENT 'PENDING/IN_REVIEW/APPROVED/DECLINED/WITHDRAWN/REFERRED',
  decision_date               STRING        COMMENT 'Date decision was made',
  decision_reason_code        STRING        COMMENT 'Decline or referral reason code',
  decision_reason_desc        STRING        COMMENT 'Human-readable decision reason',
  approved_amount             STRING        COMMENT 'Amount approved (may differ from requested)',
  approved_term_months        STRING        COMMENT 'Approved loan term in months',
  approved_interest_rate      STRING        COMMENT 'Approved annual interest rate',
  approved_monthly_instalment STRING        COMMENT 'Approved monthly repayment',
  initiating_employee_id      STRING        COMMENT 'Employee who captured / initiated application',
  underwriter_employee_id     STRING        COMMENT 'Employee who underrode the application',
  branch_code                 STRING        COMMENT 'Branch where application was captured',
  documents_received          STRING        COMMENT 'JSON list of documents received',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _file_name                  STRING        COMMENT 'Source file name',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw loan application data from the Loan Origination System. Covers personal, home, vehicle, commercial, and revolving credit applications including credit bureau scores and affordability assessments.'
PARTITIONED BY (_ingested_at DATE)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'LOAN_ORIGINATION'
);

-- ---------------------------------------------------------------------------
-- 5. bronze_loans
--    Source: Loan Management System (active loan book)
--    Frequency: Daily snapshot
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_loans;

CREATE TABLE prime_capital.bronze.bronze_loans (
  loan_id                     STRING        COMMENT 'Unique loan account identifier',
  application_id              STRING        COMMENT 'Originating application ID',
  customer_id                 STRING        COMMENT 'Borrower customer identifier',
  loan_type                   STRING        COMMENT 'PERSONAL/HOME/VEHICLE/COMMERCIAL/REVOLVING',
  loan_status                 STRING        COMMENT 'ACTIVE/SETTLED/IN_ARREARS/DEFAULT/WRITTEN_OFF/LEGAL',
  disbursement_date           STRING        COMMENT 'Date loan proceeds were disbursed',
  maturity_date               STRING        COMMENT 'Loan maturity date',
  original_amount             STRING        COMMENT 'Original approved loan amount',
  outstanding_balance         STRING        COMMENT 'Current outstanding principal balance',
  original_term_months        STRING        COMMENT 'Original approved term in months',
  remaining_term_months       STRING        COMMENT 'Remaining term in months',
  interest_rate               STRING        COMMENT 'Current annual interest rate',
  rate_type                   STRING        COMMENT 'FIXED/VARIABLE/PRIME_LINKED',
  prime_rate_margin           STRING        COMMENT 'Margin above/below prime rate (for linked rates)',
  current_instalment          STRING        COMMENT 'Current monthly instalment amount',
  principal_paid              STRING        COMMENT 'Total principal repaid to date',
  interest_paid               STRING        COMMENT 'Total interest paid to date',
  fees_paid                   STRING        COMMENT 'Total fees paid to date',
  interest_accrued            STRING        COMMENT 'Interest accrued but not yet paid',
  arrears_amount              STRING        COMMENT 'Amount in arrears',
  arrears_days                STRING        COMMENT 'Number of days in arrears',
  missed_instalments          STRING        COMMENT 'Count of missed instalments',
  last_payment_date           STRING        COMMENT 'Date of most recent payment received',
  last_payment_amount         STRING        COMMENT 'Amount of most recent payment',
  next_payment_date           STRING        COMMENT 'Next scheduled payment due date',
  next_payment_amount         STRING        COMMENT 'Next scheduled payment amount',
  collateral_type             STRING        COMMENT 'PROPERTY/VEHICLE/SURETY/CESSION/NONE',
  collateral_value            STRING        COMMENT 'Current estimated collateral value',
  collateral_ref              STRING        COMMENT 'Collateral reference (bond number / reg plate)',
  ltv_ratio                   STRING        COMMENT 'Current loan-to-value ratio',
  provision_rate              STRING        COMMENT 'IFRS 9 provision rate applied',
  provision_amount            STRING        COMMENT 'IFRS 9 provision amount',
  ecl_stage                   STRING        COMMENT 'IFRS 9 ECL stage: 1/2/3',
  restructured_flag           STRING        COMMENT 'Y if loan has been restructured',
  restructure_date            STRING        COMMENT 'Date of most recent restructure',
  legal_action_date           STRING        COMMENT 'Date legal action commenced',
  write_off_date              STRING        COMMENT 'Date loan was written off',
  write_off_amount            STRING        COMMENT 'Amount written off',
  npl_flag                    STRING        COMMENT 'Y if classified as non-performing loan',
  npl_date                    STRING        COMMENT 'Date loan first classified as NPL',
  relationship_manager_id     STRING        COMMENT 'Assigned relationship manager',
  branch_code                 STRING        COMMENT 'Administering branch code',
  product_code                STRING        COMMENT 'Product code',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _snapshot_date              DATE          COMMENT 'Date of daily snapshot',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw loan book data from Loan Management System. Daily snapshot of all active and recently closed loans including arrears, NPL status, collateral, and IFRS 9 provision information.'
PARTITIONED BY (_snapshot_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'LOAN_MANAGEMENT'
);

-- ---------------------------------------------------------------------------
-- 6. bronze_payments
--    Source: Payment Gateway / SARB payment streams
--    Frequency: Real-time / near-real-time
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_payments;

CREATE TABLE prime_capital.bronze.bronze_payments (
  payment_id                  STRING        COMMENT 'Unique payment instruction identifier',
  payment_reference           STRING        COMMENT 'Customer-facing payment reference',
  payment_type                STRING        COMMENT 'EFT/RTGS/SWIFT/INTERNAL/CASH/DEBIT_ORDER',
  payment_direction           STRING        COMMENT 'OUTBOUND/INBOUND',
  payment_status              STRING        COMMENT 'PENDING/PROCESSING/SETTLED/FAILED/RECALLED/RETURNED',
  submission_datetime         STRING        COMMENT 'Date and time payment was submitted',
  processing_datetime         STRING        COMMENT 'Date and time payment entered processing',
  settlement_datetime         STRING        COMMENT 'Date and time payment settled',
  settlement_date             STRING        COMMENT 'Business settlement date',
  originator_account_id       STRING        COMMENT 'Paying account number',
  originator_customer_id      STRING        COMMENT 'Paying customer ID',
  originator_name             STRING        COMMENT 'Paying party name',
  originator_bank_code        STRING        COMMENT 'Originator bank universal branch code',
  beneficiary_account_id      STRING        COMMENT 'Receiving account number',
  beneficiary_customer_id     STRING        COMMENT 'Receiving customer ID (if internal)',
  beneficiary_name            STRING        COMMENT 'Beneficiary name',
  beneficiary_bank_code       STRING        COMMENT 'Beneficiary bank sort code / BIC',
  beneficiary_bank_name       STRING        COMMENT 'Beneficiary bank name',
  beneficiary_country         STRING        COMMENT 'Beneficiary country ISO code',
  payment_amount              STRING        COMMENT 'Payment amount',
  payment_currency            STRING        COMMENT 'Payment currency ISO 4217',
  zar_equivalent              STRING        COMMENT 'ZAR equivalent amount',
  exchange_rate               STRING        COMMENT 'Exchange rate applied',
  fee_amount                  STRING        COMMENT 'Transaction fee charged',
  payment_narration           STRING        COMMENT 'Payment narration / statement reference',
  swift_message_type          STRING        COMMENT 'SWIFT message type: MT103/MT202/MT910/etc',
  swift_uetr                  STRING        COMMENT 'SWIFT Unique End-to-End Transaction Reference',
  sarb_reference              STRING        COMMENT 'SARB / SAMOS reference number',
  reason_code                 STRING        COMMENT 'Failure or return reason code',
  channel                     STRING        COMMENT 'ONLINE/MOBILE/BRANCH/API/BATCH',
  batch_id                    STRING        COMMENT 'Batch file identifier for bulk payments',
  failure_reason              STRING        COMMENT 'Failure reason description',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw payment data from payment gateway and SARB streams. Covers EFT, RTGS, SWIFT international payments, internal transfers, and debit orders. Includes settlement tracking and failure reasons.'
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'PAYMENT_GATEWAY'
);

-- ---------------------------------------------------------------------------
-- 7. bronze_credit_cards
--    Source: Card Management System (Visa/Mastercard processor)
--    Frequency: Daily
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_credit_cards;

CREATE TABLE prime_capital.bronze.bronze_credit_cards (
  card_id                     STRING        COMMENT 'Unique card account identifier',
  card_number_masked          STRING        COMMENT 'Masked PAN: first 6 + last 4 digits',
  card_number_token           STRING        COMMENT 'Tokenised PAN from vault',
  customer_id                 STRING        COMMENT 'Primary cardholder customer ID',
  supplementary_customer_id   STRING        COMMENT 'Supplementary card holder customer ID',
  account_id                  STRING        COMMENT 'Linked current/savings account ID',
  card_type                   STRING        COMMENT 'CREDIT/DEBIT/CHEQUE/PREPAID',
  card_scheme                 STRING        COMMENT 'VISA/MASTERCARD/AMEX',
  card_tier                   STRING        COMMENT 'STANDARD/GOLD/PLATINUM/SIGNATURE/INFINITE',
  card_status                 STRING        COMMENT 'ACTIVE/BLOCKED/EXPIRED/CANCELLED/LOST/STOLEN',
  card_status_reason          STRING        COMMENT 'Reason for non-ACTIVE status',
  issue_date                  STRING        COMMENT 'Card issue date',
  expiry_date                 STRING        COMMENT 'Card expiry date (MM/YY)',
  credit_limit                STRING        COMMENT 'Approved credit limit',
  available_credit            STRING        COMMENT 'Available credit as of snapshot date',
  current_balance             STRING        COMMENT 'Current outstanding balance',
  minimum_payment_due         STRING        COMMENT 'Minimum payment required',
  payment_due_date            STRING        COMMENT 'Payment due date',
  last_payment_date           STRING        COMMENT 'Date of last payment received',
  last_payment_amount         STRING        COMMENT 'Amount of last payment',
  overlimit_amount            STRING        COMMENT 'Amount exceeding credit limit (if overlimit)',
  interest_rate_purchase      STRING        COMMENT 'Interest rate on purchase balance',
  interest_rate_cash          STRING        COMMENT 'Interest rate on cash advance balance',
  annual_fee                  STRING        COMMENT 'Annual card fee charged',
  reward_points_balance       STRING        COMMENT 'Current loyalty/reward points balance',
  reward_points_ytd           STRING        COMMENT 'Reward points earned year to date',
  contactless_enabled         STRING        COMMENT 'Y if contactless payments enabled',
  online_transactions_enabled STRING        COMMENT 'Y if online transactions enabled',
  international_enabled       STRING        COMMENT 'Y if international transactions enabled',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _snapshot_date              DATE          COMMENT 'Date of daily snapshot',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw credit card account data from card management system. Daily snapshot of card accounts, limits, balances, and status including all Visa/Mastercard products across all tiers.'
PARTITIONED BY (_snapshot_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'CARD_MANAGEMENT'
);

-- ---------------------------------------------------------------------------
-- 8. bronze_card_transactions
--    Source: Card Authorisation / Settlement System
--    Frequency: Real-time streaming + daily settlement batch
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_card_transactions;

CREATE TABLE prime_capital.bronze.bronze_card_transactions (
  card_tx_id                  STRING        COMMENT 'Unique card transaction identifier',
  authorisation_code          STRING        COMMENT 'Card authorisation approval code',
  card_id                     STRING        COMMENT 'Card account identifier',
  card_number_masked          STRING        COMMENT 'Masked PAN',
  card_number_token           STRING        COMMENT 'Tokenised PAN',
  customer_id                 STRING        COMMENT 'Cardholder customer ID',
  transaction_datetime        STRING        COMMENT 'Date and time of transaction (local)',
  transaction_datetime_utc    STRING        COMMENT 'Date and time of transaction (UTC)',
  transaction_date            DATE          COMMENT 'Business date of transaction',
  settlement_date             STRING        COMMENT 'Settlement date',
  transaction_type            STRING        COMMENT 'PURCHASE/CASH_ADVANCE/REFUND/REVERSAL/FEE/INTEREST',
  transaction_status          STRING        COMMENT 'AUTHORISED/SETTLED/DECLINED/REVERSED',
  decline_reason              STRING        COMMENT 'Reason for declined transaction',
  amount                      STRING        COMMENT 'Transaction amount in transaction currency',
  billing_amount              STRING        COMMENT 'Amount in card account currency (ZAR)',
  transaction_currency        STRING        COMMENT 'Transaction currency ISO code',
  billing_currency            STRING        COMMENT 'Billing currency (ZAR)',
  exchange_rate               STRING        COMMENT 'FX rate applied',
  fx_markup_percent           STRING        COMMENT 'FX markup percentage charged',
  merchant_id                 STRING        COMMENT 'Merchant identifier',
  merchant_name               STRING        COMMENT 'Merchant trading name',
  merchant_mcc                STRING        COMMENT 'Merchant Category Code',
  merchant_city               STRING        COMMENT 'Merchant city',
  merchant_province           STRING        COMMENT 'Merchant province/state',
  merchant_country            STRING        COMMENT 'Merchant country ISO code',
  merchant_postal_code        STRING        COMMENT 'Merchant postal code',
  terminal_id                 STRING        COMMENT 'POS terminal identifier',
  entry_mode                  STRING        COMMENT 'CHIP/MAGNETIC_STRIPE/CONTACTLESS/ONLINE/CNP',
  is_3ds_verified             STRING        COMMENT 'Y if 3D Secure verification completed',
  is_recurring                STRING        COMMENT 'Y if recurring / subscription transaction',
  is_international            STRING        COMMENT 'Y if transaction outside South Africa',
  fraud_score                 STRING        COMMENT 'Real-time fraud score (0-100)',
  fraud_flag                  STRING        COMMENT 'Y if flagged by fraud detection engine',
  fraud_type                  STRING        COMMENT 'Fraud type code if flagged',
  chargeback_flag             STRING        COMMENT 'Y if chargeback has been raised',
  chargeback_date             STRING        COMMENT 'Date chargeback was raised',
  chargeback_reason           STRING        COMMENT 'ISO 8583 chargeback reason code',
  reward_points_earned        STRING        COMMENT 'Reward points earned on this transaction',
  cashback_amount             STRING        COMMENT 'Cashback amount if applicable',
  device_fingerprint          STRING        COMMENT 'Device fingerprint for CNP transactions',
  ip_address                  STRING        COMMENT 'IP address for online transactions',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw card transaction data from the card authorisation and settlement system. Covers all POS, online, contactless, and cash advance transactions for both credit and debit cards. Partitioned by transaction_date.'
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'CARD_AUTHORISATION'
);

-- ---------------------------------------------------------------------------
-- 9. bronze_fraud_alerts
--    Source: Fraud Detection Engine (SAS Fraud Management / Featurespace ARIC)
--    Frequency: Real-time event stream
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_fraud_alerts;

CREATE TABLE prime_capital.bronze.bronze_fraud_alerts (
  alert_id                    STRING        COMMENT 'Unique fraud alert identifier',
  alert_reference             STRING        COMMENT 'Human-readable alert reference number',
  alert_datetime              STRING        COMMENT 'Date and time alert was generated',
  alert_type                  STRING        COMMENT 'CARD_FRAUD/ACCOUNT_TAKEOVER/IDENTITY/APPLICATION/INTERNAL',
  alert_source                STRING        COMMENT 'Detection rule or model that raised alert',
  alert_priority              STRING        COMMENT 'Priority: P1_CRITICAL/P2_HIGH/P3_MEDIUM/P4_LOW',
  customer_id                 STRING        COMMENT 'Customer associated with alert',
  account_id                  STRING        COMMENT 'Account associated with alert',
  card_id                     STRING        COMMENT 'Card associated with alert (if applicable)',
  transaction_id              STRING        COMMENT 'Triggering transaction ID',
  transaction_amount          STRING        COMMENT 'Suspicious transaction amount',
  fraud_score                 STRING        COMMENT 'Composite fraud risk score (0-1000)',
  fraud_model_version         STRING        COMMENT 'Version of fraud model that generated score',
  rule_triggered              STRING        COMMENT 'Fraud rule code(s) triggered',
  alert_status                STRING        COMMENT 'OPEN/UNDER_REVIEW/CONFIRMED/FALSE_POSITIVE/CLOSED',
  analyst_id                  STRING        COMMENT 'Fraud analyst assigned to alert',
  disposition_date            STRING        COMMENT 'Date alert was dispositioned',
  disposition_reason          STRING        COMMENT 'Reason for disposition decision',
  loss_amount                 STRING        COMMENT 'Confirmed fraud loss amount',
  recovery_amount             STRING        COMMENT 'Amount recovered from fraud loss',
  channel                     STRING        COMMENT 'Channel where fraud occurred',
  device_id                   STRING        COMMENT 'Device identifier involved',
  ip_address                  STRING        COMMENT 'IP address involved',
  location                    STRING        COMMENT 'Geographic location at time of alert',
  previous_alerts_count       STRING        COMMENT 'Count of previous alerts on this customer',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw fraud alerts from the fraud detection engine. Covers card fraud, account takeover, identity fraud, application fraud, and internal fraud. Real-time stream supplemented by daily batch reconciliation.'
PARTITIONED BY (_ingested_at DATE)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'FRAUD_ENGINE'
);

-- ---------------------------------------------------------------------------
-- 10. bronze_treasury_trades
--     Source: Treasury Management System (Finastra Summit / Murex)
--     Frequency: End-of-day + intraday updates
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_treasury_trades;

CREATE TABLE prime_capital.bronze.bronze_treasury_trades (
  trade_id                    STRING        COMMENT 'Unique trade identifier from TMS',
  trade_reference             STRING        COMMENT 'Front-office trade reference number',
  trade_date                  DATE          COMMENT 'Date trade was executed',
  value_date                  STRING        COMMENT 'Value / settlement date of trade',
  maturity_date               STRING        COMMENT 'Instrument maturity date',
  trade_type                  STRING        COMMENT 'BOND/FX_SPOT/FX_FORWARD/IRS/CDS/EQUITY/MONEY_MARKET/REPO',
  trade_direction             STRING        COMMENT 'BUY/SELL/LEND/BORROW',
  instrument_id               STRING        COMMENT 'Instrument identifier / ISIN',
  instrument_name             STRING        COMMENT 'Instrument description / name',
  instrument_type             STRING        COMMENT 'GOVERNMENT_BOND/CORPORATE_BOND/EQUITY/DERIVATIVE/FX',
  currency                    STRING        COMMENT 'Trade currency ISO code',
  notional_amount             STRING        COMMENT 'Notional / face value of trade',
  price                       STRING        COMMENT 'Trade price',
  yield                       STRING        COMMENT 'Yield at trade date',
  coupon_rate                 STRING        COMMENT 'Coupon rate for bonds',
  counterparty_id             STRING        COMMENT 'Counterparty institution ID',
  counterparty_name           STRING        COMMENT 'Counterparty full name',
  counterparty_lei            STRING        COMMENT 'Counterparty Legal Entity Identifier',
  counterparty_rating         STRING        COMMENT 'Counterparty credit rating (Moodys/S&P/Fitch)',
  trader_id                   STRING        COMMENT 'Trader employee ID',
  desk                        STRING        COMMENT 'Trading desk: RATES/FX/CREDIT/EQUITY/MM',
  portfolio_id                STRING        COMMENT 'Portfolio identifier',
  book_id                     STRING        COMMENT 'Trading book identifier (FVTPL/AFS/HTM)',
  mtm_value                   STRING        COMMENT 'Mark-to-market value as of snapshot',
  unrealised_pnl              STRING        COMMENT 'Unrealised profit and loss',
  realised_pnl                STRING        COMMENT 'Realised profit and loss',
  accrued_interest            STRING        COMMENT 'Accrued interest receivable/payable',
  duration                    STRING        COMMENT 'Modified duration (for bonds)',
  dv01                        STRING        COMMENT 'DV01 / dollar value of 1bp',
  trade_status                STRING        COMMENT 'LIVE/MATURED/CANCELLED/AMENDED',
  settlement_status           STRING        COMMENT 'UNSETTLED/SETTLED/FAILED',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _snapshot_date              DATE          COMMENT 'EOD snapshot date',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw treasury trade data from Treasury Management System. Covers bonds, FX spot and forwards, interest rate swaps, repo, money market, and equity trades. End-of-day snapshot including MTM values.'
PARTITIONED BY (trade_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'TREASURY_MANAGEMENT'
);

-- ---------------------------------------------------------------------------
-- 11. bronze_gl_entries
--     Source: General Ledger System (SAP / Oracle Financials)
--     Frequency: Daily (with intraday for large transactions)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_gl_entries;

CREATE TABLE prime_capital.bronze.bronze_gl_entries (
  journal_id                  STRING        COMMENT 'Unique journal entry identifier',
  journal_line_number         STRING        COMMENT 'Line number within journal entry',
  journal_reference           STRING        COMMENT 'Journal reference number',
  journal_type                STRING        COMMENT 'MANUAL/AUTO/REVERSAL/ADJUSTMENT/OPENING/CLOSING',
  posting_date                DATE          COMMENT 'GL posting date',
  accounting_period           STRING        COMMENT 'Accounting period (YYYY-MM)',
  financial_year              STRING        COMMENT 'Financial year (April-March cycle)',
  gl_account_code             STRING        COMMENT 'General ledger account code',
  gl_account_name             STRING        COMMENT 'GL account description',
  account_type                STRING        COMMENT 'ASSET/LIABILITY/EQUITY/INCOME/EXPENSE',
  cost_centre_code            STRING        COMMENT 'Cost centre code',
  cost_centre_name            STRING        COMMENT 'Cost centre description',
  legal_entity                STRING        COMMENT 'Legal entity code (Prime Capital Bank / subsidiaries)',
  business_unit               STRING        COMMENT 'Business unit code',
  debit_amount                STRING        COMMENT 'Debit amount in functional currency (ZAR)',
  credit_amount               STRING        COMMENT 'Credit amount in functional currency (ZAR)',
  transaction_currency        STRING        COMMENT 'Transaction currency ISO code',
  transaction_amount          STRING        COMMENT 'Amount in transaction currency',
  fx_rate                     STRING        COMMENT 'FX rate used for conversion to ZAR',
  description                 STRING        COMMENT 'Journal entry description / narration',
  source_reference            STRING        COMMENT 'Source system transaction reference',
  source_module               STRING        COMMENT 'Source module: LOANS/CARDS/TREASURY/PAYMENTS/PAYROLL',
  created_by                  STRING        COMMENT 'User who created the journal',
  approved_by                 STRING        COMMENT 'User who approved the journal',
  is_intercompany             STRING        COMMENT 'Y if intercompany journal',
  intercompany_entity         STRING        COMMENT 'Intercompany counterparty entity',
  reversal_flag               STRING        COMMENT 'Y if this is a reversal entry',
  original_journal_id         STRING        COMMENT 'Original journal ID for reversals',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw general ledger journal entries from SAP/Oracle. Covers all debit/credit postings across the chart of accounts including manual adjustments, automated postings from sub-ledgers, and intercompany entries.'
PARTITIONED BY (posting_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'GENERAL_LEDGER'
);

-- ---------------------------------------------------------------------------
-- 12. bronze_branches
--     Source: Branch Management System / HR System
--     Frequency: Weekly (relatively static)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_branches;

CREATE TABLE prime_capital.bronze.bronze_branches (
  branch_id                   STRING        COMMENT 'Unique branch identifier',
  branch_code                 STRING        COMMENT 'Universal branch code (6-digit SARB code)',
  branch_name                 STRING        COMMENT 'Branch trading name',
  branch_type                 STRING        COMMENT 'FULL_SERVICE/MINI/AGENCY/KIOSK/MOBILE',
  branch_status               STRING        COMMENT 'OPEN/CLOSED/TEMPORARILY_CLOSED/RELOCATED',
  address_line1               STRING        COMMENT 'Street address line 1',
  address_line2               STRING        COMMENT 'Suburb',
  city                        STRING        COMMENT 'City / town',
  province                    STRING        COMMENT 'Province code',
  postal_code                 STRING        COMMENT 'Postal code',
  latitude                    STRING        COMMENT 'GPS latitude',
  longitude                   STRING        COMMENT 'GPS longitude',
  region_code                 STRING        COMMENT 'Internal regional grouping code',
  region_name                 STRING        COMMENT 'Region name',
  area_classification         STRING        COMMENT 'URBAN/PERI_URBAN/RURAL/TOWNSHIP',
  atm_count                   STRING        COMMENT 'Number of ATMs at/near this branch',
  teller_count                STRING        COMMENT 'Number of teller stations',
  opening_hours               STRING        COMMENT 'Operating hours in structured format',
  manager_employee_id         STRING        COMMENT 'Branch manager employee ID',
  open_date                   STRING        COMMENT 'Date branch opened',
  close_date                  STRING        COMMENT 'Date branch closed (if applicable)',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw branch master data. Covers all 200+ bank branches including full service, mini-branches, agencies, and kiosks across South Africa with geographic coordinates and operational metadata.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'BRANCH_MANAGEMENT'
);

-- ---------------------------------------------------------------------------
-- 13. bronze_employees
--     Source: HR System (SAP SuccessFactors)
--     Frequency: Daily
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_employees;

CREATE TABLE prime_capital.bronze.bronze_employees (
  employee_id                 STRING        COMMENT 'Unique employee identifier (staff number)',
  employee_uuid               STRING        COMMENT 'HR system UUID',
  id_number                   STRING        COMMENT 'Employee South African ID number',
  title                       STRING        COMMENT 'Title: Mr/Mrs/Ms/Dr',
  first_name                  STRING        COMMENT 'Employee first name',
  last_name                   STRING        COMMENT 'Employee surname',
  preferred_name              STRING        COMMENT 'Preferred working name',
  date_of_birth               STRING        COMMENT 'Date of birth',
  gender                      STRING        COMMENT 'Gender for EE reporting',
  race                        STRING        COMMENT 'Race category for BBBEE/EE: A/C/I/W',
  disability_flag             STRING        COMMENT 'Y if employee has declared disability',
  nationality                 STRING        COMMENT 'Nationality ISO code',
  job_title                   STRING        COMMENT 'Job title as per organogram',
  job_grade                   STRING        COMMENT 'Job grade / payscale band',
  job_family                  STRING        COMMENT 'Job family: BANKING/OPERATIONS/TECHNOLOGY/RISK/HR/FINANCE',
  department_code             STRING        COMMENT 'Department code',
  department_name             STRING        COMMENT 'Department name',
  division_code               STRING        COMMENT 'Division code',
  division_name               STRING        COMMENT 'Division name',
  cost_centre_code            STRING        COMMENT 'Cost centre for payroll allocation',
  branch_code                 STRING        COMMENT 'Primary work branch code (NULL for head office)',
  manager_employee_id         STRING        COMMENT 'Direct line manager employee ID',
  employment_type             STRING        COMMENT 'PERMANENT/CONTRACT/TEMPORARY/PART_TIME',
  employment_status           STRING        COMMENT 'ACTIVE/RESIGNED/DISMISSED/DECEASED/RETIRED',
  hire_date                   STRING        COMMENT 'Date employee joined the bank',
  termination_date            STRING        COMMENT 'Date employment ended (if applicable)',
  termination_reason          STRING        COMMENT 'Reason for termination',
  annual_salary               STRING        COMMENT 'Annual cost to company (CTC)',
  notice_period_weeks         STRING        COMMENT 'Contractual notice period in weeks',
  leave_balance_days          STRING        COMMENT 'Current annual leave balance',
  email_address               STRING        COMMENT 'Corporate email address',
  work_phone                  STRING        COMMENT 'Work phone number',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _snapshot_date              DATE          COMMENT 'Date of HR snapshot',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw employee/HR data from SAP SuccessFactors. Daily snapshot covering all staff including demographics, job details, department hierarchy, compensation structure, and employment status.'
PARTITIONED BY (_snapshot_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'HR_SUCCESSFACTORS'
);

-- ---------------------------------------------------------------------------
-- 14. bronze_aml_alerts
--     Source: AML Transaction Monitoring System (NICE Actimize / Temenos AML)
--     Frequency: Daily batch + real-time triggers
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.bronze.bronze_aml_alerts;

CREATE TABLE prime_capital.bronze.bronze_aml_alerts (
  alert_id                    STRING        COMMENT 'Unique AML alert identifier',
  alert_reference             STRING        COMMENT 'Alert reference number for case management',
  alert_datetime              STRING        COMMENT 'Date and time alert was generated',
  alert_type                  STRING        COMMENT 'STRUCTURING/UNUSUAL_PATTERN/HIGH_RISK_COUNTRY/PEP_ACTIVITY/CASH_INTENSIVE/LAYERING',
  alert_scenario              STRING        COMMENT 'Specific AML scenario / rule that triggered alert',
  alert_priority              STRING        COMMENT 'HIGH/MEDIUM/LOW',
  customer_id                 STRING        COMMENT 'Customer associated with alert',
  account_id                  STRING        COMMENT 'Account associated with alert',
  transaction_ids             STRING        COMMENT 'Pipe-delimited list of relevant transaction IDs',
  total_amount_flagged        STRING        COMMENT 'Total transaction amount that triggered alert',
  lookback_period_days        STRING        COMMENT 'Analysis window in days (e.g., 30, 90)',
  transaction_count           STRING        COMMENT 'Number of transactions in the pattern',
  alert_status                STRING        COMMENT 'OPEN/UNDER_REVIEW/CLOSED_NO_ACTION/SAR_FILED/REFERRED',
  analyst_id                  STRING        COMMENT 'AML analyst assigned to the alert',
  case_id                     STRING        COMMENT 'Case management case ID (if escalated)',
  review_start_date           STRING        COMMENT 'Date review commenced',
  review_end_date             STRING        COMMENT 'Date review was completed',
  closure_reason              STRING        COMMENT 'Reason for closing alert without SAR',
  sar_filed_flag              STRING        COMMENT 'Y if Suspicious Activity Report was filed with FIC',
  sar_reference               STRING        COMMENT 'FIC SAR reference number',
  sar_amount                  STRING        COMMENT 'Amount reported in SAR',
  risk_score                  STRING        COMMENT 'AML risk score at time of alert',
  -- Audit
  _source_system              STRING        COMMENT 'Source system name',
  _batch_id                   STRING        COMMENT 'Ingestion batch identifier',
  _ingested_at                TIMESTAMP     COMMENT 'UTC timestamp of ingestion',
  _record_hash                STRING        COMMENT 'MD5 hash for deduplication'
)
COMMENT 'Bronze raw AML alerts from transaction monitoring system. Covers structuring, unusual patterns, high-risk country flows, PEP activity, and cash-intensive behaviour. Includes SAR filing status for FIC reporting.'
PARTITIONED BY (_ingested_at DATE)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'bronze',
  'source.system'                    = 'AML_MONITORING'
);

-- =============================================================================
-- END OF BRONZE LAYER DDL
-- =============================================================================
