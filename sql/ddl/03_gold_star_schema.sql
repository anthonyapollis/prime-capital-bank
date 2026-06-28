-- =============================================================================
-- PRIME CAPITAL BANK — GOLD LAYER (Star Schema — Production Analytics)
-- Unity Catalog / Delta Lake — Databricks SQL
-- Layer   : Gold (curated, performance-optimised, business-ready)
-- Author  : Data Engineering Team
-- Created : 2026-06-24
-- Notes   : All dimension tables implement SCD Type 2.
--           Surrogate keys (SK) are BIGINT GENERATED ALWAYS AS IDENTITY.
--           Fact tables are partitioned for query performance at 500M+ row scale.
--           Z-ORDER columns noted in TBLPROPERTIES for OPTIMIZE runs.
-- =============================================================================

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- ---------------------------------------------------------------------------
-- DIM_CUSTOMER
--    SCD2 customer dimension with full KYC, segmentation, and risk profile.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_customer;

CREATE TABLE prime_capital.gold.dim_customer (
  -- Surrogate key
  customer_sk                 BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key (auto-increment)',
  -- Natural key
  customer_id                 STRING        NOT NULL COMMENT 'Source CRM customer identifier',
  -- SCD2 control
  effective_date              DATE          NOT NULL COMMENT 'Version effective date',
  expiry_date                 DATE          NOT NULL COMMENT 'Version expiry date (9999-12-31 for current)',
  is_current                  BOOLEAN       NOT NULL COMMENT 'True for the currently active version',
  -- Demographics
  title                       STRING        COMMENT 'Title',
  first_name                  STRING        COMMENT 'Legal first name',
  middle_name                 STRING        COMMENT 'Middle name(s)',
  last_name                   STRING        COMMENT 'Legal surname',
  full_name                   STRING        COMMENT 'Full name (derived)',
  preferred_name              STRING        COMMENT 'Preferred name',
  date_of_birth               DATE          COMMENT 'Date of birth',
  age_years                   INT           COMMENT 'Age in years at effective_date',
  age_band                    STRING        COMMENT '18-25/26-35/36-45/46-55/56-65/65+',
  gender                      STRING        COMMENT 'MALE/FEMALE/NON_BINARY/UNKNOWN',
  nationality                 STRING        COMMENT 'ISO nationality code',
  country_of_birth            STRING        COMMENT 'ISO country of birth',
  home_language               STRING        COMMENT 'Home language code',
  marital_status              STRING        COMMENT 'Marital status',
  -- KYC / compliance
  id_type                     STRING        COMMENT 'SA_ID/PASSPORT/ASYLUM/TEMP_PERMIT',
  fica_status                 STRING        COMMENT 'COMPLIANT/PENDING/EXPIRED/EXEMPTED',
  fica_expiry_date            DATE          COMMENT 'FICA documentation expiry date',
  kyc_risk_rating             STRING        COMMENT 'KYC risk rating at onboarding',
  pep_flag                    BOOLEAN       COMMENT 'Politically Exposed Person',
  sanctions_flag              BOOLEAN       COMMENT 'On sanctions list',
  -- Contact
  email_address               STRING        COMMENT 'Primary email address',
  mobile_number               STRING        COMMENT 'Mobile in E.164 format',
  -- Address
  residential_province        STRING        COMMENT 'Province code: GP/WC/KZN/EC/FS/LP/MP/NW/NC',
  residential_province_name   STRING        COMMENT 'Full province name',
  residential_city            STRING        COMMENT 'City',
  residential_postal_code     STRING        COMMENT 'Postal code',
  residential_country         STRING        COMMENT 'Country ISO code',
  -- Employment and income
  employment_status           STRING        COMMENT 'EMPLOYED/SELF_EMPLOYED/RETIRED/UNEMPLOYED/STUDENT',
  employer_name               STRING        COMMENT 'Employer name',
  industry_code               STRING        COMMENT 'ISIC industry code',
  gross_monthly_income        DECIMAL(18,2) COMMENT 'Gross monthly income in ZAR',
  net_monthly_income          DECIMAL(18,2) COMMENT 'Net monthly income in ZAR',
  annual_income               DECIMAL(18,2) COMMENT 'Annual income (gross x 12)',
  income_band                 STRING        COMMENT '<5K/5-15K/15-30K/30-60K/60-120K/120K+',
  -- Segmentation
  customer_type               STRING        COMMENT 'RETAIL/CORPORATE/SME/PRIVATE_BANKING/PREMIER',
  customer_segment            STRING        COMMENT 'Internal segment code',
  customer_status             STRING        COMMENT 'ACTIVE/INACTIVE/DECEASED/DORMANT/BLACKLISTED',
  risk_segment                STRING        COMMENT 'LOW_RISK/STANDARD/ELEVATED/HIGH_RISK/WATCHLIST',
  value_segment               STRING        COMMENT 'Value segment based on income and product holding',
  clv_tier                    STRING        COMMENT 'BRONZE/SILVER/GOLD/PLATINUM',
  -- Tenure
  onboarding_date             DATE          COMMENT 'Date customer relationship commenced',
  days_since_onboarding       INT           COMMENT 'Days since onboarding',
  tenure_years                DECIMAL(5,2)  COMMENT 'Tenure in years',
  tenure_band                 STRING        COMMENT '<1yr/1-3yr/3-5yr/5-10yr/10yr+',
  -- Consent
  marketing_consent           BOOLEAN       COMMENT 'Marketing consent',
  popi_consent                BOOLEAN       COMMENT 'POPIA consent',
  -- Audit
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp',
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold customer dimension. SCD Type 2. Covers all retail, SME, corporate, private, and premier banking customers with full KYC, demographic, segmentation, and risk profile.'
PARTITIONED BY (customer_type)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'gold',
  'scd.type'                         = '2',
  'zorder.columns'                   = 'customer_id,is_current'
);

-- ---------------------------------------------------------------------------
-- DIM_ACCOUNT
--    SCD2 account dimension.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_account;

CREATE TABLE prime_capital.gold.dim_account (
  account_sk                  BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  account_id                  STRING        NOT NULL COMMENT 'Core banking account number',
  effective_date              DATE          NOT NULL COMMENT 'Version effective date',
  expiry_date                 DATE          NOT NULL COMMENT 'Expiry date',
  is_current                  BOOLEAN       NOT NULL COMMENT 'Current version flag',
  customer_id                 STRING        COMMENT 'Owning customer ID',
  account_type_code           STRING        COMMENT 'CURRENT/SAVINGS/FIXED/MONEY_MARKET/OFFSHORE/BUSINESS',
  account_type_desc           STRING        COMMENT 'Full account type description',
  product_code                STRING        COMMENT 'Product code',
  product_name                STRING        COMMENT 'Product name',
  currency_code               STRING        COMMENT 'Account currency ISO code',
  is_foreign_currency         BOOLEAN       COMMENT 'Non-ZAR currency flag',
  account_status              STRING        COMMENT 'OPEN/CLOSED/DORMANT/FROZEN/BLOCKED',
  open_date                   DATE          COMMENT 'Account open date',
  close_date                  DATE          COMMENT 'Account close date',
  branch_code                 STRING        COMMENT 'Home branch code',
  relationship_manager_id     STRING        COMMENT 'Assigned RM employee ID',
  credit_limit                DECIMAL(18,2) COMMENT 'Credit limit (overdraft or card)',
  interest_rate               DECIMAL(8,4)  COMMENT 'Interest rate',
  is_joint_account            BOOLEAN       COMMENT 'True if joint account',
  maturity_date               DATE          COMMENT 'Maturity date (fixed deposits)',
  dormancy_flag               BOOLEAN       COMMENT 'Currently dormant flag',
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold account dimension. SCD Type 2. Covers all account types across all products with status, product, and branch linkage.'
PARTITIONED BY (account_type_code)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold',
  'scd.type'                         = '2'
);

-- ---------------------------------------------------------------------------
-- DIM_PRODUCT
--    50+ banking products across all categories.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_product;

CREATE TABLE prime_capital.gold.dim_product (
  product_sk                  BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  product_code                STRING        NOT NULL COMMENT 'Unique product code',
  product_name                STRING        COMMENT 'Full product name',
  product_short_name          STRING        COMMENT 'Short/display name',
  product_category            STRING        COMMENT 'RETAIL/BUSINESS/CORPORATE/INVESTMENT/INSURANCE',
  product_type                STRING        COMMENT 'ACCOUNT/LOAN/CARD/INVESTMENT/INSURANCE/FOREX',
  product_sub_type            STRING        COMMENT 'Specific sub-type within type',
  product_status              STRING        COMMENT 'ACTIVE/DISCONTINUED/LEGACY/PILOT',
  -- Interest and fees
  base_interest_rate          DECIMAL(8,4)  COMMENT 'Base interest rate for this product',
  rate_basis                  STRING        COMMENT 'FIXED/PRIME_LINKED/JIBAR_LINKED',
  minimum_balance             DECIMAL(18,2) COMMENT 'Minimum balance requirement',
  monthly_fee                 DECIMAL(18,2) COMMENT 'Standard monthly administration fee',
  annual_fee                  DECIMAL(18,2) COMMENT 'Annual fee (for cards)',
  -- Eligibility
  minimum_income_required     DECIMAL(18,2) COMMENT 'Minimum monthly income for eligibility',
  minimum_age                 INT           COMMENT 'Minimum customer age',
  maximum_age                 INT           COMMENT 'Maximum customer age (for credit products)',
  -- SCD2
  effective_date              DATE          COMMENT 'Product version effective date',
  expiry_date                 DATE          COMMENT 'Product version expiry',
  is_current                  BOOLEAN       COMMENT 'Current version flag',
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold product dimension. Covers all 50+ banking products: Flexi Savings, Business Current, Premier Current, Gold Cheque, Platinum Savings, 32-Day Notice, 12-Month Fixed, 24-Month Fixed, Money Market, Offshore USD, Offshore GBP, Offshore EUR, Home Loan, Home Loan Variable, Buy-to-Let, Home Equity, Personal Loan 12M, Personal Loan 24M, Personal Loan 36M, Personal Loan 60M, Vehicle Finance New, Vehicle Finance Used, Commercial Property Loan, Business Term Loan, Revolving Credit Plan, Overdraft Facility, Business Overdraft, Standard Credit Card, Gold Credit Card, Platinum Credit Card, Signature Credit Card, Infinite Credit Card, Business Credit Card, Prepaid Card, Debit Card, EFT Standard, Real-Time EFT, SWIFT International, RTGS, Forex Account, Forex Fixed Forward, Forex Options, Government Bond, Corporate Bond, Treasury Bill, Money Market Fund, Unit Trust, Linked Investment, Endowment, and Business Insurance.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- Seed product reference data
INSERT INTO prime_capital.gold.dim_product
  (product_code, product_name, product_short_name, product_category, product_type, product_sub_type, product_status, base_interest_rate, monthly_fee, effective_date, expiry_date, is_current)
VALUES
  -- Savings and deposit accounts
  ('SAV001', 'Flexi Savings Account',            'Flexi Savings',        'RETAIL',     'ACCOUNT', 'SAVINGS',       'ACTIVE', 5.50,  0.00,   '2020-01-01', '9999-12-31', true),
  ('SAV002', 'Platinum Savings Account',         'Platinum Savings',     'RETAIL',     'ACCOUNT', 'SAVINGS',       'ACTIVE', 7.25,  0.00,   '2020-01-01', '9999-12-31', true),
  ('SAV003', '32-Day Notice Deposit',            '32-Day Notice',        'RETAIL',     'ACCOUNT', 'NOTICE',        'ACTIVE', 8.10,  0.00,   '2020-01-01', '9999-12-31', true),
  ('FXD001', '12-Month Fixed Deposit',           '12M Fixed',            'RETAIL',     'ACCOUNT', 'FIXED_DEPOSIT',  'ACTIVE', 9.50,  0.00,   '2020-01-01', '9999-12-31', true),
  ('FXD002', '24-Month Fixed Deposit',           '24M Fixed',            'RETAIL',     'ACCOUNT', 'FIXED_DEPOSIT',  'ACTIVE', 9.75,  0.00,   '2020-01-01', '9999-12-31', true),
  ('FXD003', '36-Month Fixed Deposit',           '36M Fixed',            'RETAIL',     'ACCOUNT', 'FIXED_DEPOSIT',  'ACTIVE', 10.00, 0.00,   '2020-01-01', '9999-12-31', true),
  ('MMF001', 'Money Market Account',             'Money Market',         'RETAIL',     'ACCOUNT', 'MONEY_MARKET',  'ACTIVE', 8.75,  0.00,   '2020-01-01', '9999-12-31', true),
  ('OFF001', 'Offshore USD Account',             'USD Account',          'RETAIL',     'ACCOUNT', 'OFFSHORE',      'ACTIVE', 2.00,  15.00,  '2020-01-01', '9999-12-31', true),
  ('OFF002', 'Offshore GBP Account',             'GBP Account',          'RETAIL',     'ACCOUNT', 'OFFSHORE',      'ACTIVE', 2.50,  15.00,  '2020-01-01', '9999-12-31', true),
  ('OFF003', 'Offshore EUR Account',             'EUR Account',          'RETAIL',     'ACCOUNT', 'OFFSHORE',      'ACTIVE', 1.75,  15.00,  '2020-01-01', '9999-12-31', true),
  -- Transactional accounts
  ('CHQ001', 'Gold Cheque Account',              'Gold Cheque',          'RETAIL',     'ACCOUNT', 'CURRENT',       'ACTIVE', 0.00,  95.00,  '2020-01-01', '9999-12-31', true),
  ('CHQ002', 'Platinum Cheque Account',          'Platinum Cheque',      'RETAIL',     'ACCOUNT', 'CURRENT',       'ACTIVE', 0.00,  175.00, '2020-01-01', '9999-12-31', true),
  ('CHQ003', 'Premier Current Account',          'Premier Current',      'RETAIL',     'ACCOUNT', 'CURRENT',       'ACTIVE', 0.50,  350.00, '2020-01-01', '9999-12-31', true),
  ('BUS001', 'Business Current Account',         'Business Current',     'BUSINESS',   'ACCOUNT', 'CURRENT',       'ACTIVE', 0.00,  250.00, '2020-01-01', '9999-12-31', true),
  ('BUS002', 'Business Plus Account',            'Business Plus',        'BUSINESS',   'ACCOUNT', 'CURRENT',       'ACTIVE', 0.25,  450.00, '2020-01-01', '9999-12-31', true),
  -- Loans
  ('PL001',  'Personal Loan 12 Month',           'Personal Loan 12M',    'RETAIL',     'LOAN',    'PERSONAL',      'ACTIVE', 19.75, 0.00,   '2020-01-01', '9999-12-31', true),
  ('PL002',  'Personal Loan 24 Month',           'Personal Loan 24M',    'RETAIL',     'LOAN',    'PERSONAL',      'ACTIVE', 20.50, 0.00,   '2020-01-01', '9999-12-31', true),
  ('PL003',  'Personal Loan 36 Month',           'Personal Loan 36M',    'RETAIL',     'LOAN',    'PERSONAL',      'ACTIVE', 21.25, 0.00,   '2020-01-01', '9999-12-31', true),
  ('PL004',  'Personal Loan 60 Month',           'Personal Loan 60M',    'RETAIL',     'LOAN',    'PERSONAL',      'ACTIVE', 22.00, 0.00,   '2020-01-01', '9999-12-31', true),
  ('HL001',  'Home Loan Variable Rate',          'Home Loan Variable',   'RETAIL',     'LOAN',    'HOME',          'ACTIVE', 11.75, 0.00,   '2020-01-01', '9999-12-31', true),
  ('HL002',  'Home Loan Fixed Rate',             'Home Loan Fixed',      'RETAIL',     'LOAN',    'HOME',          'ACTIVE', 12.50, 0.00,   '2020-01-01', '9999-12-31', true),
  ('HL003',  'Buy-to-Let Home Loan',             'Buy-to-Let',           'RETAIL',     'LOAN',    'HOME',          'ACTIVE', 12.25, 0.00,   '2020-01-01', '9999-12-31', true),
  ('HL004',  'Home Equity Access Bond',          'Home Equity',          'RETAIL',     'LOAN',    'HOME',          'ACTIVE', 11.75, 0.00,   '2020-01-01', '9999-12-31', true),
  ('VF001',  'Vehicle Finance New Vehicle',      'Vehicle Finance New',  'RETAIL',     'LOAN',    'VEHICLE',       'ACTIVE', 13.50, 0.00,   '2020-01-01', '9999-12-31', true),
  ('VF002',  'Vehicle Finance Used Vehicle',     'Vehicle Finance Used',  'RETAIL',    'LOAN',    'VEHICLE',       'ACTIVE', 15.25, 0.00,   '2020-01-01', '9999-12-31', true),
  ('OD001',  'Overdraft Facility',               'Overdraft',            'RETAIL',     'LOAN',    'OVERDRAFT',     'ACTIVE', 21.00, 57.00,  '2020-01-01', '9999-12-31', true),
  ('RCP001', 'Revolving Credit Plan',            'Revolving Credit',     'RETAIL',     'LOAN',    'REVOLVING',     'ACTIVE', 20.75, 0.00,   '2020-01-01', '9999-12-31', true),
  ('BL001',  'Business Term Loan',               'Business Term Loan',   'BUSINESS',   'LOAN',    'COMMERCIAL',    'ACTIVE', 14.25, 0.00,   '2020-01-01', '9999-12-31', true),
  ('BL002',  'Commercial Property Loan',         'Comm Property Loan',   'BUSINESS',   'LOAN',    'COMMERCIAL',    'ACTIVE', 12.75, 0.00,   '2020-01-01', '9999-12-31', true),
  ('BOD001', 'Business Overdraft Facility',      'Business Overdraft',   'BUSINESS',   'LOAN',    'OVERDRAFT',     'ACTIVE', 19.50, 195.00, '2020-01-01', '9999-12-31', true),
  -- Credit cards
  ('CC001',  'Standard Credit Card',             'Standard Card',        'RETAIL',     'CARD',    'CREDIT',        'ACTIVE', 22.25, 0.00,   '2020-01-01', '9999-12-31', true),
  ('CC002',  'Gold Credit Card',                 'Gold Card',            'RETAIL',     'CARD',    'CREDIT',        'ACTIVE', 21.75, 0.00,   '2020-01-01', '9999-12-31', true),
  ('CC003',  'Platinum Credit Card',             'Platinum Card',        'RETAIL',     'CARD',    'CREDIT',        'ACTIVE', 21.00, 0.00,   '2020-01-01', '9999-12-31', true),
  ('CC004',  'Signature Credit Card',            'Signature Card',       'RETAIL',     'CARD',    'CREDIT',        'ACTIVE', 20.50, 0.00,   '2020-01-01', '9999-12-31', true),
  ('CC005',  'Infinite Credit Card',             'Infinite Card',        'RETAIL',     'CARD',    'CREDIT',        'ACTIVE', 20.00, 0.00,   '2020-01-01', '9999-12-31', true),
  ('CC006',  'Business Credit Card',             'Business Card',        'BUSINESS',   'CARD',    'CREDIT',        'ACTIVE', 21.50, 0.00,   '2020-01-01', '9999-12-31', true),
  ('CC007',  'Prepaid Card',                     'Prepaid Card',         'RETAIL',     'CARD',    'PREPAID',       'ACTIVE', 0.00,  25.00,  '2020-01-01', '9999-12-31', true),
  -- Investment products
  ('INV001', 'Money Market Fund',                'MM Fund',              'INVESTMENT', 'INVESTMENT', 'UNIT_TRUST',  'ACTIVE', 8.50,  0.00,  '2020-01-01', '9999-12-31', true),
  ('INV002', 'Balanced Unit Trust',              'Balanced Fund',        'INVESTMENT', 'INVESTMENT', 'UNIT_TRUST',  'ACTIVE', 0.00,  0.00,  '2020-01-01', '9999-12-31', true),
  ('INV003', 'Equity Unit Trust',                'Equity Fund',          'INVESTMENT', 'INVESTMENT', 'UNIT_TRUST',  'ACTIVE', 0.00,  0.00,  '2020-01-01', '9999-12-31', true),
  ('INV004', 'Linked Investment Plan',           'Linked Investment',    'INVESTMENT', 'INVESTMENT', 'LINKED',      'ACTIVE', 0.00,  0.00,  '2020-01-01', '9999-12-31', true),
  ('INV005', 'Endowment Policy',                 'Endowment',            'INVESTMENT', 'INVESTMENT', 'ENDOWMENT',   'ACTIVE', 0.00,  0.00,  '2020-01-01', '9999-12-31', true),
  -- Forex
  ('FX001',  'Forex Spot Account',               'Forex Spot',           'CORPORATE',  'FOREX',   'SPOT',          'ACTIVE', 0.00,  0.00,   '2020-01-01', '9999-12-31', true),
  ('FX002',  'Forex Fixed Forward',              'Forex Forward',        'CORPORATE',  'FOREX',   'FORWARD',       'ACTIVE', 0.00,  0.00,   '2020-01-01', '9999-12-31', true),
  ('FX003',  'Forex Option',                     'Forex Option',         'CORPORATE',  'FOREX',   'OPTION',        'ACTIVE', 0.00,  0.00,   '2020-01-01', '9999-12-31', true),
  -- Insurance
  ('INS001', 'Business Insurance Package',       'Business Insurance',   'BUSINESS',   'INSURANCE', 'BUSINESS',    'ACTIVE', 0.00,  0.00,   '2020-01-01', '9999-12-31', true),
  ('INS002', 'Credit Life Insurance',            'Credit Life',          'RETAIL',     'INSURANCE', 'CREDIT_LIFE', 'ACTIVE', 0.00,  0.00,   '2020-01-01', '9999-12-31', true);

-- ---------------------------------------------------------------------------
-- DIM_BRANCH
--    200 South African bank branches with full geographic metadata.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_branch;

CREATE TABLE prime_capital.gold.dim_branch (
  branch_sk                   BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  branch_id                   STRING        NOT NULL COMMENT 'Branch identifier',
  branch_code                 STRING        NOT NULL COMMENT '6-digit SARB universal branch code',
  branch_name                 STRING        COMMENT 'Branch trading name',
  branch_type                 STRING        COMMENT 'FULL_SERVICE/MINI/AGENCY/KIOSK/MOBILE',
  branch_status               STRING        COMMENT 'OPEN/CLOSED/TEMPORARILY_CLOSED',
  -- Geographic hierarchy
  address_line1               STRING        COMMENT 'Street address',
  suburb                      STRING        COMMENT 'Suburb',
  city                        STRING        COMMENT 'City / town',
  province_code               STRING        COMMENT 'Province: GP/WC/KZN/EC/FS/LP/MP/NW/NC',
  province_name               STRING        COMMENT 'Full province name',
  region_code                 STRING        COMMENT 'Internal region code',
  region_name                 STRING        COMMENT 'Region name (e.g. Gauteng North)',
  area_classification         STRING        COMMENT 'URBAN/PERI_URBAN/RURAL/TOWNSHIP',
  postal_code                 STRING        COMMENT 'Postal code',
  latitude                    DECIMAL(10,6) COMMENT 'GPS latitude',
  longitude                   DECIMAL(10,6) COMMENT 'GPS longitude',
  -- Operational
  atm_count                   INT           COMMENT 'Number of ATMs',
  teller_count                INT           COMMENT 'Number of teller stations',
  opening_hours               STRING        COMMENT 'Operating hours (JSON structure)',
  manager_employee_id         STRING        COMMENT 'Branch manager employee ID',
  open_date                   DATE          COMMENT 'Branch open date',
  -- SCD2
  effective_date              DATE          COMMENT 'Version effective date',
  expiry_date                 DATE          COMMENT 'Version expiry',
  is_current                  BOOLEAN       COMMENT 'Current version flag',
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold branch dimension. Covers all 200 Prime Capital Bank branches across South Africa including geographic coordinates, province, region, and area classification. SCD Type 2.'
PARTITIONED BY (province_code)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold',
  'scd.type'                         = '2'
);

-- ---------------------------------------------------------------------------
-- DIM_EMPLOYEE
--    Bank staff dimension with organisational hierarchy.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_employee;

CREATE TABLE prime_capital.gold.dim_employee (
  employee_sk                 BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  employee_id                 STRING        NOT NULL COMMENT 'Staff number',
  full_name                   STRING        COMMENT 'Full name',
  first_name                  STRING        COMMENT 'First name',
  last_name                   STRING        COMMENT 'Surname',
  gender                      STRING        COMMENT 'Gender (for EE reporting)',
  race                        STRING        COMMENT 'Race category: A/C/I/W',
  -- Job detail
  job_title                   STRING        COMMENT 'Job title',
  job_grade                   STRING        COMMENT 'Job grade/band',
  job_family                  STRING        COMMENT 'BANKING/OPERATIONS/TECHNOLOGY/RISK/HR/FINANCE',
  role_category               STRING        COMMENT 'TELLER/ADVISOR/MANAGER/ANALYST/SPECIALIST/EXECUTIVE',
  -- Organisational hierarchy
  department_code             STRING        COMMENT 'Department code',
  department_name             STRING        COMMENT 'Department name',
  division_code               STRING        COMMENT 'Division code',
  division_name               STRING        COMMENT 'Division name',
  cost_centre_code            STRING        COMMENT 'Cost centre',
  branch_code                 STRING        COMMENT 'Primary work branch (NULL = head office)',
  manager_employee_id         STRING        COMMENT 'Direct line manager ID',
  -- Employment
  employment_type             STRING        COMMENT 'PERMANENT/CONTRACT/TEMPORARY/PART_TIME',
  employment_status           STRING        COMMENT 'ACTIVE/RESIGNED/DISMISSED/DECEASED/RETIRED',
  hire_date                   DATE          COMMENT 'Date joined the bank',
  termination_date            DATE          COMMENT 'Date employment ended',
  service_years               DECIMAL(5,2)  COMMENT 'Years of service',
  -- SCD2
  effective_date              DATE          COMMENT 'Version effective date',
  expiry_date                 DATE          COMMENT 'Version expiry',
  is_current                  BOOLEAN       COMMENT 'Current version flag',
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold employee dimension with full organisational hierarchy, job family classification, and service tenure. SCD Type 2.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold',
  'scd.type'                         = '2'
);

-- ---------------------------------------------------------------------------
-- DIM_DATE
--    South African bank calendar: public holidays, financial year, month-end.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_date;

CREATE TABLE prime_capital.gold.dim_date (
  date_sk                     INT           NOT NULL COMMENT 'Surrogate key in YYYYMMDD integer format',
  date                        DATE          NOT NULL COMMENT 'Calendar date',
  -- Calendar attributes
  day_of_month                INT           COMMENT 'Day of month (1-31)',
  day_of_week                 INT           COMMENT 'Day of week (1=Monday, 7=Sunday)',
  day_name                    STRING        COMMENT 'Full day name (Monday, Tuesday, ...)',
  day_name_short              STRING        COMMENT 'Short day name (Mon, Tue, ...)',
  week_of_year                INT           COMMENT 'ISO week number (1-53)',
  month_number                INT           COMMENT 'Month number (1-12)',
  month_name                  STRING        COMMENT 'Full month name',
  month_name_short            STRING        COMMENT 'Short month name (Jan, Feb, ...)',
  month_start_date            DATE          COMMENT 'First day of the month',
  month_end_date              DATE          COMMENT 'Last day of the month',
  quarter_number              INT           COMMENT 'Calendar quarter (1-4)',
  quarter_name                STRING        COMMENT 'Q1/Q2/Q3/Q4',
  calendar_year               INT           COMMENT 'Calendar year (YYYY)',
  -- South African financial year (April - March)
  financial_year              STRING        COMMENT 'SA financial year label e.g. FY2025 (Apr 2024 - Mar 2025)',
  financial_year_start        DATE          COMMENT '1 April of the financial year',
  financial_year_end          DATE          COMMENT '31 March of the financial year',
  financial_quarter           STRING        COMMENT 'Financial quarter: FQ1/FQ2/FQ3/FQ4 (Apr-Jun/Jul-Sep/Oct-Dec/Jan-Mar)',
  financial_month             INT           COMMENT 'Month within financial year (1=April, 12=March)',
  -- Flags
  is_weekend                  BOOLEAN       COMMENT 'True if Saturday or Sunday',
  is_weekday                  BOOLEAN       COMMENT 'True if Monday to Friday',
  is_public_holiday           BOOLEAN       COMMENT 'True if South African public holiday',
  public_holiday_name         STRING        COMMENT 'Name of public holiday if applicable',
  is_business_day             BOOLEAN       COMMENT 'True if weekday and not public holiday',
  is_month_start              BOOLEAN       COMMENT 'True if first business day of month',
  is_month_end                BOOLEAN       COMMENT 'True if last business day of month',
  is_quarter_end              BOOLEAN       COMMENT 'True if last business day of calendar quarter',
  is_financial_year_end       BOOLEAN       COMMENT 'True if 31 March (SA financial year end)',
  is_salary_day               BOOLEAN       COMMENT 'True if 25th or last business day before 25th',
  -- Relative (populated daily as part of load)
  days_from_today             INT           COMMENT 'Positive = future, negative = past'
)
COMMENT 'Gold date dimension covering South African banking calendar with public holidays (New Years Day, Human Rights Day, Good Friday, Family Day, Freedom Day, Workers Day, Youth Day, National Womens Day, Heritage Day, Day of Reconciliation, Christmas Day, Day of Goodwill), financial year (Apr-Mar), salary day logic, and month/quarter end flags.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- ---------------------------------------------------------------------------
-- DIM_CHANNEL
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_channel;

CREATE TABLE prime_capital.gold.dim_channel (
  channel_sk                  BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  channel_code                STRING        NOT NULL COMMENT 'Channel code',
  channel_name                STRING        COMMENT 'Channel full name',
  channel_category            STRING        COMMENT 'DIGITAL/PHYSICAL/ELECTRONIC',
  channel_type                STRING        COMMENT 'SELF_SERVICE/ASSISTED/AUTOMATED',
  is_digital                  BOOLEAN       COMMENT 'True if digital channel',
  is_24_hour                  BOOLEAN       COMMENT 'True if available 24/7',
  channel_description         STRING        COMMENT 'Channel description',
  effective_date              DATE          COMMENT 'Version effective date',
  expiry_date                 DATE          COMMENT 'Expiry date',
  is_current                  BOOLEAN       COMMENT 'Current version flag'
)
COMMENT 'Gold channel dimension. Covers all customer interaction channels.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

INSERT INTO prime_capital.gold.dim_channel
  (channel_code, channel_name, channel_category, channel_type, is_digital, is_24_hour, channel_description, effective_date, expiry_date, is_current)
VALUES
  ('BRANCH',      'Branch',              'PHYSICAL',   'ASSISTED',    false, false, 'In-branch customer service at a physical location',             '2020-01-01', '9999-12-31', true),
  ('ATM',         'ATM',                 'PHYSICAL',   'SELF_SERVICE', false, true,  'Automated Teller Machine for cash and basic transactions',      '2020-01-01', '9999-12-31', true),
  ('ONLINE',      'Online Banking',      'DIGITAL',    'SELF_SERVICE', true,  true,  'Internet banking via web browser',                              '2020-01-01', '9999-12-31', true),
  ('MOBILE_APP',  'Mobile App',          'DIGITAL',    'SELF_SERVICE', true,  true,  'Prime Capital mobile banking application (iOS and Android)',     '2020-01-01', '9999-12-31', true),
  ('CALL_CENTRE', 'Call Centre',         'ELECTRONIC', 'ASSISTED',    false, true,  'Telephone banking and customer support centre',                 '2020-01-01', '9999-12-31', true),
  ('USSD',        'USSD Banking',        'DIGITAL',    'SELF_SERVICE', true,  true,  'USSD session-based banking (*120*321#) for basic transactions', '2020-01-01', '9999-12-31', true),
  ('CARDLESS_ATM','Cardless ATM',        'PHYSICAL',   'SELF_SERVICE', false, true,  'ATM cash withdrawal using mobile OTP instead of card',         '2020-01-01', '9999-12-31', true),
  ('CHATBOT',     'Chatbot',             'DIGITAL',    'AUTOMATED',   true,  true,  'AI-powered chatbot on mobile app and website',                  '2022-01-01', '9999-12-31', true),
  ('API',         'API Banking',         'DIGITAL',    'AUTOMATED',   true,  true,  'Open banking API for third-party integrations',                 '2021-06-01', '9999-12-31', true),
  ('POS',         'Point of Sale',       'ELECTRONIC', 'AUTOMATED',   false, true,  'Card POS terminal at merchant',                                '2020-01-01', '9999-12-31', true),
  ('EFT_BATCH',   'EFT Batch',          'ELECTRONIC', 'AUTOMATED',   false, true,  'Batch electronic funds transfer (debit/credit orders)',          '2020-01-01', '9999-12-31', true);

-- ---------------------------------------------------------------------------
-- DIM_CURRENCY
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_currency;

CREATE TABLE prime_capital.gold.dim_currency (
  currency_sk                 BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  currency_code               STRING        NOT NULL COMMENT 'ISO 4217 currency code',
  currency_name               STRING        COMMENT 'Full currency name',
  currency_symbol             STRING        COMMENT 'Currency symbol',
  country                     STRING        COMMENT 'Issuing country / region',
  is_base_currency            BOOLEAN       COMMENT 'True if ZAR (base reporting currency)',
  is_active                   BOOLEAN       COMMENT 'True if actively traded at Prime Capital',
  decimal_places              INT           COMMENT 'Standard decimal places for this currency'
)
COMMENT 'Gold currency dimension. Eight key currencies traded by Prime Capital Bank.'
TBLPROPERTIES ('quality.layer' = 'gold');

INSERT INTO prime_capital.gold.dim_currency
  (currency_code, currency_name, currency_symbol, country, is_base_currency, is_active, decimal_places)
VALUES
  ('ZAR', 'South African Rand',   'R',   'South Africa',  true,  true,  2),
  ('USD', 'US Dollar',            '$',   'United States', false, true,  2),
  ('EUR', 'Euro',                 '€',   'Euro Zone',     false, true,  2),
  ('GBP', 'British Pound',        '£',   'United Kingdom',false, true,  2),
  ('JPY', 'Japanese Yen',         '¥',   'Japan',         false, true,  0),
  ('CNY', 'Chinese Yuan Renminbi','¥',   'China',         false, true,  2),
  ('AED', 'UAE Dirham',           'د.إ', 'UAE',           false, true,  2),
  ('AUD', 'Australian Dollar',    'A$',  'Australia',     false, true,  2);

-- ---------------------------------------------------------------------------
-- DIM_GEOGRAPHY
--    South African provinces, cities, and suburbs.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_geography;

CREATE TABLE prime_capital.gold.dim_geography (
  geography_sk                BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  postal_code                 STRING        COMMENT '4-digit postal code',
  suburb                      STRING        COMMENT 'Suburb / area name',
  city                        STRING        COMMENT 'City or town',
  province_code               STRING        COMMENT 'Province: GP/WC/KZN/EC/FS/LP/MP/NW/NC',
  province_name               STRING        COMMENT 'Full province name',
  region                      STRING        COMMENT 'SARB operational region',
  area_type                   STRING        COMMENT 'URBAN/PERI_URBAN/RURAL/TOWNSHIP',
  municipality                STRING        COMMENT 'Local municipality name',
  district_municipality       STRING        COMMENT 'District municipality',
  latitude                    DECIMAL(10,6) COMMENT 'Centroid latitude',
  longitude                   DECIMAL(10,6) COMMENT 'Centroid longitude',
  country_code                STRING        COMMENT 'Country ISO code (ZA)',
  country_name                STRING        COMMENT 'Country name'
)
COMMENT 'Gold geography dimension. South African geographic hierarchy from postal code up to province. Used for branch, customer, and transaction geographic analysis.'
PARTITIONED BY (province_code)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- ---------------------------------------------------------------------------
-- DIM_RISK_CATEGORY
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_risk_category;

CREATE TABLE prime_capital.gold.dim_risk_category (
  risk_category_sk            BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  risk_tier_code              STRING        NOT NULL COMMENT 'Risk tier code',
  risk_tier_name              STRING        COMMENT 'Risk tier full name',
  risk_tier_order             INT           COMMENT 'Sort order (1=lowest risk)',
  risk_description            STRING        COMMENT 'Business definition of this risk tier',
  ecl_stage_mapping           INT           COMMENT 'Default IFRS 9 ECL stage for this tier',
  monitoring_frequency        STRING        COMMENT 'ANNUAL/SEMI_ANNUAL/QUARTERLY/MONTHLY',
  escalation_required         BOOLEAN       COMMENT 'True if this tier requires senior approval',
  is_watchlist                BOOLEAN       COMMENT 'True if customer is on watchlist'
)
COMMENT 'Gold risk category dimension. Five risk tiers from Low through Watchlist aligned to IFRS 9 ECL stages and Basel III requirements.'
TBLPROPERTIES ('quality.layer' = 'gold');

INSERT INTO prime_capital.gold.dim_risk_category
  (risk_tier_code, risk_tier_name, risk_tier_order, ecl_stage_mapping, monitoring_frequency, escalation_required, is_watchlist)
VALUES
  ('LOW',       'Low Risk',       1, 1, 'ANNUAL',       false, false),
  ('MEDIUM',    'Medium Risk',    2, 1, 'SEMI_ANNUAL',  false, false),
  ('HIGH',      'High Risk',      3, 2, 'QUARTERLY',    true,  false),
  ('VERY_HIGH', 'Very High Risk', 4, 2, 'MONTHLY',      true,  false),
  ('WATCHLIST', 'Watchlist',      5, 3, 'MONTHLY',      true,  true);

-- ---------------------------------------------------------------------------
-- DIM_MERCHANT
--    Card merchant reference dimension.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_merchant;

CREATE TABLE prime_capital.gold.dim_merchant (
  merchant_sk                 BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  merchant_id                 STRING        NOT NULL COMMENT 'Merchant identifier',
  merchant_name               STRING        COMMENT 'Merchant trading name',
  merchant_legal_name         STRING        COMMENT 'Merchant legal entity name',
  mcc_code                    STRING        COMMENT 'Merchant Category Code (ISO 18245)',
  mcc_description             STRING        COMMENT 'MCC code description',
  mcc_category                STRING        COMMENT 'Grouped MCC category: GROCERY/FUEL/RESTAURANT/TRAVEL/HEALTHCARE/etc',
  mcc_category_group          STRING        COMMENT 'High-level group: ESSENTIALS/LIFESTYLE/TRAVEL/FINANCIAL',
  merchant_country            STRING        COMMENT 'Country ISO code',
  merchant_city               STRING        COMMENT 'City',
  merchant_province           STRING        COMMENT 'Province (for SA merchants)',
  is_domestic                 BOOLEAN       COMMENT 'True if merchant based in South Africa',
  is_online_merchant          BOOLEAN       COMMENT 'True if primarily online merchant',
  effective_date              DATE          COMMENT 'Version effective date',
  expiry_date                 DATE          COMMENT 'Version expiry',
  is_current                  BOOLEAN       COMMENT 'Current version flag'
)
COMMENT 'Gold merchant dimension for card transaction analysis. Includes MCC codes, merchant category groupings, and domestic/international classification.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- ---------------------------------------------------------------------------
-- DIM_GL_ACCOUNT
--    5-level chart of accounts hierarchy.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_gl_account;

CREATE TABLE prime_capital.gold.dim_gl_account (
  gl_account_sk               BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  gl_account_code             STRING        NOT NULL COMMENT 'GL account code',
  gl_account_name             STRING        COMMENT 'GL account name',
  account_type                STRING        COMMENT 'ASSET/LIABILITY/EQUITY/INCOME/EXPENSE',
  normal_balance              STRING        COMMENT 'DEBIT/CREDIT (normal balance side)',
  -- 5-level hierarchy
  level1_code                 STRING        COMMENT 'Level 1: Legal Entity code',
  level1_name                 STRING        COMMENT 'Level 1: Legal Entity name (Prime Capital Bank Ltd)',
  level2_code                 STRING        COMMENT 'Level 2: Segment code (RETAIL/CORPORATE/TREASURY/SHARED)',
  level2_name                 STRING        COMMENT 'Level 2: Segment name',
  level3_code                 STRING        COMMENT 'Level 3: Division code',
  level3_name                 STRING        COMMENT 'Level 3: Division name',
  level4_code                 STRING        COMMENT 'Level 4: Department code',
  level4_name                 STRING        COMMENT 'Level 4: Department name',
  level5_code                 STRING        COMMENT 'Level 5: Account code (leaf)',
  level5_name                 STRING        COMMENT 'Level 5: Account description (leaf)',
  -- Reporting flags
  is_income_statement         BOOLEAN       COMMENT 'True if P&L account (INCOME or EXPENSE)',
  is_balance_sheet            BOOLEAN       COMMENT 'True if BS account (ASSET, LIABILITY, EQUITY)',
  is_revenue                  BOOLEAN       COMMENT 'True if revenue / interest income account',
  is_cost                     BOOLEAN       COMMENT 'True if cost / interest expense account',
  is_intercompany             BOOLEAN       COMMENT 'True if intercompany elimination account',
  -- Control
  is_active                   BOOLEAN       COMMENT 'True if account is active for posting',
  effective_date              DATE          COMMENT 'Version effective date',
  expiry_date                 DATE          COMMENT 'Version expiry',
  is_current                  BOOLEAN       COMMENT 'Current version flag'
)
COMMENT 'Gold GL account dimension. 5-level chart of accounts hierarchy: Entity > Segment > Division > Department > Account. Used for financial reporting, cost allocation, and regulatory submissions.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- ---------------------------------------------------------------------------
-- FACT_TRANSACTION
--    Grain: one row per ledger transaction posting.
--    Scale: 500M+ rows. Partitioned by transaction_date.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_transaction;

CREATE TABLE prime_capital.gold.fact_transaction (
  -- Surrogate key
  transaction_sk              BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Fact table surrogate key',
  -- Natural key
  transaction_id              STRING        NOT NULL COMMENT 'Source transaction identifier',
  -- Foreign keys to dimensions
  customer_sk                 BIGINT        COMMENT 'FK to dim_customer',
  account_sk                  BIGINT        COMMENT 'FK to dim_account',
  product_sk                  BIGINT        COMMENT 'FK to dim_product',
  branch_sk                   BIGINT        COMMENT 'FK to dim_branch',
  channel_sk                  BIGINT        COMMENT 'FK to dim_channel',
  currency_sk                 BIGINT        COMMENT 'FK to dim_currency',
  date_sk                     INT           COMMENT 'FK to dim_date (transaction_date)',
  value_date_sk               INT           COMMENT 'FK to dim_date (value_date)',
  merchant_sk                 BIGINT        COMMENT 'FK to dim_merchant (NULL if not card/POS)',
  employee_sk                 BIGINT        COMMENT 'FK to dim_employee (teller, if branch)',
  -- Degenerate dimensions (high cardinality codes kept on fact)
  transaction_type_code       STRING        COMMENT 'Standardised transaction type code',
  tx_category                 STRING        COMMENT 'DEBIT/CREDIT/FEE/INTEREST/ADJUSTMENT/REVERSAL',
  debit_credit_indicator      STRING        COMMENT 'DEBIT/CREDIT',
  reference_number            STRING        COMMENT 'Payment or transaction reference',
  -- Measures
  amount_zar                  DECIMAL(18,2) COMMENT 'Transaction amount in ZAR',
  amount_original             DECIMAL(18,2) COMMENT 'Amount in original transaction currency',
  exchange_rate               DECIMAL(18,6) COMMENT 'FX exchange rate applied',
  fee_amount_zar              DECIMAL(18,2) COMMENT 'Fee in ZAR',
  tax_amount_zar              DECIMAL(18,2) COMMENT 'VAT in ZAR',
  running_balance             DECIMAL(18,2) COMMENT 'Account running balance after transaction',
  fraud_score                 DECIMAL(8,4)  COMMENT 'Fraud risk score',
  -- Date
  transaction_date            DATE          NOT NULL COMMENT 'Business date (partition key)',
  transaction_datetime        TIMESTAMP     COMMENT 'Full transaction datetime (SAST)',
  -- Flags
  is_debit                    BOOLEAN       COMMENT 'True if debit transaction',
  is_reversal                 BOOLEAN       COMMENT 'True if reversal',
  is_digital                  BOOLEAN       COMMENT 'True if digital channel',
  is_international            BOOLEAN       COMMENT 'True if international merchant/counterparty',
  fraud_flag                  BOOLEAN       COMMENT 'Fraud flagged by detection engine',
  -- Audit
  _silver_processed_at        TIMESTAMP     COMMENT 'Silver processing timestamp',
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold transaction fact table. Grain = one row per ledger posting. 500M+ row scale. Partitioned by transaction_date. Z-ordered on customer_sk and account_sk for fast customer/account queries.'
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'  = 'true',
  'delta.autoOptimize.autoCompact'    = 'true',
  'delta.dataSkippingNumIndexedCols'  = '8',
  'quality.layer'                     = 'gold',
  'zorder.columns'                    = 'customer_sk,account_sk,channel_sk'
);

-- ---------------------------------------------------------------------------
-- FACT_LOAN_PORTFOLIO
--    Grain: one row per loan per day (daily snapshot).
--    Used for NPL tracking, IFRS 9 provisioning, and portfolio analytics.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_loan_portfolio;

CREATE TABLE prime_capital.gold.fact_loan_portfolio (
  loan_portfolio_sk           BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  loan_id                     STRING        NOT NULL COMMENT 'Loan account identifier',
  -- Foreign keys
  customer_sk                 BIGINT        COMMENT 'FK to dim_customer',
  account_sk                  BIGINT        COMMENT 'FK to dim_account',
  product_sk                  BIGINT        COMMENT 'FK to dim_product',
  branch_sk                   BIGINT        COMMENT 'FK to dim_branch',
  employee_sk                 BIGINT        COMMENT 'FK to dim_employee (RM)',
  date_sk                     INT           COMMENT 'FK to dim_date (snapshot_date)',
  risk_category_sk            BIGINT        COMMENT 'FK to dim_risk_category',
  -- Degenerate
  loan_type                   STRING        COMMENT 'PERSONAL/HOME/VEHICLE/COMMERCIAL/REVOLVING',
  loan_status                 STRING        COMMENT 'Loan status',
  ecl_stage                   INT           COMMENT 'IFRS 9 ECL stage (1/2/3)',
  -- Measures
  original_amount             DECIMAL(18,2) COMMENT 'Original disbursed amount',
  outstanding_balance         DECIMAL(18,2) COMMENT 'Current outstanding principal',
  principal_paid              DECIMAL(18,2) COMMENT 'Total principal repaid',
  interest_accrued            DECIMAL(18,2) COMMENT 'Interest accrued but unpaid',
  interest_paid_mtd           DECIMAL(18,2) COMMENT 'Interest paid month-to-date',
  arrears_amount              DECIMAL(18,2) COMMENT 'Amount in arrears',
  delinquency_days            INT           COMMENT 'Days past due',
  ltv_ratio                   DECIMAL(8,4)  COMMENT 'Loan-to-value ratio',
  provision_rate              DECIMAL(8,4)  COMMENT 'IFRS 9 provision rate',
  provision_amount            DECIMAL(18,2) COMMENT 'IFRS 9 provision amount',
  collateral_value            DECIMAL(18,2) COMMENT 'Current collateral value',
  current_instalment          DECIMAL(18,2) COMMENT 'Current monthly instalment',
  -- Flags
  npl_flag                    BOOLEAN       COMMENT 'Non-performing loan flag (>=90 DPD)',
  restructured_flag           BOOLEAN       COMMENT 'Loan has been restructured',
  legal_action_flag           BOOLEAN       COMMENT 'Legal action commenced',
  -- Date
  snapshot_date               DATE          NOT NULL COMMENT 'Daily snapshot date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold loan portfolio fact table. Daily snapshot grain covering all active loans. Used for NPL ratio, provision coverage, LTV analysis, and IFRS 9 ECL reporting. Partitioned by snapshot_date.'
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'gold',
  'zorder.columns'                   = 'customer_sk,loan_type,npl_flag'
);

-- ---------------------------------------------------------------------------
-- FACT_PAYMENT
--    Grain: one row per payment instruction.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_payment;

CREATE TABLE prime_capital.gold.fact_payment (
  payment_sk                  BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  payment_id                  STRING        NOT NULL COMMENT 'Payment identifier',
  -- Foreign keys
  originator_customer_sk      BIGINT        COMMENT 'FK to dim_customer (payer)',
  originator_account_sk       BIGINT        COMMENT 'FK to dim_account (payer)',
  beneficiary_customer_sk     BIGINT        COMMENT 'FK to dim_customer (payee, if internal)',
  beneficiary_account_sk      BIGINT        COMMENT 'FK to dim_account (payee, if internal)',
  channel_sk                  BIGINT        COMMENT 'FK to dim_channel',
  currency_sk                 BIGINT        COMMENT 'FK to dim_currency',
  date_sk                     INT           COMMENT 'FK to dim_date (settlement_date)',
  submission_date_sk          INT           COMMENT 'FK to dim_date (submission_date)',
  -- Degenerate
  payment_type                STRING        COMMENT 'EFT/RTGS/SWIFT/INTERNAL/DEBIT_ORDER',
  payment_direction           STRING        COMMENT 'OUTBOUND/INBOUND',
  payment_status              STRING        COMMENT 'Payment status',
  swift_message_type          STRING        COMMENT 'MT103/MT202/etc (for SWIFT payments)',
  -- Measures
  payment_amount              DECIMAL(18,2) COMMENT 'Payment amount in payment currency',
  zar_equivalent              DECIMAL(18,2) COMMENT 'ZAR equivalent amount',
  fee_amount                  DECIMAL(18,2) COMMENT 'Fee charged',
  exchange_rate               DECIMAL(18,6) COMMENT 'FX rate',
  processing_lag_minutes      INT           COMMENT 'Minutes from submission to settlement',
  -- Flags
  is_settled                  BOOLEAN       COMMENT 'Successfully settled',
  is_failed                   BOOLEAN       COMMENT 'Failed or returned',
  is_cross_border             BOOLEAN       COMMENT 'Cross-border SWIFT payment',
  is_internal                 BOOLEAN       COMMENT 'Internal Prime Capital transfer',
  -- Date
  settlement_date             DATE          NOT NULL COMMENT 'Settlement date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold payment fact table. Grain = one payment instruction. Covers EFT, RTGS, SWIFT, internal transfers, and debit orders. Partitioned by settlement_date.'
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- ---------------------------------------------------------------------------
-- FACT_CARD_TRANSACTION
--    Grain: one row per card transaction (authorisation or settlement).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_card_transaction;

CREATE TABLE prime_capital.gold.fact_card_transaction (
  card_tx_sk                  BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  card_tx_id                  STRING        NOT NULL COMMENT 'Card transaction identifier',
  -- Foreign keys
  customer_sk                 BIGINT        COMMENT 'FK to dim_customer',
  account_sk                  BIGINT        COMMENT 'FK to dim_account (card account)',
  merchant_sk                 BIGINT        COMMENT 'FK to dim_merchant',
  channel_sk                  BIGINT        COMMENT 'FK to dim_channel (POS/ONLINE)',
  currency_sk                 BIGINT        COMMENT 'FK to dim_currency (transaction currency)',
  date_sk                     INT           COMMENT 'FK to dim_date (transaction_date)',
  -- Degenerate
  card_id                     STRING        COMMENT 'Card identifier',
  card_scheme                 STRING        COMMENT 'VISA/MASTERCARD/AMEX',
  card_tier                   STRING        COMMENT 'STANDARD/GOLD/PLATINUM/SIGNATURE/INFINITE',
  transaction_type            STRING        COMMENT 'PURCHASE/CASH_ADVANCE/REFUND/REVERSAL/FEE',
  transaction_status          STRING        COMMENT 'AUTHORISED/SETTLED/DECLINED/REVERSED',
  entry_mode                  STRING        COMMENT 'CHIP/CONTACTLESS/CNP/ONLINE',
  authorisation_code          STRING        COMMENT 'Card authorisation code',
  -- Measures
  amount_original             DECIMAL(18,2) COMMENT 'Amount in transaction currency',
  billing_amount_zar          DECIMAL(18,2) COMMENT 'Billing amount in ZAR',
  exchange_rate               DECIMAL(18,6) COMMENT 'FX rate applied',
  fx_markup_amount_zar        DECIMAL(18,2) COMMENT 'FX markup fee in ZAR',
  fraud_score                 DECIMAL(8,4)  COMMENT 'Fraud score (0-100)',
  reward_points_earned        INT           COMMENT 'Reward points earned',
  cashback_amount             DECIMAL(18,2) COMMENT 'Cashback amount',
  -- Flags
  fraud_flag                  BOOLEAN       COMMENT 'Flagged as fraudulent',
  chargeback_flag             BOOLEAN       COMMENT 'Chargeback raised',
  is_contactless              BOOLEAN       COMMENT 'Contactless transaction',
  is_international            BOOLEAN       COMMENT 'International merchant',
  is_3ds_verified             BOOLEAN       COMMENT '3D Secure verified',
  is_recurring                BOOLEAN       COMMENT 'Recurring/subscription',
  -- Date
  transaction_date            DATE          NOT NULL COMMENT 'Transaction date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold card transaction fact table. Grain = one card authorisation/settlement. Partitioned by transaction_date. Covers all credit and debit card POS, online, contactless, and cash advance transactions.'
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'gold',
  'zorder.columns'                   = 'customer_sk,merchant_sk,fraud_flag'
);

-- ---------------------------------------------------------------------------
-- FACT_FRAUD_EVENT
--    Grain: one row per confirmed fraud event (post-disposition).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_fraud_event;

CREATE TABLE prime_capital.gold.fact_fraud_event (
  fraud_sk                    BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  alert_id                    STRING        NOT NULL COMMENT 'Source fraud alert identifier',
  -- Foreign keys
  customer_sk                 BIGINT        COMMENT 'FK to dim_customer',
  account_sk                  BIGINT        COMMENT 'FK to dim_account',
  channel_sk                  BIGINT        COMMENT 'FK to dim_channel',
  date_sk                     INT           COMMENT 'FK to dim_date (event date)',
  -- Degenerate
  fraud_type                  STRING        COMMENT 'CARD_FRAUD/ACCOUNT_TAKEOVER/IDENTITY/APPLICATION/INTERNAL',
  alert_priority              STRING        COMMENT 'P1_CRITICAL/P2_HIGH/P3_MEDIUM/P4_LOW',
  alert_status                STRING        COMMENT 'Final disposition status',
  -- Measures
  transaction_amount          DECIMAL(18,2) COMMENT 'Transaction amount involved',
  loss_amount                 DECIMAL(18,2) COMMENT 'Confirmed gross loss',
  recovery_amount             DECIMAL(18,2) COMMENT 'Amount recovered',
  net_loss_amount             DECIMAL(18,2) COMMENT 'Net loss (loss - recovery)',
  fraud_score                 DECIMAL(10,2) COMMENT 'Fraud score at time of alert',
  resolution_days             INT           COMMENT 'Days to resolve the fraud case',
  -- Flags
  is_confirmed_fraud          BOOLEAN       COMMENT 'True = confirmed fraud (not false positive)',
  breach_sla_flag             BOOLEAN       COMMENT 'True if resolution exceeded SLA',
  -- Date
  alert_date                  DATE          NOT NULL COMMENT 'Alert date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold fraud event fact table. Grain = one fraud alert event. Contains confirmed fraud and false positives for precision/recall tracking. Includes net loss, recovery, and SLA metrics.'
PARTITIONED BY (alert_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- ---------------------------------------------------------------------------
-- FACT_TREASURY_POSITION
--    Grain: one row per trade per EOD snapshot.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_treasury_position;

CREATE TABLE prime_capital.gold.fact_treasury_position (
  position_sk                 BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  trade_id                    STRING        NOT NULL COMMENT 'Trade identifier',
  -- Foreign keys
  currency_sk                 BIGINT        COMMENT 'FK to dim_currency',
  date_sk                     INT           COMMENT 'FK to dim_date (snapshot_date)',
  employee_sk                 BIGINT        COMMENT 'FK to dim_employee (trader)',
  -- Degenerate
  trade_type                  STRING        COMMENT 'BOND/FX_SPOT/FX_FORWARD/IRS/CDS/EQUITY/MONEY_MARKET/REPO',
  instrument_type             STRING        COMMENT 'GOVERNMENT_BOND/CORPORATE_BOND/EQUITY/DERIVATIVE/FX',
  instrument_id               STRING        COMMENT 'ISIN / instrument code',
  book_id                     STRING        COMMENT 'FVTPL/AFS/HTM',
  desk                        STRING        COMMENT 'Trading desk',
  counterparty_id             STRING        COMMENT 'Counterparty identifier',
  trade_direction             STRING        COMMENT 'BUY/SELL/LEND/BORROW',
  -- Measures
  notional_amount             DECIMAL(18,2) COMMENT 'Notional face value',
  notional_amount_zar         DECIMAL(18,2) COMMENT 'Notional in ZAR',
  mtm_value                   DECIMAL(18,2) COMMENT 'Mark-to-market value',
  mtm_value_zar               DECIMAL(18,2) COMMENT 'MTM in ZAR',
  unrealised_pnl              DECIMAL(18,2) COMMENT 'Unrealised P&L',
  realised_pnl                DECIMAL(18,2) COMMENT 'Realised P&L',
  total_pnl                   DECIMAL(18,2) COMMENT 'Total P&L (realised + unrealised)',
  accrued_interest            DECIMAL(18,2) COMMENT 'Accrued interest',
  yield                       DECIMAL(8,4)  COMMENT 'Current yield',
  duration                    DECIMAL(10,4) COMMENT 'Modified duration',
  dv01                        DECIMAL(18,6) COMMENT 'DV01 rate sensitivity',
  days_to_maturity            INT           COMMENT 'Days to instrument maturity',
  -- Flags
  is_live                     BOOLEAN       COMMENT 'Trade is currently live',
  -- Date
  snapshot_date               DATE          NOT NULL COMMENT 'EOD snapshot date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold treasury position fact table. EOD snapshot grain. Covers all treasury instruments by desk and book. Supports ALCO reporting, VaR calculation, and interest rate risk analysis.'
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- ---------------------------------------------------------------------------
-- FACT_ACCOUNT_BALANCE
--    Grain: one row per account per day (daily balance snapshot).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_account_balance;

CREATE TABLE prime_capital.gold.fact_account_balance (
  balance_sk                  BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  -- Foreign keys
  account_sk                  BIGINT        COMMENT 'FK to dim_account',
  customer_sk                 BIGINT        COMMENT 'FK to dim_customer',
  product_sk                  BIGINT        COMMENT 'FK to dim_product',
  branch_sk                   BIGINT        COMMENT 'FK to dim_branch',
  date_sk                     INT           COMMENT 'FK to dim_date (snapshot_date)',
  -- Measures
  opening_balance             DECIMAL(18,2) COMMENT 'Opening balance at start of day',
  closing_balance             DECIMAL(18,2) COMMENT 'Closing balance at end of day',
  average_daily_balance       DECIMAL(18,2) COMMENT 'Average balance during the day',
  total_credits               DECIMAL(18,2) COMMENT 'Sum of all credits posted during day',
  total_debits                DECIMAL(18,2) COMMENT 'Sum of all debits posted during day',
  transaction_count           INT           COMMENT 'Number of transactions posted during day',
  interest_earned             DECIMAL(18,2) COMMENT 'Interest earned/credited on this day',
  fees_charged                DECIMAL(18,2) COMMENT 'Fees charged on this day',
  hold_amount                 DECIMAL(18,2) COMMENT 'Amount under hold/lien at EOD',
  available_balance           DECIMAL(18,2) COMMENT 'Available balance at EOD',
  -- Flags
  dormancy_flag               BOOLEAN       COMMENT 'Account dormant on this date',
  below_minimum_flag          BOOLEAN       COMMENT 'Balance below minimum requirement',
  -- Date
  snapshot_date               DATE          NOT NULL COMMENT 'Balance snapshot date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold account daily balance fact table. One row per account per day. Enables average daily balance calculations, interest calculations, and deposit analytics for ALCO and treasury reporting.'
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality.layer'                    = 'gold',
  'zorder.columns'                   = 'account_sk,customer_sk'
);

-- ---------------------------------------------------------------------------
-- FACT_GL_JOURNAL
--    Grain: one row per GL journal line (double-entry accounting).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_gl_journal;

CREATE TABLE prime_capital.gold.fact_gl_journal (
  journal_sk                  BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  journal_id                  STRING        NOT NULL COMMENT 'Source journal identifier',
  journal_line_number         INT           COMMENT 'Line within journal entry',
  -- Foreign keys
  gl_account_sk               BIGINT        COMMENT 'FK to dim_gl_account',
  date_sk                     INT           COMMENT 'FK to dim_date (posting_date)',
  currency_sk                 BIGINT        COMMENT 'FK to dim_currency (transaction currency)',
  employee_sk                 BIGINT        COMMENT 'FK to dim_employee (creator)',
  -- Degenerate
  journal_type                STRING        COMMENT 'MANUAL/AUTO/REVERSAL/ADJUSTMENT/OPENING/CLOSING',
  cost_centre_code            STRING        COMMENT 'Cost centre code',
  legal_entity                STRING        COMMENT 'Legal entity',
  business_unit               STRING        COMMENT 'Business unit code',
  source_module               STRING        COMMENT 'Source: LOANS/CARDS/TREASURY/PAYMENTS/PAYROLL/MANUAL',
  accounting_period           STRING        COMMENT 'Accounting period YYYY-MM',
  financial_year              STRING        COMMENT 'Financial year',
  -- Measures
  debit_amount                DECIMAL(18,2) COMMENT 'Debit amount in ZAR',
  credit_amount               DECIMAL(18,2) COMMENT 'Credit amount in ZAR',
  net_amount                  DECIMAL(18,2) COMMENT 'Net: credit - debit',
  fx_rate                     DECIMAL(18,6) COMMENT 'FX rate for foreign currency entries',
  -- Flags
  is_reversal                 BOOLEAN       COMMENT 'Reversal journal',
  is_intercompany             BOOLEAN       COMMENT 'Intercompany elimination',
  is_month_end_adjustment     BOOLEAN       COMMENT 'Month-end accrual or adjustment',
  -- Date
  posting_date                DATE          NOT NULL COMMENT 'GL posting date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold GL journal fact table. Grain = one journal line. Double-entry accounting representation for income statement and balance sheet construction. Partitioned by posting_date.'
PARTITIONED BY (posting_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold',
  'zorder.columns'                   = 'gl_account_sk,legal_entity,accounting_period'
);

-- ---------------------------------------------------------------------------
-- FACT_AML_CASE
--    Grain: one row per AML case (consolidated from one or more alerts).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_aml_case;

CREATE TABLE prime_capital.gold.fact_aml_case (
  aml_case_sk                 BIGINT        GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  case_id                     STRING        NOT NULL COMMENT 'AML case identifier',
  -- Foreign keys
  customer_sk                 BIGINT        COMMENT 'FK to dim_customer',
  account_sk                  BIGINT        COMMENT 'FK to dim_account',
  risk_category_sk            BIGINT        COMMENT 'FK to dim_risk_category',
  date_sk                     INT           COMMENT 'FK to dim_date (alert_date)',
  analyst_employee_sk         BIGINT        COMMENT 'FK to dim_employee (AML analyst)',
  -- Degenerate
  alert_type                  STRING        COMMENT 'AML typology',
  alert_priority              STRING        COMMENT 'HIGH/MEDIUM/LOW',
  case_status                 STRING        COMMENT 'Final case status',
  -- Measures
  total_amount_flagged        DECIMAL(18,2) COMMENT 'Total suspicious transaction amount',
  transaction_count           INT           COMMENT 'Number of transactions in pattern',
  lookback_period_days        INT           COMMENT 'Analysis lookback window',
  risk_score                  DECIMAL(10,2) COMMENT 'AML risk score',
  review_duration_days        INT           COMMENT 'Days from alert to closure',
  sar_amount                  DECIMAL(18,2) COMMENT 'Amount reported in SAR (0 if no SAR)',
  alert_count                 INT           COMMENT 'Number of alerts consolidated into this case',
  -- Flags
  sar_filed_flag              BOOLEAN       COMMENT 'SAR filed with FIC',
  breach_regulatory_sla       BOOLEAN       COMMENT 'Regulatory 30-day SLA breached',
  -- Date
  alert_date                  DATE          NOT NULL COMMENT 'Alert date (partition key)',
  -- Audit
  _gold_loaded_at             TIMESTAMP     COMMENT 'Gold load timestamp'
)
COMMENT 'Gold AML case fact table. Grain = one AML case. Captures SAR amounts, risk scores, review durations, and SLA breach for FIC regulatory reporting and AML operational analytics.'
PARTITIONED BY (alert_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality.layer'                    = 'gold'
);

-- =============================================================================
-- SA FINTECH PARTNER LAYER
-- Cards & Fintech domain — Yoco / SnapScan / PayFast / Peach Payments / PayGate
-- =============================================================================

-- ---------------------------------------------------------------------------
-- DIM_FINTECH
--    South African fintech payment partners integrated with PCB's acquiring layer.
--    SCD1 (fees / merchant counts update in-place; historical via fact table).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.dim_fintech;

CREATE TABLE prime_capital.gold.dim_fintech (
  fintech_sk            BIGINT   GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  fintech_id            STRING   NOT NULL COMMENT 'Business key e.g. FT001',
  fintech_name          STRING   NOT NULL COMMENT 'Partner name e.g. Yoco',
  platform_type         STRING   COMMENT 'POS Card Acquiring / QR Code Payment / Online Payment Gateway',
  settlement_currency   STRING   NOT NULL DEFAULT 'ZAR' COMMENT 'Settlement currency (ZAR)',
  fee_rate_pct          DECIMAL(6,4) COMMENT 'Standard merchant discount rate (%)',
  min_fee_zar           DECIMAL(8,2) COMMENT 'Minimum per-transaction fee (ZAR)',
  target_segment        STRING   COMMENT 'SME / E-Commerce / Enterprise',
  founded_year          INT      COMMENT 'Year company founded',
  hq_city               STRING   COMMENT 'Head office city',
  hq_province           STRING   COMMENT 'Head office province',
  parent_company        STRING   COMMENT 'Parent / holding company',
  is_active             BOOLEAN  NOT NULL DEFAULT true COMMENT 'Active integration flag',
  integration_type      STRING   COMMENT 'REST API / SDK / Plugin / Legacy API',
  avg_basket_zar        DECIMAL(10,2) COMMENT 'Average transaction basket size (ZAR)',
  active_merchants_est  INT      COMMENT 'Estimated active merchant base',
  annual_volume_zar_bn  DECIMAL(8,2)  COMMENT 'Estimated annual settlement volume (R billions)',
  settlement_days       INT      COMMENT 'T+N settlement cycle',
  notes                 STRING   COMMENT 'Business context and market notes',
  _gold_loaded_at       TIMESTAMP COMMENT 'Gold load timestamp'
)
COMMENT 'Gold fintech partner dimension. Reference data for SA payment ecosystem partners integrated into PCBs card acquiring and settlement infrastructure.'
TBLPROPERTIES ('quality.layer' = 'gold', 'delta.autoOptimize.optimizeWrite' = 'true');

INSERT INTO prime_capital.gold.dim_fintech
  (fintech_id, fintech_name, platform_type, settlement_currency, fee_rate_pct, min_fee_zar,
   target_segment, founded_year, hq_city, hq_province, parent_company, is_active,
   integration_type, avg_basket_zar, active_merchants_est, annual_volume_zar_bn, settlement_days, notes)
VALUES
  ('FT001', 'Yoco',           'POS Card Acquiring',       'ZAR', 2.9500, 0.00,
   'SME',            2013, 'Cape Town',    'Western Cape', 'Yoco Technologies',                 true,
   'REST API / Mobile SDK', 856.40, 250000, 12.40, 1,
   'SAs largest independent payment provider. Card machines: Go (R599), Khumo (R699), Neo (R1499), Counter+Neo Touch (R2999). 250K+ merchants in retail, hospitality, and services. Raised USD83M Series C (2021).'),

  ('FT002', 'SnapScan',       'QR Code Payment',          'ZAR', 2.7500, 0.00,
   'SME and Consumer', 2012, 'Cape Town', 'Western Cape', 'Standard Bank Group',                true,
   'QR Integration / App SDK', 340.20, 80000, 3.20, 1,
   'Acquired by Standard Bank (2014). QR-code payments — no card machine required. Popular in restaurants, markets, pop-up stores, and events. Direct SME competitor to Yoco in hospitality segment.'),

  ('FT003', 'PayFast',        'Online Payment Gateway',   'ZAR', 3.5000, 2.00,
   'E-Commerce',     2007, 'Cape Town',    'Western Cape', 'DPO Group (Network International)', true,
   'Plugin (Shopify / WooCommerce / Magento) + API', 1250.60, 45000, 8.10, 2,
   'SAs most widely used online gateway. Supports 20+ payment methods: instant EFT, credit/debit cards, Mobicred, and Zapper. Critical for e-commerce with Takealot Marketplace integration. Acquired by DPO Group (2021).'),

  ('FT004', 'Peach Payments', 'Online Payment Gateway',   'ZAR', 2.9000, 1.50,
   'Mid-Market E-Commerce', 2012, 'Cape Town', 'Western Cape', 'Peach Payments (Pty) Ltd',      true,
   'REST API / SDK / Hosted Page', 980.30, 12000, 2.30, 1,
   'API-first developer-friendly gateway. Serves mid-to-large merchants including Superbalist and OneDayOnly. Supports 3D Secure 2.0, tokenisation, and recurring billing. Growing in B2B SaaS vertical.'),

  ('FT005', 'PayGate',        'Online Payment Gateway',   'ZAR', 2.8000, 1.00,
   'Enterprise E-Commerce', 2000, 'Johannesburg', 'Gauteng', 'DPO Group (Network International)', true,
   'Legacy API / Hosted Checkout', 1890.70, 8000, 1.80, 2,
   'One of SAs oldest gateways (est. 2000). Enterprise merchants including large retailers, travel agencies, and government e-services. Strong in recurring billing. Complementary to PayFast in the DPO portfolio.');


-- ---------------------------------------------------------------------------
-- FACT_FINTECH_SETTLEMENT
--    Monthly settlement summary per fintech partner.
--    Grain = one row per partner per month.
--    Source = PCB acquiring system settlement reports (S3 daily → monthly agg).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS prime_capital.gold.fact_fintech_settlement;

CREATE TABLE prime_capital.gold.fact_fintech_settlement (
  settlement_sk         BIGINT   GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  settlement_id         STRING   NOT NULL COMMENT 'Natural key e.g. FS-2024-01-FT001',
  fintech_sk            BIGINT   NOT NULL COMMENT 'FK to dim_fintech',
  fintech_id            STRING   NOT NULL COMMENT 'Business key',
  fintech_name          STRING   COMMENT 'Denormalised partner name',
  settlement_month      DATE     NOT NULL COMMENT 'Month-end date (partition key)',
  -- Volume metrics
  total_transactions    BIGINT   COMMENT 'Total transaction count for month',
  gross_volume_zar      DECIMAL(18,2) COMMENT 'Gross merchant volume settled (ZAR)',
  merchant_fees_zar     DECIMAL(18,2) COMMENT 'Fees collected from merchants (ZAR)',
  interchange_fees_zar  DECIMAL(18,2) COMMENT 'Interchange fees paid to card networks (ZAR)',
  net_settlement_zar    DECIMAL(18,2) COMMENT 'Net amount settled to merchants after fees (ZAR)',
  avg_transaction_zar   DECIMAL(10,2) COMMENT 'Average transaction value (ZAR)',
  -- Risk metrics
  chargebacks_count     INT      COMMENT 'Number of chargebacks raised',
  chargeback_ratio_pct  DECIMAL(6,4) COMMENT 'Chargeback ratio (chargebacks / total txns %)',
  failed_transactions   INT      COMMENT 'Number of failed / declined transactions',
  failure_rate_pct      DECIMAL(6,4) COMMENT 'Failure rate (failed / total txns %)',
  -- Growth metrics
  new_merchants_onboarded INT    COMMENT 'New merchants onboarded in the month',
  active_merchants      INT      COMMENT 'Active merchant count at month-end',
  -- Audit
  _gold_loaded_at       TIMESTAMP COMMENT 'Gold load timestamp'
)
COMMENT 'Gold fintech settlement fact table. Grain = one row per fintech partner per calendar month. Captures gross volume, net settlement, fee revenue, chargeback risk, and merchant growth for the SA Cards and Fintech domain.'
PARTITIONED BY (settlement_month)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.dataSkippingNumIndexedCols' = '4',
  'quality.layer'                    = 'gold'
);

-- =============================================================================
-- END OF GOLD STAR SCHEMA DDL
-- =============================================================================
