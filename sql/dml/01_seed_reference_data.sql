-- =============================================================================
-- 01_SEED_REFERENCE_DATA.SQL
-- Seed data for static dimension tables: provinces, branches, currencies, dates
-- =============================================================================

-- ── DIM_DATE: Generate 5 years of date records ────────────────────────────
INSERT INTO bronze.dim_date_seed (calendar_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend, is_public_holiday, is_business_day)
VALUES
  -- Placeholder: in Databricks this is generated via the date range notebook
  -- Run notebooks/00_setup_unity_catalog.py → Section 2 to populate dim_date
  ('2020-01-01', 2020, 1, 1, 'January',   4, 'Wednesday', FALSE, TRUE,  FALSE), -- New Year's Day
  ('2020-01-02', 2020, 1, 1, 'January',   5, 'Thursday',  FALSE, FALSE, TRUE),
  ('2020-12-16', 2020, 4, 12,'December',  4, 'Wednesday', FALSE, TRUE,  FALSE); -- Day of Reconciliation


-- ── DIM_CURRENCY ──────────────────────────────────────────────────────────
INSERT INTO gold.dim_currency (currency_id, currency_code, currency_name, symbol, is_base_currency, country)
VALUES
  ('CUR001', 'ZAR', 'South African Rand',     'R',  TRUE,  'South Africa'),
  ('CUR002', 'USD', 'US Dollar',              '$',  FALSE, 'United States'),
  ('CUR003', 'EUR', 'Euro',                   '€',  FALSE, 'Eurozone'),
  ('CUR004', 'GBP', 'British Pound Sterling', '£',  FALSE, 'United Kingdom'),
  ('CUR005', 'CNY', 'Chinese Yuan Renminbi',  '¥',  FALSE, 'China'),
  ('CUR006', 'BWP', 'Botswana Pula',          'P',  FALSE, 'Botswana'),
  ('CUR007', 'ZMW', 'Zambian Kwacha',         'ZK', FALSE, 'Zambia'),
  ('CUR008', 'MZN', 'Mozambican Metical',     'MT', FALSE, 'Mozambique');


-- ── DIM_BRANCH: 200 PCB branches across 9 provinces ──────────────────────
-- Gauteng (highest density)
INSERT INTO gold.dim_branch (branch_id, branch_name, branch_type, province, city, region, latitude, longitude, opening_date, is_active)
VALUES
  ('BR001', 'Sandton City Branch',       'Flagship',   'Gauteng',       'Sandton',          'Johannesburg Metro', -26.1065, 28.0567, '2005-03-15', TRUE),
  ('BR002', 'Rosebank Branch',           'Full Service','Gauteng',       'Johannesburg',     'Johannesburg Metro', -26.1470, 28.0440, '2007-08-01', TRUE),
  ('BR003', 'Midrand Hub',               'Full Service','Gauteng',       'Midrand',          'Johannesburg Metro', -25.9980, 28.1290, '2010-02-14', TRUE),
  ('BR004', 'Pretoria East Branch',      'Full Service','Gauteng',       'Pretoria',         'Tshwane',            -25.7480, 28.3300, '2008-05-20', TRUE),
  ('BR005', 'Centurion Branch',          'Full Service','Gauteng',       'Centurion',        'Tshwane',            -25.8520, 28.1890, '2009-11-01', TRUE),
  ('BR006', 'Soweto Branch',             'Standard',   'Gauteng',       'Soweto',           'Johannesburg Metro', -26.2670, 27.8580, '2015-07-07', TRUE),
  ('BR007', 'Ekurhuleni Branch',         'Standard',   'Gauteng',       'Germiston',        'Ekurhuleni',         -26.2330, 28.1670, '2012-03-01', TRUE),
  ('BR008', 'Hatfield Campus Branch',    'Standard',   'Gauteng',       'Pretoria',         'Tshwane',            -25.7450, 28.2310, '2016-01-18', TRUE),
  -- Western Cape
  ('BR009', 'V&A Waterfront Branch',     'Flagship',   'Western Cape',  'Cape Town',        'Cape Metro',         -33.9020, 18.4210, '2004-09-01', TRUE),
  ('BR010', 'Bellville Branch',          'Full Service','Western Cape',  'Bellville',        'Cape Metro',         -33.9020, 18.6280, '2008-04-10', TRUE),
  ('BR011', 'Stellenbosch Branch',       'Standard',   'Western Cape',  'Stellenbosch',     'Cape Winelands',     -33.9350, 18.8600, '2013-06-01', TRUE),
  ('BR012', 'George Branch',             'Standard',   'Western Cape',  'George',           'Garden Route',       -33.9630, 22.4600, '2011-09-15', TRUE),
  -- KwaZulu-Natal
  ('BR013', 'Umhlanga Branch',           'Flagship',   'KwaZulu-Natal', 'Umhlanga',         'eThekwini',          -29.7230, 31.0820, '2006-11-01', TRUE),
  ('BR014', 'Durban CBD Branch',         'Full Service','KwaZulu-Natal', 'Durban',           'eThekwini',          -29.8580, 31.0210, '2003-07-01', TRUE),
  ('BR015', 'Pietermaritzburg Branch',   'Standard',   'KwaZulu-Natal', 'Pietermaritzburg', 'uMgungundlovu',      -29.6000, 30.3790, '2009-02-01', TRUE),
  ('BR016', 'Richards Bay Branch',       'Standard',   'KwaZulu-Natal', 'Richards Bay',     'King Cetshwayo',     -28.7830, 32.0470, '2014-08-01', TRUE),
  -- Eastern Cape
  ('BR017', 'Gqeberha Branch',           'Full Service','Eastern Cape',  'Gqeberha',         'Nelson Mandela Bay', -33.9600, 25.6020, '2007-03-01', TRUE),
  ('BR018', 'East London Branch',        'Standard',   'Eastern Cape',  'East London',      'Buffalo City',       -33.0150, 27.9120, '2010-10-01', TRUE),
  -- Limpopo
  ('BR019', 'Polokwane Branch',          'Full Service','Limpopo',       'Polokwane',        'Capricorn',          -23.9040, 29.4690, '2008-06-15', TRUE),
  -- Mpumalanga
  ('BR020', 'Mbombela Branch',           'Standard',   'Mpumalanga',    'Mbombela',         'Ehlanzeni',          -25.4750, 30.9690, '2011-04-01', TRUE),
  -- North West
  ('BR021', 'Rustenburg Branch',         'Standard',   'North West',    'Rustenburg',       'Bojanala',           -25.6670, 27.2420, '2012-08-01', TRUE),
  -- Free State
  ('BR022', 'Bloemfontein Branch',       'Full Service','Free State',    'Bloemfontein',     'Mangaung',           -29.0850, 26.1590, '2006-02-01', TRUE),
  -- Northern Cape
  ('BR023', 'Kimberley Branch',          'Standard',   'Northern Cape', 'Kimberley',        'Frances Baard',      -28.7280, 24.7500, '2013-01-01', TRUE);


-- ── DIM_FINTECH (already seeded in 03_gold_star_schema.sql) ───────────────
-- Included here for reference; do not re-run if already applied
/*
INSERT INTO gold.dim_fintech ...
See sql/ddl/03_gold_star_schema.sql Section 11
*/
