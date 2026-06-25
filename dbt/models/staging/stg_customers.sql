{{
    config(
        materialized = 'view',
        tags         = ['staging', 'customers', 'daily'],
        description  = 'Staged customer master data: cleaned, cast, and lightly enriched from bronze_customers.'
    )
}}

/*
  stg_customers
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_customers
  Layer   : Silver (staging)
  Grain   : One row per customer (customer_id)
  Purpose : Canonical customer dimension used by all downstream models.
            Applies type casting, NULL validation, phone/email standardisation,
            age derivation, and segment classification.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

-- ── 1. Pull raw source records ────────────────────────────────────────────────
source as (
    select * from {{ source('bronze', 'bronze_customers') }}
),

-- ── 2. Cast columns to correct types and rename for consistency ───────────────
cast_and_rename as (
    select
        -- Primary key
        cast(customer_id          as string)                         as customer_id,

        -- Identity fields
        trim(upper(last_name))                                       as last_name,
        trim(initcap(first_name))                                    as first_name,
        trim(initcap(first_name)) || ' ' || trim(upper(last_name))   as full_name,

        -- Demographics
        cast(date_of_birth        as date)                           as date_of_birth,
        upper(trim(gender))                                          as gender,        -- M / F / X
        upper(trim(nationality))                                     as nationality,
        upper(trim(id_type))                                         as id_type,       -- RSA_ID / PASSPORT
        trim(id_number)                                              as id_number,

        -- Contact details (standardised below)
        lower(trim(email_address))                                   as email_raw,
        regexp_replace(trim(phone_number), '[^0-9+]', '')            as phone_raw,

        -- Address
        trim(residential_address_line1)                              as address_line1,
        trim(residential_address_line2)                              as address_line2,
        trim(city)                                                   as city,
        upper(trim(province))                                        as province,
        trim(postal_code)                                            as postal_code,
        upper(trim(country_code))                                    as country_code,

        -- Relationship metadata
        cast(customer_since_date  as date)                           as customer_since_date,
        upper(trim(customer_segment))                                as customer_segment,  -- RETAIL / BUSINESS / PRIVATE
        upper(trim(kyc_status))                                      as kyc_status,        -- VERIFIED / PENDING / FAILED
        cast(kyc_verified_date    as date)                           as kyc_verified_date,
        upper(trim(employment_status))                               as employment_status,
        cast(annual_income        as decimal(18,2))                  as annual_income_zar,
        upper(trim(income_source))                                   as income_source,
        cast(credit_score         as int)                            as credit_score,      -- 300–850
        upper(trim(risk_rating))                                     as risk_rating_raw,   -- bank's internal rating

        -- Flags
        cast(is_pep               as boolean)                        as is_politically_exposed,
        cast(is_sanctioned        as boolean)                        as is_sanctioned,
        cast(is_deceased          as boolean)                        as is_deceased,
        cast(is_active            as boolean)                        as is_active,

        -- Audit columns
        cast(created_at           as timestamp)                      as created_at,
        cast(updated_at           as timestamp)                      as updated_at,
        cast(_ingested_at         as timestamp)                      as _ingested_at

    from source
    where customer_id is not null   -- discard rows with no PK
),

-- ── 3. Standardise email & phone ─────────────────────────────────────────────
contact_standardised as (
    select
        *,
        -- Basic email validation: must contain @ and a dot after @
        case
            when email_raw rlike '^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$'
            then email_raw
            else null
        end                                                          as email_address,

        -- SA phone normalisation: convert 0XX to +27XX
        case
            when phone_raw like '0%' and length(phone_raw) = 10
            then '+27' || substr(phone_raw, 2)
            when phone_raw like '+27%' and length(phone_raw) = 12
            then phone_raw
            when phone_raw like '27%' and length(phone_raw) = 11
            then '+' || phone_raw
            else null   -- cannot normalise; treat as missing
        end                                                          as phone_number,

        -- Flag records with invalid contact details
        case when email_raw is null or not (email_raw rlike '^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$')
             then true else false end                                 as has_invalid_email,
        case when phone_raw is null or length(phone_raw) < 10
             then true else false end                                 as has_invalid_phone

    from cast_and_rename
),

-- ── 4. Derive age and tenure ──────────────────────────────────────────────────
enriched as (
    select
        *,
        -- Age in completed years (null-safe)
        case
            when date_of_birth is not null
            then floor(datediff(current_date(), date_of_birth) / 365.25)
            else null
        end                                                          as age_years,

        -- Customer tenure in days
        datediff(current_date(), customer_since_date)                as tenure_days,

        -- Age band for segmentation
        case
            when floor(datediff(current_date(), date_of_birth) / 365.25) < 25  then 'Youth (<25)'
            when floor(datediff(current_date(), date_of_birth) / 365.25) < 35  then 'Young Adult (25-34)'
            when floor(datediff(current_date(), date_of_birth) / 365.25) < 50  then 'Middle Age (35-49)'
            when floor(datediff(current_date(), date_of_birth) / 365.25) < 65  then 'Mature (50-64)'
            else 'Senior (65+)'
        end                                                          as age_band,

        -- Income band
        case
            when annual_income_zar < 100000               then 'Low (<100K)'
            when annual_income_zar < 350000               then 'Lower-Mid (100K-350K)'
            when annual_income_zar < 750000               then 'Mid (350K-750K)'
            when annual_income_zar < 1500000              then 'Upper-Mid (750K-1.5M)'
            else                                               'High (1.5M+)'
        end                                                          as income_band,

        -- KYC completeness
        case
            when kyc_status = 'VERIFIED' and kyc_verified_date is not null then true
            else false
        end                                                          as kyc_complete,

        -- Pipeline load timestamp
        current_timestamp()                                          as loaded_at

    from contact_standardised
),

-- ── 5. Final select — expose clean column set ─────────────────────────────────
final as (
    select
        customer_id,
        full_name,
        first_name,
        last_name,
        date_of_birth,
        age_years,
        age_band,
        gender,
        nationality,
        id_type,
        id_number,
        email_address,
        phone_number,
        has_invalid_email,
        has_invalid_phone,
        address_line1,
        address_line2,
        city,
        province,
        postal_code,
        country_code,
        customer_since_date,
        tenure_days,
        customer_segment,
        kyc_status,
        kyc_verified_date,
        kyc_complete,
        employment_status,
        annual_income_zar,
        income_band,
        income_source,
        credit_score,
        risk_rating_raw,
        is_politically_exposed,
        is_sanctioned,
        is_deceased,
        is_active,
        created_at,
        updated_at,
        _ingested_at,
        loaded_at
    from enriched
)

select * from final
