"""
Prime Capital Bank — Synthetic Data Generator
==============================================
Generates realistic South African banking data for ADLS Gen2 raw zone loading.

Datasets produced
-----------------
  customers.csv            500,000 rows
  accounts.csv           1,200,000 rows
  transactions.csv      10,000,000 rows  (written in 500K chunks)
  loans.csv                300,000 rows
  card_transactions.csv  2,000,000 rows
  payments.csv             500,000 rows
  fraud_alerts.csv          50,000 rows
  fintech_partners.csv           5 rows  (DIM_FINTECH — SA payment partners)
  fintech_settlements.csv       60 rows  (FACT_FINTECH_SETTLEMENT — 12 months × 5 partners)

Output directory: C:\\Users\\Anthony.DESKTOP-ES5HL78\\Documents\\Prime Capital Bank\\data\\

Usage
-----
  pip install pandas numpy faker
  python generate_bank_data.py

Author  : Anthony Apollis
Created : 2026-06-24
"""

import os
import re
import sys
import uuid
import random
import datetime
import pathlib
import hashlib

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = pathlib.Path(r"C:\Users\Anthony.DESKTOP-ES5HL78\Documents\Prime Capital Bank\data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# zu_ZA gives authentic ZA names/cities; en_GB fills missing providers (address, company, job)
# en_ZA locale is not available in all Faker versions — this combination works universally
fake = Faker(["zu_ZA", "en_GB"])
Faker.seed(RANDOM_SEED)

N_CUSTOMERS = 500_000
N_ACCOUNTS = 1_200_000
N_TRANSACTIONS = 10_000_000
N_LOANS = 300_000
N_CARD_TXN = 2_000_000
N_PAYMENTS = 500_000
N_FRAUD = 50_000

TX_CHUNK_SIZE = 500_000
CARD_CHUNK_SIZE = 500_000

START_DATE = datetime.date(2015, 1, 1)
END_DATE = datetime.date(2024, 12, 31)

# ---------------------------------------------------------------------------
# South African reference data
# ---------------------------------------------------------------------------

SA_PROVINCES = [
    "Gauteng", "Western Cape", "KwaZulu-Natal", "Eastern Cape",
    "Limpopo", "Mpumalanga", "North West", "Free State", "Northern Cape",
]
PROVINCE_WEIGHTS = [0.26, 0.12, 0.20, 0.13, 0.10, 0.07, 0.06, 0.05, 0.01]

CITIES_BY_PROVINCE = {
    "Gauteng":       ["Johannesburg", "Pretoria", "Soweto", "Ekurhuleni", "Centurion", "Sandton", "Midrand"],
    "Western Cape":  ["Cape Town", "Stellenbosch", "George", "Paarl", "Bellville", "Somerset West"],
    "KwaZulu-Natal": ["Durban", "Pietermaritzburg", "Richards Bay", "Newcastle", "Pinetown", "Umhlanga"],
    "Eastern Cape":  ["Port Elizabeth", "East London", "Mthatha", "Queenstown", "King William's Town"],
    "Limpopo":       ["Polokwane", "Tzaneen", "Louis Trichardt", "Mokopane", "Thohoyandou"],
    "Mpumalanga":    ["Nelspruit", "Witbank", "Middelburg", "Secunda", "Standerton"],
    "North West":    ["Rustenburg", "Klerksdorp", "Potchefstroom", "Mafikeng", "Brits"],
    "Free State":    ["Bloemfontein", "Welkom", "Sasolburg", "Bethlehem", "Kroonstad"],
    "Northern Cape": ["Kimberley", "Upington", "Springbok", "De Aar", "Kuruman"],
}

SUBURBS_BY_PROVINCE = {
    "Gauteng":       ["Sandton", "Rosebank", "Fourways", "Midrand", "Germiston", "Alberton", "Boksburg"],
    "Western Cape":  ["Sea Point", "Green Point", "Claremont", "Rondebosch", "Bellville", "Brackenfell"],
    "KwaZulu-Natal": ["Berea", "Morningside", "Glenwood", "Westville", "La Lucia", "Umhlanga Rocks"],
    "Eastern Cape":  ["Walmer", "Summerstrand", "Greenacres", "Beacon Bay", "Vincent"],
    "Limpopo":       ["Seshego", "Bendor", "Ivy Park", "Fauna Park"],
    "Mpumalanga":    ["Sonheuwel", "Riverside", "Ermelo", "Secunda"],
    "North West":    ["Rustenburg Central", "Klerksdorp Central", "Potchefstroom Central"],
    "Free State":    ["Westdene", "Universitas", "Bain's Vlei", "Dan Pienaar"],
    "Northern Cape": ["Galeshewe", "Roodepan", "Royldene"],
}

RACE_WEIGHTS = [0.806, 0.082, 0.089, 0.026, -1]   # -1 placeholder → remainder
RACES = ["Black", "White", "Coloured", "Indian", "Other"]
RACE_PROBS = [0.806, 0.082, 0.089, 0.026, 0.997 - 0.806 - 0.082 - 0.089 - 0.026]

EMPLOYMENT_STATUSES = ["Employed", "Self-Employed", "Unemployed", "Retired", "Student"]
EMPLOYMENT_WEIGHTS = [0.50, 0.15, 0.15, 0.12, 0.08]

SEGMENTS = ["Mass Market", "Emerging Affluent", "Affluent", "Private Banking"]
SEGMENT_WEIGHTS = [0.60, 0.25, 0.12, 0.03]

ONBOARDING_CHANNELS = ["Branch", "Online", "Mobile", "Agent"]
ONBOARDING_CHANNEL_WEIGHTS = [0.40, 0.35, 0.20, 0.05]

KYC_STATUSES = ["Verified", "Pending", "Expired"]
KYC_WEIGHTS = [0.82, 0.12, 0.06]

FICA_STATUSES = ["Compliant", "Non-Compliant"]
FICA_WEIGHTS = [0.88, 0.12]

INCOME_SOURCES = ["Salary", "Business", "Investment", "Pension", "Social Grant"]
INCOME_SOURCE_WEIGHTS = [0.60, 0.15, 0.10, 0.10, 0.05]

NATIONALITIES = ["South African"] * 90 + [
    "Zimbabwean", "Mozambican", "Malawian", "Nigerian", "Congolese",
    "Zambian", "Tanzanian", "Kenyan", "Ghanaian", "British",
]
COUNTRIES_OF_BIRTH = ["South Africa"] * 90 + [
    "Zimbabwe", "Mozambique", "Malawi", "Nigeria", "DRC",
    "Zambia", "Tanzania", "Kenya", "Ghana", "United Kingdom",
]

ACCOUNT_TYPES = [
    "Cheque Account", "Savings Account", "Fixed Deposit",
    "Money Market", "Credit Card", "Business Account",
]
ACCOUNT_TYPE_WEIGHTS = [0.35, 0.30, 0.10, 0.08, 0.12, 0.05]

PRODUCT_MAP = {
    "Cheque Account": ["PCB Plus Cheque", "PCB Gold Cheque", "PCB Premier Cheque", "PCB Prestige Cheque"],
    "Savings Account": ["PCB SaveSmart", "PCB PocketSave", "PCB Goal Saver", "PCB Flexi Save"],
    "Fixed Deposit": ["PCB Fixed 12M", "PCB Fixed 24M", "PCB Fixed 36M", "PCB Fixed 60M"],
    "Money Market": ["PCB Money Market Call", "PCB Money Market 32-Day", "PCB Treasury Plus"],
    "Credit Card": ["PCB Standard Credit Card", "PCB Gold Credit Card", "PCB Platinum Credit Card", "PCB Infinite Credit Card"],
    "Business Account": ["PCB Business Current", "PCB Business Premium", "PCB Enterprise Account"],
}

INTEREST_TYPES = ["Fixed", "Variable", "Prime-linked"]
INTEREST_TYPE_WEIGHTS = [0.35, 0.40, 0.25]

ACCOUNT_STATUSES = ["Active", "Dormant", "Closed", "Suspended"]
ACCOUNT_STATUS_WEIGHTS = [0.87, 0.06, 0.05, 0.02]

CURRENCIES = ["ZAR", "USD", "EUR"]
CURRENCY_WEIGHTS = [0.95, 0.03, 0.02]

SA_BANKS = ["Absa", "FNB", "Standard Bank", "Nedbank", "Capitec", "Investec", "TymeBank", "Discovery Bank"]

TX_TYPES = [
    "Debit Order", "EFT", "ATM Withdrawal", "POS", "Online Purchase",
    "Cash Deposit", "Salary Credit", "Interest", "Fee", "RTGS",
    "Internal Transfer", "Cheque",
]
TX_TYPE_WEIGHTS = [0.15, 0.15, 0.12, 0.18, 0.10, 0.05, 0.07, 0.04, 0.05, 0.02, 0.05, 0.02]

TX_CHANNELS = ["ATM", "Branch", "Online", "Mobile", "USSD", "API"]
TX_CHANNEL_WEIGHTS = [0.20, 0.10, 0.30, 0.25, 0.05, 0.10]

TX_STATUSES = ["Completed", "Pending", "Failed", "Reversed"]
TX_STATUS_WEIGHTS = [0.92, 0.04, 0.02, 0.02]

MERCHANT_CATEGORIES = {
    5411: "Grocery Stores", 5912: "Drug Stores & Pharmacies",
    5812: "Eating Places & Restaurants", 5541: "Service Stations",
    5311: "Department Stores", 5945: "Hobby & Toy Shops",
    7011: "Hotels & Motels", 4111: "Transportation",
    5734: "Electronics Stores", 5999: "Retail Stores",
    4814: "Telecommunications", 8011: "Doctors & Physicians",
    5621: "Women's Clothing Stores", 5661: "Shoe Stores",
    4816: "Online Services",
}
MCC_LIST = list(MERCHANT_CATEGORIES.keys())
MCC_NAMES = list(MERCHANT_CATEGORIES.values())

SA_MERCHANTS = [
    "Woolworths", "Pick n Pay", "Checkers", "Shoprite", "Spar", "Clicks", "Dischem",
    "Edgars", "Mr Price", "Truworths", "Foschini", "H&M SA", "Zara SA",
    "McDonald's SA", "KFC SA", "Spur", "Steers", "Debonairs",
    "Shell SA", "Engen", "BP SA", "Total SA", "Caltex SA",
    "Eskom", "City Power", "Telkom", "MTN", "Vodacom", "Cell C", "Rain",
    "Netflix SA", "Amazon SA", "Takealot", "Superbalist",
    "Naspers", "Discovery Health", "Momentum", "Old Mutual",
    "Virgin Active", "Planet Fitness", "Gym Company",
    "Tiger Brands", "Pioneer Foods", "Bidvest",
]

LOAN_TYPES = ["Home Loan", "Personal Loan", "Vehicle Finance", "Business Loan", "Revolving Credit"]
LOAN_TYPE_WEIGHTS = [0.30, 0.35, 0.20, 0.10, 0.05]

LOAN_STATUSES = ["Current", "1-30 DPD", "31-60 DPD", "61-90 DPD", "NPL"]
LOAN_STATUS_WEIGHTS = [0.75, 0.08, 0.05, 0.04, 0.08]

PROVISION_RATES = {"Current": 0.0, "1-30 DPD": 0.25, "31-60 DPD": 0.50, "61-90 DPD": 0.75, "NPL": 1.00}

COLLATERAL_TYPES = ["Property", "Vehicle", "None", "Cession"]
COLLATERAL_WEIGHTS = [0.30, 0.20, 0.35, 0.15]

LOAN_PURPOSES = {
    "Home Loan": ["Primary Residence", "Investment Property", "Holiday Home", "Land Purchase"],
    "Personal Loan": ["Debt Consolidation", "Home Renovation", "Education", "Medical", "Holiday", "General Expenses"],
    "Vehicle Finance": ["Private Vehicle", "Commercial Vehicle", "Motorbike", "Boat"],
    "Business Loan": ["Working Capital", "Equipment", "Expansion", "Property", "Stock"],
    "Revolving Credit": ["General", "Emergency", "Home Improvement"],
}

ORIGINATION_CHANNELS = ["Branch", "Online", "Broker", "Mobile", "Call Centre"]

PAYMENT_TYPES = ["EFT", "RTGS", "SWIFT", "Debit Order", "Internal", "Payroll"]
PAYMENT_TYPE_WEIGHTS = [0.45, 0.10, 0.05, 0.20, 0.15, 0.05]

PAYMENT_STATUSES = ["Settled", "Pending", "Failed", "Returned", "Recalled"]
PAYMENT_STATUS_WEIGHTS = [0.88, 0.05, 0.04, 0.02, 0.01]

FRAUD_TYPES = [
    "Card Not Present", "Account Takeover", "Identity Fraud",
    "Phishing", "ATM Skimming", "Internal Fraud", "Other",
]
FRAUD_TYPE_WEIGHTS = [0.35, 0.20, 0.15, 0.10, 0.08, 0.05, 0.07]

FRAUD_SOURCES = ["Rules Engine", "ML Model", "Customer Report", "Staff Report"]
FRAUD_SOURCE_WEIGHTS = [0.45, 0.30, 0.15, 0.10]

FRAUD_PRIORITIES = ["Low", "Medium", "High", "Critical"]
FRAUD_PRIORITY_WEIGHTS = [0.30, 0.35, 0.25, 0.10]

FRAUD_STATUSES = ["Open", "Under Investigation", "Confirmed Fraud", "False Positive", "Closed"]
FRAUD_STATUS_WEIGHTS = [0.10, 0.15, 0.30, 0.25, 0.20]

CARD_BRANDS = ["Visa", "Mastercard"]
CARD_TYPES = ["Credit", "Debit"]

CARD_TX_TYPES = [
    "Purchase", "Cash Advance", "Refund", "Chargeback",
    "Contactless Payment", "Online Purchase", "International Purchase",
]
CARD_TX_WEIGHTS = [0.45, 0.05, 0.10, 0.02, 0.20, 0.12, 0.06]

CARD_TX_STATUSES = ["Approved", "Declined", "Reversed", "Pending"]
CARD_TX_STATUS_WEIGHTS = [0.88, 0.06, 0.04, 0.02]

# SA Fintech partner reference data
SA_FINTECH_PARTNERS = [
    {
        "fintech_id": "FT001", "fintech_name": "Yoco",
        "platform_type": "POS Card Acquiring", "settlement_currency": "ZAR",
        "fee_rate_pct": 2.95, "min_fee_zar": 0.00,
        "target_segment": "SME", "founded_year": 2013,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "Yoco Technologies", "is_active": True,
        "integration_type": "REST API / Mobile SDK",
        "avg_basket_zar": 856.40, "active_merchants_est": 250_000,
        "annual_volume_zar_bn": 12.4, "settlement_days": 1,
        "notes": (
            "SAs largest independent payment provider. Card machines: "
            "Yoco Go (R599), Yoco Khumo (R699), Yoco Neo (R1499), "
            "Yoco Counter+Neo Touch (R2999). Raised USD83M Series C (2021). "
            "250K+ merchants in retail, hospitality, and services."
        ),
    },
    {
        "fintech_id": "FT002", "fintech_name": "SnapScan",
        "platform_type": "QR Code Payment", "settlement_currency": "ZAR",
        "fee_rate_pct": 2.75, "min_fee_zar": 0.00,
        "target_segment": "SME and Consumer", "founded_year": 2012,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "Standard Bank Group", "is_active": True,
        "integration_type": "QR Integration / App SDK",
        "avg_basket_zar": 340.20, "active_merchants_est": 80_000,
        "annual_volume_zar_bn": 3.2, "settlement_days": 1,
        "notes": (
            "Acquired by Standard Bank (2014). QR-code payments — no card machine "
            "required. Popular in restaurants, markets, pop-up stores, and events. "
            "Competes directly with Yoco for the SME hospitality segment."
        ),
    },
    {
        "fintech_id": "FT003", "fintech_name": "PayFast",
        "platform_type": "Online Payment Gateway", "settlement_currency": "ZAR",
        "fee_rate_pct": 3.50, "min_fee_zar": 2.00,
        "target_segment": "E-Commerce", "founded_year": 2007,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "DPO Group (Network International)", "is_active": True,
        "integration_type": "Plugin (Shopify / WooCommerce / Magento) + API",
        "avg_basket_zar": 1250.60, "active_merchants_est": 45_000,
        "annual_volume_zar_bn": 8.1, "settlement_days": 2,
        "notes": (
            "SAs most widely used online gateway. Supports 20+ payment methods "
            "including instant EFT, credit/debit cards, Mobicred, and Zapper. "
            "Critical for e-commerce with Takealot Marketplace integration. "
            "Acquired by DPO Group (2021)."
        ),
    },
    {
        "fintech_id": "FT004", "fintech_name": "Peach Payments",
        "platform_type": "Online Payment Gateway", "settlement_currency": "ZAR",
        "fee_rate_pct": 2.90, "min_fee_zar": 1.50,
        "target_segment": "Mid-Market E-Commerce", "founded_year": 2012,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "Peach Payments (Pty) Ltd", "is_active": True,
        "integration_type": "REST API / SDK / Hosted Page",
        "avg_basket_zar": 980.30, "active_merchants_est": 12_000,
        "annual_volume_zar_bn": 2.3, "settlement_days": 1,
        "notes": (
            "API-first developer-friendly gateway. Serves mid-to-large merchants "
            "including Superbalist and OneDayOnly. Supports 3D Secure 2.0, "
            "tokenisation, and recurring billing. Growing in B2B SaaS vertical."
        ),
    },
    {
        "fintech_id": "FT005", "fintech_name": "PayGate",
        "platform_type": "Online Payment Gateway", "settlement_currency": "ZAR",
        "fee_rate_pct": 2.80, "min_fee_zar": 1.00,
        "target_segment": "Enterprise E-Commerce", "founded_year": 2000,
        "hq_city": "Johannesburg", "hq_province": "Gauteng",
        "parent_company": "DPO Group (Network International)", "is_active": True,
        "integration_type": "Legacy API / Hosted Checkout",
        "avg_basket_zar": 1890.70, "active_merchants_est": 8_000,
        "annual_volume_zar_bn": 1.8, "settlement_days": 2,
        "notes": (
            "One of SAs oldest gateways (est. 2000). Enterprise merchants including "
            "large retailers, travel agencies, and government e-services. Strong in "
            "recurring billing. Complementary to PayFast in the DPO portfolio."
        ),
    },
    {
        "fintech_id": "FT006", "fintech_name": "Ozow",
        "platform_type": "Instant EFT Gateway", "settlement_currency": "ZAR",
        "fee_rate_pct": 1.50, "min_fee_zar": 1.00,
        "target_segment": "E-Commerce", "founded_year": 2015,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "Ozow (Pty) Ltd", "is_active": True,
        "integration_type": "REST API / Bank Redirect",
        "avg_basket_zar": 720.90, "active_merchants_est": 28_000,
        "annual_volume_zar_bn": 4.6, "settlement_days": 1,
        "notes": (
            "Instant EFT gateway — customer authorises payment directly in their own "
            "banking app, no card details captured. Lower merchant fees than card "
            "acquiring since it bypasses interchange. Strong in gaming, utilities, "
            "and airtime/data top-up."
        ),
    },
    {
        "fintech_id": "FT007", "fintech_name": "Zapper",
        "platform_type": "QR Code Payment", "settlement_currency": "ZAR",
        "fee_rate_pct": 2.60, "min_fee_zar": 0.00,
        "target_segment": "SME and Consumer", "founded_year": 2013,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "Zapper (Pty) Ltd", "is_active": True,
        "integration_type": "QR Integration / App SDK",
        "avg_basket_zar": 298.50, "active_merchants_est": 42_000,
        "annual_volume_zar_bn": 2.1, "settlement_days": 1,
        "notes": (
            "One of SAs original QR-code payment apps. Also processes loyalty and "
            "voucher redemption alongside payment, positioning it as an engagement "
            "layer for retail and QSR chains rather than a pure acquirer."
        ),
    },
    {
        "fintech_id": "FT008", "fintech_name": "iKhokha",
        "platform_type": "POS Card Acquiring", "settlement_currency": "ZAR",
        "fee_rate_pct": 2.75, "min_fee_zar": 0.00,
        "target_segment": "SME", "founded_year": 2012,
        "hq_city": "Durban", "hq_province": "KwaZulu-Natal",
        "parent_company": "iKhokha (Pty) Ltd", "is_active": True,
        "integration_type": "REST API / Mobile SDK",
        "avg_basket_zar": 612.30, "active_merchants_est": 130_000,
        "annual_volume_zar_bn": 7.8, "settlement_days": 1,
        "notes": (
            "Card-machine acquirer competing directly with Yoco in the SME segment, "
            "strongest outside the Western Cape. Bundles working-capital advances "
            "against processing history — a direct input to PCB's SME credit "
            "scoring where iKhokha cash flow is used as alt-data."
        ),
    },
    {
        "fintech_id": "FT009", "fintech_name": "Stitch",
        "platform_type": "Open Banking / Payments API", "settlement_currency": "ZAR",
        "fee_rate_pct": 1.20, "min_fee_zar": 0.50,
        "target_segment": "Developer / Enterprise", "founded_year": 2020,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "Stitch Money (Pty) Ltd", "is_active": True,
        "integration_type": "REST API / Open Banking",
        "avg_basket_zar": 2140.80, "active_merchants_est": 3_200,
        "annual_volume_zar_bn": 1.4, "settlement_days": 1,
        "notes": (
            "Open-banking infrastructure: pay-by-bank, account verification and "
            "recurring debit orders via direct bank API connections rather than "
            "card rails. Smallest merchant base of the twelve but highest average "
            "transaction size — used mainly for high-value B2B and subscription flows."
        ),
    },
    {
        "fintech_id": "FT010", "fintech_name": "Kazang",
        "platform_type": "Prepaid & Bill Payment Terminal", "settlement_currency": "ZAR",
        "fee_rate_pct": 3.20, "min_fee_zar": 0.00,
        "target_segment": "Informal Trader", "founded_year": 2007,
        "hq_city": "Johannesburg", "hq_province": "Gauteng",
        "parent_company": "Blue Label Telecoms", "is_active": True,
        "integration_type": "Terminal / USSD",
        "avg_basket_zar": 84.60, "active_merchants_est": 65_000,
        "annual_volume_zar_bn": 3.9, "settlement_days": 1,
        "notes": (
            "Prepaid airtime, electricity and bill-payment terminal network reaching "
            "spaza shops and informal traders that card acquirers rarely reach. "
            "High transaction count, very low average basket — the transaction-count "
            "outlier of the twelve partners."
        ),
    },
    {
        "fintech_id": "FT011", "fintech_name": "Adumo",
        "platform_type": "POS Card Acquiring", "settlement_currency": "ZAR",
        "fee_rate_pct": 2.85, "min_fee_zar": 0.00,
        "target_segment": "SME and Enterprise", "founded_year": 2019,
        "hq_city": "Johannesburg", "hq_province": "Gauteng",
        "parent_company": "Adumo Holdings", "is_active": True,
        "integration_type": "REST API / Mobile SDK / Hosted Page",
        "avg_basket_zar": 715.20, "active_merchants_est": 38_000,
        "annual_volume_zar_bn": 3.3, "settlement_days": 2,
        "notes": (
            "Payment technology group formed from the merger of several legacy SA "
            "acquirers (including SwitchPay and iVeri), spanning in-store card "
            "machines and online gateway under one brand."
        ),
    },
    {
        "fintech_id": "FT012", "fintech_name": "Mukuru",
        "platform_type": "Cross-Border Remittance", "settlement_currency": "ZAR",
        "fee_rate_pct": 4.50, "min_fee_zar": 15.00,
        "target_segment": "Remittance / Consumer", "founded_year": 2004,
        "hq_city": "Cape Town", "hq_province": "Western Cape",
        "parent_company": "Mukuru Africa", "is_active": True,
        "integration_type": "Agent Network / App / API",
        "avg_basket_zar": 1480.00, "active_merchants_est": 4_500,
        "annual_volume_zar_bn": 3.5, "settlement_days": 1,
        "notes": (
            "Cross-border remittance for the SADC migrant-worker corridor — "
            "cash-in at an agent or the app, cash-out in Zimbabwe, Mozambique, "
            "Malawi and other regional destinations. Highest fee rate of the "
            "twelve, reflecting FX conversion and cross-border compliance cost."
        ),
    },
]

# Monthly seasonality multipliers for fintech settlement volume (Jan–Dec)
# Reflects Black Friday (Nov +35%), Festive Season (Dec +45%), Jan dip (-12%)
FINTECH_SEASONAL = [0.88, 0.81, 0.93, 0.97, 0.84, 0.81, 0.96, 1.02, 0.91, 1.05, 1.37, 1.60]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def random_date(start: datetime.date, end: datetime.date) -> datetime.date:
    delta = (end - start).days
    return start + datetime.timedelta(days=random.randint(0, delta))


def random_date_arr(start: datetime.date, end: datetime.date, n: int) -> np.ndarray:
    delta = (end - start).days
    offsets = np.random.randint(0, delta + 1, size=n)
    base = np.datetime64(start)
    return base + offsets.astype("timedelta64[D]")


def luhn_checksum(number_str: str) -> int:
    """Return Luhn check digit for a partial number string."""
    digits = [int(d) for d in number_str]
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    total = sum(odd_digits)
    for d in even_digits:
        total += sum(divmod(d * 2, 10))
    return (10 - (total % 10)) % 10


def generate_sa_id(dob: datetime.date, gender: str) -> str:
    """
    Build a valid-looking 13-digit South African ID number.
    Format: YYMMDD GGGG C RR
      YYMMDD = date of birth
      GGGG   = 0000-4999 female, 5000-9999 male
      C      = citizenship (0=SA, 1=permanent resident)
      R      = race digit (deprecated, set to 8)
      RR     = last two digits before checksum (set to 8 here)
    Digit 13 = Luhn check digit over first 12.
    """
    yy = str(dob.year)[-2:]
    mm = f"{dob.month:02d}"
    dd = f"{dob.day:02d}"
    if gender == "M":
        g_seq = random.randint(5000, 9999)
    else:
        g_seq = random.randint(0, 4999)
    citizenship = 0
    race_digit = 8
    partial = f"{yy}{mm}{dd}{g_seq:04d}{citizenship}{race_digit}"
    check = luhn_checksum(partial)
    return partial + str(check)


def za_mobile() -> str:
    prefixes = ["060", "061", "062", "063", "064", "065", "066", "067", "068", "069",
                "071", "072", "073", "074", "076", "078", "079",
                "081", "082", "083", "084", "085", "086", "087"]
    prefix = random.choice(prefixes)
    suffix = "".join([str(random.randint(0, 9)) for _ in range(7)])
    return f"{prefix}{suffix}"


def za_postal_code() -> str:
    return f"{random.randint(1, 9999):04d}"


def weighted_choice(population, weights, size=None):
    if size is None:
        return random.choices(population, weights=weights, k=1)[0]
    return random.choices(population, weights=weights, k=size)


def chunk_print(label: str, done: int, total: int):
    pct = done / total * 100
    print(f"  [{label}] {done:>10,} / {total:,}  ({pct:.1f}%)")


# ---------------------------------------------------------------------------
# 1. CUSTOMERS
# ---------------------------------------------------------------------------

def generate_customers() -> pd.DataFrame:
    print("\n[1/7] Generating customers.csv ...")
    n = N_CUSTOMERS

    # --- segment & income ---
    segments = np.array(weighted_choice(SEGMENTS, SEGMENT_WEIGHTS, n))
    income_mu = np.where(segments == "Mass Market", 12.0,
                np.where(segments == "Emerging Affluent", 13.0,
                np.where(segments == "Affluent", 13.8, 14.5)))
    income_sigma = 0.55
    annual_income = np.clip(
        np.exp(np.random.normal(income_mu, income_sigma, n)), 18_000, 20_000_000
    ).astype(int)

    # --- credit score correlated with income ---
    income_norm = (np.log(annual_income) - np.log(18_000)) / (np.log(20_000_000) - np.log(18_000))
    credit_base = 300 + income_norm * 550
    credit_noise = np.random.normal(0, 40, n)
    credit_score = np.clip(credit_base + credit_noise, 300, 850).astype(int)

    # --- dates ---
    onboarding_dates = random_date_arr(START_DATE, END_DATE, n)
    dob_start = datetime.date(1950, 1, 1)
    dob_end = datetime.date(2005, 12, 31)
    dob_dates = random_date_arr(dob_start, dob_end, n)

    # --- gender (random) ---
    genders = np.random.choice(["M", "F"], size=n, p=[0.48, 0.52])

    # --- race ---
    races = np.array(weighted_choice(RACES, RACE_PROBS, n))

    # --- employment ---
    emp_statuses = np.array(weighted_choice(EMPLOYMENT_STATUSES, EMPLOYMENT_WEIGHTS, n))

    # --- provinces ---
    provinces = np.array(weighted_choice(SA_PROVINCES, PROVINCE_WEIGHTS, n))

    rows = []
    batch = 100_000
    for start in range(0, n, batch):
        end = min(start + batch, n)
        for i in range(start, end):
            prov = provinces[i]
            city = random.choice(CITIES_BY_PROVINCE[prov])
            suburb = random.choice(SUBURBS_BY_PROVINCE[prov])
            dob = datetime.date.fromordinal(int(dob_dates[i].astype("datetime64[D]").astype(int)) + 719163)
            id_number = generate_sa_id(dob, genders[i])
            emp = emp_statuses[i]
            seg = segments[i]

            employer = ""
            job_title = ""
            if emp in ("Employed", "Self-Employed"):
                employer = fake.company()
                job_title = fake.job()

            churn_date = ""
            is_active = random.random() > 0.08
            if not is_active:
                onb = datetime.date.fromordinal(int(onboarding_dates[i].astype("datetime64[D]").astype(int)) + 719163)
                max_churn = min(onb + datetime.timedelta(days=365*3), END_DATE)
                if max_churn > onb:
                    churn_date = str(random_date(onb + datetime.timedelta(days=30), max_churn))

            rm_id = ""
            if seg in ("Affluent", "Private Banking"):
                rm_id = f"RM{random.randint(1000, 9999)}"

            nationality_idx = random.randint(0, len(NATIONALITIES) - 1)
            vat_number = ""
            if emp == "Self-Employed" and random.random() < 0.3:
                vat_number = f"4{random.randint(100000000, 999999999)}"

            inc_src_choices = INCOME_SOURCES
            inc_src_weights = INCOME_SOURCE_WEIGHTS
            if emp == "Retired":
                inc_src_weights = [0.05, 0.05, 0.15, 0.60, 0.15]
            elif emp == "Unemployed":
                inc_src_weights = [0.00, 0.05, 0.10, 0.00, 0.85]
            elif emp == "Student":
                inc_src_weights = [0.20, 0.05, 0.05, 0.00, 0.70]

            rows.append({
                "customer_id": str(uuid.uuid4()),
                "customer_number": f"PCB{random.randint(1000000000, 9999999999)}",
                "first_name": fake.first_name_male() if genders[i] == "M" else fake.first_name_female(),
                "last_name": fake.last_name(),
                "id_number": id_number,
                "date_of_birth": str(dob),
                "gender": genders[i],
                "race": races[i],
                "email": fake.email(),
                "phone": za_mobile(),
                "mobile_phone": za_mobile(),
                "street_address": fake.street_address(),
                "suburb": suburb,
                "city": city,
                "province": prov,
                "postal_code": za_postal_code(),
                "employment_status": emp,
                "employer_name": employer,
                "job_title": job_title,
                "annual_income": int(annual_income[i]),
                "income_source": weighted_choice(inc_src_choices, inc_src_weights),
                "credit_score": int(credit_score[i]),
                "customer_segment": seg,
                "onboarding_date": str(datetime.date.fromordinal(
                    int(onboarding_dates[i].astype("datetime64[D]").astype(int)) + 719163
                )),
                "onboarding_channel": weighted_choice(ONBOARDING_CHANNELS, ONBOARDING_CHANNEL_WEIGHTS),
                "kyc_status": weighted_choice(KYC_STATUSES, KYC_WEIGHTS),
                "fica_status": weighted_choice(FICA_STATUSES, FICA_WEIGHTS),
                "is_active": is_active,
                "churn_date": churn_date,
                "relationship_manager_id": rm_id,
                "nationality": NATIONALITIES[nationality_idx],
                "country_of_birth": COUNTRIES_OF_BIRTH[nationality_idx],
                "tax_number": f"{random.randint(1000000000, 9999999999)}",
                "vat_number": vat_number,
                "marketing_consent": random.random() > 0.25,
                "digital_consent": random.random() > 0.15,
            })

        chunk_print("customers", end, n)

    df = pd.DataFrame(rows)
    out = OUTPUT_DIR / "customers.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}  ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 2. ACCOUNTS
# ---------------------------------------------------------------------------

def generate_accounts(customers: pd.DataFrame) -> pd.DataFrame:
    print("\n[2/7] Generating accounts.csv ...")
    n = N_ACCOUNTS

    cust_ids = customers["customer_id"].values
    cust_segments = customers["customer_segment"].values
    cust_incomes = customers["annual_income"].values
    cust_onboard = customers["onboarding_date"].values

    # Map segment to rough balance multiplier
    seg_balance_mult = {
        "Mass Market": 1.0, "Emerging Affluent": 3.0,
        "Affluent": 10.0, "Private Banking": 50.0,
    }

    branch_codes = [f"{random.randint(100000, 999999)}" for _ in range(200)]

    # build branch lookup: code -> name & province
    branch_info = {}
    for bc in branch_codes:
        prov = weighted_choice(SA_PROVINCES, PROVINCE_WEIGHTS)
        city = random.choice(CITIES_BY_PROVINCE[prov])
        branch_info[bc] = {"name": f"PCB {city} Branch", "province": prov}

    # Assign accounts to customers — roughly 2.4 accounts per customer
    cust_indices = np.random.randint(0, len(cust_ids), size=n)

    account_types = np.array(weighted_choice(ACCOUNT_TYPES, ACCOUNT_TYPE_WEIGHTS, n))
    currencies = np.array(weighted_choice(CURRENCIES, CURRENCY_WEIGHTS, n))
    statuses = np.array(weighted_choice(ACCOUNT_STATUSES, ACCOUNT_STATUS_WEIGHTS, n))
    interest_types = np.array(weighted_choice(INTEREST_TYPES, INTEREST_TYPE_WEIGHTS, n))

    rows = []
    account_ids = []

    batch = 100_000
    for start in range(0, n, batch):
        end = min(start + batch, n)
        for i in range(start, end):
            ci = cust_indices[i]
            cid = cust_ids[ci]
            seg = cust_segments[ci]
            income = float(cust_incomes[ci])
            mult = seg_balance_mult.get(seg, 1.0)

            acc_type = account_types[i]
            product_name = random.choice(PRODUCT_MAP[acc_type])
            bc = random.choice(branch_codes)
            bi = branch_info[bc]

            # Balance distribution
            if acc_type == "Credit Card":
                balance = round(random.uniform(-50000 * mult, 0), 2)
                credit_limit = round(income * random.uniform(0.1, 0.5), 2)
            elif acc_type == "Fixed Deposit":
                balance = round(random.uniform(10000, 2000000) * mult / 10.0, 2)
                credit_limit = 0.0
            elif acc_type == "Money Market":
                balance = round(random.uniform(50000, 5000000) * mult / 10.0, 2)
                credit_limit = 0.0
            elif acc_type == "Business Account":
                balance = round(random.uniform(5000, 500000) * mult, 2)
                credit_limit = round(balance * random.uniform(0.5, 2.0), 2)
            else:
                # Cheque / Savings
                monthly = income / 12
                balance = round(max(0, np.random.exponential(monthly * 0.5)), 2)
                credit_limit = round(monthly * random.uniform(1, 3), 2) if acc_type == "Cheque Account" else 0.0

            available = round(balance + credit_limit * random.uniform(0, 1), 2) if acc_type == "Credit Card" \
                else round(max(0, balance - random.uniform(0, balance * 0.1)), 2)
            ledger = round(balance + random.uniform(-500, 500), 2)

            interest_rate = round(random.uniform(0.005, 0.225), 4)
            status = statuses[i]
            currency = currencies[i]

            open_date_str = cust_onboard[ci]
            open_date = datetime.date.fromisoformat(str(open_date_str))
            if (END_DATE - open_date).days > 0:
                acc_open = random_date(open_date, END_DATE)
            else:
                acc_open = open_date

            close_date = ""
            if status == "Closed":
                if (END_DATE - acc_open).days > 30:
                    close_date = str(random_date(
                        acc_open + datetime.timedelta(days=30), END_DATE
                    ))

            last_tx_date = str(random_date(
                acc_open, END_DATE
            )) if status != "Closed" else close_date

            aid = str(uuid.uuid4())
            account_ids.append(aid)
            rows.append({
                "account_id": aid,
                "account_number": f"{random.randint(1000000000, 9999999999)}",
                "customer_id": cid,
                "account_type": acc_type,
                "product_name": product_name,
                "branch_code": bc,
                "branch_name": bi["name"],
                "branch_province": bi["province"],
                "open_date": str(acc_open),
                "close_date": close_date,
                "status": status,
                "current_balance": balance,
                "available_balance": available,
                "ledger_balance": ledger,
                "interest_rate": interest_rate,
                "interest_type": interest_types[i],
                "currency": currency,
                "credit_limit": credit_limit,
                "last_transaction_date": last_tx_date,
            })

        chunk_print("accounts", end, n)

    df = pd.DataFrame(rows)
    out = OUTPUT_DIR / "accounts.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}  ({len(df):,} rows)")
    return df, np.array(account_ids)


# ---------------------------------------------------------------------------
# 3. TRANSACTIONS
# ---------------------------------------------------------------------------

def generate_transactions(accounts: pd.DataFrame, account_ids: np.ndarray):
    print(f"\n[3/7] Generating transactions.csv ({N_TRANSACTIONS:,} rows in chunks of {TX_CHUNK_SIZE:,}) ...")
    out = OUTPUT_DIR / "transactions.csv"
    header_written = False

    acc_id_arr = accounts["account_id"].values
    cust_id_arr = accounts["customer_id"].values
    balance_arr = accounts["current_balance"].values.copy()
    open_date_arr = accounts["open_date"].values
    currency_arr = accounts["currency"].values

    total_written = 0

    while total_written < N_TRANSACTIONS:
        chunk_size = min(TX_CHUNK_SIZE, N_TRANSACTIONS - total_written)
        acc_indices = np.random.randint(0, len(acc_id_arr), size=chunk_size)
        tx_types = np.array(weighted_choice(TX_TYPES, TX_TYPE_WEIGHTS, chunk_size))
        tx_channels = np.array(weighted_choice(TX_CHANNELS, TX_CHANNEL_WEIGHTS, chunk_size))
        tx_statuses = np.array(weighted_choice(TX_STATUSES, TX_STATUS_WEIGHTS, chunk_size))

        rows = []
        for j in range(chunk_size):
            idx = acc_indices[j]
            acc_id = acc_id_arr[idx]
            cust_id = cust_id_arr[idx]
            currency = currency_arr[idx]

            tx_type = tx_types[j]
            channel = tx_channels[j]
            status = tx_statuses[j]

            # Amount by transaction type
            if tx_type == "Salary Credit":
                amount = round(random.uniform(8_000, 80_000), 2)
                is_debit = False
            elif tx_type == "ATM Withdrawal":
                amount = round(random.uniform(200, 3_000), 2)
                is_debit = True
            elif tx_type == "POS":
                amount = round(random.uniform(50, 5_000), 2)
                is_debit = True
            elif tx_type == "Online Purchase":
                amount = round(random.uniform(50, 20_000), 2)
                is_debit = True
            elif tx_type == "Cash Deposit":
                amount = round(random.uniform(200, 50_000), 2)
                is_debit = False
            elif tx_type == "Debit Order":
                amount = round(random.uniform(100, 10_000), 2)
                is_debit = True
            elif tx_type == "EFT":
                amount = round(random.uniform(50, 500_000), 2)
                is_debit = random.random() > 0.5
            elif tx_type == "RTGS":
                amount = round(random.uniform(50_000, 5_000_000), 2)
                is_debit = random.random() > 0.5
            elif tx_type == "Interest":
                amount = round(random.uniform(1, 5_000), 2)
                is_debit = False
            elif tx_type == "Fee":
                amount = round(random.uniform(5, 500), 2)
                is_debit = True
            elif tx_type == "Internal Transfer":
                amount = round(random.uniform(100, 100_000), 2)
                is_debit = random.random() > 0.5
            elif tx_type == "Cheque":
                amount = round(random.uniform(500, 200_000), 2)
                is_debit = True
            else:
                amount = round(random.uniform(10, 10_000), 2)
                is_debit = True

            # Running balance update
            if is_debit:
                balance_arr[idx] -= amount
            else:
                balance_arr[idx] += amount
            running_balance = round(float(balance_arr[idx]), 2)

            tx_date = random_date(START_DATE, END_DATE)
            value_date = tx_date + datetime.timedelta(days=random.randint(0, 2))

            merchant_name = ""
            mcc = ""
            if tx_type in ("POS", "Online Purchase"):
                merchant_name = random.choice(SA_MERCHANTS)
                mcc = str(random.choice(MCC_LIST))

            exc_rate = 1.0
            if currency == "USD":
                exc_rate = round(random.uniform(17.5, 19.5), 4)
            elif currency == "EUR":
                exc_rate = round(random.uniform(19.0, 21.5), 4)

            rows.append({
                "transaction_id": str(uuid.uuid4()),
                "transaction_date": str(tx_date),
                "value_date": str(value_date),
                "account_id": acc_id,
                "customer_id": cust_id,
                "transaction_type": tx_type,
                "amount": amount,
                "running_balance": running_balance,
                "description": f"{tx_type} - {fake.bs()[:40]}" if not merchant_name else merchant_name,
                "reference_number": f"REF{random.randint(100000000, 999999999)}",
                "channel": channel,
                "merchant_name": merchant_name,
                "merchant_category_code": mcc,
                "is_debit": is_debit,
                "status": status,
                "currency": currency,
                "exchange_rate": exc_rate,
            })

        df_chunk = pd.DataFrame(rows)
        df_chunk.to_csv(out, index=False, header=not header_written, mode="a")
        header_written = True
        total_written += chunk_size
        chunk_print("transactions", total_written, N_TRANSACTIONS)

    print(f"  Saved: {out}  ({total_written:,} rows)")


# ---------------------------------------------------------------------------
# 4. LOANS
# ---------------------------------------------------------------------------

def generate_loans(customers: pd.DataFrame, accounts: pd.DataFrame) -> pd.DataFrame:
    print("\n[4/7] Generating loans.csv ...")
    n = N_LOANS

    cust_ids = customers["customer_id"].values
    cust_incomes = customers["annual_income"].values
    cust_segments = customers["customer_segment"].values
    acc_ids = accounts["account_id"].values
    acc_cust_ids = accounts["customer_id"].values

    # Build customer -> account mapping (first account per customer)
    cust_to_acc = {}
    for ai, ci in zip(acc_ids, acc_cust_ids):
        if ci not in cust_to_acc:
            cust_to_acc[ci] = ai

    loan_types = np.array(weighted_choice(LOAN_TYPES, LOAN_TYPE_WEIGHTS, n))
    loan_statuses = np.array(weighted_choice(LOAN_STATUSES, LOAN_STATUS_WEIGHTS, n))
    orig_channels = np.array(weighted_choice(ORIGINATION_CHANNELS, [0.40, 0.25, 0.20, 0.10, 0.05], n))

    rows = []
    for i in range(n):
        ci_idx = random.randint(0, len(cust_ids) - 1)
        cid = cust_ids[ci_idx]
        income = float(cust_incomes[ci_idx])
        seg = cust_segments[ci_idx]
        aid = cust_to_acc.get(cid, str(acc_ids[0]))

        ltype = loan_types[i]
        lstatus = loan_statuses[i]

        # Amount ranges
        if ltype == "Home Loan":
            orig_amount = round(random.uniform(800_000, 3_000_000), 2)
            term_months = random.choice([120, 180, 240, 300, 360])
            collateral = "Property"
            interest_rate = round(random.uniform(0.09, 0.125), 4)
            ltv = round(random.uniform(0.60, 0.95), 4)
            collateral_value = round(orig_amount / ltv, 2)
        elif ltype == "Personal Loan":
            orig_amount = round(random.uniform(10_000, 500_000), 2)
            term_months = random.choice([12, 24, 36, 48, 60, 72])
            collateral = weighted_choice(["None", "Cession"], [0.70, 0.30])
            interest_rate = round(random.uniform(0.12, 0.28), 4)
            ltv = None
            collateral_value = 0.0
        elif ltype == "Vehicle Finance":
            orig_amount = round(random.uniform(80_000, 600_000), 2)
            term_months = random.choice([48, 60, 72, 84])
            collateral = "Vehicle"
            interest_rate = round(random.uniform(0.10, 0.165), 4)
            ltv = round(random.uniform(0.70, 0.90), 4)
            collateral_value = round(orig_amount / ltv, 2)
        elif ltype == "Business Loan":
            orig_amount = round(min(income * 3, random.uniform(50_000, 10_000_000)), 2)
            term_months = random.choice([12, 24, 36, 48, 60])
            collateral = weighted_choice(["Property", "Cession", "None"], [0.40, 0.35, 0.25])
            interest_rate = round(random.uniform(0.11, 0.22), 4)
            ltv = None
            collateral_value = orig_amount * random.uniform(1.0, 2.0) if collateral != "None" else 0.0
        else:  # Revolving Credit
            orig_amount = round(income * random.uniform(0.05, 0.30), 2)
            term_months = random.choice([12, 24, 36])
            collateral = "None"
            interest_rate = round(random.uniform(0.15, 0.28), 4)
            ltv = None
            collateral_value = 0.0

        months_elapsed = random.randint(1, term_months)
        frac_remaining = max(0, (term_months - months_elapsed) / term_months)
        outstanding = round(orig_amount * frac_remaining * random.uniform(0.85, 1.0), 2)

        monthly_instalment = round(
            orig_amount * interest_rate / 12 / (1 - (1 + interest_rate / 12) ** (-term_months)), 2
        )

        orig_date = random_date(START_DATE, datetime.date(2024, 6, 30))
        maturity_date = orig_date + datetime.timedelta(days=30 * term_months)
        disbursement_date = orig_date + datetime.timedelta(days=random.randint(1, 5))

        dpd_map = {"Current": 0, "1-30 DPD": random.randint(1, 30),
                   "31-60 DPD": random.randint(31, 60),
                   "61-90 DPD": random.randint(61, 90), "NPL": random.randint(91, 365)}
        dpd = dpd_map[lstatus]
        arrears = round(monthly_instalment * (dpd // 30), 2) if dpd > 0 else 0.0

        last_pay_date = orig_date + datetime.timedelta(days=30 * (months_elapsed - 1))
        next_pay_date = last_pay_date + datetime.timedelta(days=30)
        last_pay_amount = monthly_instalment * random.uniform(0.95, 1.05)

        loan_number = f"PCB-LOAN-{orig_date.strftime('%Y%m')}-{random.randint(100000, 999999)}"

        rows.append({
            "loan_id": str(uuid.uuid4()),
            "loan_number": loan_number,
            "customer_id": cid,
            "account_id": aid,
            "loan_type": ltype,
            "origination_date": str(orig_date),
            "maturity_date": str(maturity_date),
            "term_months": term_months,
            "original_amount": orig_amount,
            "outstanding_balance": outstanding,
            "interest_rate": interest_rate,
            "monthly_instalment": monthly_instalment,
            "loan_status": lstatus,
            "days_past_due": dpd,
            "arrears_amount": arrears,
            "collateral_type": collateral,
            "collateral_value": collateral_value,
            "ltv_ratio": ltv if ltv else "",
            "purpose": random.choice(LOAN_PURPOSES[ltype]),
            "disbursement_date": str(disbursement_date),
            "next_payment_date": str(next_pay_date),
            "last_payment_date": str(last_pay_date),
            "last_payment_amount": round(last_pay_amount, 2),
            "provision_rate": PROVISION_RATES[lstatus],
            "origination_channel": orig_channels[i],
            "origination_officer_id": f"OFC{random.randint(1000, 9999)}",
        })

        if (i + 1) % 100_000 == 0:
            chunk_print("loans", i + 1, n)

    chunk_print("loans", n, n)
    df = pd.DataFrame(rows)
    out = OUTPUT_DIR / "loans.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}  ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 5. CARD TRANSACTIONS
# ---------------------------------------------------------------------------

def generate_card_transactions(accounts: pd.DataFrame):
    print(f"\n[5/7] Generating card_transactions.csv ({N_CARD_TXN:,} rows) ...")
    out = OUTPUT_DIR / "card_transactions.csv"
    header_written = False

    # Filter credit card accounts
    cc_accounts = accounts[accounts["account_type"] == "Credit Card"]
    if len(cc_accounts) == 0:
        cc_accounts = accounts.sample(min(10000, len(accounts)))

    cc_acc_ids = cc_accounts["account_id"].values
    cc_cust_ids = cc_accounts["customer_id"].values

    total_written = 0
    while total_written < N_CARD_TXN:
        chunk_size = min(CARD_CHUNK_SIZE, N_CARD_TXN - total_written)
        indices = np.random.randint(0, len(cc_acc_ids), size=chunk_size)

        tx_types = np.array(weighted_choice(CARD_TX_TYPES, CARD_TX_WEIGHTS, chunk_size))
        tx_statuses = np.array(weighted_choice(CARD_TX_STATUSES, CARD_TX_STATUS_WEIGHTS, chunk_size))

        rows = []
        for j in range(chunk_size):
            idx = indices[j]
            acc_id = cc_acc_ids[idx]
            cust_id = cc_cust_ids[idx]
            tx_type = tx_types[j]
            status = tx_statuses[j]

            if tx_type == "Cash Advance":
                amount = round(random.uniform(200, 5_000), 2)
            elif tx_type == "International Purchase":
                amount = round(random.uniform(100, 50_000), 2)
            elif tx_type in ("Refund", "Chargeback"):
                amount = round(random.uniform(50, 10_000), 2)
            else:
                amount = round(random.uniform(50, 15_000), 2)

            is_international = tx_type == "International Purchase"
            currency = random.choice(["USD", "EUR", "GBP", "ZAR"]) if is_international else "ZAR"
            exc_rate = 1.0
            if currency == "USD":
                exc_rate = round(random.uniform(17.5, 19.5), 4)
            elif currency == "EUR":
                exc_rate = round(random.uniform(19.0, 21.5), 4)
            elif currency == "GBP":
                exc_rate = round(random.uniform(22.0, 25.0), 4)

            tx_date = random_date(START_DATE, END_DATE)
            mcc_idx = random.randint(0, len(MCC_LIST) - 1)

            rows.append({
                "card_transaction_id": str(uuid.uuid4()),
                "transaction_date": str(tx_date),
                "posting_date": str(tx_date + datetime.timedelta(days=random.randint(0, 2))),
                "account_id": acc_id,
                "customer_id": cust_id,
                "card_brand": random.choice(CARD_BRANDS),
                "card_type": random.choice(CARD_TYPES),
                "card_last_four": f"{random.randint(1000, 9999)}",
                "transaction_type": tx_type,
                "amount": amount,
                "currency": currency,
                "exchange_rate": exc_rate,
                "amount_zar": round(amount * exc_rate, 2) if currency != "ZAR" else amount,
                "merchant_name": random.choice(SA_MERCHANTS),
                "merchant_category_code": MCC_LIST[mcc_idx],
                "merchant_category_name": MCC_NAMES[mcc_idx],
                "merchant_city": fake.city(),
                "merchant_country": "ZA" if not is_international else fake.country_code(),
                "is_international": is_international,
                "authorization_code": f"AUTH{random.randint(100000, 999999)}",
                "status": status,
                "is_contactless": tx_type == "Contactless Payment",
                "is_chip": random.random() > 0.3,
            })

        df_chunk = pd.DataFrame(rows)
        df_chunk.to_csv(out, index=False, header=not header_written, mode="a")
        header_written = True
        total_written += chunk_size
        chunk_print("card_transactions", total_written, N_CARD_TXN)

    print(f"  Saved: {out}  ({total_written:,} rows)")


# ---------------------------------------------------------------------------
# 6. PAYMENTS
# ---------------------------------------------------------------------------

def generate_payments(accounts: pd.DataFrame) -> pd.DataFrame:
    print("\n[6/7] Generating payments.csv ...")
    n = N_PAYMENTS

    acc_ids = accounts["account_id"].values
    acc_numbers = accounts["account_number"].values
    cust_ids = accounts["customer_id"].values

    payment_types = np.array(weighted_choice(PAYMENT_TYPES, PAYMENT_TYPE_WEIGHTS, n))
    pay_statuses = np.array(weighted_choice(PAYMENT_STATUSES, PAYMENT_STATUS_WEIGHTS, n))
    currencies = np.array(weighted_choice(CURRENCIES + ["GBP"], [0.90, 0.05, 0.03, 0.02], n))

    rows = []
    for i in range(n):
        deb_idx = random.randint(0, len(acc_ids) - 1)
        cred_idx = random.randint(0, len(acc_ids) - 1)
        debtor_acc_id = acc_ids[deb_idx]

        ptype = payment_types[i]
        is_external = ptype in ("SWIFT", "EFT") and random.random() > 0.4
        if is_external:
            cred_acc_id = "EXTERNAL"
            cred_bank = random.choice(SA_BANKS)
            cred_acc_num = f"{random.randint(1000000000, 9999999999)}"
        else:
            cred_acc_id = acc_ids[cred_idx]
            cred_bank = "Prime Capital Bank"
            cred_acc_num = acc_numbers[cred_idx]

        currency = currencies[i]
        exc_rate = 1.0
        if currency == "USD":
            exc_rate = round(random.uniform(17.5, 19.5), 4)
        elif currency == "EUR":
            exc_rate = round(random.uniform(19.0, 21.5), 4)
        elif currency == "GBP":
            exc_rate = round(random.uniform(22.0, 25.0), 4)

        if ptype == "RTGS":
            amount = round(random.uniform(100_000, 10_000_000), 2)
            lag = 0
        elif ptype == "SWIFT":
            amount = round(random.uniform(5_000, 5_000_000), 2)
            lag = random.randint(1, 3)
        elif ptype == "Payroll":
            amount = round(random.uniform(5_000, 100_000), 2)
            lag = 0
        elif ptype == "Internal":
            amount = round(random.uniform(100, 500_000), 2)
            lag = 0
        else:
            amount = round(random.uniform(50, 200_000), 2)
            lag = random.randint(0, 2)

        pay_date = random_date(START_DATE, END_DATE)
        settle_date = pay_date + datetime.timedelta(days=lag)

        fee_type = random.choice(["Fixed", "Percentage", "None"])
        fee_amount = 0.0
        if fee_type == "Fixed":
            fee_amount = random.choice([8.50, 15.00, 25.00, 50.00, 100.00])
        elif fee_type == "Percentage":
            fee_amount = round(amount * random.uniform(0.001, 0.005), 2)

        rows.append({
            "payment_id": str(uuid.uuid4()),
            "payment_date": str(pay_date),
            "value_date": str(settle_date),
            "debtor_account_id": debtor_acc_id,
            "creditor_account_id": cred_acc_id,
            "payment_type": ptype,
            "amount": amount,
            "currency": currency,
            "exchange_rate": exc_rate,
            "creditor_bank": cred_bank,
            "creditor_account_number": cred_acc_num,
            "creditor_name": fake.name() if is_external else f"PCB Customer",
            "payment_reference": f"PAY{random.randint(100000000, 999999999)}",
            "payment_status": pay_statuses[i],
            "settlement_date": str(settle_date),
            "settlement_lag_days": lag,
            "fee_amount": fee_amount,
            "fee_type": fee_type,
        })

        if (i + 1) % 100_000 == 0:
            chunk_print("payments", i + 1, n)

    chunk_print("payments", n, n)
    df = pd.DataFrame(rows)
    out = OUTPUT_DIR / "payments.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}  ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 7. FRAUD ALERTS
# ---------------------------------------------------------------------------

def generate_fraud_alerts(customers: pd.DataFrame, accounts: pd.DataFrame) -> pd.DataFrame:
    print("\n[7/7] Generating fraud_alerts.csv ...")
    n = N_FRAUD

    cust_ids = customers["customer_id"].values
    acc_ids = accounts["account_id"].values

    # Higher fraud for digital channels (simulate with higher weights on those records)
    fraud_types = np.array(weighted_choice(FRAUD_TYPES, FRAUD_TYPE_WEIGHTS, n))
    alert_sources = np.array(weighted_choice(FRAUD_SOURCES, FRAUD_SOURCE_WEIGHTS, n))
    priorities = np.array(weighted_choice(FRAUD_PRIORITIES, FRAUD_PRIORITY_WEIGHTS, n))
    statuses = np.array(weighted_choice(FRAUD_STATUSES, FRAUD_STATUS_WEIGHTS, n))

    rows = []
    for i in range(n):
        cid = cust_ids[random.randint(0, len(cust_ids) - 1)]
        aid = acc_ids[random.randint(0, len(acc_ids) - 1)]
        ftype = fraud_types[i]
        status = statuses[i]
        priority = priorities[i]

        risk_score = random.randint(0, 100)
        if priority == "Critical":
            risk_score = random.randint(85, 100)
        elif priority == "High":
            risk_score = random.randint(65, 85)
        elif priority == "Medium":
            risk_score = random.randint(40, 65)
        else:
            risk_score = random.randint(0, 40)

        loss_amount = 0.0
        if status == "Confirmed Fraud":
            loss_amount = round(random.uniform(100, 200_000), 2)
        recovery_amount = round(loss_amount * random.uniform(0, 0.6), 2) if loss_amount > 0 else 0.0
        net_loss = round(loss_amount - recovery_amount, 2)

        alert_date = random_date(START_DATE, END_DATE)
        days_to_resolve = 0
        if status in ("Confirmed Fraud", "False Positive", "Closed"):
            days_to_resolve = random.randint(1, 90)

        rows.append({
            "alert_id": str(uuid.uuid4()),
            "alert_date": str(alert_date),
            "customer_id": cid,
            "account_id": aid,
            "transaction_id": str(uuid.uuid4()),   # reference to a tx (not joined for perf)
            "fraud_type": ftype,
            "alert_source": alert_sources[i],
            "risk_score": risk_score,
            "alert_priority": priority,
            "loss_amount": loss_amount,
            "recovery_amount": recovery_amount,
            "net_loss": net_loss,
            "alert_status": status,
            "days_to_resolve": days_to_resolve,
            "analyst_id": f"ANL{random.randint(100, 999)}",
        })

        if (i + 1) % 10_000 == 0:
            chunk_print("fraud_alerts", i + 1, n)

    chunk_print("fraud_alerts", n, n)
    df = pd.DataFrame(rows)
    out = OUTPUT_DIR / "fraud_alerts.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}  ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# Fintech partner & settlement generation
# ---------------------------------------------------------------------------

def generate_fintech_partners() -> pd.DataFrame:
    """Write DIM_FINTECH seed data — 5 SA payment partners."""
    print("\n[6/8] Generating fintech_partners.csv ...")
    df = pd.DataFrame(SA_FINTECH_PARTNERS)
    out = OUTPUT_DIR / "fintech_partners.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}  ({len(df):,} rows)")
    return df


def generate_fintech_settlements() -> pd.DataFrame:
    """Generate FACT_FINTECH_SETTLEMENT — monthly grain, 12 months × 5 partners (2024)."""
    print("\n[7/8] Generating fintech_settlements.csv ...")

    rows = []
    year = 2024
    rng = np.random.default_rng(RANDOM_SEED + 99)

    # Base monthly transaction counts per partner (before seasonality)
    BASE_TX = {
        "FT001": 920_000,   # Yoco — highest volume
        "FT002": 330_000,   # SnapScan
        "FT003": 152_000,   # PayFast
        "FT004":  62_000,   # Peach Payments
        "FT005":  41_000,   # PayGate
        "FT006": 258_000,   # Ozow
        "FT007": 293_000,   # Zapper
        "FT008": 530_000,   # iKhokha
        "FT009":   8_500,   # Stitch — smallest merchant base, highest ticket
        "FT010": 1_900_000, # Kazang — highest transaction count, lowest basket
        "FT011": 190_000,   # Adumo
        "FT012":  95_000,   # Mukuru
    }
    # Chargeback rate per partner (annualised %)
    CB_RATE = {"FT001": 0.0013, "FT002": 0.0015, "FT003": 0.0020, "FT004": 0.0020, "FT005": 0.0020,
               "FT006": 0.0008, "FT007": 0.0015, "FT008": 0.0014, "FT009": 0.0005,
               "FT010": 0.0003, "FT011": 0.0018, "FT012": 0.0022}
    # Base active merchant count at Jan 2024 and monthly growth
    MERCH_BASE   = {"FT001": 247_300, "FT002": 79_240, "FT003": 44_120, "FT004": 11_680, "FT005": 7_840,
                     "FT006": 27_600, "FT007": 41_650, "FT008": 128_400, "FT009": 3_150,
                     "FT010": 64_300, "FT011": 37_500, "FT012": 4_420}
    MERCH_GROWTH = {"FT001": 1_850,   "FT002": 550,    "FT003": 390,    "FT004": 175,    "FT005": 100,
                     "FT006": 320,    "FT007": 260,    "FT008": 1_100,  "FT009": 45,
                     "FT010": 480,    "FT011": 380,    "FT012": 62}

    for partner in SA_FINTECH_PARTNERS:
        pid   = partner["fintech_id"]
        name  = partner["fintech_name"]
        fee   = partner["fee_rate_pct"] / 100
        min_f = partner["min_fee_zar"]
        avg_b = partner["avg_basket_zar"]
        sdays = partner["settlement_days"]

        for month_idx in range(12):
            month_num   = month_idx + 1
            season      = FINTECH_SEASONAL[month_idx]
            month_end   = datetime.date(year, month_num,
                            [31,29,31,30,31,30,31,31,30,31,30,31][month_idx])
            settle_id   = f"FS-{year}-{month_num:02d}-{pid}"

            # Transaction count with seasonality + small noise
            tx_base  = int(BASE_TX[pid] * season)
            noise    = rng.integers(-int(tx_base * 0.03), int(tx_base * 0.03))
            total_tx = max(1, tx_base + noise)

            # Volume
            gross    = round(total_tx * avg_b, 2)
            merch_f  = round(max(total_tx * min_f, gross * fee), 2)
            # Interchange: ~2% of gross for POS/QR, ~1.5% for online
            ix_rate  = 0.020 if sdays == 1 else 0.015
            interch  = round(gross * ix_rate, 2)
            net_set  = round(gross - merch_f - interch, 2)

            # Chargebacks
            cb_count = int(total_tx * CB_RATE[pid])
            cb_ratio = round(CB_RATE[pid] * 100, 4)

            # Failed transactions (1% baseline)
            fail_ct  = int(total_tx * 0.01)
            fail_r   = 1.00

            # Merchant growth
            new_m    = int(MERCH_GROWTH[pid] * season * (1 + rng.uniform(-0.1, 0.1)))
            active_m = MERCH_BASE[pid] + month_idx * MERCH_GROWTH[pid]

            rows.append({
                "settlement_id":          settle_id,
                "settlement_month":       month_end.isoformat(),
                "fintech_id":             pid,
                "fintech_name":           name,
                "platform_type":          partner["platform_type"],
                "total_transactions":     total_tx,
                "gross_volume_zar":       gross,
                "merchant_fees_zar":      merch_f,
                "interchange_fees_zar":   interch,
                "net_settlement_zar":     net_set,
                "avg_transaction_zar":    round(avg_b + rng.uniform(-2, 2), 2),
                "chargebacks_count":      cb_count,
                "chargeback_ratio_pct":   cb_ratio,
                "new_merchants_onboarded": new_m,
                "active_merchants":       int(active_m),
                "failed_transactions":    fail_ct,
                "failure_rate_pct":       fail_r,
            })

    df = pd.DataFrame(rows)
    out = OUTPUT_DIR / "fintech_settlements.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}  ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary():
    print("\n" + "=" * 70)
    print(f"{'PRIME CAPITAL BANK — DATA GENERATION SUMMARY':^70}")
    print("=" * 70)
    print(f"{'File':<30} {'Rows':>12} {'Size (MB)':>12}")
    print("-" * 70)

    files = [
        "customers.csv", "accounts.csv", "transactions.csv",
        "loans.csv", "card_transactions.csv", "payments.csv", "fraud_alerts.csv",
        "fintech_partners.csv", "fintech_settlements.csv",
    ]
    total_size = 0.0
    for fname in files:
        fpath = OUTPUT_DIR / fname
        if fpath.exists():
            size_mb = fpath.stat().st_size / (1024 * 1024)
            total_size += size_mb
            # Count rows (fast wc -l equivalent)
            with open(fpath, "rb") as f:
                row_count = sum(1 for _ in f) - 1   # subtract header
            print(f"  {fname:<28} {row_count:>12,} {size_mb:>11.1f}")
        else:
            print(f"  {fname:<28} {'NOT FOUND':>12} {'':>12}")

    print("-" * 70)
    print(f"  {'TOTAL':<28} {'':<12} {total_size:>11.1f}")
    print("=" * 70)
    print(f"\n  Output directory: {OUTPUT_DIR}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import time
    t0 = time.time()

    print("=" * 70)
    print("  Prime Capital Bank — Synthetic Data Generator")
    print(f"  Target: {N_CUSTOMERS:,} customers | {N_ACCOUNTS:,} accounts | {N_TRANSACTIONS:,} transactions")
    print("=" * 70)

    customers_df = generate_customers()
    accounts_df, account_ids = generate_accounts(customers_df)
    generate_transactions(accounts_df, account_ids)
    generate_loans(customers_df, accounts_df)
    generate_card_transactions(accounts_df)
    generate_payments(accounts_df)
    generate_fraud_alerts(customers_df, accounts_df)
    generate_fintech_partners()
    generate_fintech_settlements()

    elapsed = time.time() - t0
    print(f"\n  Total generation time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print_summary()
