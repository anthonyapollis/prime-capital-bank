# -*- coding: utf-8 -*-
"""
Prime Capital Bank — Asset Management / Wealth division dataset.

Adds the investment side of the bank to the existing retail/lending/fintech
platform: discretionary portfolios, security master, holdings, daily prices
and trade blotter, plus Regulation 28 compliance testing.

South-African context throughout: JSE-listed equities, SA government bonds,
local ETFs and offshore feeders; benchmarks ALSI / SWIX / ALBI / STeFI;
Reg 28 limits (equity 75%, offshore 45%, property 25%, hedge 10%) applied to
retirement-fund mandates.

Writes to data/ and emits docs/ml_outputs/asset_management_summary.json for
the dashboard page.
"""
import csv, json, os, random
from datetime import date, timedelta

RNG = random.Random(20260805)
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(ROOT, "data")
OUTJ = os.path.join(ROOT, "docs", "ml_outputs")
os.makedirs(DATA, exist_ok=True); os.makedirs(OUTJ, exist_ok=True)

TODAY = date(2026, 7, 31)
DAYS = 260                                   # ~1 trading year

def w(name, rows, header):
    p = os.path.join(DATA, name + ".csv")
    with open(p, "w", newline="", encoding="utf-8") as f:
        c = csv.writer(f); c.writerow(header); c.writerows(rows)
    print(f"  {name:28s} {len(rows):>7,} rows")

# ---------------------------------------------------------------- securities
# ticker, name, asset_class, sector, listing, base_price, annual_drift, annual_vol
SEC = [
 ("NPN","Naspers Ltd","Equity","Technology","JSE",3450.00,0.14,0.30),
 ("PRX","Prosus NV","Equity","Technology","JSE",1180.00,0.13,0.31),
 ("BHG","BHP Group","Equity","Resources","JSE",5200.00,0.06,0.26),
 ("AGL","Anglo American","Equity","Resources","JSE",5850.00,0.05,0.29),
 ("FSR","FirstRand Ltd","Equity","Financials","JSE",7150.00,0.10,0.22),
 ("SBK","Standard Bank Group","Equity","Financials","JSE",21500.00,0.09,0.21),
 ("CPI","Capitec Bank Holdings","Equity","Financials","JSE",265000.00,0.16,0.25),
 ("ABG","Absa Group","Equity","Financials","JSE",17200.00,0.08,0.23),
 ("NED","Nedbank Group","Equity","Financials","JSE",24800.00,0.08,0.22),
 ("INL","Investec Ltd","Equity","Financials","JSE",13100.00,0.09,0.24),
 ("SOL","Sasol Ltd","Equity","Energy","JSE",11400.00,-0.02,0.38),
 ("MTN","MTN Group","Equity","Telecoms","JSE",9800.00,0.07,0.32),
 ("VOD","Vodacom Group","Equity","Telecoms","JSE",10600.00,0.06,0.21),
 ("SHP","Shoprite Holdings","Equity","Consumer","JSE",27400.00,0.11,0.20),
 ("WHL","Woolworths Holdings","Equity","Consumer","JSE",6250.00,0.05,0.24),
 ("MRP","Mr Price Group","Equity","Consumer","JSE",21300.00,0.07,0.26),
 ("CLS","Clicks Group","Equity","Consumer","JSE",31200.00,0.10,0.19),
 ("TBS","Tiger Brands","Equity","Consumer","JSE",18900.00,0.04,0.22),
 ("CFR","Richemont","Equity","Luxury","JSE",26500.00,0.12,0.27),
 ("GFI","Gold Fields","Equity","Resources","JSE",29800.00,0.18,0.36),
 ("ANG","AngloGold Ashanti","Equity","Resources","JSE",42500.00,0.19,0.37),
 ("SSW","Sibanye Stillwater","Equity","Resources","JSE",2180.00,0.03,0.45),
 ("EXX","Exxaro Resources","Equity","Resources","JSE",16400.00,0.04,0.30),
 ("BVT","Bidvest Group","Equity","Industrials","JSE",27600.00,0.08,0.21),
 ("REM","Remgro Ltd","Equity","Industrials","JSE",14300.00,0.06,0.20),
 ("DSY","Discovery Ltd","Equity","Insurance","JSE",17800.00,0.09,0.25),
 ("SLM","Sanlam Ltd","Equity","Insurance","JSE",8450.00,0.10,0.20),
 ("OMU","Old Mutual Ltd","Equity","Insurance","JSE",1290.00,0.07,0.23),
 ("GRT","Growthpoint Properties","Property","REIT","JSE",1340.00,0.06,0.19),
 ("RDF","Redefine Properties","Property","REIT","JSE",470.00,0.05,0.21),
 ("APN","Aspen Pharmacare","Equity","Healthcare","JSE",19700.00,0.06,0.28),
 ("R186","RSA 10.5% 2026 Bond","Bond","Government","BESA",10850.00,0.085,0.05),
 ("R2030","RSA 8.0% 2030 Bond","Bond","Government","BESA",9420.00,0.092,0.07),
 ("R2032","RSA 8.25% 2032 Bond","Bond","Government","BESA",9180.00,0.098,0.08),
 ("R2035","RSA 8.875% 2035 Bond","Bond","Government","BESA",8760.00,0.104,0.09),
 ("R2040","RSA 9.0% 2040 Bond","Bond","Government","BESA",8310.00,0.108,0.11),
 ("STX40","Satrix Top 40 ETF","ETF","Index","JSE",8950.00,0.11,0.19),
 ("STXWDM","Satrix MSCI World ETF","Offshore","Global Equity","JSE",9840.00,0.13,0.17),
 ("STX500","Satrix S&P 500 ETF","Offshore","US Equity","JSE",12650.00,0.15,0.18),
 ("ASHGEQ","Ashburton Global Equity","Offshore","Global Equity","JSE",7420.00,0.12,0.18),
 ("STXCLR","Satrix SA Bond ETF","Bond","Corporate","JSE",5680.00,0.089,0.06),
 ("MMKT","PCB Money Market Fund","Cash","Money Market","OTC",100.00,0.082,0.004),
]
w("securities", [(t, n, ac, s, l, round(p,2)) for t,n,ac,s,l,p,_,_ in SEC],
  ["ticker","security_name","asset_class","sector","listing","base_price_cents"])

# ---------------------------------------------------------------- prices
# geometric brownian motion per security, shared market factor for correlation
prices, series = [], {}
mkt = [0.0]
for _ in range(DAYS):
    mkt.append(mkt[-1] + RNG.gauss(0.00035, 0.0092))     # ALSI-ish market factor
for t, n, ac, s, l, p0, drift, vol in SEC:
    beta = {"Equity":1.0,"Property":0.8,"ETF":1.0,"Offshore":0.55,"Bond":0.15,"Cash":0.0}[ac]
    px, out = p0, []
    d = TODAY - timedelta(days=DAYS)
    for i in range(DAYS):
        d += timedelta(days=1)
        if d.weekday() >= 5:                              # trading days only
            continue
        shock = beta*(mkt[i+1]-mkt[i]) + RNG.gauss(drift/252, vol/252**0.5)
        px = max(px*(1+shock), p0*0.25)
        out.append((d, round(px, 2)))
        prices.append((t, d.isoformat(), round(px, 2)))
    series[t] = out
w("security_prices", prices, ["ticker","price_date","close_price_cents"])

# ---------------------------------------------------------------- portfolios
MANDATES = [
 # name, equity, property, bond, offshore, cash, reg28, fee_bps
 ("Balanced Growth",        0.42,0.06,0.20,0.26,0.06, True, 95),
 ("Conservative Income",    0.14,0.04,0.52,0.10,0.20, True, 70),
 ("Aggressive Equity",      0.66,0.05,0.02,0.24,0.03, True, 120),
 ("Offshore Feeder",        0.05,0.00,0.03,0.90,0.02, False,105),
 ("Absolute Return",        0.30,0.05,0.30,0.20,0.15, True, 110),
 ("Money Market",           0.00,0.00,0.10,0.00,0.90, False, 35),
 ("Property Focus",         0.18,0.52,0.10,0.12,0.08, False,100),
]
RISK = {"Conservative Income":"Conservative","Money Market":"Conservative",
        "Balanced Growth":"Moderate","Absolute Return":"Moderate",
        "Property Focus":"Moderate","Aggressive Equity":"Aggressive",
        "Offshore Feeder":"Aggressive"}
BENCH = {"Balanced Growth":"ALSI","Conservative Income":"ALBI","Aggressive Equity":"SWIX",
         "Offshore Feeder":"MSCI World","Absolute Return":"STeFI + 4%",
         "Money Market":"STeFI","Property Focus":"SAPY"}
TYPES = [("Retirement Annuity",.26),("Pension Fund",.14),("Living Annuity",.16),
         ("Discretionary",.28),("Tax-Free Savings",.10),("Endowment",.06)]
PROV = [("Gauteng",.38),("Western Cape",.24),("KwaZulu-Natal",.14),("Eastern Cape",.07),
        ("Free State",.05),("Mpumalanga",.05),("Limpopo",.03),("North West",.03),("Northern Cape",.01)]

def pick(pairs):
    r, a = RNG.random(), 0.0
    for k, p in pairs:
        a += p
        if r <= a: return k
    return pairs[-1][0]

ADV = [f"ADV{i:03d}" for i in range(1, 29)]
portfolios, holdings, trades = [], [], []
by_class = {}
for t,n,ac,s,l,p0,_,_ in SEC: by_class.setdefault(ac, []).append(t)

for i in range(1, 3401):
    pid = f"PF{i:05d}"
    mandate, eq, pr, bd, off, csh, reg28, fee = RNG.choice(MANDATES)
    acct = pick(TYPES)
    # retirement vehicles are always Reg 28 regulated
    reg28 = reg28 or acct in ("Retirement Annuity", "Pension Fund", "Pension Preservation")
    inception = TODAY - timedelta(days=RNG.randint(200, 4200))
    # market value: lognormal, so a few institutional mandates carry most of the AUM
    # (median ~R9m affluent private client, tail into R500m+ institutional)
    mv = round(RNG.lognormvariate(16.0, 1.28), 2)
    portfolios.append((pid, f"CUST{RNG.randint(1,500000):06d}", mandate, RISK[mandate],
                       acct, BENCH[mandate], "ZAR", inception.isoformat(),
                       RNG.choice(ADV), pick(PROV), int(reg28), fee, round(mv, 2)))

    # build holdings to the mandate's target weights, with drift
    targets = [("Equity",eq),("Property",pr),("Bond",bd),("Offshore",off),("Cash",csh)]
    for ac, tw in targets:
        if tw <= 0: continue
        pool = by_class.get(ac) or by_class["Equity"]
        if ac == "Equity": pool = by_class["Equity"] + by_class["ETF"]
        k = min(len(pool), RNG.randint(2, 7) if ac == "Equity" else RNG.randint(1, 3))
        picks = RNG.sample(pool, k)
        drifted = tw * RNG.uniform(0.86, 1.16)
        for tk in picks:
            wgt = drifted / k * RNG.uniform(0.75, 1.25)
            val = mv * wgt
            last = series[tk][-1][1]
            units = round(val / (last/100.0), 4) if last else 0
            cost = round(last * RNG.uniform(0.72, 1.06), 2)
            holdings.append((pid, tk, units, cost, last, round(val, 2),
                             TODAY.isoformat()))

    # trade blotter
    for _ in range(RNG.randint(1, 9)):
        tk = RNG.choice([h[1] for h in holdings if h[0] == pid] or ["STX40"])
        d = inception + timedelta(days=RNG.randint(0, max(1,(TODAY-inception).days)))
        if d > TODAY: d = TODAY
        side = pick([("BUY",.62),("SELL",.38)])
        px = next((p for dd, p in series[tk] if dd >= d), series[tk][-1][1])
        u = round(RNG.uniform(50, 4000), 4)
        gross = u * px/100.0
        trades.append((f"TRD{len(trades)+1:07d}", pid, tk, d.isoformat(), side,
                       u, px, round(gross,2), round(gross*0.0035,2),
                       pick([("Rebalance",.34),("Client instruction",.28),
                             ("Model change",.22),("Cash flow",.16)])))

w("portfolios", portfolios, ["portfolio_id","customer_id","mandate","risk_profile",
  "account_type","benchmark","currency","inception_date","adviser_id","province",
  "reg28_regulated","fee_bps","market_value_zar"])
w("portfolio_holdings", holdings, ["portfolio_id","ticker","units","avg_cost_cents",
  "last_price_cents","market_value_zar","valuation_date"])
w("portfolio_trades", trades, ["trade_id","portfolio_id","ticker","trade_date","side",
  "units","price_cents","gross_value_zar","brokerage_zar","reason"])

# ---------------------------------------------------------------- summary
cls = {t: ac for t,_,ac,_,_,_,_,_ in SEC}
sec_name = {t: n for t,n,_,_,_,_,_,_ in SEC}
aum = sum(p[12] for p in portfolios)
alloc, by_mandate, by_prov, by_sec = {}, {}, {}, {}
for pid, tk, u, c, lp, val, _ in holdings:
    alloc[cls[tk]] = alloc.get(cls[tk], 0) + val
    by_sec[tk] = by_sec.get(tk, 0) + val
for p in portfolios:
    by_mandate[p[2]] = by_mandate.get(p[2], 0) + p[12]
    by_prov[p[9]]   = by_prov.get(p[9], 0) + p[12]

hold_val = sum(alloc.values())

# Reg 28 breach test (equity 75 / offshore 45 / property 25)
pf_alloc = {}
for pid, tk, u, c, lp, val, _ in holdings:
    pf_alloc.setdefault(pid, {}).setdefault(cls[tk], 0.0)
    pf_alloc[pid][cls[tk]] += val
LIMITS = {"Equity": 0.75, "Offshore": 0.45, "Property": 0.25}
breaches = []
for p in portfolios:
    if not p[10]:                                   # not Reg 28 regulated
        continue
    a = pf_alloc.get(p[0], {}); tot = sum(a.values()) or 1
    for k, lim in LIMITS.items():
        share = a.get(k, 0)/tot
        if share > lim:
            breaches.append({"portfolio_id": p[0], "mandate": p[2], "limit": k,
                             "actual_pct": round(share*100, 1),
                             "limit_pct": round(lim*100, 1),
                             "excess_zar": round((share-lim)*tot, 2)})
breaches.sort(key=lambda b: -b["excess_zar"])

# 12m performance per mandate (value-weighted from price series)
def ret(tk):
    s = series[tk]
    return (s[-1][1]/s[0][1] - 1) if s and s[0][1] else 0.0
mandate_perf = {}
for pid, tk, u, c, lp, val, _ in holdings:
    m = next(p[2] for p in portfolios if p[0] == pid)
    d = mandate_perf.setdefault(m, [0.0, 0.0])
    d[0] += ret(tk)*val; d[1] += val
BM = {"ALSI":0.124,"SWIX":0.111,"ALBI":0.095,"MSCI World":0.138,
      "STeFI":0.081,"STeFI + 4%":0.121,"SAPY":0.088}

summary = {
 "generated": TODAY.isoformat(),
 "aum_zar": round(aum, 2),
 "portfolios": len(portfolios),
 "holdings": len(holdings),
 "trades": len(trades),
 "securities": len(SEC),
 "advisers": len(ADV),
 "avg_portfolio_zar": round(aum/len(portfolios), 2),
 "avg_fee_bps": round(sum(p[11] for p in portfolios)/len(portfolios), 1),
 "annual_fee_zar": round(sum(p[12]*p[11]/10000 for p in portfolios), 2),
 "reg28_regulated": sum(1 for p in portfolios if p[10]),
 "reg28_breaches": len(breaches),
 "reg28_breach_value_zar": round(sum(b["excess_zar"] for b in breaches), 2),
 "top_breaches": breaches[:8],
 "allocation": [{"asset_class": k, "value_zar": round(v,2),
                 "pct": round(v/hold_val*100,1)}
                for k,v in sorted(alloc.items(), key=lambda kv:-kv[1])],
 "by_mandate": [{"mandate": k, "aum_zar": round(v,2), "pct": round(v/aum*100,1),
                 "return_12m_pct": round(mandate_perf[k][0]/mandate_perf[k][1]*100,2)
                     if k in mandate_perf and mandate_perf[k][1] else 0,
                 "benchmark": BENCH[k],
                 "benchmark_pct": round(BM[BENCH[k]]*100,2),
                 "alpha_pct": round((mandate_perf[k][0]/mandate_perf[k][1] - BM[BENCH[k]])*100,2)
                     if k in mandate_perf and mandate_perf[k][1] else 0}
                for k,v in sorted(by_mandate.items(), key=lambda kv:-kv[1])],
 "by_province": [{"province": k, "aum_zar": round(v,2), "pct": round(v/aum*100,1)}
                 for k,v in sorted(by_prov.items(), key=lambda kv:-kv[1])],
 "top_holdings": [{"ticker": k, "name": sec_name[k], "asset_class": cls[k],
                   "value_zar": round(v,2), "pct": round(v/hold_val*100,2),
                   "return_12m_pct": round(ret(k)*100,1)}
                  for k,v in sorted(by_sec.items(), key=lambda kv:-kv[1])[:12]],
 "market_series": [{"date": d.isoformat(), "index": round(p,2)}
                   for d,p in series["STX40"][::5]],
}
with open(os.path.join(OUTJ, "asset_management_summary.json"), "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=1)

print(f"\nAUM              R{aum/1e9:.2f}bn across {len(portfolios)} portfolios")
print(f"Annual fee income R{summary['annual_fee_zar']/1e6:.1f}m at {summary['avg_fee_bps']}bps avg")
print(f"Reg 28 breaches   {len(breaches)} of {summary['reg28_regulated']} regulated "
      f"(R{summary['reg28_breach_value_zar']/1e6:.1f}m excess)")
print("Summary ->", os.path.join(OUTJ, "asset_management_summary.json"))
