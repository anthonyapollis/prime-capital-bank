# -*- coding: utf-8 -*-
"""Render docs/fintech_analytics.html from fintech_partners.csv +
fintech_settlements.csv (12 partners, 144 monthly settlement rows).

Self-contained: hand-rolled SVG charts, no CDN, works offline. Uses the
Prime Capital Bank navy/gold/cyan design system shared with
asset_management.html and settlement_reconciliation.html - the standing
PCB palette, kept identical across every page in this repo.
"""
import csv, os, html, json
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
OUT  = os.path.join(ROOT, "docs", "fintech_analytics.html")

with open(os.path.join(DATA, "fintech_partners.csv"), encoding="utf-8") as f:
    partners = list(csv.DictReader(f))
with open(os.path.join(DATA, "fintech_settlements.csv"), encoding="utf-8") as f:
    settlements = list(csv.DictReader(f))

esc = lambda t: html.escape(str(t))
def zar(v, dp=1):
    v = float(v)
    if abs(v) >= 1e9: return f"R{v/1e9:.{dp}f}bn"
    if abs(v) >= 1e6: return f"R{v/1e6:.{dp}f}m"
    if abs(v) >= 1e3: return f"R{v/1e3:.0f}k"
    return f"R{v:.0f}"

PALETTE = ["#C9A84C","#00B4CC","#16a34a","#7C6BD6","#E8845C","#4C8FD9","#D96BA0",
           "#F59E0B","#0EA5A0","#DC2626","#8B5CF6","#65A30D"]
ICON = {"FT001":"🟥","FT002":"📱","FT003":"🛒","FT004":"🔗","FT005":"🏦","FT006":"⚡",
        "FT007":"🍽️","FT008":"💳","FT009":"🔌","FT010":"🏪","FT011":"🖥️","FT012":"🌍"}

# ---------------------------------------------------------------- roll-ups
by_ft = defaultdict(list)
for r in settlements:
    by_ft[r["fintech_id"]].append(r)

total_gross = sum(float(r["gross_volume_zar"]) for r in settlements)
total_fees  = sum(float(r["merchant_fees_zar"]) for r in settlements)
total_net   = sum(float(r["net_settlement_zar"]) for r in settlements)
total_tx    = sum(int(r["total_transactions"]) for r in settlements)
total_cb    = sum(int(r["chargebacks_count"]) for r in settlements)

rows = []
for p in partners:
    fid = p["fintech_id"]
    recs = by_ft[fid]
    gross = sum(float(r["gross_volume_zar"]) for r in recs)
    fees  = sum(float(r["merchant_fees_zar"]) for r in recs)
    tx    = sum(int(r["total_transactions"]) for r in recs)
    cb    = sum(int(r["chargebacks_count"]) for r in recs)
    fail  = sum(int(r["failed_transactions"]) for r in recs)
    m0    = int(recs[0]["active_merchants"]) if recs else 0
    m11   = int(recs[-1]["active_merchants"]) if recs else 0
    growth = (m11/m0 - 1) * 100 if m0 else 0
    rows.append({
        "fintech_id": fid, "name": p["fintech_name"], "platform": p["platform_type"],
        "segment": p["target_segment"], "city": p["hq_city"], "province": p["hq_province"],
        "founded": p["founded_year"], "parent": p["parent_company"],
        "fee_pct": float(p["fee_rate_pct"]), "days": p["settlement_days"],
        "gross_zar": gross, "fees_zar": fees, "tx": tx,
        "cb_ratio": (cb/tx*100) if tx else 0, "fail_ratio": (fail/tx*100) if tx else 0,
        "merchants_start": m0, "merchants_end": m11, "merchant_growth_pct": growth,
        "notes": p["notes"], "monthly": [(r["settlement_month"][:7], float(r["gross_volume_zar"])) for r in recs],
    })
rows.sort(key=lambda r: -r["gross_zar"])

def hbar(items, label_k, val_k, fmt, color="#C9A84C", width=560, rowh=30, lw=170):
    if not items: return ""
    mx = max(float(r[val_k]) for r in items) or 1
    bw = width-lw-84; h = len(items)*rowh+6
    o = [f'<svg viewBox="0 0 {width} {h}" class="chart">']
    for i, r in enumerate(items):
        y = i*rowh+4; v = float(r[val_k]); bwid = max(2, bw*v/mx)
        o.append(f'<text x="0" y="{y+15}" class="bl">{esc(r[label_k])[:22]}</text>')
        o.append(f'<rect x="{lw}" y="{y+3}" width="{bwid:.1f}" height="15" rx="3" fill="{color}"/>')
        o.append(f'<text x="{lw+bwid+8:.1f}" y="{y+15}" class="bv">{fmt(v)}</text>')
    return "".join(o)+"</svg>"

def donut(items, label_k, val_k, size=210):
    import math
    tot = sum(float(r[val_k]) for r in items) or 1
    cx = cy = size/2; ro, ri = size/2-4, size/2-46
    a = -math.pi/2; out = [f'<svg viewBox="0 0 {size} {size}" class="donut">']
    for i, r in enumerate(items):
        f = float(r[val_k])/tot; a2 = a + f*2*math.pi; lg = 1 if f > .5 else 0
        x1,y1 = cx+ro*math.cos(a), cy+ro*math.sin(a); x2,y2 = cx+ro*math.cos(a2), cy+ro*math.sin(a2)
        x3,y3 = cx+ri*math.cos(a2), cy+ri*math.sin(a2); x4,y4 = cx+ri*math.cos(a), cy+ri*math.sin(a)
        out.append(f'<path d="M{x1:.1f},{y1:.1f} A{ro},{ro} 0 {lg},1 {x2:.1f},{y2:.1f} '
                   f'L{x3:.1f},{y3:.1f} A{ri},{ri} 0 {lg},0 {x4:.1f},{y4:.1f} Z" '
                   f'fill="{PALETTE[i%len(PALETTE)]}"/>')
        a = a2
    out.append(f'<text x="{cx}" y="{cy-4}" class="dn" text-anchor="middle">{zar(tot)}</text>')
    out.append(f'<text x="{cx}" y="{cy+15}" class="dl" text-anchor="middle">gross volume</text></svg>')
    return "".join(out)

def multi_line(rows_subset, width=1180, h=220):
    months = sorted({m for r in rows_subset for m,_ in r["monthly"]})
    vals_all = [v for r in rows_subset for _,v in r["monthly"]]
    lo, hi = min(vals_all), max(vals_all); rng = (hi-lo) or 1
    step = width/(len(months)-1 or 1)
    o = [f'<svg viewBox="0 0 {width} {h}" class="chart" preserveAspectRatio="none">']
    for i, r in enumerate(rows_subset):
        byM = dict(r["monthly"])
        pts = [f"{j*step:.1f},{h-24-((byM.get(m,lo)-lo)/rng)*(h-56):.1f}" for j, m in enumerate(months)]
        c = PALETTE[i % len(PALETTE)]
        o.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="{c}" stroke-width="2.5"/>')
    for j, m in enumerate(months):
        if j % 2 == 0:
            o.append(f'<text x="{j*step:.1f}" y="{h-6}" class="bs" text-anchor="middle">{m[5:]}</text>')
    return "".join(o)+"</svg>"

def scatter(items, xk, yk, xf, yf, width=560, h=280):
    xs = [float(r[xk]) for r in items]; ys = [float(r[yk]) for r in items]
    xlo, xhi = min(xs), max(xs); ylo, yhi = min(ys), max(ys)
    xr = (xhi-xlo) or 1; yr = (yhi-ylo) or 1
    pad = 34
    o = [f'<svg viewBox="0 0 {width} {h}" class="chart">']
    o.append(f'<line x1="{pad}" y1="{h-pad}" x2="{width-10}" y2="{h-pad}" stroke="#DDE5F0"/>')
    o.append(f'<line x1="{pad}" y1="10" x2="{pad}" y2="{h-pad}" stroke="#DDE5F0"/>')
    for i, r in enumerate(items):
        x = pad + (float(r[xk])-xlo)/xr*(width-pad-24)
        y = (h-pad) - (float(r[yk])-ylo)/yr*(h-pad-20)
        o.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="7" fill="{PALETTE[i%len(PALETTE)]}" opacity="0.88"/>')
        o.append(f'<text x="{x:.1f}" y="{y-11:.1f}" class="bs" text-anchor="middle">{esc(r["name"])[:10]}</text>')
    o.append(f'<text x="{pad}" y="{h-8}" class="bs">{xf}</text>')
    o.append(f'<text x="6" y="16" class="bs">{yf}</text>')
    return "".join(o)+"</svg>"

# ---------------------------------------------------------------- markup
alloc_legend = "".join(
    f'<div class="lg"><i style="background:{PALETTE[i%len(PALETTE)]}"></i>'
    f'<span>{esc(r["name"])}</span><b>{r["gross_zar"]/total_gross*100:.1f}%</b>'
    f'<em>{zar(r["gross_zar"])}</em></div>'
    for i, r in enumerate(rows))

partner_rows = "".join(
    f'<tr id="{r["fintech_id"]}"><td class="ic">{ICON.get(r["fintech_id"],"💠")}</td>'
    f'<td class="nm">{esc(r["name"])}<span class="sub">{esc(r["fintech_id"])} · {esc(r["platform"])}</span></td>'
    f'<td class="num">{zar(r["gross_zar"])}</td>'
    f'<td class="num">{r["fee_pct"]:.2f}%</td>'
    f'<td class="num">{zar(r["fees_zar"])}</td>'
    f'<td class="num">{r["tx"]:,}</td>'
    f'<td class="num">{r["merchants_end"]:,}</td>'
    f'<td class="num {"pos" if r["merchant_growth_pct"]>=0 else "neg"}">{r["merchant_growth_pct"]:+.1f}%</td>'
    f'<td class="num {"neg" if r["cb_ratio"]>0.18 else ""}">{r["cb_ratio"]:.2f}%</td></tr>'
    for r in rows)

card_details = "".join(
    f'<div class="pdcard" id="detail-{r["fintech_id"]}">'
    f'<div class="pdhead"><span class="ic">{ICON.get(r["fintech_id"],"💠")}</span>'
    f'<div><div class="pdname">{esc(r["name"])} · {esc(r["fintech_id"])}</div>'
    f'<div class="pdsub">{esc(r["platform"])} · {esc(r["segment"])} · {esc(r["city"])}, {esc(r["province"])} '
    f'· est. {esc(r["founded"])} · {esc(r["parent"])}</div></div></div>'
    f'<div class="pdnote">{esc(r["notes"])}</div>'
    f'<div class="pdstats">'
    f'<div><b>{zar(r["gross_zar"])}</b><span>Gross volume (12m)</span></div>'
    f'<div><b>{r["fee_pct"]:.2f}%</b><span>MDR</span></div>'
    f'<div><b>{zar(r["fees_zar"])}</b><span>Fee income</span></div>'
    f'<div><b>{r["merchants_end"]:,}</b><span>Active merchants</span></div>'
    f'<div><b class="{"pos" if r["merchant_growth_pct"]>=0 else "neg"}">{r["merchant_growth_pct"]:+.1f}%</b><span>Merchant growth (12m)</span></div>'
    f'<div><b>{r["cb_ratio"]:.2f}%</b><span>Chargeback ratio</span></div>'
    f'</div></div>'
    for r in rows)

HTML = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Fintech Partner Analytics — Prime Capital Bank</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{--navy:#0A1628;--navy2:#0F2040;--gold:#C9A84C;--cyan:#00B4CC;
--bg:#F0F4FA;--card:#FFF;--text:#1C2B3A;--dim:#6B7E92;--border:#DDE5F0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text)}}
.tabbar{{background:var(--navy);border-bottom:1px solid rgba(201,168,76,.25);display:flex;
justify-content:center;gap:4px;flex-wrap:wrap;position:sticky;top:0;z-index:50;padding:0 12px}}
.tabbar a{{color:rgba(255,255,255,.62);text-decoration:none;font-size:12.5px;font-weight:600;
padding:13px 18px;border-bottom:2px solid transparent;transition:color .15s,border-color .15s;white-space:nowrap}}
.tabbar a:hover{{color:#fff;border-bottom-color:rgba(201,168,76,.5)}}
.tabbar a.active{{color:var(--gold);border-bottom-color:var(--gold)}}
.tabbar a .dot{{font-size:13px;margin-right:6px}}
@media(max-width:760px){{.tabbar a{{padding:11px 12px;font-size:11.5px}}}}
.hero{{background:linear-gradient(135deg,var(--navy),var(--navy2) 60%,#0A2545);
padding:34px 40px 30px;position:relative;overflow:hidden}}
.hero::before{{content:'';position:absolute;inset:0;
background:radial-gradient(ellipse 80% 60% at 50% 130%,rgba(0,180,204,.14),transparent)}}
.back{{color:rgba(255,255,255,.5);text-decoration:none;font-size:12px}}
.back:hover{{color:var(--gold)}}
.hero h1{{font-size:27px;font-weight:800;color:#fff;margin-top:10px}}
.hero h1 span{{color:var(--gold)}}
.hero p{{font-size:13px;color:rgba(255,255,255,.55);margin-top:7px}}
.stats-bar{{background:var(--navy2);border-bottom:1px solid rgba(201,168,76,.2);
display:flex;justify-content:center;flex-wrap:wrap}}
.stat{{padding:14px 28px;text-align:center;border-right:1px solid rgba(255,255,255,.06)}}
.stat-value{{font-size:19px;font-weight:800;color:var(--gold)}}
.stat-label{{font-size:9.5px;color:rgba(255,255,255,.42);letter-spacing:1px;
text-transform:uppercase;margin-top:2px}}
.main{{max-width:1240px;margin:0 auto;padding:28px 22px 50px}}
.section-title{{font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;
color:var(--dim);margin:26px 0 12px}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:12px;
padding:20px 22px;box-shadow:0 1px 3px rgba(10,22,40,.05)}}
.card h3{{font-size:13px;font-weight:700;margin-bottom:14px}}
.card .note{{font-size:11.5px;color:var(--dim);margin-top:12px;line-height:1.65}}
.kpis{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:6px}}
.kpi{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px 18px}}
.kpi .l{{font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--dim)}}
.kpi .v{{font-size:26px;font-weight:800;margin-top:7px;letter-spacing:-.5px}}
.kpi .s{{font-size:11px;color:var(--dim);margin-top:4px}}
.gold{{color:var(--gold)}}.cy{{color:var(--cyan)}}.pos{{color:#16a34a}}.neg{{color:#dc2626}}
table{{width:100%;border-collapse:collapse;font-size:12.5px}}
th{{text-align:left;font-size:9.5px;letter-spacing:.09em;text-transform:uppercase;
color:var(--dim);padding:0 9px 9px 0;border-bottom:1px solid var(--border)}}
td{{padding:9px 9px 9px 0;border-bottom:1px solid #EEF2F8}}
.num{{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}}
.nm{{font-weight:600}}.sub{{display:block;font-weight:400;color:var(--dim);font-size:11px;margin-top:2px}}
.ic{{font-size:16px}}
.donut .dn{{fill:var(--text);font-size:17px;font-weight:800}}
.donut .dl{{fill:var(--dim);font-size:10px}}
.chart .bl{{fill:var(--text);font-size:11px}}
.chart .bv{{fill:var(--dim);font-size:10.5px;font-variant-numeric:tabular-nums}}
.chart .bs{{fill:var(--dim);font-size:9.5px}}
.lg{{display:flex;align-items:center;gap:9px;padding:6px 0;border-bottom:1px solid #EEF2F8;font-size:12px}}
.lg:last-child{{border:0}}
.lg i{{width:10px;height:10px;border-radius:3px;flex:0 0 auto}}
.lg b{{margin-left:auto;font-variant-numeric:tabular-nums}}
.lg em{{font-style:normal;color:var(--dim);width:60px;text-align:right;font-size:11px}}
.alloc-wrap{{display:flex;gap:22px;align-items:center}}
.pdgrid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
.pdcard{{background:var(--card);border:1px solid var(--border);border-radius:12px;
padding:16px 18px;scroll-margin-top:60px}}
.pdcard:target{{border-color:var(--gold);box-shadow:0 0 0 2px rgba(201,168,76,.35)}}
.pdhead{{display:flex;gap:10px;align-items:flex-start}}
.pdhead .ic{{font-size:22px}}
.pdname{{font-weight:700;font-size:13.5px}}
.pdsub{{font-size:11px;color:var(--dim);margin-top:2px}}
.pdnote{{font-size:11.5px;color:var(--text);opacity:.82;margin:10px 0;line-height:1.6}}
.pdstats{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px 4px;border-top:1px solid #EEF2F8;padding-top:10px}}
.pdstats div{{text-align:center}}
.pdstats b{{display:block;font-size:14px;font-weight:800}}
.pdstats span{{font-size:9px;color:var(--dim);letter-spacing:.03em;text-transform:uppercase}}
footer{{max-width:1240px;margin:0 auto;padding:0 22px 40px;color:var(--dim);
font-size:11.5px;line-height:1.75}}
@media(max-width:1000px){{.kpis{{grid-template-columns:repeat(2,1fr)}}.grid2,.pdgrid{{grid-template-columns:1fr}}
.alloc-wrap{{flex-direction:column}}}}
</style></head><body>

<nav class="tabbar">
  <a href="index.html"><span class="dot">🏛</span>Overview</a>
  <a href="asset_management.html"><span class="dot">📈</span>Asset Management</a>
  <a href="fintech_analytics.html" class="active"><span class="dot">🔌</span>Fintech Partners</a>
  <a href="settlement_reconciliation.html"><span class="dot">⚖️</span>Settlements</a>
  <a href="finance_selfserve_report.html"><span class="dot">📊</span>Self-Serve BI</a>
  <a href="ebook/index.html"><span class="dot">📖</span>Ebook</a>
  <a href="ebook/assets/PCB_COMPLETE_ERD.html"><span class="dot">🗂</span>ERD</a>
  <a href="data_dictionary/index.html"><span class="dot">📕</span>Data Dictionary</a>
</nav>

<div class="hero">
  <a class="back" href="index.html">← Prime Capital Bank · Data Intelligence Platform</a>
  <h1>Fintech Partner <span>Analytics</span></h1>
  <p>{len(rows)} settlement partners · acquiring, gateway, instant-EFT, QR, open-banking and
     remittance · 12 months of settlement volume feeding the reconciliation engine</p>
</div>

<div class="stats-bar">
  <div class="stat"><div class="stat-value">{zar(total_gross,2)}</div><div class="stat-label">Gross Volume (12m)</div></div>
  <div class="stat"><div class="stat-value">{len(rows)}</div><div class="stat-label">Partners</div></div>
  <div class="stat"><div class="stat-value">{total_tx/1e6:.1f}M</div><div class="stat-label">Transactions</div></div>
  <div class="stat"><div class="stat-value">{zar(total_fees)}</div><div class="stat-label">Fee Income</div></div>
  <div class="stat"><div class="stat-value">{total_cb:,}</div><div class="stat-label">Chargebacks</div></div>
  <div class="stat"><div class="stat-value">{sum(r["merchants_end"] for r in rows):,}</div><div class="stat-label">Active Merchants</div></div>
</div>

<div class="main">
  <div class="kpis">
    <div class="kpi"><div class="l">Gross volume</div><div class="v gold">{zar(total_gross,2)}</div>
      <div class="s">across {len(rows)} partners, 12 months</div></div>
    <div class="kpi"><div class="l">Net settlement</div><div class="v cy">{zar(total_net,2)}</div>
      <div class="s">after fees &amp; interchange</div></div>
    <div class="kpi"><div class="l">Take rate</div><div class="v">{total_fees/total_gross*100:.2f}%</div>
      <div class="s">blended merchant fee</div></div>
    <div class="kpi"><div class="l">Largest partner</div><div class="v">{esc(rows[0]["name"])}</div>
      <div class="s">{zar(rows[0]["gross_zar"])} · {rows[0]["gross_zar"]/total_gross*100:.0f}% of volume</div></div>
    <div class="kpi"><div class="l">Avg chargeback ratio</div><div class="v {"neg" if sum(r["cb_ratio"] for r in rows)/len(rows)>0.15 else ""}">{sum(r["cb_ratio"] for r in rows)/len(rows):.2f}%</div>
      <div class="s">across all partners</div></div>
  </div>

  <div class="section-title">Volume trend · 12 months</div>
  <div class="card">{multi_line(rows[:6])}
    <div class="note">Top 6 partners by volume, one line each. Colours match the allocation legend below.
    Black Friday (Nov) and festive-season (Dec) seasonality is applied uniformly across partners.</div>
  </div>

  <div class="section-title">Volume share &amp; economics</div>
  <div class="grid2">
    <div class="card"><h3>Gross volume by partner</h3>
      <div class="alloc-wrap"><div>{donut(rows,'name','gross_zar')}</div>
      <div style="flex:1">{alloc_legend}</div></div>
    </div>
    <div class="card"><h3>Fee rate vs merchant growth</h3>
      {scatter(rows,'fee_pct','merchant_growth_pct','MDR %','Merchant growth % →')}
      <div class="note">Cheaper rails (instant EFT, open banking) tend to grow merchant count faster
      than higher-MDR card acquiring — visible as points trending up-left.</div>
    </div>
  </div>

  <div class="section-title">Partner comparison</div>
  <div class="card" style="overflow-x:auto">
    <table><thead><tr><th></th><th>Partner</th><th class="num">Gross volume</th>
    <th class="num">MDR</th><th class="num">Fee income</th><th class="num">Transactions</th>
    <th class="num">Active merchants</th><th class="num">Merchant growth</th>
    <th class="num">Chargeback %</th></tr></thead><tbody>{partner_rows}</tbody></table>
    <div class="note">Click a partner name to jump to its detail card below. Fee income is what the
    partner pays Prime Capital Bank for acquiring/gateway services — the revenue line this business
    contributes, separate from net settlement (what flows back to the partner after fees).</div>
  </div>

  <div class="section-title">Partner profiles</div>
  <div class="pdgrid">{card_details}</div>
</div>

<footer>
  <b>Prime Capital Bank — Fintech Partner Analytics.</b> Generated by
  <code>scripts/build_fintech_analytics_page.py</code> from
  <code>data/fintech_partners.csv</code> (12 partners) and
  <code>data/fintech_settlements.csv</code> (144 monthly settlement rows, 2024).
  Reconciliation of these settlements against bank receipts — including clawback,
  in-transit and unmatched-receipt detection — is on the
  <a href="settlement_reconciliation.html" style="color:var(--cyan)">Settlements</a> page.<br>
  <b>Synthetic data.</b> Every settlement figure is generated. Partner names, platform types,
  headquarters and founding years are real market references used for authenticity; no actual
  merchant, transaction or partner financial data is used.
</footer>
</body></html>"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(HTML)
print("Wrote", OUT, f"({len(HTML)/1024:.0f} KB)")
