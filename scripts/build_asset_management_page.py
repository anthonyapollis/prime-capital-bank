# -*- coding: utf-8 -*-
"""Render docs/asset_management.html from the generated summary JSON.

Self-contained: hand-rolled SVG charts, no CDN, works offline. Matches the
Prime Capital Bank navy/gold design system used by index.html.
"""
import json, os, html

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC  = os.path.join(ROOT, "docs", "ml_outputs", "asset_management_summary.json")
OUT  = os.path.join(ROOT, "docs", "asset_management.html")
S = json.load(open(SRC, encoding="utf-8"))

esc = lambda t: html.escape(str(t))
def zar(v, dp=1):
    v = float(v)
    if abs(v) >= 1e9: return f"R{v/1e9:.{dp}f}bn"
    if abs(v) >= 1e6: return f"R{v/1e6:.{dp}f}m"
    if abs(v) >= 1e3: return f"R{v/1e3:.0f}k"
    return f"R{v:.0f}"

PALETTE = ["#C9A84C","#00B4CC","#16a34a","#7C6BD6","#E8845C","#4C8FD9","#D96BA0"]

def donut(rows, label_k, val_k, size=210):
    import math
    tot = sum(float(r[val_k]) for r in rows) or 1
    cx = cy = size/2; ro, ri = size/2-4, size/2-46
    a = -math.pi/2; out = [f'<svg viewBox="0 0 {size} {size}" class="donut">']
    for i, r in enumerate(rows):
        f = float(r[val_k])/tot; a2 = a + f*2*math.pi; lg = 1 if f > .5 else 0
        x1,y1 = cx+ro*math.cos(a), cy+ro*math.sin(a); x2,y2 = cx+ro*math.cos(a2), cy+ro*math.sin(a2)
        x3,y3 = cx+ri*math.cos(a2), cy+ri*math.sin(a2); x4,y4 = cx+ri*math.cos(a), cy+ri*math.sin(a)
        out.append(f'<path d="M{x1:.1f},{y1:.1f} A{ro},{ro} 0 {lg},1 {x2:.1f},{y2:.1f} '
                   f'L{x3:.1f},{y3:.1f} A{ri},{ri} 0 {lg},0 {x4:.1f},{y4:.1f} Z" '
                   f'fill="{PALETTE[i%len(PALETTE)]}"/>')
        a = a2
    out.append(f'<text x="{cx}" y="{cy-4}" class="dn" text-anchor="middle">{zar(tot)}</text>')
    out.append(f'<text x="{cx}" y="{cy+15}" class="dl" text-anchor="middle">invested</text></svg>')
    return "".join(out)

def hbar(rows, label_k, val_k, fmt, color="#C9A84C", width=560, rowh=30, lw=150):
    if not rows: return ""
    mx = max(float(r[val_k]) for r in rows) or 1
    bw = width-lw-88; h = len(rows)*rowh+6
    o = [f'<svg viewBox="0 0 {width} {h}" class="chart">']
    for i, r in enumerate(rows):
        y = i*rowh+4; v = float(r[val_k]); bwid = max(2, bw*v/mx)
        o.append(f'<text x="0" y="{y+15}" class="bl">{esc(r[label_k])[:24]}</text>')
        o.append(f'<rect x="{lw}" y="{y+3}" width="{bwid:.1f}" height="15" rx="3" fill="{color}"/>')
        o.append(f'<text x="{lw+bwid+8:.1f}" y="{y+15}" class="bv">{fmt(v)}</text>')
    return "".join(o)+"</svg>"

def line(series, width=1180, h=200):
    vals = [float(p["index"]) for p in series]
    lo, hi = min(vals), max(vals); rng = (hi-lo) or 1
    step = width/(len(vals)-1 or 1)
    pts = [f"{i*step:.1f},{h-8-((v-lo)/rng)*(h-38):.1f}" for i, v in enumerate(vals)]
    first, last = vals[0], vals[-1]
    chg = (last/first-1)*100
    col = "#16a34a" if chg >= 0 else "#dc2626"
    area = f"M0,{h-8} L" + " L".join(pts) + f" L{width},{h-8} Z"
    return (f'<svg viewBox="0 0 {width} {h}" class="chart" preserveAspectRatio="none">'
            f'<path d="{area}" fill="{col}" opacity="0.10"/>'
            f'<polyline points="{" ".join(pts)}" fill="none" stroke="{col}" stroke-width="2.5"/>'
            f'</svg>'), chg

def bipolar(rows, width=560, h=250):
    """alpha vs benchmark, +/- around zero"""
    vals = [float(r["alpha_pct"]) for r in rows]
    lo, hi = min(vals+[0]), max(vals+[0]); rng = (hi-lo) or 1
    bw = width/len(rows); zero = 18+(hi/rng)*(h-70)
    o = [f'<svg viewBox="0 0 {width} {h}" class="chart">',
         f'<line x1="0" y1="{zero:.1f}" x2="{width}" y2="{zero:.1f}" stroke="#DDE5F0"/>']
    for i, r in enumerate(rows):
        v = float(r["alpha_pct"]); x = 6+i*bw; bh = abs(v)/rng*(h-70)
        y = zero-bh if v >= 0 else zero
        c = "#16a34a" if v >= 0 else "#dc2626"
        o.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw-14:.1f}" height="{max(2,bh):.1f}" rx="3" fill="{c}"/>')
        o.append(f'<text x="{x+(bw-14)/2:.1f}" y="{(y-6) if v>=0 else (y+bh+14):.1f}" class="bv" text-anchor="middle">{v:+.1f}</text>')
        o.append(f'<text x="{x+(bw-14)/2:.1f}" y="{h-6}" class="bs" text-anchor="middle" '
                 f'transform="rotate(-16 {x+(bw-14)/2:.1f} {h-6})">{esc(r["mandate"])[:16]}</text>')
    return "".join(o)+"</svg>"

spark, chg = line(S["market_series"])

alloc_legend = "".join(
    f'<div class="lg"><i style="background:{PALETTE[i%len(PALETTE)]}"></i>'
    f'<span>{esc(a["asset_class"])}</span><b>{a["pct"]}%</b>'
    f'<em>{zar(a["value_zar"])}</em></div>'
    for i, a in enumerate(S["allocation"]))

mandate_rows = "".join(
    f'<tr><td class="nm">{esc(m["mandate"])}</td>'
    f'<td class="num">{zar(m["aum_zar"])}</td><td class="num">{m["pct"]}%</td>'
    f'<td class="num">{m["return_12m_pct"]:+.2f}%</td>'
    f'<td>{esc(m["benchmark"])}</td><td class="num">{m["benchmark_pct"]:+.2f}%</td>'
    f'<td class="num {"pos" if m["alpha_pct"]>=0 else "neg"}">{m["alpha_pct"]:+.2f}%</td></tr>'
    for m in S["by_mandate"])

hold_rows = "".join(
    f'<tr><td class="tk">{esc(h["ticker"])}</td><td class="nm">{esc(h["name"])}</td>'
    f'<td><span class="pill">{esc(h["asset_class"])}</span></td>'
    f'<td class="num">{zar(h["value_zar"])}</td><td class="num">{h["pct"]:.2f}%</td>'
    f'<td class="num {"pos" if h["return_12m_pct"]>=0 else "neg"}">{h["return_12m_pct"]:+.1f}%</td></tr>'
    for h in S["top_holdings"])

breach_rows = "".join(
    f'<tr><td class="tk">{esc(b["portfolio_id"])}</td><td class="nm">{esc(b["mandate"])}</td>'
    f'<td><span class="pill warn">{esc(b["limit"])}</span></td>'
    f'<td class="num neg">{b["actual_pct"]}%</td><td class="num">{b["limit_pct"]}%</td>'
    f'<td class="num neg">{zar(b["excess_zar"])}</td></tr>'
    for b in S["top_breaches"]) or '<tr><td colspan="6" class="ok">No Reg 28 breaches.</td></tr>'

HTML = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Asset Management &amp; Portfolio Analytics — Prime Capital Bank</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{--navy:#0A1628;--navy2:#0F2040;--gold:#C9A84C;--cyan:#00B4CC;
--bg:#F0F4FA;--card:#FFF;--text:#1C2B3A;--dim:#6B7E92;--border:#DDE5F0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text)}}
.hero{{background:linear-gradient(135deg,var(--navy),var(--navy2) 60%,#0A2545);
padding:34px 40px 30px;position:relative;overflow:hidden}}
.hero::before{{content:'';position:absolute;inset:0;
background:radial-gradient(ellipse 80% 60% at 50% 130%,rgba(201,168,76,.14),transparent)}}
.back{{color:rgba(255,255,255,.5);text-decoration:none;font-size:12px;position:relative;z-index:1}}
.back:hover{{color:var(--gold)}}
.hero h1{{font-size:27px;font-weight:800;color:#fff;margin-top:10px;position:relative;z-index:1}}
.hero h1 span{{color:var(--gold)}}
.hero p{{font-size:13px;color:rgba(255,255,255,.55);margin-top:7px;position:relative;z-index:1}}
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
.nm{{font-weight:600}}.tk{{font-family:ui-monospace,Consolas,monospace;font-size:11.5px;color:var(--dim)}}
.pill{{background:#EEF3FA;border:1px solid var(--border);border-radius:20px;
padding:2px 9px;font-size:10.5px;color:var(--dim)}}
.pill.warn{{background:#FEF2F2;border-color:#FECACA;color:#B91C1C}}
.ok{{color:#16a34a;text-align:center;padding:16px}}
.donut .dn{{fill:var(--text);font-size:17px;font-weight:800}}
.donut .dl{{fill:var(--dim);font-size:10px}}
.chart .bl{{fill:var(--text);font-size:11px}}
.chart .bv{{fill:var(--dim);font-size:10.5px;font-variant-numeric:tabular-nums}}
.chart .bs{{fill:var(--dim);font-size:9.5px}}
.lg{{display:flex;align-items:center;gap:9px;padding:7px 0;border-bottom:1px solid #EEF2F8;font-size:12.5px}}
.lg:last-child{{border:0}}
.lg i{{width:10px;height:10px;border-radius:3px;flex:0 0 auto}}
.lg b{{margin-left:auto;font-variant-numeric:tabular-nums}}
.lg em{{font-style:normal;color:var(--dim);width:64px;text-align:right;font-size:11.5px}}
.alloc-wrap{{display:flex;gap:22px;align-items:center}}
footer{{max-width:1240px;margin:0 auto;padding:0 22px 40px;color:var(--dim);
font-size:11.5px;line-height:1.75}}
.tabbar{{background:var(--navy);border-bottom:1px solid rgba(201,168,76,.25);display:flex;
justify-content:center;gap:4px;flex-wrap:wrap;position:sticky;top:0;z-index:50;padding:0 12px}}
.tabbar a{{color:rgba(255,255,255,.62);text-decoration:none;font-size:12.5px;font-weight:600;
padding:13px 18px;border-bottom:2px solid transparent;transition:color .15s,border-color .15s;white-space:nowrap}}
.tabbar a:hover{{color:#fff;border-bottom-color:rgba(201,168,76,.5)}}
.tabbar a.active{{color:var(--gold);border-bottom-color:var(--gold)}}
.tabbar a .dot{{font-size:13px;margin-right:6px}}
@media(max-width:1000px){{.kpis{{grid-template-columns:repeat(2,1fr)}}.grid2{{grid-template-columns:1fr}}
.alloc-wrap{{flex-direction:column}}}}
@media(max-width:760px){{.tabbar a{{padding:11px 12px;font-size:11.5px}}}}
</style></head><body>

<nav class="tabbar">
  <a href="index.html"><span class="dot">🏛</span>Overview</a>
  <a href="asset_management.html" class="active"><span class="dot">📈</span>Asset Management</a>
  <a href="fintech_analytics.html"><span class="dot">🔌</span>Fintech Partners</a>
  <a href="settlement_reconciliation.html"><span class="dot">⚖️</span>Settlements</a>
  <a href="finance_selfserve_report.html"><span class="dot">📊</span>Self-Serve BI</a>
  <a href="ebook/index.html"><span class="dot">📖</span>Ebook</a>
  <a href="ebook/assets/PCB_COMPLETE_ERD.html"><span class="dot">🗂</span>ERD</a>
  <a href="data_dictionary/index.html"><span class="dot">📕</span>Data Dictionary</a>
</nav>

<div class="hero">
  <a class="back" href="index.html">← Prime Capital Bank · Data Intelligence Platform</a>
  <h1>Asset Management &amp; <span>Portfolio Analytics</span></h1>
  <p>Discretionary wealth · {S['portfolios']:,} portfolios · JSE equities, SA government bonds,
     offshore feeders · Regulation 28 compliance monitoring</p>
</div>

<div class="stats-bar">
  <div class="stat"><div class="stat-value">{zar(S['aum_zar'],2)}</div><div class="stat-label">Assets Under Management</div></div>
  <div class="stat"><div class="stat-value">{S['portfolios']:,}</div><div class="stat-label">Portfolios</div></div>
  <div class="stat"><div class="stat-value">{S['securities']}</div><div class="stat-label">Instruments</div></div>
  <div class="stat"><div class="stat-value">{S['holdings']:,}</div><div class="stat-label">Holdings</div></div>
  <div class="stat"><div class="stat-value">{S['trades']:,}</div><div class="stat-label">Trades</div></div>
  <div class="stat"><div class="stat-value">{S['advisers']}</div><div class="stat-label">Advisers</div></div>
</div>

<div class="main">
  <div class="kpis">
    <div class="kpi"><div class="l">AUM</div><div class="v gold">{zar(S['aum_zar'],2)}</div>
      <div class="s">across {S['portfolios']:,} mandates</div></div>
    <div class="kpi"><div class="l">Annual fee income</div><div class="v cy">{zar(S['annual_fee_zar'])}</div>
      <div class="s">{S['avg_fee_bps']} bps average</div></div>
    <div class="kpi"><div class="l">Average portfolio</div><div class="v">{zar(S['avg_portfolio_zar'])}</div>
      <div class="s">median well below mean</div></div>
    <div class="kpi"><div class="l">Reg 28 breaches</div><div class="v neg">{S['reg28_breaches']}</div>
      <div class="s">of {S['reg28_regulated']:,} regulated</div></div>
    <div class="kpi"><div class="l">Excess exposure</div><div class="v neg">{zar(S['reg28_breach_value_zar'])}</div>
      <div class="s">above statutory limits</div></div>
  </div>

  <div class="section-title">Market · Satrix Top 40 proxy, trailing 12 months</div>
  <div class="card">{spark}
    <div class="note">Index level over the trailing year, <b class="{'pos' if chg>=0 else 'neg'}">{chg:+.1f}%</b>.
    All security prices are simulated with a shared market factor plus instrument-specific
    beta, drift and volatility, so correlations behave realistically across the book.</div>
  </div>

  <div class="section-title">Allocation &amp; mandate performance</div>
  <div class="grid2">
    <div class="card"><h3>Asset allocation</h3>
      <div class="alloc-wrap"><div>{donut(S['allocation'],'asset_class','value_zar')}</div>
      <div style="flex:1">{alloc_legend}</div></div>
      <div class="note">Aggregate look-through across every mandate. Offshore exposure is the
      binding Reg 28 constraint for most retirement vehicles.</div>
    </div>
    <div class="card"><h3>Alpha vs benchmark — 12 months</h3>
      {bipolar(S['by_mandate'])}
      <div class="note">Return above (green) or below (red) each mandate's stated benchmark:
      ALSI, SWIX, ALBI, MSCI World, STeFI and SAPY.</div>
    </div>
  </div>

  <div class="section-title">Mandate book</div>
  <div class="card">
    <table><thead><tr><th>Mandate</th><th class="num">AUM</th><th class="num">Share</th>
    <th class="num">Return 12m</th><th>Benchmark</th><th class="num">Bmk</th>
    <th class="num">Alpha</th></tr></thead><tbody>{mandate_rows}</tbody></table>
  </div>

  <div class="section-title">Concentration &amp; regional split</div>
  <div class="grid2">
    <div class="card"><h3>Largest positions</h3>
      <table><thead><tr><th>Ticker</th><th>Security</th><th>Class</th>
      <th class="num">Value</th><th class="num">Book %</th><th class="num">12m</th></tr></thead>
      <tbody>{hold_rows}</tbody></table>
      <div class="note">Single-issuer concentration is the second-order risk after Reg 28:
      a large aggregate position is a house view, whether intended or not.</div>
    </div>
    <div class="card"><h3>AUM by province</h3>
      {hbar(S['by_province'],'province','aum_zar',lambda v: zar(v),color="#00B4CC")}
      <div class="note">Client domicile. Gauteng and the Western Cape dominate private
      wealth, consistent with the bank's branch and adviser footprint.</div>
    </div>
  </div>

  <div class="section-title">Regulation 28 compliance</div>
  <div class="card">
    <table><thead><tr><th>Portfolio</th><th>Mandate</th><th>Limit breached</th>
    <th class="num">Actual</th><th class="num">Limit</th><th class="num">Excess</th></tr></thead>
    <tbody>{breach_rows}</tbody></table>
    <div class="note"><b>{S['reg28_breaches']} of {S['reg28_regulated']:,}</b> regulated portfolios
    breach a statutory limit, {zar(S['reg28_breach_value_zar'])} above cap in aggregate.
    Regulation 28 of the Pension Funds Act caps equity at 75%, offshore at 45% and property at 25%
    for retirement vehicles. Breaches are usually drift, not intent — markets move the weights
    after the last rebalance — but they must be corrected regardless.</div>
  </div>
</div>

<footer>
  <b>Prime Capital Bank — Asset Management.</b> Generated by
  <code>scripts/data_gen/generate_asset_management.py</code>; page rendered by
  <code>scripts/build_asset_management_page.py</code>. Source tables:
  <code>securities</code>, <code>security_prices</code>, <code>portfolios</code>,
  <code>portfolio_holdings</code>, <code>portfolio_trades</code>.<br>
  <b>Synthetic data.</b> Every portfolio, holding, price and client is generated. JSE tickers and
  Regulation 28 limits are real market references used for authenticity; no actual client,
  issuer or price data is used. Valuation date {esc(S['generated'])}.
</footer>
</body></html>"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(HTML)
print("Wrote", OUT, f"({len(HTML)/1024:.0f} KB)")
