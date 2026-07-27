"""Injects the Q&A assistant widget and Next-Best-Product recommender
section into docs/index.html, between idempotent HTML comment markers so
this can be re-run safely after re-running build_recommender.py.

Zero-cost by design (same rationale as Pet Business Intelligence): the
Q&A widget is local keyword-matching, not a live LLM call, so nothing
here can incur an API bill. The recommender demo data is inlined as JSON
rather than fetched, so the page still works opened directly via
file:// with no server.
"""
import json
import os
import re

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_INDEX = os.path.join(BASE, "docs", "index.html")
OUT = os.path.join(BASE, "docs", "ml_outputs")

BEGIN_CSS = "<!-- BEGIN widgets-css -->"
END_CSS = "<!-- END widgets-css -->"
BEGIN_SECTION = "<!-- BEGIN nbp-section -->"
END_SECTION = "<!-- END nbp-section -->"
BEGIN_SCRIPT = "<!-- BEGIN widgets-script -->"
END_SCRIPT = "<!-- END widgets-script -->"

WIDGETS_CSS_CONTENT = """
/* -- Next Best Product recommender -- */
.nbp-box{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:22px;margin-bottom:40px;}
.nbp-note{font-size:12px;color:var(--dim);line-height:1.6;margin-bottom:16px;}
.nbp-metrics{display:flex;gap:24px;flex-wrap:wrap;margin-bottom:18px;}
.nbp-metric{text-align:center;}
.nbp-metric b{display:block;font-size:20px;font-weight:800;color:var(--cyan);}
.nbp-metric span{font-size:10px;color:var(--dim);letter-spacing:.5px;text-transform:uppercase;}
.nbp-cols{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;}
@media(max-width:760px){.nbp-cols{grid-template-columns:1fr;}}
.nbp-cols select{width:100%;padding:9px;border-radius:8px;border:1px solid var(--border);font-size:13px;margin-bottom:12px;}
.nbp-list{list-style:none;font-size:12.5px;}
.nbp-list li{padding:7px 10px;background:var(--bg);border-radius:6px;margin-bottom:6px;}
.nbp-list.rec li{background:rgba(0,180,204,0.10);color:#0A7A8C;font-weight:600;}
.nbp-cols h4{font-size:11px;text-transform:uppercase;letter-spacing:1px;color:var(--dim);margin-bottom:8px;}

/* -- Q&A assistant widget -- */
#qa-toggle{
  position:fixed;bottom:22px;right:22px;z-index:200;background:var(--navy);color:#fff;
  border:none;border-radius:30px;padding:13px 20px;font-size:13px;font-weight:700;
  box-shadow:0 6px 20px rgba(10,22,40,0.35);cursor:pointer;display:flex;align-items:center;gap:8px;
}
#qa-toggle:hover{background:var(--navy2);}
#qa-panel{
  position:fixed;bottom:78px;right:22px;z-index:200;width:360px;max-width:92vw;max-height:70vh;
  background:var(--card);border-radius:14px;box-shadow:0 12px 40px rgba(10,22,40,0.28);
  display:none;flex-direction:column;overflow:hidden;border:1px solid var(--border);
}
#qa-panel.open{display:flex;}
#qa-head{background:var(--navy);color:#fff;padding:13px 16px;font-size:13px;font-weight:700;display:flex;justify-content:space-between;align-items:center;}
#qa-head button{background:none;border:none;color:#fff;font-size:16px;cursor:pointer;opacity:.7;}
#qa-log{flex:1;overflow-y:auto;padding:14px;font-size:12.5px;display:flex;flex-direction:column;gap:8px;}
#qa-log .bubble{padding:9px 12px;border-radius:10px;max-width:88%;line-height:1.5;}
#qa-log .bot{background:var(--bg);align-self:flex-start;}
#qa-log .user{background:var(--navy);color:#fff;align-self:flex-end;}
#qa-chips{display:flex;gap:6px;flex-wrap:wrap;padding:0 14px 10px;}
#qa-chips button{font-size:10.5px;background:var(--bg);border:1px solid var(--border);border-radius:14px;padding:5px 10px;cursor:pointer;color:var(--text);}
#qa-chips button:hover{border-color:var(--cyan);}
#qa-input-row{display:flex;border-top:1px solid var(--border);}
#qa-input{flex:1;border:none;padding:11px 12px;font-size:12.5px;outline:none;}
#qa-send{background:var(--cyan);color:#fff;border:none;padding:0 16px;font-weight:700;cursor:pointer;font-size:12px;}
"""

WIDGET_HTML = """
<button id="qa-toggle">&#128172; Ask about this platform</button>
<div id="qa-panel">
  <div id="qa-head"><span>Ask about Prime Capital Bank</span><button id="qa-close">&times;</button></div>
  <div id="qa-log"><div class="bubble bot">Hi! Ask me about the customers, portfolio, ML models, fraud, fintech partners, or tech stack.</div></div>
  <div id="qa-chips">
    <button>What is Prime Capital Bank?</button>
    <button>How much data is in this?</button>
    <button>Which model performs best?</button>
    <button>What's the tech stack?</button>
  </div>
  <div id="qa-input-row">
    <input id="qa-input" type="text" placeholder="Ask a question...">
    <button id="qa-send">Send</button>
  </div>
</div>
"""


def wrap(begin, content, end):
    return "\n" + begin + "\n" + content.strip() + "\n" + end + "\n"


def replace_or_insert(html, begin, end, content, fallback_anchor, insert_before=True):
    block = wrap(begin, content, end)
    pattern = re.compile(re.escape(begin) + r".*?" + re.escape(end), re.DOTALL)
    if pattern.search(html):
        return pattern.sub(lambda m: block.strip("\n"), html)
    if insert_before:
        return html.replace(fallback_anchor, block.strip("\n") + "\n" + fallback_anchor)
    return html.replace(fallback_anchor, fallback_anchor + "\n" + block.strip("\n"))


def build_nbp_section(metrics, demo):
    top_customers = demo[:60]
    opts = "\n".join(
        f'<option value="{i}">{c["customer_number"]} ({len(c["current_products"])} products, {c["segment"]})</option>'
        for i, c in enumerate(top_customers)
    )
    return f"""
<div class="section-title">Next Best Product &mdash; Cross-Sell Recommender</div>
<div class="nbp-box">
  <div class="nbp-note">
    Item-based collaborative filtering over product ownership (which of the 22 PCB products each customer holds) &mdash;
    &ldquo;customers who hold X also hold Y.&rdquo; Evaluated with a leave-one-product-out test, refitting the similarity
    matrix per test to avoid leaking the held-out holding into its own evaluation.
    Honest caveat: this synthetic generator assigns each account's product independently at random per customer rather
    than by deliberate affinity (income, segment, or existing relationship), so the {metrics['lift_over_random']}&times; lift
    below reflects sampling structure in a small 22-product catalogue, not a designed behavioural signal &mdash; the same
    limitation disclosed for the Pet Business Intelligence shop-assistant recommender this pattern is based on.
  </div>
  <div class="nbp-metrics">
    <div class="nbp-metric"><b>{metrics['hit_rate_at_3']*100:.1f}%</b><span>Hit rate @ 3</span></div>
    <div class="nbp-metric"><b>{metrics['random_baseline_hit_rate_at_3']*100:.1f}%</b><span>Random baseline</span></div>
    <div class="nbp-metric"><b>{metrics['lift_over_random']}&times;</b><span>Lift</span></div>
    <div class="nbp-metric"><b>{metrics['customers_with_accounts']}</b><span>Customers scored</span></div>
    <div class="nbp-metric"><b>{metrics['products']}</b><span>Products in catalogue</span></div>
  </div>
  <div class="nbp-cols">
    <div>
      <h4>Look up a customer</h4>
      <select id="nbp-select">{opts}</select>
    </div>
    <div>
      <h4>Current products</h4>
      <ul class="nbp-list" id="nbp-current"></ul>
    </div>
    <div>
      <h4>Recommended next</h4>
      <ul class="nbp-list rec" id="nbp-rec"></ul>
    </div>
  </div>
</div>
"""


def build_widget_script(demo, faq):
    return f"""
{WIDGET_HTML}
<script>
var NBP_DEMO = {json.dumps(demo[:60])};
var FAQ = {json.dumps(faq)};
document.addEventListener("DOMContentLoaded", function () {{

  // ---------------- next-best-product selector ----------------
  var sel = document.getElementById("nbp-select");
  var curEl = document.getElementById("nbp-current");
  var recEl = document.getElementById("nbp-rec");
  function renderNbp(i) {{
    var c = NBP_DEMO[i];
    if (!c) return;
    curEl.innerHTML = c.current_products.map(function (p) {{ return "<li>" + p + "</li>"; }}).join("");
    recEl.innerHTML = c.recommended_next.length
      ? c.recommended_next.map(function (p) {{ return "<li>" + p + "</li>"; }}).join("")
      : "<li>No confident recommendation (holds most of the catalogue already)</li>";
  }}
  if (sel) {{
    sel.addEventListener("change", function () {{ renderNbp(this.value); }});
    renderNbp(0);
  }}

  // ---------------- Q&A assistant ----------------
  var toggle = document.getElementById("qa-toggle");
  var panel = document.getElementById("qa-panel");
  var log = document.getElementById("qa-log");
  var input = document.getElementById("qa-input");
  var send = document.getElementById("qa-send");

  function addBubble(text, who) {{
    var b = document.createElement("div");
    b.className = "bubble " + who;
    b.textContent = text;
    log.appendChild(b);
    log.scrollTop = log.scrollHeight;
  }}

  function answer(q) {{
    var ql = q.toLowerCase();
    var best = null, bestScore = 0;
    FAQ.forEach(function (item) {{
      var score = 0;
      item.kw.forEach(function (k) {{ if (ql.indexOf(k) !== -1) score += k.length; }});
      if (score > bestScore) {{ bestScore = score; best = item; }}
    }});
    return best ? best.a : "I don't have a grounded answer for that yet - try asking about the customers, transactions, portfolio, fraud model, fintech partners, or tech stack.";
  }}

  function ask(q) {{
    if (!q.trim()) return;
    addBubble(q, "user");
    input.value = "";
    setTimeout(function () {{ addBubble(answer(q), "bot"); }}, 200);
  }}

  if (toggle) {{
    toggle.addEventListener("click", function () {{
      panel.classList.toggle("open");
    }});
    document.getElementById("qa-close").addEventListener("click", function () {{
      panel.classList.remove("open");
    }});
    send.addEventListener("click", function () {{ ask(input.value); }});
    input.addEventListener("keydown", function (e) {{ if (e.key === "Enter") ask(input.value); }});
    document.querySelectorAll("#qa-chips button").forEach(function (btn) {{
      btn.addEventListener("click", function () {{ ask(btn.textContent); }});
    }});
  }}
}});
</script>
"""


def build_faq(metrics):
    return [
        {"q": "What is Prime Capital Bank?",
         "kw": ["what is prime capital", "about prime capital", "about this project", "what company"],
         "a": "A fictional Fortune 500 South African retail and corporate bank invented for this portfolio project - "
              "not a real company. It's a production-shaped Azure Databricks data platform: 500,000 SA customers, "
              "12M+ annual transactions, R500B loan portfolio, 200 branches across all 9 provinces."},
        {"q": "How much data is in this?",
         "kw": ["how much data", "how many rows", "scale", "size of the data"],
         "a": "500,000 customers, 820K accounts, 10M+ transactions, 280K loans, 3.2M card transactions, "
              "1.8M EFT/SWIFT/PayShap payments, 48K fraud alerts, across 14 Bronze, 11 Silver and 22 Gold tables."},
        {"q": "Which ML model performs best?",
         "kw": ["best model", "which model", "highest auc", "top model"],
         "a": "Fraud Detection, at AUC 0.921 (Random Forest ensemble, real-time). Credit PD scoring (LightGBM) and "
              "AML risk scoring (Gradient Boost + GraphFrames) follow at 0.847 and 0.889."},
        {"q": "What's the tech stack?",
         "kw": ["tech stack", "technology", "what's it built with", "stack"],
         "a": "Azure Databricks (Premium) on ADLS Gen2 with Delta Lake and Unity Catalog governance, dbt Core for "
              "transformation, MLflow + LightGBM/XGBoost/GraphFrames for ML, Structured Streaming (Auto Loader), "
              "Terraform + Azure CLI for IaC, and Power BI (DirectQuery) for consumption."},
        {"q": "What regulations does it cover?",
         "kw": ["regulation", "regulatory", "compliance", "ifrs", "basel", "popia", "fica", "aml"],
         "a": "IFRS 9 (Stage 1/2/3, ECL), Basel III (RWA, CET1, LCR, NSFR), SARB BA700 capital adequacy reporting, "
              "POPIA (PII masking in Silver), FICA/KYC tracking, and AML/CFT transaction monitoring with SAR filing."},
        {"q": "What are the fintech partners?",
         "kw": ["fintech", "partners", "yoco", "snapscan", "payfast", "settlement"],
         "a": "12 SA fintech partners settle into the bank's fintech settlement mart - Yoco, SnapScan, PayFast, "
              "Peach Payments, PayGate, Ozow, Zapper, iKhokha, Stitch, Kazang, Adumo and Mukuru - covering POS "
              "acquiring, QR payments, gateways, instant EFT, open banking and cross-border remittance."},
        {"q": "What's the next-best-product recommender?",
         "kw": ["next best product", "recommender", "cross-sell", "recommendation"],
         "a": f"An item-based collaborative filter over which of the 22 PCB products each customer holds - "
              f"\"customers who hold X also hold Y.\" Leave-one-out tested: {metrics['hit_rate_at_3']*100:.1f}% hit "
              f"rate @3 vs a {metrics['random_baseline_hit_rate_at_3']*100:.1f}% random baseline "
              f"({metrics['lift_over_random']}x lift) - modest and honestly caveated, since the generator assigns "
              "products independently rather than by designed customer affinity."},
        {"q": "Where does the bank operate?",
         "kw": ["where", "branches", "provinces", "locations", "map"],
         "a": "200 branches across all 9 South African provinces. The Merchant Intelligence Map (Leaflet.js) plots "
              "merchant transaction volume and fraud hotspots province by province, with Sales/Fraud/AML view toggles."},
        {"q": "Is the data real?",
         "kw": ["real data", "real customers", "is this real", "actual bank"],
         "a": "No - Prime Capital Bank and every customer, transaction and loan in it are synthetic, generated "
              "locally with numpy/pandas/Faker for this portfolio project. No real personal or financial data is used."},
        {"q": "Who built this?",
         "kw": ["who built", "who made", "author", "anthony"],
         "a": "Anthony Apollis, as a portfolio project demonstrating enterprise data engineering, analytics "
              "engineering and ML on Azure - not a real bank."},
    ]


def main():
    with open(os.path.join(OUT, "recommender_metrics.json")) as f:
        metrics = json.load(f)
    with open(os.path.join(OUT, "customer_recommendations.json")) as f:
        demo = json.load(f)

    with open(DOCS_INDEX, "r", encoding="utf-8") as f:
        html = f.read()

    html = replace_or_insert(html, BEGIN_CSS, END_CSS, WIDGETS_CSS_CONTENT, "</style>", insert_before=True)

    section_content = build_nbp_section(metrics, demo)
    html = replace_or_insert(html, BEGIN_SECTION, END_SECTION, section_content,
                              "<!-- CODE ASSETS -->", insert_before=True)

    faq = build_faq(metrics)
    script_content = build_widget_script(demo, faq)
    html = replace_or_insert(html, BEGIN_SCRIPT, END_SCRIPT, script_content, "</body>", insert_before=True)

    with open(DOCS_INDEX, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"written: {DOCS_INDEX} ({os.path.getsize(DOCS_INDEX)} bytes)")


if __name__ == "__main__":
    main()
