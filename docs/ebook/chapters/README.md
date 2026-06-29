# Ebook Chapter Source Files

The primary ebook deliverable is the single-page HTML app at `../index.html`.

This folder holds standalone chapter exports — useful for sharing individual chapters
with recruiters, emailing specific sections, or generating per-chapter PDFs.

## Chapter List

| # | Title | Domain | Status |
|---|-------|--------|--------|
| 01 | Platform Overview & Architecture | Infrastructure | ✅ |
| 02 | Data Sources & Ingestion (Bronze) | Pipeline | ✅ |
| 03 | Silver Transformation Layer | Pipeline | ✅ |
| 04 | Gold Star Schema Design | Data Modelling | ✅ |
| 05 | Credit Scoring ML Model | Machine Learning | ✅ |
| 06 | Fraud Detection System | Machine Learning | ✅ |
| 07 | AML Risk Scoring | Compliance | ✅ |
| 08 | Customer 360 & CLV | Analytics | ✅ |
| 09 | SARB Regulatory Reporting | Compliance | ✅ |
| 10 | Business Intelligence & Dashboards | BI | ✅ |
| 11 | dbt Transformation Layer | Engineering | ✅ |
| 12 | SA Fintech Ecosystem & Cards | Fintech | ✅ |
| 13 | Appendix — Glossary & References | Reference | ✅ |

## Generating Per-Chapter PDFs

```powershell
$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$ebook  = "C:\Users\Anthony.DESKTOP-ES5HL78\Documents\Prime Capital Bank\docs\ebook\index.html"
$out    = "C:\Users\Anthony.DESKTOP-ES5HL78\Documents\Prime Capital Bank\docs\pdf_exports"

# Print full ebook
& $chrome --headless --disable-gpu `
  --print-to-pdf="$out\PCB_Ebook_Full.pdf" `
  --print-to-pdf-no-header $ebook
```

## Navigation

- [Full Ebook](../index.html)
- [Interactive ERD](../assets/PCB_COMPLETE_ERD.html)
- [Merchant Intelligence Map](../assets/PCB_MERCHANT_MAP.html)
- [Project Hub](../../index.html)
