# PDF Export Instructions — Prime Capital Bank

## Prerequisites

Install one of: Chrome/Edge (headless), wkhtmltopdf, or Pandoc.

---

## Option 1 — Chrome Headless (Best Quality)

```powershell
$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$base   = "C:\Users\Anthony.DESKTOP-ES5HL78\Documents\Prime Capital Bank"
$out    = "$base\docs\pdf_exports"

# Ebook
& $chrome --headless --disable-gpu --print-to-pdf="$out\PCB_Ebook.pdf" `
  --print-to-pdf-no-header "$base\docs\ebook\index.html"

# ERD
& $chrome --headless --disable-gpu --print-to-pdf="$out\PCB_Complete_ERD.pdf" `
  --print-to-pdf-no-header "$base\docs\ebook\assets\PCB_COMPLETE_ERD.html"

# Executive Presentation
& $chrome --headless --disable-gpu --print-to-pdf="$out\PCB_Executive_Presentation.pdf" `
  --print-to-pdf-no-header "$base\presentation\prime_capital_bank_overview.html"
```

---

## Option 2 — Microsoft Edge Headless

```powershell
$edge = "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
$base = "C:\Users\Anthony.DESKTOP-ES5HL78\Documents\Prime Capital Bank"
$out  = "$base\docs\pdf_exports"

& $edge --headless --disable-gpu --print-to-pdf="$out\PCB_Ebook.pdf" `
  "$base\docs\ebook\index.html"
```

---

## Option 3 — Python (pdfkit / weasyprint)

```powershell
pip install pdfkit weasyprint

python -c "
import pdfkit
base = r'C:\Users\Anthony.DESKTOP-ES5HL78\Documents\Prime Capital Bank'
pdfkit.from_file(f'{base}/docs/ebook/index.html',          f'{base}/docs/pdf_exports/PCB_Ebook.pdf')
pdfkit.from_file(f'{base}/docs/ebook/assets/PCB_COMPLETE_ERD.html', f'{base}/docs/pdf_exports/PCB_ERD.pdf')
pdfkit.from_file(f'{base}/presentation/prime_capital_bank_overview.html', f'{base}/docs/pdf_exports/PCB_Presentation.pdf')
print('Done')
"
```

---

## Output Files

| PDF | Source HTML | Purpose |
|-----|-------------|---------|
| `PCB_Ebook.pdf` | `docs/ebook/index.html` | 12-chapter technical ebook |
| `PCB_Complete_ERD.pdf` | `docs/ebook/assets/PCB_COMPLETE_ERD.html` | Full ERD with table catalogue |
| `PCB_Executive_Presentation.pdf` | `presentation/prime_capital_bank_overview.html` | 15-slide exec deck |
| `PCB_Architecture_Diagram.pdf` | `docs/ebook/assets/architecture_diagram.svg` | Azure architecture |

---

## Notes

- All HTML files are self-contained (no external CDN). PDFs render offline.
- For the ERD file: set Chrome window size `--window-size=1400,1000` for best layout.
- The Mermaid ERD diagram requires JavaScript — use Chrome/Edge headless, not wkhtmltopdf.
