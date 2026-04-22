# Insurance Rate Filings — Three-State Dataset

**Canonical deliverable:** `output/all_states_final_rates.xlsx` (sheet `rate_filings`) and `output/all_states_final_rates.csv`.

## What this dataset contains

125 rate-filing rows for personal-lines insurance across **Idaho, Washington, and Colorado**, structured to match AM Best's Disposition Page Data export. Each row represents one carrier subsidiary's per-program rate impact under a specific SERFF filing.

| State | Rows |
|------:|-----:|
| ID    |   31 |
| WA    |   15 |
| CO    |   79 |
| **Σ** | **125** |

## Methodology

1. **Discover.** Search SERFF Public Filing Access (`filingaccess.serff.com`) per state by carrier-group keyword.
2. **Filter.** Keep only target NAIC TOI codes (19.0 Personal Auto, 04.0 Homeowners, 03.0 Personal Farmowners) for the six target carrier groups (State Farm, GEICO, Progressive, Allstate, Travelers, Liberty Mutual + named subsidiaries).
3. **Download.** From each filing's detail page, click "Download Zip File" with **no checkboxes selected** to receive a ~20 KB minimal zip containing the system-generated Filing Summary PDF.
4. **Parse.** Extract the Disposition / Company Rate Information table from the PDF. Three row layouts are handled (full / blank-indicated / sparse).
5. **Exclude.** Drop Form-only / Rule-only filings, new-program launches ("Introduction of …"), and filings the filer flagged with "Rate data does NOT apply to filing."
6. **Expand.** One row per subsidiary listed in the per-company rate table. For multi-company filings the `Multiple` company label is replaced by the actual subsidiary name from the table.

## Validation

**Anchor:** Idaho filing **SFMA-134676753** matches AM Best Disposition Page Data on **all 14 fields** (effective date, indicated %, impact %, written premium change, policyholders, written premium for program, max %, min %, rate activity, tracking number, disposition status, filing date, company, line of business).

## Field definitions

| Column | Meaning |
|---|---|
| `state` | Two-letter state code |
| `effective_date` | Requested effective date (Renewal preferred over New) |
| `company_name` | Subsidiary writing the rate; per-row expansion when multiple |
| `line_of_business` | NAIC parent TOI code + label (kept for AM Best compatibility) |
| `sub_type_of_insurance` | NAIC Sub-TOI code + label (e.g. `19.0001 Private Passenger Auto (PPA)`, `19.0002 Motorcycle`, `19.0003 RV`) |
| `overall_indicated_change` | Filer's actuarially indicated change (may be blank when filer omits) |
| `overall_rate_impact` | Filed rate impact (the change actually requested) |
| `written_premium_change` | Effect of rate filing on written premium, USD |
| `policyholders_affected` | Count of policyholders impacted |
| `written_premium_for_program` | Total written premium for the program, USD |
| `maximum_percent_change` | Largest individual policyholder increase |
| `minimum_percent_change` | Largest individual policyholder decrease |
| `rate_activity` | `rate_change` / `rate_change_withdrawn` / `rate_change_disapproved` / `rate_change_pending` |
| `serff_tracking_number` | SERFF filing tracking number (carrier-prefixed) |
| `disposition_status` | State decision: `Approved` / `Filed` / `Withdrawn` / `Disapproved` / `Pending` (case as filed) |
| `filing_date` | Date submitted to the state |
| `source_pdf` | Relative path to the cached system PDF |

## Field completion (126 rows)

All 16 columns are 100% populated **except** `overall_indicated_change`, which is ~94% — the remainder are blank because the filer omitted the indicated value (sparse-row pattern, kept as `None` rather than guessed).

## Scope and limitations

- **States:** ID, WA, CO only.
- **Lines:** Personal Auto (TOI 19.0) and Homeowners (TOI 04.0) only. Farmowners explicitly out of scope.
- **Carriers:** Six national groups + named subsidiaries; no regional or single-state carriers.
- **Date range:** Whatever was visible in SERFF Public Access at run time (2026-04-22). No explicit date filter applied.
- **Filer flag:** When the filer flagged "Rate data does NOT apply to filing," the row is excluded — this flag is taken at face value.
- **PDF parsing:** Three Disposition row patterns are supported. Layouts outside these patterns may be missed (none observed in the 248 filings probed).
- **Disposition cases:** ID uses ALL-CAPS (`APPROVED`); WA uses `Approved`; CO uses `Filed` (file-and-use). Casing preserved as filed.
- **WA row count is genuinely thin (15 vs ID 31, CO 79).** Verified — not a scraper gap:
  - Same date window applied to all three states (2025-01-01 → 2026-04-17).
  - Fresh re-search (State Farm WA): 28 filings vs 28 in raw archive (100% match).
  - Of the 54 WA target-TOI target-carrier filings, half (27) are Form-only (no rate impact); 12 are filer-flagged "Rate data does NOT apply to filing." (heavily Travelers); 12 emit rows.
  - 2026-effective WA rows are sparse (1 of 15) because Allstate's 2026 WA submissions are all Form-only and Travelers' are all filer-flagged "does not apply." This is filer behavior, not missing data.
  - WA OIC publishes rate filings via SERFF Filing Access (no separate portal).

## Recommended use

- Comparative analysis of approved/filed rate changes across ID/WA/CO for the named carriers.
- Cross-reference to AM Best Disposition Page Data using `serff_tracking_number`.
- **Not** a substitute for full-state market analysis — scope is bounded by the carrier and line filters above.

## Reproducibility

```bash
.venv/Scripts/python run_final_rates.py ID
.venv/Scripts/python run_final_rates.py WA
.venv/Scripts/python run_final_rates.py CO
.venv/Scripts/python build_all_states.py
```

System PDFs are cached idempotently under `output/pdfs/{state}/{filing_id}/filing_summary.pdf`, so re-runs only re-parse.
