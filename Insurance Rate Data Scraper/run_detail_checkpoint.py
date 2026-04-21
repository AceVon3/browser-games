"""Step 5 checkpoint: enrich 3 pre-selected Rate/Rule filings end-to-end
and write to output/step5_checkpoint.xlsx.

Validates:
  - detail-page field extraction (dates, status, NAIC, product)
  - Supporting-Documentation-first PDF prioritization + 10-cap
  - PDF download to output/pdfs/{state}/{filing_id}/
  - rate-effect parsing merges into Filing fields
  - premium-neutral filings correctly record overall_rate_effect=0.0

Test trio:
  SFMA-134559519  State Farm    Commercial Auto — expected rate change (~19.1%)
  ALSE-134489276  Allstate      Personal Auto    — expected rate change (unknown %)
  PRGS-134458809  Progressive   Symbol Addendum  — expected premium-neutral (0.0)
"""
from __future__ import annotations

from pathlib import Path

from src.config import OUTPUT_DIR
from src.detail import enrich_filings
from src.models import Filing
from src.output import write_excel


def main() -> int:
    checkpoint = [
        Filing(
            state="WA",
            serff_tracking_number="SFMA-134559519",
            filing_id="134559519",
            company_name="Multiple",
            target_company="State Farm",
            filing_type="Rate/Rule",
        ),
        Filing(
            state="WA",
            serff_tracking_number="ALSE-134489276",
            filing_id="134489276",
            company_name="Allstate Fire and Casualty Insurance Company",
            target_company="Allstate",
            filing_type="Rate/Rule",
        ),
        Filing(
            state="WA",
            serff_tracking_number="PRGS-134458809",
            filing_id="134458809",
            company_name="Multiple",
            target_company="Progressive",
            filing_type="Rate/Rule",
        ),
    ]

    enrich_filings(checkpoint)

    out = Path(OUTPUT_DIR) / "step5_checkpoint.xlsx"
    write_excel(checkpoint, out)

    print("\n=== Checkpoint results ===")
    for f in checkpoint:
        print(f"\n{f.serff_tracking_number} — {f.target_company}")
        print(f"  product_name:        {f.product_name}")
        print(f"  type_of_insurance:   {f.type_of_insurance}")
        print(f"  sub_type:            {f.sub_type_of_insurance}")
        print(f"  submission_date:     {f.submission_date}")
        print(f"  disposition_date:    {f.disposition_date}")
        print(f"  disposition_status:  {f.disposition_status}")
        print(f"  state_status:        {f.state_status}")
        print(f"  naic_codes:          {f.naic_codes}")
        print(f"  overall_rate_effect: {f.overall_rate_effect}")
        print(f"  requested_rate_effect: {f.requested_rate_effect}")
        print(f"  approved_rate_effect:  {f.approved_rate_effect}")
        print(f"  affected_policyholders: {f.affected_policyholders}")
        print(f"  written_premium:     {f.written_premium_volume}")
        print(f"  pdf_parse_status:    {f.pdf_parse_status}")
        print(f"  fields_found:        {f.pdf_parse_fields_found}")
        print(f"  pdfs ({len(f.pdfs)}):")
        for p in f.pdfs:
            local = Path(p.local_path) if p.local_path else None
            size = f"{local.stat().st_size/1024:.1f}KB" if local and local.exists() else "?"
            print(f"    [{p.category}] {p.display_name}  ({size})")
    print(f"\nWrote: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
