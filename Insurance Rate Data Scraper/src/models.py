"""Domain models for rate filings."""

from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Optional


@dataclass
class AttachedPdf:
    category: str
    display_name: str
    url: str
    local_path: Optional[str] = None


@dataclass
class Filing:
    state: str
    serff_tracking_number: str
    filing_id: str
    company_name: str
    target_company: str
    naic_codes: list[str] = field(default_factory=list)
    product_name: Optional[str] = None
    type_of_insurance: Optional[str] = None
    sub_type_of_insurance: Optional[str] = None
    filing_type: Optional[str] = None
    filing_status: Optional[str] = None
    submission_date: Optional[date] = None
    disposition_date: Optional[date] = None
    disposition_status: Optional[str] = None
    state_status: Optional[str] = None

    requested_rate_effect: Optional[float] = None
    approved_rate_effect: Optional[float] = None
    overall_rate_effect: Optional[float] = None
    affected_policyholders: Optional[int] = None
    written_premium_volume: Optional[float] = None
    annual_premium_impact_dollars: Optional[float] = None
    current_avg_premium: Optional[float] = None
    proposed_avg_premium: Optional[float] = None
    premium_change_dollars: Optional[float] = None

    program_name: Optional[str] = None
    filing_reason: Optional[str] = None
    prior_approval: Optional[bool] = None

    pdfs: list[AttachedPdf] = field(default_factory=list)
    pdf_parse_status: str = "not_attempted"
    pdf_parse_fields_found: list[str] = field(default_factory=list)
    detail_url: Optional[str] = None

    in_target_lines: Optional[bool] = None
    is_resubmission_of: Optional[str] = None

    def compute_premium_change(self) -> None:
        if self.current_avg_premium is not None and self.proposed_avg_premium is not None:
            self.premium_change_dollars = round(
                self.proposed_avg_premium - self.current_avg_premium, 2
            )

    def to_row(self) -> dict:
        d = asdict(self)
        d["naic_codes"] = ";".join(self.naic_codes)
        d["pdfs"] = ";".join(p.url for p in self.pdfs)
        d["pdf_parse_fields_found"] = ";".join(self.pdf_parse_fields_found)
        for k in ("submission_date", "disposition_date"):
            if d[k] is not None:
                d[k] = d[k].isoformat()
        return d
