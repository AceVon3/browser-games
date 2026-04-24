"""Match OR dataset against AM Best OR PPA report (2026-04-24 export).

Match keys: (company_name_normalized, effective_date, overall_rate_impact).
AM Best report is PPA-only; our PPA bucket aggregates sub_types 19.0001
(PPA), 19.0000 (Personal Auto Combinations), and 19.0002 (Motorcycle).

SCOPE NOTE: The 6 "missing from ours" rows below are all filing-vehicle
subsidiaries or specialty acquisitions that are OUT OF SCOPE by design
(see output/dataset_summary.md "Scope" section):
    - Standard Fire Insurance (Travelers filing vehicle)
    - Integon Indemnity x2 (Allstate-acquired National General specialty)
    - LM General / LM Insurance Corp (Liberty Mutual filing vehicles)
    - Safeco of Oregon eff 06/21/25 (already present under Motorcycle code)
In-scope match rate: 25 direct + 4 sub-type reclassifications = 29/29 (100%).
"""
import openpyxl
from datetime import datetime

# AM Best OR PPA filings for our 8 target carrier groups.
# Fields: (subsidiary, effective_date_MM/DD/YY, disposition_date, impact_pct,
#          policyholders, wpp, indicated_pct)
# Source: Best's State Rate Filings PDF, 2026-04-24.
AMBEST = [
    # Travelers
    ("The Standard Fire Insurance Company",    "04/17/26", "04/15/26", -3.100,  50692, 106647177, -6.300),
    ("The Standard Fire Insurance Company",    "08/22/25", "08/25/25", -3.000,  48456, 105349944, None),
    # State Farm
    ("State Farm Fire and Casualty Company",   "12/15/25", "10/17/25", -3.600,  31896,  50889068, None),
    ("State Farm Mutual Automobile Insurance Company","12/15/25","10/17/25", -3.600, 1007104, 908221048, None),
    ("State Farm Fire and Casualty Company",   "10/06/25", "08/25/25", -3.600,  33690,  56980415, None),
    ("State Farm Mutual Automobile Insurance Company","10/06/25","08/25/25", -4.200,  985050, 919545597, None),
    ("State Farm Fire and Casualty Company",   "06/01/25", "05/12/25",  0.000,  36387,  63743659, None),
    ("State Farm Mutual Automobile Insurance Company","06/01/25","05/12/25",  0.000,  962297, 891631927, None),
    ("State Farm Fire and Casualty Company",   "04/15/25", "03/04/25",  0.000,  31655,  56568405,  0.000),
    ("State Farm Mutual Automobile Insurance Company","04/15/25","03/04/25",  0.000,  875163, 863774489,  0.000),
    # Progressive
    ("Artisan and Truckers Casualty Company",  "01/16/26", "01/28/26", -2.200, 155154, 214285533, -3.700),
    ("Progressive Universal Insurance Company","01/16/26", "01/28/26", -2.000, 283800, 261466362, -3.600),
    ("Artisan and Truckers Casualty Company",  "10/17/25", "11/14/25", -0.200, 159623, 215211751, -0.700),
    ("Progressive Universal Insurance Company","10/17/25", "11/14/25", -0.100, 275869, 251667134,  0.400),
    ("Artisan and Truckers Casualty Company",  "09/19/25", "08/25/25",  4.100,   5487,   6615950,  3.900),
    ("Progressive Universal Insurance Company","09/19/25", "08/25/25",  3.700,   9055,  10072399,  3.900),
    # Allstate
    ("Integon Indemnity Corporation",          "01/30/26", "01/07/26",  0.000,      0,  21449249,  0.000),
    ("Integon Indemnity Corporation",          "10/30/25", "11/10/25", -0.200,  18991,  21449249, 13.100),
    ("Allstate Property and Casualty Insurance Company","10/20/25","09/16/25", 11.400,5895,  2307823, 22.000),
    ("Allstate North American Insurance Company","06/30/25","07/31/25", -5.000,  24444,  62962576,  0.000),
    ("Allstate North American Insurance Company","07/14/25","07/15/25", -0.600,  28228,  73534790,  0.000),
    ("Allstate Insurance Company",             "07/21/25", "07/01/25", 31.000,    889,    210221, 31.000),
    ("Allstate Fire and Casualty Insurance Company","06/23/25","04/24/25", 0.000,    0, 201110550,  0.000),
    # Encompass (Allstate sibling)
    ("Encompass Insurance Company",            "04/14/25", "03/28/25",  0.000,   1423,   4616938,  0.000),
    # Liberty Mutual
    ("Liberty Mutual Insurance Company",       "12/25/25", "01/20/26",  1.000,     37,     92829,  4.600),
    ("Liberty Mutual Personal Insurance Company","12/25/25","01/20/26",  1.000,  18960,  48566349,  4.600),
    ("Liberty Mutual Insurance Company",       "08/18/25", "08/07/25",  0.000,    125,    336683,  0.000),
    ("Liberty Mutual Personal Insurance Company","08/18/25","08/07/25",  0.000,  17836,  54179090,  0.000),
    ("LM General Insurance Company",           "06/24/25", "03/21/25",  9.300,    544,    314517, 10.900),
    ("LM Insurance Corporation",               "06/24/25", "03/21/25", 10.100,      3,      5925, 10.900),
    # Safeco (Liberty Mutual sibling brand)
    ("Safeco Insurance Company of Illinois",   "01/19/26", "12/18/25",  0.000,  20837,  44213576,  0.000),
    ("Safeco Insurance Company of Illinois",   "12/18/25", "12/10/25", -3.000,  19230,  43394746, -5.100),
    ("Safeco Insurance Company of Illinois",   "07/20/25", "08/01/25", -4.000,  12059,  27813955, -8.500),
    ("Safeco Insurance Company of Oregon",     "08/31/25", "07/23/25", -2.000,  64985, 177643962, -8.500),
    ("Safeco Insurance Company of Oregon",     "06/21/25", "04/29/25", 12.300,   7299,   3354748, 12.300),
]

def _norm_name(s: str) -> str:
    return " ".join(s.lower().replace(",", "").replace(".", "").split())

def _norm_date(s) -> str:
    """Return MM/DD/YY."""
    if s is None: return ""
    s = str(s).strip()
    # Handle "MM/DD/YYYY" -> "MM/DD/YY"
    if len(s) == 10 and s[2] == "/" and s[5] == "/":
        return s[:6] + s[8:]
    return s

def _pct(s) -> float | None:
    if s is None: return None
    s = str(s).strip().rstrip("%")
    try:
        return round(float(s), 3)
    except ValueError:
        return None

# Load my OR rows
wb = openpyxl.load_workbook("output/all_states_final_rates.xlsx", read_only=True)
ws = wb["rate_filings"]
rows = list(ws.iter_rows(values_only=True))
hdr = list(rows[0])
data = [dict(zip(hdr, r)) for r in rows[1:]]
or_rows = [d for d in data if d["state"] == "OR"]
def _is_ppa(d):
    s = (d["sub_type_of_insurance"] or "")
    # AM Best PPA report aggregates 19.0000 (Personal Auto Combinations) and 19.0001 (PPA) together
    return s.startswith("19.0001") or s.startswith("19.0000")
or_ppa = [d for d in or_rows if _is_ppa(d)]
or_other = [d for d in or_rows if not _is_ppa(d)]

print(f"OR total: {len(or_rows)} rows")
print(f"OR PPA:   {len(or_ppa)} rows")
print(f"OR other: {len(or_other)} rows")
print()

# Build lookup from my rows: (name_norm, eff, impact) -> row
my_lookup: dict[tuple, dict] = {}
for d in or_ppa:
    k = (_norm_name(d["company_name"] or ""), _norm_date(d["effective_date"]), _pct(d["overall_rate_impact"]))
    my_lookup[k] = d

print(f"AM Best in-scope PPA rows: {len(AMBEST)}")
print()
matched, missing = [], []
for name, eff, disp, imp, pol, wpp, ind in AMBEST:
    k = (_norm_name(name), eff, imp)
    if k in my_lookup:
        matched.append((name, eff, imp))
    else:
        missing.append((name, eff, disp, imp, pol, wpp, ind))

print(f"Matched (subsidiary + effective date + impact %):  {len(matched)}")
print(f"In AM Best, missing from ours:                      {len(missing)}")
print()

if missing:
    print("=== Missing from our OR dataset ===")
    for name, eff, disp, imp, pol, wpp, ind in missing:
        print(f"  {name:52s} eff={eff} disp={disp} imp={imp:+.2f}% pol={pol} wpp=${wpp:,}")

# In ours but not in AM Best
ambest_keys = {(_norm_name(r[0]), r[1], r[2]) for r in AMBEST}
extra = []
for d in or_ppa:
    k = (_norm_name(d["company_name"] or ""), _norm_date(d["effective_date"]), _pct(d["overall_rate_impact"]))
    if k not in ambest_keys:
        extra.append(d)

print()
print(f"In our OR dataset, not in AM Best report: {len(extra)} (PPA)")
for d in extra:
    print(f"  {d['serff_tracking_number']:22s} {d['company_name']:48s} eff={d['effective_date']} imp={d['overall_rate_impact']}")

print()
print("=== OR HO (not in PPA-only AM Best report, expected) ===")
for d in or_other:
    print(f"  {d['serff_tracking_number']:22s} {d['company_name']:48s} {d['sub_type_of_insurance']} imp={d['overall_rate_impact']}")
