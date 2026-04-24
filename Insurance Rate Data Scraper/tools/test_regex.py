import re

DISP_LINE_RE = re.compile(
    r"^([A-Z][A-Za-z &\.,'/\-]+?(?:Company|Co|Inc|Corp|Corporation|of America|of Illinois|of Oregon|Connecticut|Insurance Co|Insurance Company))"
    r"\s*([\-\u2013\u2014\u2212\u00ad]?[\d,\.]+%|N/A)\s+"
    r"([\-\u2013\u2014\u2212\u00ad]?[\d,\.]+%|N/A)\s+"
    r"\$([\-\u2013\u2014\u2212\u00ad]?[\d,]+)\s+"
    r"([\d,]+)\s+"
    r"\$([\d,]+)\s+"
    r"([\-\u2013\u2014\u2212\u00ad]?[\d,\.]+%|N/A)\s+"
    r"([\-\u2013\u2014\u2212\u00ad]?[\d,\.]+%|N/A)\s*$",
    re.MULTILINE,
)

samples = [
    "Allstate North American Insurance Company0.000% \xad2.400% $\xad313,375 8,451 $13,057,296 0.000% \xad5.000%",
    "GEICO General Insurance Company 0.000% 0.000% $0 2,090 $1,077,172 24.800% \xad17.900%",
    "Government Employees Insurance Company0.000% 0.000% $0 946 $493,777 24.400% \xad17.500%",
    "GEICO Advantage Insurance Company0.000% 0.000% $0 6,838 $3,570,611 22.700% \xad18.900%",
    "Encompass Indemnity Company0.000% 0.000% $0 5,324 $22,469,034 0.000% 0.000%",
]
for s in samples:
    m = DISP_LINE_RE.match(s)
    if m:
        print(f"MATCH: {m.group(1)!r}  imp={m.group(3)}")
    else:
        print(f"NO:    {s[:80]!r}")
