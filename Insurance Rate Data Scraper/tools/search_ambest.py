import re
text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()

queries = [
    ("Allstate North American Insurance Company", "-13.7"),
    ("Allstate North American", "-13.7"),
    ("Allstate Fire and Casualty Insurance Company", "7/21/25"),
    ("Allstate Property and Casualty", "7/21/25"),
    ("GEICO General", "9/18/25"),
    ("Government Employees", "9/18/25"),
    ("GEICO Advantage", "9/18/25"),
    ("GEICO Secure", "9/18/25"),
    ("State Farm Fire and Casualty", "8/1/25"),
    ("Encompass Indemnity", "7/12/25"),
]

for name, hint in queries:
    count = text.count(name)
    print(f"\n{name!r}: {count} occurrences (hint={hint})")
    # Show first 2 contexts with % nearby
    for m in list(re.finditer(re.escape(name), text))[:2]:
        s = max(0, m.start()-60)
        e = min(len(text), m.end()+250)
        print(f"  ctx: {text[s:e]!r}")
