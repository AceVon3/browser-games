text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()
queries = [
    "Allstate Insurance Group",
    "Allstate Insurance\nGroup",
    "Allstate Insurance",
    "Allstate",
    "State Farm Group",
    "State Farm",
    "Travelers Group",
    "Travelers",
    "Berkshire",
    "GEICO",
    "Liberty Mutual",
    "Progressive Group",
    "Progressive",
    "Encompass",
]
for q in queries:
    print(f"  {q!r:35s} -> {text.count(q)}")
