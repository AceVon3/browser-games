import re
text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()
# Look for patterns ending in "Group"
groups = re.findall(r"([A-Z][A-Za-z &\.\-]+(?: [A-Z][A-Za-z &\.\-]+)*)\nGroup", text)
from collections import Counter
print("Top group names (split across lines):")
for n, c in Counter(groups).most_common(20):
    print(f"  {c:6d}  {n!r}")

print()
print("--- Allstate context (first 2 occurrences) ---")
for m in list(re.finditer(r"Allstate Insurance", text))[:2]:
    s = max(0, m.start()-50)
    e = min(len(text), m.end()+200)
    print(repr(text[s:e]))
    print()

print("--- Progressive context (first 2 occurrences with Group nearby) ---")
for m in list(re.finditer(r"Progressive [A-Z]", text))[:3]:
    s = max(0, m.start()-50)
    e = min(len(text), m.end()+200)
    print(repr(text[s:e]))
    print()
