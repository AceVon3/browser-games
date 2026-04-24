import re
text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()

# Look for Allstate Fire and Casualty in any form
print("=== All 'Fire and Casualty' lines ===")
for m in list(re.finditer(r"Fire and\s*Casualty", text))[:8]:
    s = max(0, m.start()-80)
    e = min(len(text), m.end()+200)
    print(repr(text[s:e]))
    print()

print()
print("=== Blocks containing 'Allstate Fire' (after removing newlines from name) ===")
# The name might be split across lines: "Allstate Fire and\nCasualty Insurance A+"
# Let's look for that pattern
for m in list(re.finditer(r"Allstate Fire", text))[:5]:
    s = max(0, m.start()-100)
    e = min(len(text), m.end()+500)
    print(repr(text[s:e]))
    print("===")
