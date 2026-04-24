import re
text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()

# Look for -13.7% which is our unique ALSE-134416811
print("=== Searching for -13.7% (our ALSE-134416811) ===")
for m in re.finditer(r"13\.7", text):
    s = max(0, m.start()-200)
    e = min(len(text), m.end()+100)
    ctx = text[s:e]
    if "Allstate" in ctx or "\xad13" in ctx:
        print(f"---\n{ctx!r}\n")

print("\n=== Searching for Allstate Fire and Casualty (our ALSE-134489276, 0%) ===")
# Try variants
for q in ["Allstate Fire", "Fire and Casualty Insurance", "ALSE-134489276"]:
    print(f"{q!r}: {text.count(q)} occurrences")
    for m in list(re.finditer(re.escape(q), text))[:2]:
        s = max(0, m.start()-100)
        e = min(len(text), m.end()+300)
        print(f"  {text[s:e]!r}")
        print()

print("\n=== Searching for Allstate Property and Casualty (our ALSE-134489925, 0%) ===")
for q in ["Allstate Property", "Property and Casualty"]:
    print(f"{q!r}: {text.count(q)} occurrences")

print("\n=== All Allstate North American disposition rows with dates ===")
# Find every "Allstate North American" disposition row context
for m in re.finditer(r"Allstate North American Insurance Company[\d \.,\$%\-\u00ad]+", text):
    s = max(0, m.start()-200)
    e = min(len(text), m.end()+50)
    ctx = text[s:e]
    # Extract eff date from the preceding header
    date_m = re.search(r"(\d{2}/\d{2}/\d{2})\s+(\d{2}/\d{2}/\d{2})\s+\*+\s+Passenger", ctx)
    print(f"  row: {m.group(0)!r}")
    if date_m:
        print(f"    eff={date_m.group(1)} filed={date_m.group(2)}")
    print()
