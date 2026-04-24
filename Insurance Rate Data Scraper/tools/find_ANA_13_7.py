import re
text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()

# Find unique blocks containing the -13.700% row
END = "Further information may be available"
blocks = text.split(END)

hits = [b for b in blocks if "\xad13.700%$\xad109,613" in b]
print(f"Blocks with Allstate ANA -13.7% ($-109,613, 472 pol): {len(hits)}")
# Show ONE block in full
if hits:
    print("---")
    print(hits[0][-2000:])
    print("---")
