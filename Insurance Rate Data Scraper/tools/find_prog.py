text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()

# Find Progressive Casualty blocks with 46,504 pol
if "46,504" in text:
    idx = text.find("46,504")
    s = max(0, idx-1500); e = min(len(text), idx+300)
    print("Progressive Casualty block:")
    print(text[s:e])
    print("---")
