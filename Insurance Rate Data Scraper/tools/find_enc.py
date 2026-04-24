text = open('output/ambest_wa_ppa_text.txt', encoding='utf-8').read()

for tag, marker in [
    ("Encompass 07/12/25 (22.5%)", "6,098 $20,568,379"),
    ("Encompass 12/13/25 (0%)", "5,324 $22,469,034"),
    ("Allstate North American 07/24/25 (-2.9%)", "472 $690,507"),
]:
    if marker in text:
        idx = text.find(marker)
        s = max(0, idx-1500); e = min(len(text), idx+200)
        print(f"=== {tag} ===")
        print(text[s:e])
        print()
